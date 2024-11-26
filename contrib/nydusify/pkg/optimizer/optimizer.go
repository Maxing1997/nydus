// Copyright 2023 Nydus Developers. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package optimizer

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/containerd/containerd/content/local"

	"github.com/containerd/containerd/errdefs"

	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/namespaces"
	"github.com/containerd/nydus-snapshotter/pkg/converter"
	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/committer"
	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/parser"
	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/provider"
	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/utils"
	"github.com/goharbor/acceleration-service/pkg/remote"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const EntryBootstrap = "image.boot"
const EntryPrefetchFiles = "prefetch.files"

type Opt struct {
	WorkDir        string
	NydusImagePath string

	Source string
	Target string

	SourceInsecure bool
	TargetInsecure bool

	OptimizePolicy    string
	PrefetchFilesPath string

	AllPlatforms bool
	Platforms    string

	PushChunkSize int64
}

type File struct {
	Name   string
	Reader io.Reader
	Size   int64
}

func hosts(opt Opt) remote.HostFunc {
	maps := map[string]bool{
		opt.Source: opt.SourceInsecure,
		opt.Target: opt.TargetInsecure,
	}
	return func(ref string) (remote.CredentialFunc, bool, error) {
		return remote.NewDockerConfigCredFunc(), maps[ref], nil
	}
}

// readerat implements io.ReaderAt in a completely stateless manner by opening
// the referenced file for each call to ReadAt.
type sizeReaderAt struct {
	size int64
	fp   *os.File
}

// OpenReader creates ReaderAt from a file
func OpenReader(p string) (content.ReaderAt, error) {
	fi, err := os.Stat(p)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}

		return nil, fmt.Errorf("blob not found: %w", errdefs.ErrNotFound)
	}

	fp, err := os.Open(p)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}

		return nil, fmt.Errorf("blob not found: %w", errdefs.ErrNotFound)
	}

	return sizeReaderAt{size: fi.Size(), fp: fp}, nil
}

func (ra sizeReaderAt) ReadAt(p []byte, offset int64) (int, error) {
	return ra.fp.ReadAt(p, offset)
}

func (ra sizeReaderAt) Size() int64 {
	return ra.size
}

func (ra sizeReaderAt) Close() error {
	return ra.fp.Close()
}

func (ra sizeReaderAt) Reader() io.Reader {
	return io.LimitReader(ra.fp, ra.size)
}

func makeDesc(x interface{}, oldDesc ocispec.Descriptor) ([]byte, *ocispec.Descriptor, error) {
	data, err := json.MarshalIndent(x, "", "  ")
	if err != nil {
		return nil, nil, errors.Wrap(err, "json marshal")
	}
	dgst := digest.SHA256.FromBytes(data)

	newDesc := oldDesc
	newDesc.Size = int64(len(data))
	newDesc.Digest = dgst

	return data, &newDesc, nil
}

// packToTar packs files to .tar(.gz) stream then return reader.
func packToTar(files []File, compress bool) io.ReadCloser {
	dirHdr := &tar.Header{
		Name:     "image",
		Mode:     0755,
		Typeflag: tar.TypeDir,
	}

	pr, pw := io.Pipe()

	go func() {
		// Prepare targz writer
		var tw *tar.Writer
		var gw *gzip.Writer
		var err error

		if compress {
			gw = gzip.NewWriter(pw)
			tw = tar.NewWriter(gw)
		} else {
			tw = tar.NewWriter(pw)
		}

		defer func() {
			err1 := tw.Close()
			var err2 error
			if gw != nil {
				err2 = gw.Close()
			}

			var finalErr error

			// Return the first error encountered to the other end and ignore others.
			switch {
			case err != nil:
				finalErr = err
			case err1 != nil:
				finalErr = err1
			case err2 != nil:
				finalErr = err2
			}

			pw.CloseWithError(finalErr)
		}()

		// Write targz stream
		if err = tw.WriteHeader(dirHdr); err != nil {
			return
		}

		for _, file := range files {
			hdr := tar.Header{
				Name: filepath.Join("image", file.Name),
				Mode: 0444,
				Size: file.Size,
			}
			if err = tw.WriteHeader(&hdr); err != nil {
				return
			}
			if _, err = io.Copy(tw, file.Reader); err != nil {
				return
			}
		}
	}()

	return pr
}

// Optimize covert and push a new optimized nydus image
func Optimize(ctx context.Context, opt Opt) error {
	ctx = namespaces.WithNamespace(ctx, "nydusify")

	sourceRemote, err := provider.DefaultRemote(opt.Source, opt.SourceInsecure)
	if err != nil {
		return errors.Wrap(err, "Init source image parser")
	}
	sourceParser, err := parser.New(sourceRemote, "amd64")
	if sourceParser == nil {
		return errors.Wrap(err, "failed to create parser")
	}

	sourceParsed, err := sourceParser.Parse(ctx)
	if err != nil {
		return errors.Wrap(err, "parse source image")
	}

	//platformMC, err := platformutil.ParsePlatforms(opt.AllPlatforms, opt.Platforms)
	if err != nil {
		return err
	}

	if _, err := os.Stat(opt.WorkDir); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if err := os.MkdirAll(opt.WorkDir, 0755); err != nil {
				return errors.Wrap(err, "prepare work directory")
			}
			// We should only clean up when the work directory not exists
			// before, otherwise it may delete user data by mistake.
			defer os.RemoveAll(opt.WorkDir)
		} else {
			return errors.Wrap(err, "stat work directory")
		}
	}
	tmpDir, err := os.MkdirTemp(opt.WorkDir, "nydusify-")
	if err != nil {
		return errors.Wrap(err, "create temp directory")
	}
	//defer os.RemoveAll(tmpDir)

	target := filepath.Join(tmpDir, "nydus_bootstrap")
	logrus.Infof("Pulling Nydus bootstrap to %s", target)
	bootstrapReader, err := sourceParser.PullNydusBootstrap(ctx, sourceParsed.NydusImage)
	if err != nil {
		return errors.Wrap(err, "pull Nydus bootstrap layer")
	}
	defer bootstrapReader.Close()

	if err := utils.UnpackFile(bootstrapReader, utils.BootstrapFileNameInLayer, target); err != nil {
		return errors.Wrap(err, "unpack Nydus bootstrap layer")
	}

	newBootstrapPath := filepath.Join(tmpDir, "optimized_bootstrap")
	builderOpt := BuildOption{
		BuilderPath:       opt.NydusImagePath,
		PrefetchFilesPath: opt.PrefetchFilesPath,
		BootstrapPath:     target,
		BlobDir:           opt.WorkDir,
		NewBootstrapPath:  newBootstrapPath,
		OutputPath:        filepath.Join(tmpDir, "output-blob-id"),
	}
	fmt.Println(builderOpt)
	blobid, err := Build(builderOpt)
	if err != nil {
		return errors.Wrap(err, "optimize nydus image")
	}

	pushManifest(ctx, *sourceParsed.NydusImage, blobid,
		newBootstrapPath, opt.PrefetchFilesPath, opt.Target, "6", opt.WorkDir, tmpDir, opt.TargetInsecure)

	return nil
}

// push blob
func pushBlob(ctx context.Context, blobDir, blobName string, blobDigest digest.Digest, targetRef string, insecure bool) (*ocispec.Descriptor, error) {
	fmt.Println(filepath.Join(blobDir, blobName))
	blobRa, err := local.OpenReader(filepath.Join(blobDir, blobName))
	if err != nil {
		return nil, errors.Wrap(err, "open reader for upper blob")
	}

	blobDesc := ocispec.Descriptor{
		Digest:    blobDigest,
		Size:      blobRa.Size(),
		MediaType: utils.MediaTypeNydusBlob,
		Annotations: map[string]string{
			utils.LayerAnnotationUncompressed: blobDigest.String(),
			utils.LayerAnnotationNydusBlob:    "true",
		},
	}

	remoter, err := provider.DefaultRemote(targetRef, insecure)
	if err != nil {
		return nil, errors.Wrap(err, "create remote")
	}

	if err := remoter.Push(ctx, blobDesc, true, io.NewSectionReader(blobRa, 0, blobRa.Size())); err != nil {
		fmt.Print("catch me")
		if utils.RetryWithHTTP(err) {
			remoter.MaybeWithHTTP(err)
			if err := remoter.Push(ctx, blobDesc, true, io.NewSectionReader(blobRa, 0, blobRa.Size())); err != nil {
				return nil, errors.Wrap(err, "push blob")
			}
		} else {
			return nil, errors.Wrap(err, "push blob")
		}
	}
	return &blobDesc, nil
}

func pushManifest(ctx context.Context, nydusImage parser.Image, blobid string,
	targetBootstrapPath, prefetchfilesPath, TargetRef, fsversion, blobDir, workDir string,
	insecure bool) error {

	//pushblob
	hotBlob, err := pushBlob(ctx, blobDir, blobid, "sha256:"+digest.Digest(blobid), TargetRef, insecure)
	if err != nil {
		return errors.Wrap(err, "create hot blob desc")
	}

	originalBlobLayers := []ocispec.Descriptor{}
	for idx := range nydusImage.Manifest.Layers {
		layer := nydusImage.Manifest.Layers[idx]
		if layer.MediaType == utils.MediaTypeNydusBlob {
			originalBlobLayers = append(originalBlobLayers, layer)
		}
	}

	targetRef, err := committer.ValidateRef(TargetRef)

	bootstrapRa, err := OpenReader(targetBootstrapPath)
	prefetchfilesRa, err := OpenReader(prefetchfilesPath)
	files := append([]File{
		{
			Name:   EntryBootstrap,
			Reader: content.NewReader(bootstrapRa),
			Size:   bootstrapRa.Size(),
		}, {
			Name:   EntryPrefetchFiles,
			Reader: content.NewReader(prefetchfilesRa),
			Size:   prefetchfilesRa.Size(),
		}})
	rc := packToTar(files, false)
	digester := digest.SHA256.Digester()
	if _, err := io.Copy(digester.Hash(), rc); err != nil {
		return errors.Wrap(err, "get tar digest")
	}
	bootstrapDiffID := digester.Digest()

	// Push image config
	config := nydusImage.Config

	config.RootFS.DiffIDs = []digest.Digest{}
	for idx := range originalBlobLayers {
		config.RootFS.DiffIDs = append(config.RootFS.DiffIDs, originalBlobLayers[idx].Digest)
	}
	// fmt.Println(hotBlob)
	config.RootFS.DiffIDs = append(config.RootFS.DiffIDs, "sha256:"+digest.Digest(blobid))
	//Note: bootstrap diffid is tar
	config.RootFS.DiffIDs = append(config.RootFS.DiffIDs, bootstrapDiffID)

	configBytes, configDesc, err := makeDesc(config, nydusImage.Manifest.Config)
	if err != nil {
		return errors.Wrap(err, "make config desc")
	}

	remoter, err := provider.DefaultRemote(targetRef, insecure)
	if err != nil {
		return errors.Wrap(err, "create remote")
	}

	if err := remoter.Push(ctx, *configDesc, true, bytes.NewReader(configBytes)); err != nil {
		if utils.RetryWithHTTP(err) {
			remoter.MaybeWithHTTP(err)
			if err := remoter.Push(ctx, *configDesc, true, bytes.NewReader(configBytes)); err != nil {
				return errors.Wrap(err, "push image config")
			}
		} else {
			fmt.Println("push config failed")
			return errors.Wrap(err, "push image config")
		}
	}

	bootstrapTarGzPath := filepath.Join(workDir, "bootstrap.tar.gz")
	bootstrapTarGz, err := os.Create(bootstrapTarGzPath)
	if err != nil {
		return errors.Wrap(err, "create bootstrap tar.gz file")
	}
	defer bootstrapTarGz.Close()
	fmt.Println(files)
	bootstrapRa2, err := OpenReader(targetBootstrapPath)
	prefetchfilesRa2, err := OpenReader(prefetchfilesPath)
	files2 := append([]File{
		{
			Name:   EntryBootstrap,
			Reader: content.NewReader(bootstrapRa2),
			Size:   bootstrapRa.Size(),
		}, {
			Name:   EntryPrefetchFiles,
			Reader: content.NewReader(prefetchfilesRa2),
			Size:   prefetchfilesRa.Size(),
		}})
	rd := packToTar(files2, true)
	if _, err := io.Copy(bootstrapTarGz, rd); err != nil {
		return errors.Wrap(err, "compress bootstrap & prefetchfiles to tar.gz")
	}

	bootstrapTarGzRa, err := OpenReader(bootstrapTarGzPath)
	defer bootstrapTarGzRa.Close()
	digester2 := digest.SHA256.Digester()
	rf, err := os.Open(bootstrapTarGzPath)
	if err != nil {
		return errors.Wrapf(err, "open bootstrap %s", bootstrapTarGzPath)
	}
	if _, err := io.Copy(digester2.Hash(), rf); err != nil {
		return errors.Wrap(err, "get tar digest")
	}
	//push bootstrap
	bootstrapDesc := ocispec.Descriptor{
		Digest:    digester2.Digest(),
		Size:      bootstrapTarGzRa.Size(),
		MediaType: ocispec.MediaTypeImageLayerGzip,
		Annotations: map[string]string{
			converter.LayerAnnotationFSVersion:      fsversion,
			converter.LayerAnnotationNydusBootstrap: "true",
			utils.LayerAnnotationNyudsPrefetchFiles: blobid,
		},
	}

	bootstrapRc, err := os.Open(bootstrapTarGzPath)
	if err != nil {
		return errors.Wrapf(err, "open bootstrap %s", bootstrapTarGzPath)
	}
	defer bootstrapRc.Close()
	if err := remoter.Push(ctx, bootstrapDesc, true, bootstrapRc); err != nil {
		return errors.Wrap(err, "push bootstrap layer")
	}

	//push image manifest
	layers := originalBlobLayers
	layers = append(layers, *hotBlob)
	layers = append(layers, bootstrapDesc)
	nydusImage.Manifest.Config = *configDesc
	nydusImage.Manifest.Layers = layers

	manifestBytes, manifestDesc, err := makeDesc(nydusImage.Manifest, nydusImage.Desc)
	if err != nil {
		return errors.Wrap(err, "make config desc")
	}
	if err := remoter.Push(ctx, *manifestDesc, false, bytes.NewReader(manifestBytes)); err != nil {
		return errors.Wrap(err, "push image manifest")
	}
	fmt.Println("success")

	return nil
}
