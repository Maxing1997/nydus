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

	"github.com/goharbor/acceleration-service/pkg/platformutil"

	"github.com/containerd/containerd/content/local"
	"github.com/containerd/containerd/reference/docker"

	"github.com/containerd/containerd/errdefs"

	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/namespaces"
	"github.com/containerd/nydus-snapshotter/pkg/converter"
	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/committer"
	converterpvd "github.com/dragonflyoss/nydus/contrib/nydusify/pkg/converter/provider"
	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/parser"
	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/provider"
	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/utils"
	accerr "github.com/goharbor/acceleration-service/pkg/errdefs"
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

func fetchBlob(ctx context.Context, opt Opt, tmpDir, blobDir string) error {
	platformMC, err := platformutil.ParsePlatforms(opt.AllPlatforms, opt.Platforms)
	if err != nil {
		fmt.Println("parse platform: %v", err)
		return err
	}
	pvd, err := converterpvd.New(tmpDir, hosts(opt), 200, "v1", platformMC, opt.PushChunkSize)
	sourceNamed, err := docker.ParseDockerRef(opt.Source)
	if err != nil {
		fmt.Println("parse source reference: %v", err)
		return errors.Wrap(err, "parse source reference")
	}
	source := sourceNamed.String()
	if err := pvd.Pull(ctx, source); err != nil {
		if accerr.NeedsRetryWithHTTP(err) {
			pvd.UsePlainHTTP()
			if err := pvd.Pull(ctx, source); err != nil {
				return errors.Wrap(err, "try to pull image")
			}
		} else {
			return errors.Wrap(err, "pull source image")
		}
	}
	logrus.Infof("pulled source image %s", source)
	fmt.Println("source: ", source)
	sourceImage, err := pvd.Image(ctx, source)
	fmt.Println("source image: ", sourceImage)
	if err != nil {
		fmt.Println("find image from store: ", err)
		return errors.Wrap(err, "find image from store")
	}
	logrus.Infof("exporting source image to %s", blobDir)
	outputPath := filepath.Join(blobDir, "blob.tar")
	f, err := os.OpenFile(outputPath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := pvd.Export(ctx, f, sourceImage, source); err != nil {
		return errors.Wrap(err, "export source image to target tar file")

	}
	UnTar(blobDir, outputPath)
	return nil
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
	blobDir := filepath.Join(tmpDir, "blob-dir")
	if err := os.MkdirAll(blobDir, 0755); err != nil {
		return err
	}
	fetchBlob(ctx, opt, tmpDir, blobDir)
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

	blobDir = filepath.Join(blobDir + "/blobs/sha256")
	newBootstrapPath := filepath.Join(tmpDir, "optimized_bootstrap")
	builderOpt := BuildOption{
		BuilderPath:       opt.NydusImagePath,
		PrefetchFilesPath: opt.PrefetchFilesPath,
		BootstrapPath:     target,
		BlobDir:           blobDir,
		NewBootstrapPath:  newBootstrapPath,
		OutputPath:        filepath.Join(tmpDir, "output-blob-id"),
	}
	fmt.Println(builderOpt)
	blobid, err := Build(builderOpt)
	if err != nil {
		return errors.Wrap(err, "optimize nydus image")
	}

	pushManifest(ctx, *sourceParsed.NydusImage, blobid,
		newBootstrapPath, opt.PrefetchFilesPath, opt.Target, "6", blobDir, tmpDir, opt.TargetInsecure)

	return nil
}

func UnTar(dst, src string) (err error) {
	// 打开准备解压的 tar 包
	fr, err := os.Open(src)
	if err != nil {
		return
	}
	defer fr.Close()

	// 通过 gr 创建 tar.Reader
	tr := tar.NewReader(fr)

	// 现在已经获得了 tar.Reader 结构了，只需要循环里面的数据写入文件就可以了
	for {
		hdr, err := tr.Next()

		switch {
		case err == io.EOF:
			return nil
		case err != nil:
			return err
		case hdr == nil:
			continue
		}

		// 处理下保存路径，将要保存的目录加上 header 中的 Name
		// 这个变量保存的有可能是目录，有可能是文件，所以就叫 FileDir 了……
		dstFileDir := filepath.Join(dst, hdr.Name)

		// 根据 header 的 Typeflag 字段，判断文件的类型
		switch hdr.Typeflag {
		case tar.TypeDir: // 如果是目录时候，创建目录
			// 判断下目录是否存在，不存在就创建
			if b := ExistDir(dstFileDir); !b {
				// 使用 MkdirAll 不使用 Mkdir ，就类似 Linux 终端下的 mkdir -p，
				// 可以递归创建每一级目录
				if err := os.MkdirAll(dstFileDir, 0775); err != nil {
					return err
				}
			}
		case tar.TypeReg: // 如果是文件就写入到磁盘
			// 创建一个可以读写的文件，权限就使用 header 中记录的权限
			// 因为操作系统的 FileMode 是 int32 类型的，hdr 中的是 int64，所以转换下
			file, err := os.OpenFile(dstFileDir, os.O_CREATE|os.O_RDWR, os.FileMode(hdr.Mode))
			if err != nil {
				return err
			}
			n, err := io.Copy(file, tr)
			if err != nil {
				return err
			}
			// 将解压结果输出显示
			fmt.Printf("成功解压： %s , 共处理了 %d 个字符\n", dstFileDir, n)

			// 不要忘记关闭打开的文件，因为它是在 for 循环中，不能使用 defer
			// 如果想使用 defer 就放在一个单独的函数中
			file.Close()
		}
	}

	return nil
}

// 判断目录是否存在
func ExistDir(dirname string) bool {
	fi, err := os.Stat(dirname)
	return (err == nil || os.IsExist(err)) && fi.IsDir()
}

// push blob
func pushBlob(ctx context.Context, blobDir, blobName string, blobDigest digest.Digest, targetRef string, insecure bool) (*ocispec.Descriptor, error) {
	fmt.Println("blob dir:", filepath.Join(blobDir, blobName))
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
	var blobListInAnnotation []string
	for idx := range originalBlobLayers {
		blobListInAnnotation = append(blobListInAnnotation, originalBlobLayers[idx].Digest.Hex())
	}
	// fmt.Println(hotBlob)
	blobListInAnnotation = append(blobListInAnnotation, digest.Digest(("sha256:" + digest.Digest(blobid))).Hex())
	blobListBytes, err := json.Marshal(blobListInAnnotation)
	bootstrapDesc := ocispec.Descriptor{
		Digest:    digester2.Digest(),
		Size:      bootstrapTarGzRa.Size(),
		MediaType: ocispec.MediaTypeImageLayerGzip,
		Annotations: map[string]string{
			converter.LayerAnnotationFSVersion:      fsversion,
			converter.LayerAnnotationNydusBootstrap: "true",
			utils.LayerAnnotationNyudsPrefetchFiles: blobid,
			utils.LayerAnnotationNydusBlobIDs:       string(blobListBytes),
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
