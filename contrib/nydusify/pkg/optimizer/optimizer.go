// Copyright 2023 Nydus Developers. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package optimizer

import (
	"context"
	"fmt"
	"github.com/containerd/containerd/namespaces"
	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/parser"
	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/provider"
	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/utils"
	"github.com/goharbor/acceleration-service/pkg/remote"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"os"
	"path/filepath"
)

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

func hosts(opt Opt) remote.HostFunc {
	maps := map[string]bool{
		opt.Source: opt.SourceInsecure,
		opt.Target: opt.TargetInsecure,
	}
	return func(ref string) (remote.CredentialFunc, bool, error) {
		return remote.NewDockerConfigCredFunc(), maps[ref], nil
	}
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

	builderOpt := BuildOption{
		BuilderPath:       opt.NydusImagePath,
		PrefetchFilesPath: opt.PrefetchFilesPath,
		BootstrapPath:     target,
		BlobDir:           opt.WorkDir,
		NewBootstrapPath:  filepath.Join(tmpDir, "optimized_bootstrap"),
		OutputPath:        filepath.Join(tmpDir, "output-blob-id"),
	}
	fmt.Println(builderOpt)
	if _, err := Build(builderOpt); err != nil {
		return errors.Wrap(err, "optimize nydus image")
	}
	return nil
}
