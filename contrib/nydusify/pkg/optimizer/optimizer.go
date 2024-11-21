// Copyright 2023 Nydus Developers. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package optimizer

import (
	"context"
	"github.com/containerd/containerd/namespaces"
	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/converter/provider"
	"github.com/goharbor/acceleration-service/pkg/platformutil"
	"github.com/goharbor/acceleration-service/pkg/remote"
	"github.com/pkg/errors"
	"os"
)

type Opt struct {
	WorkDir        string
	NydusImagePath string

	Source string
	Target string

	SourceInsecure bool
	TargetInsecure bool

	SourceBackendType   string
	SourceBackendConfig string

	TargetBackendType   string
	TargetBackendConfig string

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

	platformMC, err := platformutil.ParsePlatforms(opt.AllPlatforms, opt.Platforms)
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
	pvd, err := provider.New(tmpDir, hosts(opt), 200, "v1", platformMC, opt.PushChunkSize)
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)
}
