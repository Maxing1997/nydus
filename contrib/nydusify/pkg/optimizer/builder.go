package optimizer

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var logger = logrus.WithField("module", "optimizer")

func isSignalKilled(err error) bool {
	return strings.Contains(err.Error(), "signal: killed")
}

type BuildOption struct {
	BuilderPath       string
	PrefetchFilesPath string
	BootstrapPath     string
	BlobDir           string
	NewBootstrapPath  string
	OutputPath        string
	Timeout           *time.Duration
}

func Build(option BuildOption) (string, error) {
	args := []string{
		"optimize",
		"--log-level",
		"warn",
		"--prefetch-files",
		option.PrefetchFilesPath,
		"--bootstrap",
		option.BootstrapPath,
		"--blob-dir",
		option.BlobDir,
		"--new-bootstrap",
		option.NewBootstrapPath,
		"--output-path",
		option.OutputPath,
	}

	ctx := context.Background()
	var cancel context.CancelFunc
	if option.Timeout != nil {
		ctx, cancel = context.WithTimeout(ctx, *option.Timeout)
		defer cancel()
	}
	logrus.Debugf("\tCommand: %s %s", option.BuilderPath, strings.Join(args, " "))

	cmd := exec.CommandContext(ctx, option.BuilderPath, args...)
	cmd.Stdout = logger.Writer()
	cmd.Stderr = logger.Writer()

	if err := cmd.Run(); err != nil {
		if isSignalKilled(err) && option.Timeout != nil {
			logrus.WithError(err).Errorf("fail to run %v %+v, possibly due to timeout %v", option.BuilderPath, args, *option.Timeout)
		} else {
			logrus.WithError(err).Errorf("fail to run %v %+v", option.BuilderPath, args)
		}
		return "", errors.Wrap(err, "run merge command")
	}

	BlobID, err := os.ReadFile(option.OutputPath)
	if err != nil {
		return "", errors.Wrap(err, "failed to read blob id file")
	}

	logrus.Infof("build success for prefetch blob : %s", BlobID)
	return string(BlobID), nil
}
