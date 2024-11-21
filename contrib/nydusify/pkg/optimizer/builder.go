package optimizer

import (
	"context"
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
	//OutputJSONPath    string
	Timeout *time.Duration
}

type outputJSON struct {
	NewBootstrapPath string
	HotBlob          string
}

func Build(option BuildOption) (outputJSON, error) {
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
		//"--output-json",
		//option.OutputJSONPath,
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
		return outputJSON{}, errors.Wrap(err, "run merge command")
	}
	return outputJSON{}, nil

	//outputBytes, err := os.ReadFile(option.OutputJSONPath)
	//if err != nil {
	//	return outputJSON{}, errors.Wrapf(err, "read file %s", option.OutputJSONPath)
	//}
	//var output outputJSON
	//err = json.Unmarshal(outputBytes, &output)
	//if err != nil {
	//	return outputJSON{}, errors.Wrapf(err, "unmarshal output json file %s", option.OutputJSONPath)
	//}
	//
	//return output, nil
}
