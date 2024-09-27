package process

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"

	"github.com/spf13/viper"
)

type ProcessArgs struct {
	Name     string
	PipeName string
	Args     []string
	PassEnv  bool
	Stdout   io.Writer
	StdErr   io.Writer
}

func Create(ctx context.Context, a *ProcessArgs) *exec.Cmd {
	cmd := exec.CommandContext(ctx, a.Name, a.Args...)
	cmd.Env = append(cmd.Env, fmt.Sprintf("STATE_PATH=%s", viper.GetString("state")))
	cmd.Env = append(cmd.Env, fmt.Sprintf("CONFIG_PATH=%s", viper.GetString("config")))
	cmd.Env = append(cmd.Env, fmt.Sprintf("CATALOG_PATH=%s", viper.GetString("catalog")))
	if a.PipeName != "" {
		cmd.Env = append(cmd.Env, fmt.Sprintf("PIPE_NAME=%s", a.PipeName))
	}
	if a.PassEnv {
		cmd.Env = append(cmd.Env, os.Environ()...)
	}
	if a.Stdout != nil {
		cmd.Stdout = os.Stdout
	}
	if a.StdErr != nil {
		cmd.Stderr = os.Stdin
	}
	return cmd
}
