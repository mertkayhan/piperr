package custom

import (
	"context"
	"os"

	"github.com/mertkayhan/piperr/pkg/config"
	"github.com/mertkayhan/piperr/pkg/process"
)

func NewCustomSource(ctx context.Context, cfg *config.Config, pipeName string) (*Custom, error) {
	p := process.Create(ctx, &process.ProcessArgs{
		Name:     *cfg.GetSourceArgs().Cmd,
		PipeName: pipeName,
		Args:     cfg.GetSourceArgs().Args,
		PassEnv:  true,
		Stdout:   os.Stdout,
		StdErr:   os.Stderr,
	})
	return &Custom{p: p}, nil
}
