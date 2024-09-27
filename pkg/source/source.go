package source

import (
	"context"
	"fmt"
	"sync"

	"github.com/mertkayhan/piperr/pkg/config"
	"github.com/mertkayhan/piperr/pkg/custom"
)

type PiperrSource interface {
	Run(context.Context) error
}

func Run(ctx context.Context, wg *sync.WaitGroup, errCh chan<- error, pipeName string, cfg *config.Config) {
	defer wg.Done()
	src, err := resolveSource(ctx, pipeName, cfg)
	if err != nil {
		errCh <- err
		return
	}
	if err := src.Run(ctx); err != nil {
		errCh <- err
		return
	}
}

func resolveSource(ctx context.Context, pipeName string, cfg *config.Config) (PiperrSource, error) {
	switch *cfg.GetSourceType() {
	case config.CustomSource:
		return custom.NewCustomSource(ctx, cfg, pipeName)
	default:
		return nil, fmt.Errorf("unknown source %s", *cfg.GetSourceType())
	}
}
