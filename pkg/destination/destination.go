package destination

import (
	"context"
	"fmt"
	"sync"

	"github.com/mertkayhan/piperr/pkg/bq"
	"github.com/mertkayhan/piperr/pkg/catalog"
	"github.com/mertkayhan/piperr/pkg/config"
)

type PiperrDestination interface {
	Run(context.Context) error
}

func Run(ctx context.Context, wg *sync.WaitGroup, errCh chan<- error, pipeName string, cfg *config.Config, streams *catalog.Catalog) {
	defer wg.Done()
	dest, err := resolveDestination(ctx, pipeName, cfg, streams)
	if err != nil {
		errCh <- err
		return
	}
	if err := dest.Run(ctx); err != nil {
		errCh <- err
		return
	}
}

func resolveDestination(ctx context.Context, pipeName string, cfg *config.Config, streams *catalog.Catalog) (PiperrDestination, error) {
	switch *cfg.GetDestinationType() {
	case config.BigQueryDestination:
		return bq.NewClient(pipeName, cfg.GetDestinationConfig(), streams)
	case config.CustomDestination:
		return nil, fmt.Errorf("not implemented")
	default:
		return nil, fmt.Errorf("unknown destination %s", *cfg.GetDestinationType())
	}
}
