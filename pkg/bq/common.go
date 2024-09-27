package bq

import (
	"fmt"

	"github.com/mertkayhan/piperr/pkg/catalog"
	"github.com/mertkayhan/piperr/pkg/config"
	"golang.org/x/net/context"
)

type ClientType string

const (
	BQDirectClient   ClientType = "direct"
	BQIndirectClient ClientType = "indirect"
)

type Client interface {
	Run(context.Context) error
}

type ClientOpts struct {
	ProjectID     string
	DatasetID     string
	Location      string
	PipeName      string
	ClientType    string
	StreamCatalog *catalog.Catalog
}

func NewClient(pipeName string, cfg config.ComponentConfig, streamCatalog *catalog.Catalog) (Client, error) {
	opts, err := parseCfg(cfg)
	if err != nil {
		return nil, fmt.Errorf("NewClient: %w", err)
	}
	opts.PipeName = pipeName
	opts.StreamCatalog = streamCatalog
	t, err := getClientType(opts.ClientType)
	if err != nil {
		return nil, fmt.Errorf("NewClient: %w", err)
	}
	switch t {
	case BQDirectClient:
		return NewDirectClient(opts)
	default:
		return nil, fmt.Errorf("NewClient: unknown client type %s", opts.ClientType)
	}
}

func parseCfg(cfg config.ComponentConfig) (*ClientOpts, error) {
	c := &ClientOpts{}
	projectId, err := cfg.GetString("project_id")
	if err != nil {
		return nil, fmt.Errorf("parseCfg: %w", err)
	}
	c.ProjectID = projectId
	datasetId, err := cfg.GetString("dataset_id")
	if err != nil {
		return nil, fmt.Errorf("parseCfg: %w", err)
	}
	c.DatasetID = datasetId
	loc, err := cfg.GetString("location")
	if err != nil {
		return nil, fmt.Errorf("parseCfg: %w", err)
	}
	c.Location = loc
	clientType, err := cfg.GetString("client_type")
	if err != nil {
		return nil, fmt.Errorf("parseCfg: %w", err)
	}
	c.ClientType = clientType
	return c, nil
}

func getClientType(kind string) (ClientType, error) {
	switch kind {
	case "direct":
		return BQDirectClient, nil
	case "indirect":
		return BQIndirectClient, nil
	default:
		return "", fmt.Errorf("unknown client type %s", kind)
	}
}
