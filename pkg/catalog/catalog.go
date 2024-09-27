package catalog

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/mertkayhan/piperr/pkg/utils"
	"github.com/spf13/viper"
)

type SyncMode string

const (
	SyncModeIncrementalAppend           SyncMode = "IncrementalAppend"
	SyncModeIncrementalAppendDeduped    SyncMode = "IncrementalAppendDeduped"
	SyncModeFullRefreshAppend           SyncMode = "FullRefreshAppend"
	SyncModeFullRefreshOverwrite        SyncMode = "FullRefreshOverwrite"
	SyncModeFullRefreshOverwriteDeduped SyncMode = "FullRefreshOverwriteDeduped"
)

type streamConfig struct {
	mode         *SyncMode
	primaryKey   *string
	compositeKey []string
	ticker       *string
}

type Catalog struct {
	streams map[string]*streamConfig
}

func (c *Catalog) GetStreamNames() []string {
	var out []string
	for name := range c.streams {
		out = append(out, name)
	}
	return out
}

func (c *Catalog) GetMode(streamName string) (*SyncMode, error) {
	n, ok := c.streams[streamName]
	if !ok {
		return nil, fmt.Errorf("unknown stream name %s", streamName)
	}
	return n.mode, nil
}

func (c *Catalog) GetPrimaryKey(streamName string) (*string, error) {
	n, ok := c.streams[streamName]
	if !ok {
		return nil, fmt.Errorf("unknown stream name %s", streamName)
	}
	return n.primaryKey, nil
}

func (c *Catalog) GetCompositeKey(streamName string) ([]string, error) {
	n, ok := c.streams[streamName]
	if !ok {
		return nil, fmt.Errorf("unknown stream name %s", streamName)
	}
	return n.compositeKey, nil
}

func (c *Catalog) GetTicker(streamName string) (*string, error) {
	n, ok := c.streams[streamName]
	if !ok {
		return nil, fmt.Errorf("unknown stream name %s", streamName)
	}
	return n.ticker, nil
}

func BuildCatalog() (*Catalog, error) {
	catalogPath := viper.GetString("catalog")
	if _, err := os.Stat(catalogPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("BuildCatalog: path does not exist %s", catalogPath)
	}
	bytes, err := utils.ReadFile(catalogPath)
	if err != nil {
		return nil, fmt.Errorf("BuildCatalog: %w", err)
	}
	streams, err := parseCatalog(bytes)
	if err != nil {
		return nil, fmt.Errorf("BuildCatalog: %w", err)
	}
	return &Catalog{
		streams: streams,
	}, nil
}

func parseCatalog(catalogBytes []byte) (map[string]*streamConfig, error) {
	var tmp map[string]struct {
		Mode         *string  `json:"mode,omitempty"`
		PrimaryKey   *string  `json:"primary_key,omitempty"`
		CompositeKey []string `json:"composite_key,omitempty"`
		Ticker       *string  `json:"ticker,omitempty"`
	}
	if err := json.Unmarshal(catalogBytes, &tmp); err != nil {
		return nil, fmt.Errorf("parseCatalog: %w", err)
	}
	streams := make(map[string]*streamConfig)
	for key := range tmp {
		if tmp[key].Mode == nil {
			return nil, fmt.Errorf("parseCatalog: missing sync mode for %s", key)
		}
		mode, err := getSyncMode(*tmp[key].Mode)
		if err != nil {
			return nil, fmt.Errorf("parseCatalog: %w", err)
		}
		if tmp[key].CompositeKey != nil && tmp[key].Ticker == nil {
			return nil, fmt.Errorf("if composite key is set, ticker should also be set")
		}
		streams[key] = &streamConfig{
			mode:         &mode,
			primaryKey:   tmp[key].PrimaryKey,
			compositeKey: tmp[key].CompositeKey,
			ticker:       tmp[key].Ticker,
		}
	}
	return streams, nil
}

func getSyncMode(mode string) (SyncMode, error) {
	switch mode {
	case "IncrementalAppend":
		return SyncModeIncrementalAppend, nil
	case "IncrementalAppendDeduped":
		return SyncModeIncrementalAppendDeduped, nil
	case "FullRefreshAppend":
		return SyncModeFullRefreshAppend, nil
	case "FullRefreshOverwrite":
		return SyncModeFullRefreshOverwrite, nil
	case "FullRefreshOverwriteDeduped":
		return SyncModeFullRefreshOverwriteDeduped, nil
	default:
		return "", fmt.Errorf("unknown sync mode %s", mode)
	}
}
