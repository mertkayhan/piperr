package config

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/mertkayhan/piperr/pkg/utils"
	"github.com/spf13/viper"
)

type SourceType string
type DestinationType string
type ComponentType string
type ComponentConfig map[string]any

func (c ComponentConfig) GetString(key string) (string, error) {
	val, ok := c[key]
	if !ok {
		return "", fmt.Errorf("%s is not set", key)
	}
	strVal, ok := val.(string)
	if !ok {
		return "", fmt.Errorf("%s cannot be converted to string", key)
	}
	return strVal, nil
}

const (
	CustomSource SourceType = "Custom"
)

const (
	BigQueryDestination DestinationType = "BigQuery"
	CustomDestination   DestinationType = "Custom"
)

type Config struct {
	source      *component
	destination *component
}

type component struct {
	kind   *ComponentType
	args   *Args
	config ComponentConfig
}

type Args struct {
	Cmd  *string
	Args []string
}

func (c *Config) GetSourceType() *SourceType {
	return (*SourceType)(c.source.kind)
}

func (c *Config) GetSourceArgs() *Args {
	return c.source.args
}

func (c *Config) GetSourceConfig() map[string]any {
	return c.source.config
}

func (c *Config) GetDestinationType() *DestinationType {
	return (*DestinationType)(c.destination.kind)
}

func (c *Config) GetDestinationArgs() *Args {
	return c.destination.args
}

func (c *Config) GetDestinationConfig() map[string]any {
	return c.destination.config
}

func BuildConfig() (*Config, error) {
	configPath := viper.GetString("config")
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("BuildConfig: path does not exist %s", configPath)
	}
	bytes, err := utils.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("BuildConfig: %w", err)
	}
	cfg, err := parseConfig(bytes)
	if err != nil {
		return nil, fmt.Errorf("BuildConfig: %w", err)
	}
	return cfg, nil
}

func parseConfig(configBytes []byte) (*Config, error) {
	var tmp struct {
		SourceType *string `json:"source_type,omitempty"`
		SourceArgs *struct {
			Cmd  *string  `json:"cmd,omitempty"`
			Args []string `json:"args,omitempty"`
		} `json:"source_args,omitempty"`
		SourceConfig    map[string]any `json:"source_config,omitempty"`
		DestinationType *string        `json:"destination_type,omitempty"`
		DestinationArgs *struct {
			Cmd  *string  `json:"cmd,omitempty"`
			Args []string `json:"args,omitempty"`
		} `json:"destination_args,omitempty"`
		DestinationConfig map[string]any `json:"destination_config,omitempty"`
	}
	if err := json.Unmarshal(configBytes, &tmp); err != nil {
		return nil, fmt.Errorf("parseConfig: %w", err)
	}
	if tmp.SourceType == nil {
		return nil, fmt.Errorf("source_type is a required field")
	}
	sourceType, err := getSourceType(*tmp.SourceType)
	if err != nil {
		return nil, fmt.Errorf("parseConfig: %w", err)
	}
	if sourceType == CustomSource && (tmp.SourceArgs == nil || tmp.SourceArgs.Cmd == nil) {
		return nil, fmt.Errorf("source_args.cmd is a required field for custom sources")
	}
	if tmp.DestinationType == nil {
		return nil, fmt.Errorf("destination_type is a required field")
	}
	destinationType, err := getDestinationType(*tmp.DestinationType)
	if err != nil {
		return nil, fmt.Errorf("parseConfig: %w", err)
	}
	if destinationType == CustomDestination && (tmp.DestinationArgs == nil || tmp.DestinationArgs.Cmd == nil) {
		return nil, fmt.Errorf("destination_args.cmd is a required field for custom destinations")
	}

	return &Config{
		source: &component{
			kind: (*ComponentType)(&sourceType),
			args: &Args{
				Cmd:  tmp.SourceArgs.Cmd,
				Args: tmp.SourceArgs.Args,
			},
			config: tmp.SourceConfig,
		},
		destination: &component{
			kind: (*ComponentType)(&destinationType),
			args: &Args{
				Cmd:  tmp.DestinationArgs.Cmd,
				Args: tmp.DestinationArgs.Args,
			},
			config: tmp.DestinationConfig,
		},
	}, nil
}

func getSourceType(kind string) (SourceType, error) {
	switch kind {
	case "Custom":
		return CustomSource, nil
	default:
		return "", fmt.Errorf("unknown source type %s", kind)
	}
}

func getDestinationType(kind string) (DestinationType, error) {
	switch kind {
	case "BigQuery":
		return BigQueryDestination, nil
	default:
		return "", fmt.Errorf("unknown destination type %s", kind)
	}
}
