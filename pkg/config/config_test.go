package config

import (
	"errors"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestParseConfigWithoutSourceType(t *testing.T) {
	config := []byte(`{}`)
	_, err := parseConfig(config)
	assert.NotNil(t, err)
	assert.Errorf(t, err, "source_type is a required field")
}

func TestParseConfigWithUnknownSourceType(t *testing.T) {
	config := []byte(`{ "source_type": "" }`)
	_, err := parseConfig(config)
	assert.NotNil(t, err)
	assert.Errorf(t, err, "parseConfig: %w", errors.New("unknown source type "))
}

func TestParseConfigWithCustomSourceMissingCmd(t *testing.T) {
	config := []byte(`{ "source_type": "Custom" }`)
	_, err := parseConfig(config)
	assert.NotNil(t, err)
	assert.Errorf(t, err, "source_args.cmd is a required field for custom sources")
}

func TestParseConfigWithoutDestinationType(t *testing.T) {
	config := []byte(`{ "source_type": "Custom", "source_args": { "cmd": "sleep", "args": ["3600"] } }`)
	_, err := parseConfig(config)
	assert.NotNil(t, err)
	assert.Errorf(t, err, "destination_type is a required field")
}

func TestParseConfigWithUnknownDestination(t *testing.T) {
	config := []byte(`{ "source_type": "Custom", "source_args": { "cmd": "sleep", "args": ["3600"] }, "destination_type": "" }`)
	_, err := parseConfig(config)
	assert.NotNil(t, err)
	assert.Errorf(t, err, "parseConfig: %w", errors.New("unknown destination type "))
}

func TestParseConfigWithCustomDestinationMissingCmd(t *testing.T) {
	config := []byte(`{ "source_type": "Custom", "source_args": { "cmd": "sleep", "args": ["3600"] }, "destination_type": "Custom" }`)
	_, err := parseConfig(config)
	assert.NotNil(t, err)
	assert.Errorf(t, err, "destination_args.cmd is a required field for custom destinations")
}

func TestBuildConfig(t *testing.T) {
	viper.Set("config", "data/config.json")
	config, err := BuildConfig()
	assert.Nil(t, err)
	assert.EqualValues(t, CustomSource, *config.GetSourceType())
	expectedCmd := "sleep"
	assert.EqualValues(t, &Args{Cmd: &expectedCmd, Args: []string{"3600"}}, config.GetSourceArgs())
	assert.EqualValues(t, map[string]any{}, config.GetSourceConfig())
	assert.EqualValues(t, BigQueryDestination, *config.GetDestinationType())
	assert.EqualValues(t, &Args{}, config.GetDestinationArgs())
	assert.EqualValues(t, map[string]any{
		"project_id": "test",
		"dataset_id": "test",
		"table_id":   "test",
	}, config.GetDestinationConfig())
}
