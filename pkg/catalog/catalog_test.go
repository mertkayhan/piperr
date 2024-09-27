package catalog

import (
	"errors"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestUnknownStreamMode(t *testing.T) {
	catalog := []byte(`
		{
			"TestStream": {
				"mode": "whatever"
			}
		}
	`)
	_, err := parseCatalog(catalog)
	assert.NotNil(t, err)
	assert.Errorf(t, err, "parseCatalog: %w", errors.New("unknown sync mode whatever"))
}

func TestBuildCatalogWithWrongPath(t *testing.T) {
	viper.Set("catalog", "whatever")
	_, err := BuildCatalog()
	assert.NotNil(t, err)
	assert.Errorf(t, err, "NewCatalog: path does not exist %s", "whatever")
}

func TestKnownStreamMode(t *testing.T) {
	catalog := []byte(`
		{
			"TestStream": {
				"mode": "IncrementalAppend"
			}
		}
	`)
	_, err := parseCatalog(catalog)
	assert.Nil(t, err)
}

func TestMissingStreamMode(t *testing.T) {
	catalog := []byte(`
		{
			"TestStream": {}
		}
	`)
	_, err := parseCatalog(catalog)
	assert.NotNil(t, err)
	assert.Errorf(t, err, "parseCatalog: missing sync mode for %s", "TestStream")
}

func TestCompositeKeyWithMissingTicker(t *testing.T) {
	catalog := []byte(`
		{
			"TestStream": {
				"mode": "whatever",
				"composite_key": ["key1", "key2"]
			}
		}
	`)
	_, err := parseCatalog(catalog)
	assert.NotNil(t, err)
	assert.Errorf(t, err, "if composite key is set, ticker should also be set")
}

func TestParseCatalog(t *testing.T) {
	viper.Set("catalog", "data/catalog.json")
	c, err := BuildCatalog()
	assert.Nil(t, err)
	compositeKey, err := c.GetCompositeKey("TestStream")
	assert.Nil(t, err)
	assert.EqualValues(t, []string{"user_id", "email_id"}, compositeKey)
	primaryKey, err := c.GetPrimaryKey("TestStream")
	assert.Nil(t, err)
	assert.EqualValues(t, "id", *primaryKey)
	mode, err := c.GetMode("TestStream")
	assert.Nil(t, err)
	assert.EqualValues(t, SyncModeIncrementalAppend, *mode)
	ticker, err := c.GetTicker("TestStream")
	assert.Nil(t, err)
	assert.EqualValues(t, "updated_date", *ticker)
}
