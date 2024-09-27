package tmpl

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEmbed(t *testing.T) {
	f, err := os.Open("full_refresh_overwrite_finalizer.sql.tmpl")
	assert.Nil(t, err, "err is not nil")
	stream, err := io.ReadAll(f)
	assert.Nil(t, err, "err is not nil")
	assert.Equal(t, string(stream), FullRefreshOverwriteFinalizer)
}
