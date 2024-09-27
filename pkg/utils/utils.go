package utils

import (
	"fmt"
	"io"
	"os"
	"regexp"
)

var re = regexp.MustCompile(`[^a-zA-Z0-9_]`)

// SanitizeFieldName replaces invalid characters with underscores
func SanitizeFieldName(name string) string {
	// Regular expression to match invalid characters
	sanitized := re.ReplaceAllString(name, "_")
	return sanitized
}

func ReadFile(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("ReadFile: %w", err)
	}
	bytes, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("ReadFile: %w", err)
	}
	return bytes, err
}
