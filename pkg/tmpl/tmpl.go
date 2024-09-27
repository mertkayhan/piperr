package tmpl

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"strings"
	"text/template"
)

var funcs = template.FuncMap{
	"join":   strings.Join,
	"printf": fmt.Sprintf,
	"stringmap": func(format string, elems []string) []string {
		out := make([]string, len(elems))
		for i := 0; i < len(elems); i++ {
			out[i] = fmt.Sprintf(format, elems[i], elems[i])
		}
		return out
	},
	"append": func(slice []string, elems ...string) []string { return append(slice, elems...) },
	"list":   func(elems ...string) []string { return elems },
}

type Query struct {
	OriginalTable string
	TmpTable      string
}

func CreateFullRefreshOverwriteFinalizer(q *Query) (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("CreateFullRefreshOverwriteFinalizer: %w", err)
	}
	file := path.Join(cwd, "pkg/tmpl/full_refresh_overwrite_finalizer.sql.tmpl")
	name := "full_refresh_overwrite_finalizer.sql.tmpl"
	tmpl, err := template.New(name).Funcs(funcs).ParseFiles(file)
	if err != nil {
		return "", fmt.Errorf("CreateFullRefreshOverwriteFinalizer: %w", err)
	}
	var buf bytes.Buffer
	err = tmpl.Execute(&buf, q)
	if err != nil {
		return "", fmt.Errorf("CreateFullRefreshOverwriteFinalizer: %w", err)
	}
	return buf.String(), nil
}
