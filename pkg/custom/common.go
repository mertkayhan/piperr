package custom

import (
	"context"
	"os/exec"
)

type Custom struct {
	p *exec.Cmd
}

func (c *Custom) Run(ctx context.Context) error {
	return c.p.Run()
}
