package worker

import "context"

type Config struct {
	node string
}

func (c *Config) Apply(_ context.Context) (*Worker, error) {
	return &Worker{node: c.node}, nil
}
