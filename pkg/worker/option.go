package worker

import (
	"context"
	"fmt"

	"github.com/spf13/pflag"
)

type Options struct {
	node string
}

func NewOptions() *Options {
	return &Options{}
}

func (o *Options) AddFlags(flags *pflag.FlagSet) {
	flags.StringVar(&o.node, "demo-node", "lab-node", "name of the example node")
}

func (o *Options) Validate(_ context.Context) error {
	if o.node == "" {
		return fmt.Errorf("demo-node must have a name")
	}
	return nil
}

func (o *Options) Complete(_ context.Context) (*Config, error) {
	return &Config{node: o.node}, nil
}
