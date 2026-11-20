package worker

import (
	"context"
	"fmt"
)

type Worker struct {
	node string
}

func (w *Worker) Prepare(_ context.Context) error {
	return nil
}

func (w *Worker) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := fmt.Printf("worker ready for %s\n", w.node)
	return err
}
