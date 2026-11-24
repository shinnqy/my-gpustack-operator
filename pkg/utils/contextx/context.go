package contextx

import (
	"context"

	"gpustack.ai/gpustack/pkg/utils/gox"
)

// WithCancel 保留主上下文的值，任一传入的上下文结束时一起取消。
func WithCancel(ctx context.Context, ctxs ...context.Context) (context.Context, context.CancelFunc) {
	if len(ctxs) == 0 {
		ctxs = []context.Context{ctx}
	} else {
		ctxs = append(ctxs, ctx)
	}

	sctx, scancel := context.WithCancel(ctx)

	for _, c := range ctxs {
		if c == nil {
			continue
		}

		select {
		case <-c.Done():
			scancel()
			return sctx, scancel
		default:
		}

		parent := c
		gox.Go(func() {
			select {
			case <-parent.Done():
				scancel()
			case <-sctx.Done():
			}
		})
	}

	return sctx, scancel
}

// WithoutCancel 只向调用方返回组合上下文。请求断开或服务关闭仍然会取消它。
func WithoutCancel(ctx context.Context, ctxs ...context.Context) context.Context {
	ctx, _ = WithCancel(ctx, ctxs...)
	return ctx
}
