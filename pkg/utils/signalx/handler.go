package signalx

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// Handler 在收到中断或终止信号时取消上下文，供子命令退出。
func Handler() context.Context {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ctx.Done()
		stop()
	}()
	return ctx
}
