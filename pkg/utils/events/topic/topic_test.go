package topic

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// 不同资源的 Watch 共用一张按类型索引的主题表。新订阅与已有主题的发布可以同时进行。
func TestConcurrentTopicsDeliverToTheirOwnSubscribers(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	start := make(chan struct{})
	var workers sync.WaitGroup
	for i := range 16 {
		workers.Go(func() {
			<-start
			name := Topic(fmt.Sprintf("%s/%d", t.Name(), i))
			sub, err := Subscribe[int](name)
			if err != nil {
				t.Error(err)
				return
			}
			defer sub.Unsubscribe()
			for seq := range 8 {
				if err := Publish(ctx, name, seq); err != nil {
					t.Error(err)
					return
				}
				got, err := sub.Receive(ctx)
				if err != nil {
					t.Error(err)
					return
				}
				if got.Topic != name || got.Data != seq {
					t.Errorf("%s 收到 %+v，期望序号 %d", name, got, seq)
					return
				}
			}
		})
	}
	close(start)
	workers.Wait()
}
