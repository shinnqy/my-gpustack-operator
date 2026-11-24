package manager

import (
	"context"
	"time"

	"k8s.io/client-go/rest"
)

type (
	// ConstructRestConfigFunc 使用集群标识和访问令牌构建连接该集群的 REST 配置。
	ConstructRestConfigFunc = func(cluster, token string) (*rest.Config, error)

	Config struct {
		ConstructRestConfig ConstructRestConfigFunc
		ResyncPeriod        time.Duration
		// ReadinessCheckTimeout 限制单次 APIService 就绪检查所用的时间。未指定时使用默认值，
		// 避免零时长让每次检查立即超时。
		ReadinessCheckTimeout time.Duration
	}
)

func (c *Config) Apply(ctx context.Context) (Manager, error) {
	return New(ctx, c)
}
