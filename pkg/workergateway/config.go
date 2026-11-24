package workergateway

import (
	"context"

	"gpustack.ai/gpustack/pkg/webserver"
	"gpustack.ai/gpustack/pkg/workergateway/manager"
)

type Config struct {
	// 集群连接管理器的配置。
	ManagerConfig *manager.Config

	// 对外提供 HTTP 服务的配置。
	ServerConfig *webserver.Config
}

func (c *Config) Apply(ctx context.Context) (*WorkerGateway, error) {
	mgr, err := c.ManagerConfig.Apply(ctx)
	if err != nil {
		return nil, err
	}

	srv, err := c.ServerConfig.Apply(ctx)
	if err != nil {
		return nil, err
	}

	return &WorkerGateway{
		Manager: mgr,
		Server:  srv,
	}, nil
}
