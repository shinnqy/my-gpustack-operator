package exporter

import (
	"context"
	"time"

	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
)

type Config struct {
	// ClientReader 会读取该节点的 Instance pods。它必须由携带 deviceplugin.IndexingPodsByNodeName 的
	// informer 支持。若未设置，则采用配置的 pkg/manager 控制器运行时。
	ClientReader ctrlcli.Reader

	// MonitorPeriod 表示该节点的实例被采样的频率。
	//
	// Device Manager 会分配检测器的周期：两者会在同一端点上每周期发布一个样本，而一个受周期限制的消费
	// 者缩放功能无法处理两个不同的样本。
	MonitorPeriod time.Duration
}

func (c *Config) Apply(_ context.Context) (*Poller, error) {
	return New(c)
}
