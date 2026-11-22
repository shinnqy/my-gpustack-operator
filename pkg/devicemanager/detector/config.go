package detector

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"
)

// Config 保存厂商集合、PCI 检查策略和监控周期。常驻服务用通道把发现的厂商通知分配器。
type Config struct {
	NoPCICheck    bool
	Manufacturers sets.Set[string]

	NoFastFailed            bool
	MonitorPeriod           time.Duration
	DetectedManufacturersCh chan<- sets.Set[string]
}

func (c *Config) Apply(_ context.Context) (*Detector, error) {
	return New(c)
}
