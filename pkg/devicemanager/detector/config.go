package detector

import (
	"context"

	"k8s.io/apimachinery/pkg/util/sets"
)

// Config 保存这一次探测使用的厂商集合和 PCI 检查策略。
type Config struct {
	NoPCICheck    bool
	Manufacturers sets.Set[string]
}

func (c *Config) Apply(_ context.Context) (*Detector, error) {
	return New(c)
}
