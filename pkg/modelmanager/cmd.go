// Package modelmanager 是从节点本地缓存交付模型权重的节点侧组件：
// 包含提供内联临时卷的 CSI 节点插件、背后的缓存存储、下载器与垃圾回收器，
// 以及将节点状态写回 NodeModelStore status 的上报器。
package modelmanager

import (
	"context"
	"errors"
	"fmt"

	"github.com/spf13/cobra"
)

// NewCommand 返回 model-manager 子命令。
func NewCommand() *cobra.Command {
	o := NewOptions()

	c := &cobra.Command{
		Use: "model-manager",
		Aliases: []string{
			"mm",
		},
		Short: "materializes, verifies and mounts model weights from the node's cache.",
		PreRunE: func(c *cobra.Command, args []string) error {
			return o.Validate(c.Context())
		},
		RunE: func(c *cobra.Command, args []string) error {
			ctx := c.Context()

			cfg, err := o.Complete(ctx)
			if err != nil {
				return fmt.Errorf("complete config: %w", err)
			}
			m, err := cfg.Apply(ctx)
			if err != nil {
				return fmt.Errorf("apply config: %w", err)
			}
			if err := m.Prepare(ctx); err != nil {
				return fmt.Errorf("prepare manager: %w", err)
			}
			if err := m.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
				return err
			}
			return nil
		},
	}

	o.AddFlags(c.Flags())

	return c
}
