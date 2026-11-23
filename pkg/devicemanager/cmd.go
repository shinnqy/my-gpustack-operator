package devicemanager

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	yaml "gopkg.in/yaml.v3"

	"gpustack.ai/gpustack/pkg/devicemanager/detector"
	"gpustack.ai/gpustack/pkg/devicemanager/preflight"
)

// IMPORTANT: 入口
// NewCommand 注册设备管理命令，serve 持续上报，detect 读取库存，monitor 采样一次，preflight 检查节点的分配能力。
func NewCommand() *cobra.Command {
	c := &cobra.Command{
		Use: "device-manager",
		Aliases: []string{
			"dm",
		},
		Short: "assists in managing hardware resources.",
	}

	c.AddCommand(newServeCommand())
	c.AddCommand(newDetectCommand())
	c.AddCommand(newMonitorCommand())
	c.AddCommand(newPreflightCommand())

	return c
}

// 已经有 detect，为什么还要加 serve？
// 问题是，一次输出只能告诉你当时看到了什么。 如果 API Server 当时没连接上、后来有卡变得不健康，或者驱动晚几秒才启动，已经退出的命令就没机会更新清单。
func newServeCommand() *cobra.Command {
	// 给 Web 服务、Kubernetes 连接、探测器和分配器配置默认值，再把字段绑定到命令行参数
	o := NewOptions()

	c := &cobra.Command{
		Use:   "serve",
		Short: "serve manager to detect/monitor local devices, report to Kubernetes and provide device injection.",
		PreRunE: func(c *cobra.Command, args []string) error {
			// 检查厂商名有效、监控周期大于 1 秒等
			return o.Validate(c.Context())
		},
		RunE: func(c *cobra.Command, args []string) error {
			ctx := c.Context()

			// 补齐运行配置
			cfg, err := o.Complete(ctx)
			if err != nil {
				return fmt.Errorf("complete config: %w", err)
			}
			// 用配置创建实际的 Web 服务、控制器管理器、探测器、分配器和指标采集器
			m, err := cfg.Apply(ctx)
			if err != nil {
				return fmt.Errorf("apply config: %w", err)
			}
			// 先准备管理器，再注册 Prometheus 指标采集器
			err = m.Prepare(ctx)
			if err != nil {
				return fmt.Errorf("prepare manager: %w", err)
			}
			// 启动后台任务并等待它们结束
			err = m.Start(ctx)
			if err != nil && !errors.Is(err, context.Canceled) {
				return err
			}
			return nil
		},
	}

	o.AddFlags(c.Flags())

	return c
}

func newDetectCommand() *cobra.Command {
	o := detector.NewOptions()

	c := &cobra.Command{
		Use:   "detect",
		Short: "detect local devices once.",
		PreRunE: func(c *cobra.Command, args []string) error {
			return o.Validate(c.Context())
		},
		RunE: func(c *cobra.Command, args []string) error {
			ctx := c.Context()

			cfg, err := o.Complete(ctx)
			if err != nil {
				return fmt.Errorf("complete config: %w", err)
			}
			// 把配置变成可执行的对象
			d, err := cfg.Apply(ctx)
			if err != nil {
				return fmt.Errorf("apply config: %w", err)
			}

			enc := yaml.NewEncoder(os.Stdout)
			enc.SetIndent(4)
			// IMPORTANT: 真正开始读硬件
			// 公共探测器已经记录各厂商的读取错误。一次性命令输出本轮清单后即退出。
			grpList, _ := d.DetectAccelerator(ctx)
			return enc.Encode(grpList)
		},
	}

	o.AddFlags(c.Flags(), detector.WithoutMonitorOptions())

	return c
}

// 一次性体检节点是否具备提供各种分配模式的能力
// newPreflightCommand 单独注册运行能力预检。detect 仅读取库存，preflight 会启动持有设备的容器，
// 部分驱动还需暂时切换并恢复模式。
func newPreflightCommand() *cobra.Command {
	o := preflight.NewOptions()

	c := &cobra.Command{
		Use:   "preflight",
		Short: "check once whether local devices can serve the allocation modes their allocators offer.",
		PreRunE: func(c *cobra.Command, args []string) error {
			return o.Validate(c.Context())
		},
		RunE: func(c *cobra.Command, args []string) error {
			ctx := c.Context()

			cfg, err := o.Complete(ctx)
			if err != nil {
				return fmt.Errorf("complete config: %w", err)
			}
			p, err := cfg.Apply(ctx)
			if err != nil {
				return fmt.Errorf("apply config: %w", err)
			}

			groups := p.PreflightAccelerator(ctx)

			// Ctrl-C 或 SIGTERM 会取消子命令。中断产生的容器错误不能用来判断卡的状态，因此在发布结果之前先检查上下文。
			if err = ctx.Err(); err != nil {
				return fmt.Errorf("preflight was interrupted before it could report: %w", err)
			}

			// 每个被请求的厂商都要进入报告。网卡属于节点，单独读取，即使加速器不可读也能报告链路。主机扫描后再次检查是否中断。
			network := preflight.PreflightNetwork()
			// kubelet 的拓扑策略也是节点属性，单独记录，不影响已经得到的加速器检查结果。
			topology := p.PreflightTopology()
			if err := ctx.Err(); err != nil {
				return fmt.Errorf("preflight was interrupted before it could report: %w", err)
			}

			return preflight.Report(os.Stdout, groups, network, topology)
		},
	}

	o.AddFlags(c.Flags())

	return c
}

// 采样一次动态监控数据（温度、使用率、健康状态）并打印 YAML，复用 detector 的 Options → Validate → Complete → Apply → MonitorAccelerator。
func newMonitorCommand() *cobra.Command {
	o := detector.NewOptions()

	c := &cobra.Command{
		Use:   "monitor",
		Short: "monitor local devices once.",
		PreRunE: func(c *cobra.Command, args []string) error {
			return o.Validate(c.Context())
		},
		RunE: func(c *cobra.Command, args []string) error {
			ctx := c.Context()

			cfg, err := o.Complete(ctx)
			if err != nil {
				return fmt.Errorf("complete config: %w", err)
			}
			d, err := cfg.Apply(ctx)
			if err != nil {
				return fmt.Errorf("apply config: %w", err)
			}

			enc := yaml.NewEncoder(os.Stdout)
			enc.SetIndent(4)
			// 采样器已记录各厂商的读取错误，一次性命令输出本轮能够取得的样本。
			grpList, _ := d.MonitorAccelerator(ctx)
			return enc.Encode(grpList)
		},
	}

	o.AddFlags(c.Flags(), detector.WithoutMonitorOptions())

	return c
}
