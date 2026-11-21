package devicemanager

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	yaml "gopkg.in/yaml.v3"

	"gpustack.ai/gpustack/pkg/devicemanager/detector"
)

// IMPORTANT: 入口
// NewCommand 注册设备管理命令和一次性硬件探测子命令。
func NewCommand() *cobra.Command {
	c := &cobra.Command{
		Use: "device-manager",
		Aliases: []string{
			"dm",
		},
		Short: "assists in managing hardware resources.",
	}

	c.AddCommand(newDetectCommand())

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
