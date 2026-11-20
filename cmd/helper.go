package cmd

import (
	"flag"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/pkg/utils/osx"
	"gpustack.ai/gpustack/pkg/utils/version"
)

func Harness(c *cobra.Command) *cobra.Command {
	gcl, pcl := flag.CommandLine, pflag.CommandLine

	// 注册 klog 的日志选项。
	klog.InitFlags(gcl)
	// 设置 klog 的默认值。
	{
		_ = gcl.Set("logtostderr", "true")
		_ = gcl.Set("v", "0")
		_ = gcl.Set("add_dir_header", "false")
		_ = gcl.Set("skip_headers", "false")
	}
	// 把 klog 选项接入 pflag，供 Cobra 解析命令行参数。
	pcl.AddGoFlag(gcl.Lookup("v"))                // --v
	pcl.AddGoFlag(gcl.Lookup("vmodule"))          // --vmodule
	pcl.AddGoFlag(gcl.Lookup("log_backtrace_at")) // --log_backtrace_at
	pcl.AddGoFlag(gcl.Lookup("add_dir_header"))   // --add_dir_header
	pcl.AddGoFlag(gcl.Lookup("logtostderr"))      // --logtostderr
	pcl.AddGoFlag(gcl.Lookup("skip_headers"))     // --skip_headers

	// 按需打印补齐环境变量后得到的命令行。
	printCmdline := pcl.Bool("print-cmdline", false,
		"print cmdline, which includes the arguments retrieved from environment.")
	c.PersistentPreRunE = func(c *cobra.Command, args []string) error {
		if *printCmdline {
			c.Printf("%s\n\n", strings.Join(os.Args, " "))
		}
		return nil
	}

	// 运行报错时省略用法，解析参数失败时显示帮助。
	c.SilenceUsage = true
	c.SetFlagErrorFunc(func(c *cobra.Command, err error) error {
		_ = c.Help()
		return err
	})

	// 根命令和一级子命令都允许未知参数。
	c.FParseErrWhitelist.UnknownFlags = true
	for _, sc := range c.Commands() {
		sc.FParseErrWhitelist.UnknownFlags = true
	}

	// 注册版本信息。
	c.Version = version.Get()

	// 将环境变量补充到命令行参数中。
	osx.RetrieveArgsFromEnvInto(c)

	return c
}
