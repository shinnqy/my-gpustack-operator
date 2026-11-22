package devicemanager

import (
	"context"

	"github.com/spf13/pflag"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"

	"gpustack.ai/gpustack/pkg/devicemanager/allocator"
	"gpustack.ai/gpustack/pkg/devicemanager/detector"
	"gpustack.ai/gpustack/pkg/devicemanager/exporter"
	"gpustack.ai/gpustack/pkg/manager"
	"gpustack.ai/gpustack/pkg/webserver"
)

type Options struct {
	// 配置服务端。
	ServerOptions *webserver.Options

	// 配置管理器。
	ManagerOptions *manager.Options

	// 配置设备探测器。
	DetectorOptions *detector.Options

	// 配置设备分配器。
	AllocatorOptions *allocator.Options
}

// 给 Web 服务、Kubernetes 连接、探测器和分配器配置默认值，再把字段绑定到命令行参数
func NewOptions() *Options {
	opts := &Options{
		// 配置服务端。
		ServerOptions: webserver.NewOptions(),

		// 配置管理器。
		ManagerOptions: manager.NewOptions(),

		// 配置设备探测器。
		DetectorOptions: detector.NewOptions(),

		// 配置设备分配器。
		AllocatorOptions: allocator.NewOptions(),
	}
	opts.ServerOptions.BindPort = 32443
	opts.ManagerOptions.KubeContentType = runtime.ContentTypeJSON

	return opts
}

func (o *Options) AddFlags(fs *pflag.FlagSet) {
	// 配置服务端。
	o.ServerOptions.AddFlags(fs, webserver.WithoutBindUnixPath())

	// 配置管理器。
	o.ManagerOptions.AddFlags(fs, manager.WithoutKubeElectionOptions())

	// 配置设备探测器。
	o.DetectorOptions.AddFlags(fs)

	// 配置设备分配器。
	o.AllocatorOptions.AddFlags(fs)
}

// 检查厂商名有效、监控周期大于 1 秒等
func (o *Options) Validate(ctx context.Context) error {
	// 配置服务端。
	if err := o.ServerOptions.Validate(ctx); err != nil {
		return err
	}

	// 配置管理器。
	if err := o.ManagerOptions.Validate(ctx); err != nil {
		return err
	}

	// 配置设备探测器。
	if err := o.DetectorOptions.Validate(ctx); err != nil {
		return err
	}

	// 配置设备分配器。
	if err := o.AllocatorOptions.Validate(ctx); err != nil {
		return err
	}

	return nil
}

// 补齐运行配置
func (o *Options) Complete(ctx context.Context) (*Config, error) {
	// 配置服务端。
	srvConfig, err := o.ServerOptions.Complete(ctx)
	if err != nil {
		return nil, err
	}

	// 配置管理器。
	mgrConfig, err := o.ManagerOptions.Complete(ctx)
	if err != nil {
		return nil, err
	}

	// 配置设备探测器。
	detConfig, err := o.DetectorOptions.Complete(ctx)
	if err != nil {
		return nil, err
	}

	// 配置设备分配器。
	alcConfig, err := o.AllocatorOptions.Complete(ctx)
	if err != nil {
		return nil, err
	}

	// 建一个共用通道。探测器发出本轮发现的厂商集合，分配器接收后决定要启动或停止哪些厂商插件。
	detectedManufacturersCh := make(chan sets.Set[string], 4)
	detConfig.DetectedManufacturersCh = detectedManufacturersCh
	alcConfig.DetectedManufacturersCh = detectedManufacturersCh

	return &Config{
		ServerConfig:    srvConfig,
		ManagerConfig:   mgrConfig,
		DetectorConfig:  detConfig,
		AllocatorConfig: alcConfig,
		// 指标导出器使用探测器的采样周期。Config.Apply 负责传递这个值，无需单独配置命令行参数。
		ExporterConfig: &exporter.Config{},
	}, nil
}
