package workergateway

import (
	"context"
	"errors"
	"fmt"

	"github.com/spf13/pflag"
	"go.uber.org/automaxprocs/maxprocs"
	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/pkg/utils/gox"
	"gpustack.ai/gpustack/pkg/webserver"
	"gpustack.ai/gpustack/pkg/workergateway/manager"
)

type Options struct {
	// goroutine 池的运行参数。
	GopoolWorkerFactor int

	// 上游 Worker 管理器参数。
	ManagerOptions *manager.Options

	// 网关监听参数。
	ServerOptions *webserver.Options
}

func NewOptions() *Options {
	opts := &Options{
		// 设置 goroutine 池的默认容量系数。
		GopoolWorkerFactor: 100,

		// 设置 Worker 连接的默认值。
		ManagerOptions: manager.NewOptions(),

		// 设置 Web 服务的默认值。
		ServerOptions: webserver.NewOptions(),
	}

	opts.ServerOptions.BindUnixPath = "/var/lib/gpustack/gpustack-operator-worker-gateway.sock"
	opts.ServerOptions.BindPort = 0
	return opts
}

func (o *Options) AddFlags(fs *pflag.FlagSet) {
	// 注册 goroutine 池的容量参数。
	fs.IntVar(&o.GopoolWorkerFactor, "gopool-worker-factor", o.GopoolWorkerFactor,
		"the number of tasks of the goroutine worker pool, "+
			"it is calculated by the number of CPU cores multiplied by this factor.")

	// 注册上游集群连接参数。
	o.ManagerOptions.AddFlags(fs)

	// 注册监听地址、套接字与证书参数。
	o.ServerOptions.AddFlags(fs)
}

func (o *Options) Validate(ctx context.Context) error {
	// 校验 goroutine 池容量。
	if o.GopoolWorkerFactor < 100 {
		return errors.New("--gopool-worker-factor: less than 100")
	}

	// 校验 Worker 连接配置。
	if err := o.ManagerOptions.Validate(ctx); err != nil {
		return err
	}

	// 校验网关监听配置。
	if err := o.ServerOptions.Validate(ctx); err != nil {
		return err
	}

	return nil
}

func (o *Options) Complete(ctx context.Context) (*Config, error) {
	// 按运行环境设置 Go 的并行度，随后可通过 runtime.GOMAXPROCS(0) 读取。
	_, err := maxprocs.Set(maxprocs.Logger(klog.NewStandardLogger("INFO").Printf))
	if err != nil {
		return nil, fmt.Errorf("set maxprocs: %w", err)
	}

	// 依据并行度和容量系数配置 goroutine 池。
	gox.Configure(o.GopoolWorkerFactor)

	// 将 Worker 连接参数转为管理器配置。
	mgrConfig, err := o.ManagerOptions.Complete(ctx)
	if err != nil {
		return nil, err
	}

	// 将监听参数转为可启动的 Web 服务配置。
	srvConfig, err := o.ServerOptions.Complete(ctx)
	if err != nil {
		return nil, err
	}

	return &Config{
		ManagerConfig: mgrConfig,
		ServerConfig:  srvConfig,
	}, nil
}
