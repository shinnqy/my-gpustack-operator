package deviceplugin

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	core "k8s.io/api/core/v1"
	klog "k8s.io/klog/v2"
	deviceplugin "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"

	"gpustack.ai/gpustack/pkg/utils/gox"
	"gpustack.ai/gpustack/pkg/utils/osx"
)

// IMPORTANT: 本文件负责：管理 Unix Socket 生命周期、gRPC 服务启停、每秒巡检 Socket 是否还在并自动向 kubelet 重注册。

// servingOptions 定义了某个推理服务循环所服务的对象：负责响应 gRPC 调用的插件、它向 kubelet 注册的资源名
// 称，以及它拥有的套接字的基础名称（与 kubelet 的自身名称并列）。
type servingOptions struct {
	Plugin       deviceplugin.DevicePluginServer
	ResourceName core.ResourceName
	SocketName   string
}

// 推理服务拥有一个设备插件服务器的生命周期管理：除了 kubelet 的套接字外，还包含位于其上的 gRPC 服务器，以
// 及将两者与 kubelet 关联的注册机制。其零值为从未启动的服务器。该机制被嵌入到 ResourceServer 中，任何能够
// 命名其套接字、资源和插件的服务器都可以使用它。
type serving struct {
	// mu 守护着下方的推理服务状态。当状态从一代服务切换到下一代服务时，开始进行交换。而 Stop 和注册读
	// 取数据则由其他 goroutine 传递至该状态。
	mu   sync.Mutex
	stop context.CancelFunc
	// 当推理服务返回后，状态即为确定，这是 Stop 所等待的。如果 Stop 在代次尚未设置时返回，那么该代次
	// 将处于空闲状态，可以接受下一个分配器已占用的套接字路径。
	done       chan struct{}
	server     *grpc.Server
	registered bool
}

// pluginSocketPeriod 表示推理服务在多久后重新检查两个决定其是否仍正常工作的条件：自己的套接字是否仍保存在
// 磁盘上，以及 kubelet 是否已接受其注册。它同时作为注册的重试间隔。
const pluginSocketPeriod = time.Second

// registerTimeout 限定了一次 Register 调用的时间范围。kubelet 会在自己的处理程序中从插件端点地址回拨，并
// 在 10 秒的超时时间内阻塞，因此一个合法的调用需要达到这个时长。超过这个时间后，kubelet 将不会响应，无限
// 制调用则会让推理服务循环——以及终止代次的 socket 探针——持续运行，直到 kubelet 被卡住为止。
const registerTimeout = 15 * time.Second

// isRegistered 报告了 kubelet 是否接受了当前正在运行的代次的注册。当没有任何服务在运行时，该值为假。从某
// 个代次被废弃到其继任者完成注册为止，该值再次为假。
func (s *serving) isRegistered() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.registered
}

// 开始将插件、套接字和资源选项名称传递给 kubelet，只要 ctx 允许，每当 kubelet 移除它们时，就会重新建立服
// 务。
//
// 一代推理服务包含一个与 kubelet 相连的套接字、运行在其上的 gRPC 服务器，以及用于将两者与 kubelet 关联的
// 注册机制。这一代服务不会持续整个进程生命周期：每当 kubelet 启动自己的注册服务器时，就会删除插件目录中的
// 所有套接字，从而无需该进程参与即可使这一代服务失效。因此，Start 循环会在观察到当前代服务消失或服务器不
// 再通过仍存在的套接字提供服务时，立即启动新一代服务。注册机制并非可选，也不是一次性操作——如果 kubelet 不
// 知道某代服务，该节点就会无法分配任何资源。
//
// 它会阻塞，直到 ctx 被取消或调用 Stop 指令，只有在无法建立任何代次时才会报告错误。在已启动的推理服务上调
// 用它只会造成无操作状态，仅会阻塞直到 ctx 完成，因此重试的监督者无法让两个服务器共享一个套接字。
func (s *serving) Start(ctx context.Context, kubeSocket string, opts servingOptions, logger klog.Logger) error {
	caller := ctx
	ctx, stop := context.WithCancel(caller)
	defer stop()

	if !s.begin(stop) {
		logger.Error(nil, "server already started")
		<-ctx.Done()
		return ctx.Err()
	}
	defer s.end()

	socket := filepath.Join(filepath.Dir(kubeSocket), opts.SocketName)

	for {
		if err := s.serve(ctx, kubeSocket, socket, opts, logger); err != nil {
			return err
		}
		if ctx.Err() != nil {
			// 只有放弃的调用者才需要报告：根据要求被停止正是“Stop”功能的用途，而若主管将其视
			// 为失败，就会让其余分配器以及该服务器（它要求退役的）一同失效。
			return caller.Err()
		}
		logger.Info("generation is over, serving again", "socket", socket)
	}
}

// 该服务运行一个代次：它监听自己的套接字，在那里提供 device-plugin gRPC API 服务，并将该资源注册到
// kubelet 中，只要该代次存在，就会不断重试注册过程——在 kubelet 清空插件目录后，其注册服务器可能尚未开始监
// 听，如果放弃的话，该资源对节点来说就会变得不可知。
//
// 当代次结束——即其套接字从磁盘上消失，或者推理服务不再提供，这都是调用者开始提供新代次的信号，或者 ctx 结
// 束——时，它会返回 nil。只有当无法建立代次时才会出现错误。移除套接字属于代次开始的阶段，而非结束阶段：一
// 个即将退出的代次如果解绑了路径，那么此时它所依赖的一切也会被解绑，而一旦有替代分配器接管，这些内容就会
// 成为替代代次的套接字。一个代次留下的，是一个没有监听程序的路径，无论是下一个代次还是 kubelet 自身的启动
// 清理过程都会将其移除。
func (s *serving) serve(ctx context.Context, kubeSocket, socket string, opts servingOptions, logger klog.Logger) error {
	if err := os.Remove(socket); err != nil && !os.IsNotExist(err) {
		logger.Error(err, "clean up socket", "socket", socket)
		return err
	}

	lis, err := net.Listen("unix", socket)
	if err != nil {
		logger.Error(err, "listen on socket", "socket", socket)
		return err
	}
	// 断开路径连接并非监听器的职责。在推理服务 goroutine 之前被淘汰的代次会迫使该 goroutine 关闭监听
	// 器本身，而 Unix 监听器在关闭时也会断开其路径——此时后续代次的套接字就会消失，kubelet 会注意到这
	// 一变化，而此进程则不会。
	if ul, ok := lis.(*net.UnixListener); ok {
		ul.SetUnlinkOnClose(false)
	}

	srv := grpc.NewServer(
		grpc.Creds(insecure.NewCredentials()),
	)
	deviceplugin.RegisterDevicePluginServer(srv, opts.Plugin)
	s.publish(srv)
	defer s.retire(srv, logger)

	// 这里不需要等待推理服务：grpc.Server.Serve 在监听器关闭后仅会返回一次，这正是取消上述代次所做的
	// 事情，因此等待它会导致该函数自身的销毁过程也被延迟。其结束状态仍会被监控，因为自动停止服务的服
	// 务器会保留套接字路径，而该路径正是下面所有检查所能够看到的。
	served := make(chan struct{})
	gox.Go(func() {
		defer close(served)

		logger.Info("serving on socket", "socket", socket)
		// 在这个协程运行之前，该代次已经停止了服务，说明正在执行关闭操作，而非服务失败。
		if err := srv.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			logger.Error(err, "serve on socket", "socket", socket)
		}
	})

	// 维持代次存活的两部分机制都是基于级别的，因此由一个计时器来驱动它们：将注册任务交给 kubelet 直到
	// 其接收完毕，当套接字消失后便将其退役。套接字在计时结束后会被检查，而不是在之前，因此每个代次至
	// 少会存活一个周期，且无法读取的路径不会触发调用者的循环。
	tick := time.NewTicker(pluginSocketPeriod)
	defer tick.Stop()
	for {
		// 在以下注册之前进行询问，不仅在于选择：将 kubelet 的端点交给本代次正在被拆除的组件，这将
		// 引发基于层级循环的边缘触发工作，并会报告一个实际上属于关闭的失败。
		if ctx.Err() != nil {
			return nil
		}
		if !s.isRegistered() {
			logger.Info("registering to kubelet")
			if err := s.register(ctx, srv, kubeSocket, socket, opts); err != nil {
				logger.Error(err, "registering to kubelet, retrying", "socket", kubeSocket)
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-served:
			// 报告而非返回：在建立后停止提供服务的代次需要被替换，而不是表明应放弃该资源。进
			// 行报告是有必要的，因为这里没有其他方法可以做到这一点——下面的套接字检查仍然能找
			// 到路径，因此这个代次可能会永远看起来处于活跃状态。
			if ctx.Err() == nil {
				logger.Error(nil, "stopped serving with its socket still in place", "socket", socket)
			}

			return nil
		case <-tick.C:
		}
		if _, err := os.Stat(socket); err != nil {
			logger.Info("socket is gone", "socket", socket)
			return nil
		}
	}
}

// “begin”标记表示推理服务已启动，并记录如何停止它。如果服务正在运行，则会产生错误报告——在这种情况下，调用
// 者不得进行推理服务操作。
func (s *serving) begin(stop context.CancelFunc) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stop != nil {
		return false
	}
	s.stop, s.done = stop, make(chan struct{})

	return true
}

// end 会清除运行状态，并释放任何正在等待它的 Stop 对象，因此后续的 Start 可以再次生效。
func (s *serving) end() {
	s.mu.Lock()
	done := s.done
	s.stop, s.done = nil, nil
	s.mu.Unlock()

	close(done)
}

// publish 将该代次的 gRPC 服务器交还给 Stop，并清除新代次尚未完成的注册。
func (s *serving) publish(srv *grpc.Server) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.server, s.registered = srv, false
}

// 退出 Stop 服务，该服务会关闭其监听器，并取消指向 kubelet 的注册。除非 Stop 服务仍在提供对应代次的服务，
// 否则它只是一个无操作的函数——因此，一个在后续启动后能够恢复的 Stop 函数不会清除任何内容，也不会关闭本不
// 应被关闭的服务器。
//
// 在锁定被释放后，服务器会停止运行，绝不会继续在锁定状态下运行：grpc 的 Stop 等待 Serve 返回，而持有互斥
// 锁使得服务路径上的任何内容都可能通过这种等待机制读取，这就是导致关闭死锁的原因。
func (s *serving) retire(srv *grpc.Server, logger klog.Logger) {
	s.mu.Lock()
	if srv == nil || s.server != srv {
		s.mu.Unlock()

		return
	}
	s.server, s.registered = nil, false
	s.mu.Unlock()

	logger.Info("stopping")
	srv.Stop()
}

// Stop 会停止 gRPC 服务器并结束推理服务循环，在 Start 执行完毕且没有剩余代次可操作套接字之前不会返回。这
// 是一个无操作函数，会被记录，适用于从未启动的推理服务。
//
// 稍后的启动操作在之前的启动操作返回后可以再次执行——单个启动保护机制在启动操作离开时会被解除，因此，一个
// 启动操作同时执行自己的停止操作会被拒绝，无法允许在同一个套接字上放置第二个服务器。
func (s *serving) Stop(logger klog.Logger) {
	// 该代次与取消功能一起被读取，因此在此处被废弃的，是本次停止操作所涉及的代次，而非它到达时正在提
	// 供的任何内容。
	s.mu.Lock()
	stop, srv, done := s.stop, s.server, s.done
	s.mu.Unlock()

	if stop == nil {
		logger.Errorf(nil, "server not started")
		return
	}

	stop()
	s.retire(srv, logger)
	// 等待而非任其自行完成：处于启动与发布之间的代次没有服务器供上述 retirement 操作使用，但它仍会监
	// 听——并首先移除——套接字。在完成之前返回会使其对替代分配器已采用的路径进行干预。
	<-done
}

// 在 kubelet 上注册 srv 的端点，该代次将被服务，并记录对其的接受情况。srv 会识别该代次，因此仅在其代次退
// 场时接受的注册不会记录在其后续版本上。
//
// 故障被报告但未记录：只要 kubelet 未运行，每周期会执行一次，因此这里的一条记录和调用者处的另一条记录会使
// 每秒错误日志量翻倍，而正是调用者知道该尝试会被重新尝试。每个错误记录都标注了对应的步骤。
func (s *serving) register(ctx context.Context, srv *grpc.Server, kubeSocket, socket string, opts servingOptions) error {
	regOpts, err := opts.Plugin.GetDevicePluginOptions(ctx, &Empty{})
	if err != nil {
		return fmt.Errorf("getting device plugin options: %w", err)
	}

	cli, err := grpc.NewClient("unix://"+kubeSocket,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("dialing kubelet socket: %w", err)
	}
	defer osx.Close(cli)

	regCli := deviceplugin.NewRegistrationClient(cli)
	regReq := &deviceplugin.RegisterRequest{
		Version:      deviceplugin.Version,
		Endpoint:     filepath.Base(socket),
		ResourceName: string(opts.ResourceName),
		Options:      regOpts,
	}
	regCtx, cancel := context.WithTimeout(ctx, registerTimeout)
	defer cancel()
	if _, err = regCli.Register(regCtx, regReq); err != nil {
		return fmt.Errorf("registering %s: %w", opts.ResourceName, err)
	}

	// 根据已注册的代次进行记录，与当前任何情况都无关：如果退休操作发生在调用返回与此锁定之间，那么其
	// 已清除标志将会被恢复，导致一个停止的服务器报告自己已注册到 kubelet。
	s.mu.Lock()
	if s.server == srv {
		s.registered = true
	}
	s.mu.Unlock()

	return nil
}
