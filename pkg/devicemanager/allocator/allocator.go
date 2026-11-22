package allocator

import (
	"context"

	"k8s.io/apimachinery/pkg/util/sets"
	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/utils/gox"
)

var logger = klog.Background().WithName("allocator")

type Allocator struct {
	kubeSocket              string
	noShared                bool
	noSliced                bool
	noPartitioned           bool
	detectedManufacturersCh <-chan sets.Set[string]
}

func New(c *Config) (*Allocator, error) {
	return &Allocator{
		kubeSocket:              c.KubeSocket,
		noShared:                c.NoShared,
		noSliced:                c.NoSliced,
		noPartitioned:           c.NoPartitioned,
		detectedManufacturersCh: c.DetectedManufacturersCh,
	}, nil
}

func (a *Allocator) Start(ctx context.Context) error {
	allocators := make(map[string]device.Allocator)
	defer func() {
		for m := range allocators {
			allocators[m].Stop()
		}
	}()

	errCh := make(chan error)
	sCtx, sCancel := context.WithCancel(ctx)
	defer sCancel()

	createOpts := device.AllocatorOptions{
		Logger:        logger.V(3),
		KubeSocket:    a.kubeSocket,
		NoShared:      a.noShared,
		NoSliced:      a.noSliced,
		NoPartitioned: a.noPartitioned,
	}

	// 网卡属于节点，每台节点只需启动一次 RDMA 分配器。支持 RDMA 的平台注册创建函数，其他平台跳过，这
	// 样没有 kubelet 套接字目录的机器也能启动管理器。
	if rdmaAllocatorCreator != nil {
		allocator := rdmaAllocatorCreator(createOpts)
		gox.Go(func() {
			if err := allocator.Start(sCtx); err != nil {
				logger.Error(err, "failed to start allocator", "family", allocator.Name())
				errCh <- err
			}
		})
		defer allocator.Stop()
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errCh:
			return err
		case manus := <-a.detectedManufacturersCh:
			logger.V(2).Info("received detected manufacturers")

			// 上次启动的分配器中，若对应厂商这轮已消失，停止并移除它。已在运行的厂商从待启动
			// 集合中去掉。
			for m := range allocators {
				if !manus.Has(m) {
					allocators[m].Stop()
					delete(allocators, m)
					continue
				}
				manus.Delete(m)
			}

			// 为本轮新发现、且尚未启动分配器的厂商创建分配器。
			for _, m := range manus.UnsortedList() {
				// IMPORTANT: 发现新厂商，查表，创建出 NVIDIA Device Plugin，起协程运行 allocator.Start(sCtx)，去监听 kubelet 的 Unix Socket 并注册设备资源
				creator := supportedAllocatorCreators[m]
				if creator == nil {
					logger.V(2).Info("unsupported manufacturer, skipping")
					continue
				}
				allocators[m] = creator(createOpts)
				allocator := allocators[m]
				gox.Go(func() {
					if err := allocator.Start(sCtx); err != nil {
						logger.Error(err, "failed to start allocator", "manufacturer", m)
						errCh <- err
					}
				})
			}
		}
	}
}
