package deviceplugin

import (
	"context"
	"time"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

// IMPORTANT: 本文件负责驱动有状态资源的后台回收循环（比如 NVIDIA MIG 分区、寒武纪 sMLU、沐曦 sGPU，Pod 没了就自动销毁释放；外加每 60 秒定时兜底防通道丢消息）。

// reclaimResyncInterval 是指每个供应商的回收循环重新运行的周期，该循环基于最后已知的活跃节点集进行，与协
// 调器的广播无关。广播通道具有丢包特性（缓冲式、非阻塞式发送在满时会有丢包），因此定时器确保回收过程最终
// 会执行，无论是否存在丢包情况。
const reclaimResyncInterval = 60 * time.Second

// RunReclaimLoop 驱动每个供应商的有状态回收功能，该功能从 DevicesReconciler 的广播 livePodUIDs 以及周期性
// 同步计时器中获取数据。无论是有状态的逻辑切分（MetaX sysfs、Cambricon cnDev），还是硬件分区（NVIDIA MIG），
// 都会创建驱动级设备，这些设备无法仅通过 Pod 的消失而被释放——设备插件响应器没有`Release`回调——因此它们通
// 过基于级别的回收机制进行回收：每一次更新，就会将当前的 livePodUIDs 集合传递给协调器，从而销毁那些 Pod
// 已消失的设备。`mode`用于标识向协调器订阅的家族。每个通知者都会收到相同的广播信息。它会阻塞，直到`ctx`被
// 取消。
func RunReclaimLoop(
	ctx context.Context,
	reconciler *DevicesReconciler,
	manufacturer string,
	mode workercore.DeviceAllocationMode,
	reconcile func(livePodUIDs []string),
) {
	notifier, release := reconciler.getReconcileNotifier(manufacturer, mode)
	defer release()
	ticker := time.NewTicker(reclaimResyncInterval)
	defer ticker.Stop()
	reclaimLoop(ctx, reconcile, notifier, ticker.C)
}

// reclaimLoop 是 RunReclaimLoop 背后的纯选集循环，已单独分离出来，可通过注入的计时器源进行单元测试。同步
// 计时器会与最后一次实际广播的集合同步，在第一次广播填充该集之前不会执行任何操作——因此，无声启动（计时器
// 在任意广播之前启动）时不会针对空集回收任何实际实例。
func reclaimLoop(ctx context.Context, reconcile func([]string), notifier <-chan []string, resync <-chan time.Time) {
	var lastLive []string
	seeded := false
	for {
		select {
		case <-ctx.Done():
			return
		case live := <-notifier:
			lastLive = live
			seeded = true
			reconcile(live)
		case <-resync:
			if seeded {
				reconcile(lastLive)
			}
		}
	}
}
