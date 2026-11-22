package worker

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/resource"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/controller"
	"gpustack.ai/gpustack/pkg/utils/quantityx"
	"gpustack.ai/gpustack/pkg/utils/strconvx"
)

// InstanceReconciler 协调 v1alpha1.Instance 的资源和状态。
//   - 根据 Instance spec 创建、更新或删除承载实例的 Pod。
//   - 如规格声明了端口，则为 Instance 创建 Service。Instance 拥有 Service，停止 Pod 时仍可保留已分配的节点端口。
//   - 根据 Pod 状态更新 Instance 状态。
type InstanceReconciler struct {
	Client    ctrlcli.Client
	APIReader ctrlcli.Reader
}

var _ ctrlreconcile.Reconciler = (*InstanceReconciler)(nil)

const (
	// InstancePhaseStarting 表示正在创建或更新 Pod，使其符合 Instance 规格。
	InstancePhaseStarting = "Starting"
	// InstancePhaseStopping 表示实例正在停止，正在删除其 Pod。
	InstancePhaseStopping = "Stopping"
	// InstancePhaseStopped 表示 Pod 已删除，实例已停止。
	InstancePhaseStopped = "Stopped"
	// InstancePhaseReady 表示 Pod 已运行并就绪。
	InstancePhaseReady = "Ready"
	// InstancePhaseDeleting 表示 Instance 已进入删除流程，正在删除其 Pod。
	InstancePhaseDeleting = "Deleting"
)

const (
	// InstanceTypePhaseInactive 表示规格的 ClusterQueue 已因管理员停用而进入 Hold，
	// 或已在 HoldAndDrain 下排空所有预留。
	// 该值与 apistatus.GetSummaryOfClusterQueue 返回的 Inactive 一致。
	InstanceTypePhaseInactive = "Inactive"
)

func (r *InstanceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// 第 7 章实现此函数。
	var result0 ctrl.Result
	var result1 error
	return result0, result1
}

// additionalVolumeName 根据附加卷的索引生成 Pod 中的卷名。
func additionalVolumeName(i int) string {
	return "additional-" + strconvx.Itoa(i)
}

func (r *InstanceReconciler) SetupController(_ context.Context, opts controller.SetupOptions) error {
	// 第 7 章实现此函数。
	var result0 error
	return result0
}

// PartitionProfileMemoryPercent 返回指定硬件分区规格占单卡显存的比例，以 1 到 100 的整数百分比表示。
//
// 若池提供该规格，但探测或设备管理器滚动升级期间尚未上报每实例显存或单卡总显存，
// 则返回 sizeable=false。调用方应提示稍后重试，避免按整卡计算。
//
// 请求不涉及分区，或池未提供指定的分区规格时，返回 (0, true)。后者属于规格不匹配，
// 调用方会在校验时列出实际提供的规格。
//
// Instance webhook 和 ModelDeployment 渲染器共用此函数，因此把它放在 getResourceRequirements 旁。
// 前者把默认值写入 Instance，后者的 webhook 只补卡数，渲染时才换算规格。
// 共用算法可以让 Pod 的主机 CPU 和内存与实际占用的卡份额对应。
func PartitionProfileMemoryPercent(
	instType *workercore.InstanceType, profile string,
) (pct int64, sizeable bool) {
	if profile == "" {
		return 0, true
	}
	prof, _, found := PartitionProfile(instType, profile)
	if !found {
		return 0, true
	}
	if prof.MemoryMib <= 0 {
		return 0, false
	}
	cardVRAMMib, err := InstanceTypeCardVRAMMib(instType)
	if err != nil || cardVRAMMib <= 0 {
		return 0, false
	}
	return min(max(prof.MemoryMib*100/cardVRAMMib, 1), 100), true
}

// PartitionProfile 在 Status.Detail 的物理分区清单中查找指定规格。
// 结果包含每个实例的 MemoryMib、池内实例总上限 Count，以及是否已生成加速器详情、是否找到该规格。
func PartitionProfile(
	instType *workercore.InstanceType, profile string,
) (prof workercore.AcceleratorSlicedPhysicalDetailProfile, ready, found bool) {
	ready = instType.Status.Detail.AcceleratorReady()
	for _, p := range instType.Status.Detail.SlicedDetail.Physical.Profiles {
		if p.Name == profile {
			return p, ready, true
		}
	}
	return workercore.AcceleratorSlicedPhysicalDetailProfile{}, ready, false
}

// InstanceTypeCardVRAMMib 从 Status.Detail.Memory 解析单卡显存，单位为 MiB。空值表示尚未就绪，
// 可以重试。非正数或无法解析的值会返回错误。
func InstanceTypeCardVRAMMib(instType *workercore.InstanceType) (int64, error) {
	memStr := instType.Status.Detail.Memory
	if memStr == "" {
		return 0, fmt.Errorf("instance type %s has no per-card memory yet (detail not ready)", instType.Name)
	}
	q, err := resource.ParseQuantity(memStr)
	if err != nil {
		return 0, fmt.Errorf("parse memory %q of instance type %s: %w", memStr, instType.Name, err)
	}
	mib := q.Value() / quantityx.Mi
	if mib <= 0 {
		return 0, fmt.Errorf("instance type %s has non-positive memory %q", instType.Name, memStr)
	}
	return mib, nil
}
