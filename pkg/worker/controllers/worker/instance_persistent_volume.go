package worker

import (
	"context"
	"slices"

	core "k8s.io/api/core/v1"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/worker/settings"
)

// instancePersistentVolumePlacement 读取本次协调是否依据持久卷放置 Pod。使用函数变量便于单元测试选择设置值。
var instancePersistentVolumePlacement = func(ctx context.Context) bool {
	return settings.InstancePersistentVolumePlacement.ShouldValueBool(ctx)
}

// resolveInstancePersistentVolumes 为工作卷和每个附加持久卷读取 PVC，返回已绑定 PV 的节点亲和性或继续等待的原因。
// 按首个使用者所选节点制备的卷由调度过程触发，其他尚未绑定的 PVC 需要先等待。
// 亲和性仅在创建 Pod 时写入，因此后续绑定变化会在下一次创建时生效。关闭此设置时直接使用声明的 PVC 名称。
func (r *InstanceReconciler) resolveInstancePersistentVolumes(
	ctx context.Context, inst *workercore.Instance,
) ([]*core.NodeSelector, string, error) {
	if !instancePersistentVolumePlacement(ctx) {
		return nil, "", nil
	}

	// 只读取 Pod 实际挂载的 PVC。临时工作卷无需查 PVC，模型卷有单独的解析路径。
	// 同一 PVC 只检查一次，避免对它的节点约束求重复交集。
	var claims []string
	if inst.Spec.Volume.Ephemeral == nil && inst.Spec.Volume.Persistent != nil {
		claims = append(claims, inst.Spec.Volume.Persistent.Name)
	}
	for i := range inst.Spec.AdditionalVolumes {
		av := &inst.Spec.AdditionalVolumes[i]
		if av.Model == nil && av.Persistent != nil && !slices.Contains(claims, av.Persistent.Name) {
			claims = append(claims, av.Persistent.Name)
		}
	}

	var affinities []*core.NodeSelector
	for _, claim := range claims {
		// 此处只有一个 Pod 使用 PVC，卷的单 Pod 访问模式也能满足要求。
		w := &modelArtifactWeights{}
		if err := placeModelArtifactClaim(ctx, r.Client, inst.Namespace, claim, 1, w); err != nil {
			return nil, "", err
		}
		if w.Blocked {
			return nil, w.Message, nil
		}
		if w.Affinity != nil {
			affinities = append(affinities, w.Affinity)
		}
	}

	return affinities, "", nil
}
