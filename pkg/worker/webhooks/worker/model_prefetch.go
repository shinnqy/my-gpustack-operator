package worker

import (
	"context"
	"fmt"
	"sort"

	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrladmission "sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/webhook"
	workerctrl "gpustack.ai/gpustack/pkg/worker/controllers/worker"
)

// ModelPrefetchWebhook 用于校验 v1alpha1.ModelPrefetch。
//
// 该 Webhook 仅执行校验，检查的均为 schema 无法覆盖的约束：artifact 与授权是否存在、放置策略是否无歧义、目标节点集合是否位于授权实际覆盖的 store 内、预算估算是否不超限，以及固定缓存（pinning）是否依托于允许该操作的授权。
//
// 已存在但尚未完成解析的 artifact 允许被准入，这与基于未解析 artifact 的 ModelDeployment 一致：prefetch 会在状态中等待权重确定身份标识，从而避免 GitOps 工具必须对两个对象的创建顺序排序。预算估算基于当前已知信息进行（未解析的 artifact 尚无大小），准入估算无法感知的偏差由记账控制器上报。
//
// nolint: lll
// +k8s:webhook-gen:validating:group="worker.gpustack.ai",version="v1alpha1",resource="modelprefetches",scope="Namespaced"
// +k8s:webhook-gen:validating:operations=["CREATE","UPDATE"],failurePolicy="Fail",sideEffects="None",matchPolicy="Equivalent",timeoutSeconds=10
type ModelPrefetchWebhook struct {
	Client    ctrlcli.Client
	APIReader ctrlcli.Reader
}

func (r *ModelPrefetchWebhook) SetupWebhook(
	_ context.Context, opts webhook.SetupOptions,
) (runtime.Object, error) {
	r.Client = opts.Manager.GetClient()
	r.APIReader = opts.Manager.GetAPIReader()

	return &workercore.ModelPrefetch{}, nil
}

var (
	_ ctrladmission.Validator[runtime.Object] = (*ModelPrefetchWebhook)(nil)
	_ webhook.ReceiveDeletionUpdate           = (*ModelPrefetchWebhook)(nil)
)

// ReceiveDeletionUpdate 是一个无实际行为的标记方法：正在删除的 prefetch 不会影响本 Webhook 的任何校验逻辑，固定缓存（pin）由控制器负责释放。
func (r *ModelPrefetchWebhook) ReceiveDeletionUpdate() {}

func (r *ModelPrefetchWebhook) ValidateCreate(
	ctx context.Context, obj runtime.Object,
) (ctrladmission.Warnings, error) {
	pf := obj.(*workercore.ModelPrefetch)

	if errs := r.validateModelPrefetch(ctx, pf); len(errs) > 0 {
		return nil, kerrors.NewInvalid(workercore.Kind("ModelPrefetch"), pf.Name, errs)
	}

	return nil, nil
}

func (r *ModelPrefetchWebhook) ValidateUpdate(
	ctx context.Context, oldObj, newObj runtime.Object,
) (ctrladmission.Warnings, error) {
	pf := newObj.(*workercore.ModelPrefetch)

	// spec 有意设计为可变的：修改 placement 会重新指定分发目标，修改 retention 会改变保留策略的含义。每次更新都会重新执行全部校验规则，因为重新指定目标意味着对预算和 store 发起新的占用声明。
	if errs := r.validateModelPrefetch(ctx, pf); len(errs) > 0 {
		return nil, kerrors.NewInvalid(workercore.Kind("ModelPrefetch"), pf.Name, errs)
	}

	return nil, nil
}

func (r *ModelPrefetchWebhook) ValidateDelete(
	_ context.Context, _ runtime.Object,
) (ctrladmission.Warnings, error) {
	return nil, nil
}

// validateModelPrefetch 按排查问题的自然顺序拒绝不可行的配置：prefetch 引用的对象、指向的节点位置、授权允许的范围，以及预算可容纳的额度。
func (r *ModelPrefetchWebhook) validateModelPrefetch(ctx context.Context, pf *workercore.ModelPrefetch) field.ErrorList {
	errs := validateModelPrefetchPlacement(pf)
	if len(errs) > 0 {
		return errs
	}

	artifact := new(workercore.ModelArtifact)
	if err := r.APIReader.Get(ctx, ctrlcli.ObjectKey{Namespace: pf.Namespace, Name: pf.Spec.ArtifactRef.Name}, artifact); err != nil {
		if kerrors.IsNotFound(err) {
			return field.ErrorList{field.NotFound(field.NewPath("spec", "artifactRef", "name"),
				pf.Spec.ArtifactRef.Name)}
		}

		return field.ErrorList{field.InternalError(field.NewPath("spec", "artifactRef", "name"),
			fmt.Errorf("get model artifact %q: %w", pf.Spec.ArtifactRef.Name, err))}
	}
	if artifact.Spec.Source.Image != nil {
		return field.ErrorList{field.Forbidden(field.NewPath("spec", "artifactRef"),
			"an image artifact delivers on demand through kubelet and never enters the node cache: "+
				"there is nothing to warm, no budget to count against, and no pinning to grant")}
	}

	binding := new(workercore.ModelStoreBinding)
	if err := r.APIReader.Get(ctx, ctrlcli.ObjectKey{Namespace: pf.Namespace, Name: pf.Spec.BindingRef.Name}, binding); err != nil {
		if kerrors.IsNotFound(err) {
			return field.ErrorList{field.NotFound(field.NewPath("spec", "bindingRef", "name"),
				pf.Spec.BindingRef.Name)}
		}

		return field.ErrorList{field.InternalError(field.NewPath("spec", "bindingRef", "name"),
			fmt.Errorf("get model store binding %q: %w", pf.Spec.BindingRef.Name, err))}
	}

	if pf.Spec.Retention.Pinned && !modelStoreBindingAllowsPinned(binding) {
		return field.ErrorList{field.Invalid(field.NewPath("spec", "retention", "pinned"),
			pf.Spec.Retention.Pinned,
			"pinning is an admin-granted capability and this namespace's grant does not allow it; "+
				"ask for allowPinned on the grant, or warm without pinning")}
	}

	nodes := new(core.NodeList)
	if err := r.APIReader.List(ctx, nodes); err != nil {
		return field.ErrorList{field.InternalError(field.NewPath("spec", "placement"), err)}
	}
	deps := new(workercore.ModelDeploymentList)
	if err := r.APIReader.List(ctx, deps, ctrlcli.InNamespace(pf.Namespace)); err != nil {
		return field.ErrorList{field.InternalError(field.NewPath("spec", "placement"), err)}
	}
	pools, err := workerctrl.ResolveInstanceTypeFlavorNames(ctx, r.APIReader, pf, deps.Items)
	if err != nil {
		return field.ErrorList{field.InternalError(field.NewPath("spec", "placement"), err)}
	}
	targets, err := workerctrl.PrefetchTargetNodes(pf, nodes.Items, deps.Items, pools)
	if err != nil {
		return field.ErrorList{field.Invalid(field.NewPath("spec", "placement"), "", err.Error())}
	}

	if err := r.validateModelPrefetchGranted(ctx, binding, nodes.Items, targets); err != nil {
		return err
	}

	if int32(len(targets)) < pf.Spec.MinReady {
		return field.ErrorList{field.Invalid(field.NewPath("spec", "minReady"), pf.Spec.MinReady,
			fmt.Sprintf("asks for more ready nodes than the placement resolves to (%d); "+
				"a bar above the target set would hold the prefetch unavailable forever", len(targets)))}
	}

	return r.validateModelPrefetchBudget(ctx, pf, binding, artifact, len(targets))
}

// validateModelPrefetchGranted 拒绝没有任何已授权 store 覆盖的目标节点集合：授权指定了当前 Namespace 可预热的缓存池，若 prefetch 涉及位于所有指定 store 之外的节点，则相当于向未获授权的池进行预热，而节点自身策略由该池的归属者控制。
func (r *ModelPrefetchWebhook) validateModelPrefetchGranted(
	ctx context.Context, binding *workercore.ModelStoreBinding, nodes []core.Node, targets []string,
) field.ErrorList {
	var granted []workercore.ModelStore
	for _, ref := range binding.Spec.StoreRefs {
		store := new(workercore.ModelStore)
		if err := r.APIReader.Get(ctx, ctrlcli.ObjectKey{Name: ref.Name}, store); err != nil {
			if kerrors.IsNotFound(err) {
				continue // Binding 自身的 Webhook 会拒绝不存在的引用；此处仅将其视为不覆盖任何节点
			}

			return field.ErrorList{field.InternalError(field.NewPath("spec", "placement"),
				fmt.Errorf("get model store %q: %w", ref.Name, err))}
		}
		granted = append(granted, *store)
	}

	uncovered := map[string]bool{}
	for _, node := range targets {
		nd := nodeByName(nodes, node)
		if nd == nil {
			continue
		}
		covered := false
		for i := range granted {
			sel, err := meta.LabelSelectorAsSelector(&granted[i].Spec.NodeSelector)
			if err != nil {
				continue
			}
			if sel.Matches(labels.Set(nd.Labels)) {
				covered = true
				break
			}
		}
		if !covered {
			uncovered[node] = true
		}
	}
	if len(uncovered) == 0 {
		return nil
	}

	names := make([]string, 0, len(uncovered))
	for name := range uncovered {
		names = append(names, name)
	}
	sort.Strings(names)

	return field.ErrorList{field.Invalid(field.NewPath("spec"), names, fmt.Sprintf(
		"the target set leaves the grant: none of the stores %s names covers the node(s) %s, so "+
			"warming there was never granted. Widen storeRefs on %s, or point the placement at "+
			"nodes the grant covers", binding.Name, names, binding.Name,
	))}
}

// validateModelPrefetchBudget 将 Namespace 的预热占用估算映射到授权配额上，并拒绝超出部分。
//
// 该估算在准入阶段复现记账逻辑：针对每个不同的 digest，按 artifact 已解析的大小乘以同 Namespace 内任意 prefetch 请求的最大节点数计算。尚未解析的 artifact 暂不计入大小——其 prefetch 按剩余预算额度准入，估算无法感知的偏差由记账控制器上报。配额校验会比较具体数值，因此拒绝信息中会同时给出两个数值。
func (r *ModelPrefetchWebhook) validateModelPrefetchBudget(
	ctx context.Context, pf *workercore.ModelPrefetch, binding *workercore.ModelStoreBinding,
	artifact *workercore.ModelArtifact, targetCount int,
) field.ErrorList {
	pfs := new(workercore.ModelPrefetchList)
	if err := r.APIReader.List(ctx, pfs, ctrlcli.InNamespace(pf.Namespace)); err != nil {
		return field.ErrorList{field.InternalError(field.NewPath("spec"), err)}
	}

	// sizes：digest 到已解析大小的映射，来自各已解析的被引用 artifact。asked：
	// digest 到请求它的最大节点数的映射（包含当前 prefetch）。
	sizes := map[string]int64{}
	asked := map[string]int64{}
	for i := range pfs.Items {
		other := &pfs.Items[i]
		otherArtifact := artifact
		if other.Name != pf.Name {
			otherArtifact = new(workercore.ModelArtifact)
			if err := r.APIReader.Get(ctx, ctrlcli.ObjectKey{Namespace: other.Namespace, Name: other.Spec.ArtifactRef.Name}, otherArtifact); err != nil {
				if !kerrors.IsNotFound(err) {
					// 配额校验关卡上的瞬态读取错误不能视为放行：应触发准入重试，而非基于遗漏同级对象的估算结果直接放行。
					return field.ErrorList{field.InternalError(field.NewPath("spec"),
						fmt.Errorf("get model artifact %q for budget projection: %w", other.Spec.ArtifactRef.Name, err))}
				}
				continue
			}
		}
		resolved := otherArtifact.Status.Resolved
		if resolved == nil || resolved.ManifestDigest == "" {
			continue
		}
		sizes[resolved.ManifestDigest] = resolved.SizeBytes

		// 同级对象仅按其自身实际情况估算：尚未统计目标节点的 prefetch 在此处计为 0，而不会借用当前 prefetch 的节点数。
		count := int64(0)
		if other.Name == pf.Name {
			count = int64(targetCount)
		} else if other.Status.DesiredNodes > 0 {
			count = int64(other.Status.DesiredNodes)
		}
		if count > asked[resolved.ManifestDigest] {
			asked[resolved.ManifestDigest] = count
		}
	}

	// 正在准入的对象尚未出现在列表中：其自身占用声明在最后加入，按当前 placement 解析出的目标节点数计算。
	if resolved := artifact.Status.Resolved; resolved != nil && resolved.ManifestDigest != "" {
		if _, ok := sizes[resolved.ManifestDigest]; !ok {
			sizes[resolved.ManifestDigest] = resolved.SizeBytes
		}
		if int64(targetCount) > asked[resolved.ManifestDigest] {
			asked[resolved.ManifestDigest] = int64(targetCount)
		}
	}

	var projected int64
	for digest, count := range asked {
		projected += sizes[digest] * count
	}

	quota := binding.Spec.Quota.Bytes.Value()
	if projected <= quota {
		return nil
	}

	return field.ErrorList{field.Invalid(field.NewPath("spec"), fmt.Sprintf("%d bytes over %d node(s) per digest",
		projected, targetCount), fmt.Sprintf("the namespace's warm footprint would reach %d bytes, past "+
		"the grant's %s in %s: warm fewer nodes, or ask for a larger budget",
		projected, binding.Spec.Quota.Bytes.String(), binding.Name))}
}

// validateModelPrefetchPlacement 拒绝 schema 无法检查的错误：同时设置了两种 placement 类型。此处复用控制器的共享错误，以确保准入阶段与分发阶段对同一错误给出一致的提示。
func validateModelPrefetchPlacement(pf *workercore.ModelPrefetch) field.ErrorList {
	placement := pf.Spec.Placement
	if placement != nil && len(placement.InstanceTypes) > 0 && placement.NodeSelector != nil {
		return field.ErrorList{field.Invalid(field.NewPath("spec", "placement"), "",
			workerctrl.ErrPrefetchPlacementAmbiguous.Error())}
	}

	return nil
}

func nodeByName(nodes []core.Node, name string) *core.Node {
	for i := range nodes {
		if nodes[i].Name == name {
			return &nodes[i]
		}
	}

	return nil
}
