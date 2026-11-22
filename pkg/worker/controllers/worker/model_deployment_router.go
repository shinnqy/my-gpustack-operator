package worker

import (
	"context"
	"maps"
	"strings"

	app "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	rbac "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/systemmeta"
	"gpustack.ai/gpustack/pkg/worker/kvcache/router"
)

const (
	modelDeploymentRouterConfigKey            = "epp.yaml"
	modelDeploymentRouterEnvoyConfigKey       = "envoy.yaml"
	modelDeploymentRouterSelectorKey          = "endpoint-selector"
	modelDeploymentRouterTargetPortsKey       = "endpoint-target-ports"
	modelDeploymentRouterExtraArgsKey         = "extra-args.json"
	modelDeploymentRouterConfigHash           = "modeldeployment.gpustack.ai/router-config-hash"
	modelDeploymentRouterLabelKey             = "modeldeployment.gpustack.ai/router"
	modelDeploymentRouterHTTPPort       int32 = 8000
	modelDeploymentRouterEPPPort        int32 = 9002
	// modelDeploymentRouterMetricsPort 是所有路由器暴露 Prometheus 指标的端口，以便使用同一套
	// 抓取配置即可覆盖全部路由器。llm-d picker 默认监听此端口；通过 argv 配置的路由器则会
	// 显式指定监听该端口，因为它们自身的默认端口不同。
	modelDeploymentRouterMetricsPort  int32 = 9090
	modelDeploymentResourceNoteRouter       = "router"
)

// ModelDeploymentRouterObjects 包含单个托管路由器所需的完整命名空间级对象集合，以及
// 随之渲染出的类型化契约。
type ModelDeploymentRouterObjects struct {
	Deployment     *app.Deployment
	ConfigMap      *core.ConfigMap
	Service        *core.Service
	ServiceAccount *core.ServiceAccount
	Role           *rbac.Role
	RoleBinding    *rbac.RoleBinding

	// Contract 是上述对象渲染所用相同参数的类型化映射。
	Contract workercore.ModelDeploymentRouterStatus
}

func renderModelDeploymentRouterObjects(
	ctx context.Context, md *workercore.ModelDeployment, manufacturers map[string]string,
	retainedEligibility bool,
) (ModelDeploymentRouterObjects, error) {
	// 在第 20 章中实现此函数。
	var result0 ModelDeploymentRouterObjects
	var result1 error
	return result0, result1
}

func (r *ModelDeploymentReconciler) syncModelDeploymentRouter(
	ctx context.Context, md *workercore.ModelDeployment, eligibilityDecided bool,
) error {
	var rendered ModelDeploymentRouterObjects
	if md.Spec.Router != nil {
		// 保留机制：服务发现等式条件一旦发布，在所有内置 profile 的静默轮次中都会继续保留。
		// llm-d profile 会发布 ConfigMap；而 vllm-router 和 sglang-gateway profile 通过 argv 配置，
		// 不渲染 ConfigMap，因此缺少 ConfigMap 并不能证明路由面从未被收窄——还需要同时读取
		// 线上路由器 Deployment 的 args 和 env。静态可激活条件部分仍然起决定作用：若部署形态
		// 不再具备可激活条件，本轮会移除该过滤项。
		//
		// 除了 selector 键外，还会从配置文档中读取该条件，因为同一要求可能存放在这两处之一。
		// 当部署中各角色的可激活状态不一致时，该要求位于阶段过滤器而非 selector 中；若某轮
		// 只读取 selector，就会查无所获，误判门控从未激活过，进而恢复因故障而撤回的端点。
		// 对文档采用解析而非字符串搜索：用户指定的模型名称恰好可能与该标签字符串相同，若直接
		// 搜索字符串，可能会误将未启用门控的部署当成已激活处理。
		needle := modelDeploymentLabelKeyEndpointEligible + "=" + modelDeploymentEndpointEligibleValue
		// 该标记是这一要求所使用的另一种名称，当部署中各角色状态不一致时，携带的是该标记而非
		// 资格条件。静默轮次必须在任何发布位置都能识别出该要求，否则会误判门控从未触发，
		// 进而恢复因故障而撤回的端点。
		candidateNeedle := modelDeploymentRouterCandidateLabel + "=" + modelDeploymentEndpointEligibleValue
		retained := false
		liveConfig := new(core.ConfigMap)
		err := r.Client.Get(ctx, ctrlcli.ObjectKey{Namespace: md.Namespace, Name: md.Name + "-router"}, liveConfig)
		switch {
		case err == nil:
			retained = strings.Contains(liveConfig.Data[modelDeploymentRouterSelectorKey], needle) ||
				strings.Contains(liveConfig.Data[modelDeploymentRouterSelectorKey], candidateNeedle) ||
				router.DocumentStageFilterDemands(
					liveConfig.Data[modelDeploymentRouterConfigKey],
					modelDeploymentLabelKeyEndpointEligible, modelDeploymentEndpointEligibleValue,
				) ||
				router.DocumentStageFilterDemands(
					liveConfig.Data[modelDeploymentRouterConfigKey],
					modelDeploymentRouterCandidateLabel, modelDeploymentEndpointEligibleValue,
				)
		case apierrors.IsNotFound(err):
		default:
			return err
		}
		if !retained {
			liveDeployment := new(app.Deployment)
			err = r.Client.Get(ctx, ctrlcli.ObjectKey{Namespace: md.Namespace, Name: md.Name + "-router"}, liveDeployment)
			switch {
			case err == nil:
				for _, container := range liveDeployment.Spec.Template.Spec.Containers {
					for _, arg := range container.Args {
						if strings.Contains(arg, needle) || strings.Contains(arg, candidateNeedle) {
							retained = true
						}
					}
					for _, env := range container.Env {
						if strings.Contains(env.Value, needle) || strings.Contains(env.Value, candidateNeedle) {
							retained = true
						}
					}
				}
			case apierrors.IsNotFound(err):
			default:
				return err
			}
		}

		rendered, err = renderModelDeploymentRouterObjects(
			ctx, md, r.modelDeploymentRoleManufacturers(ctx, md), retained || eligibilityDecided,
		)
		if err != nil {
			return err
		}
	}

	wanted := md.Spec.Router != nil
	if err := r.syncModelDeploymentOwnedChildren(ctx, md, objectIfPresent(rendered.Deployment, wanted),
		func() ctrlcli.Object { return new(app.Deployment) }, new(app.DeploymentList),
		func(actual, expected ctrlcli.Object) bool {
			return alignModelDeploymentRouterDeployment(actual.(*app.Deployment), expected.(*app.Deployment))
		}, modelDeploymentResourceNoteRouter, "deployment"); err != nil {
		return err
	}
	// 完全通过 argv 配置的路由器不渲染 ConfigMap，因此其期望列表为空，清理路径会移除此前
	// 形态遗留的 ConfigMap。此处检查的是带类型的 nil 指针而非接口本身，因为持有类型化 nil 的
	// 接口值本身不为 nil。
	if err := r.syncModelDeploymentOwnedChildren(ctx, md,
		objectIfPresent(rendered.ConfigMap, wanted && rendered.ConfigMap != nil),
		func() ctrlcli.Object { return new(core.ConfigMap) }, new(core.ConfigMapList),
		func(actual, expected ctrlcli.Object) bool {
			return alignModelDeploymentRouterConfigMap(actual.(*core.ConfigMap), expected.(*core.ConfigMap))
		}, modelDeploymentResourceNoteRouter, "configmap"); err != nil {
		return err
	}
	if err := r.syncModelDeploymentOwnedChildren(ctx, md, objectIfPresent(rendered.Service, wanted),
		func() ctrlcli.Object { return new(core.Service) }, new(core.ServiceList),
		func(actual, expected ctrlcli.Object) bool {
			changed := alignModelDeploymentService(actual.(*core.Service), expected.(*core.Service))
			return alignModelDeploymentChildMetadata(actual, expected) || changed
		}, modelDeploymentResourceNoteRouter, "service"); err != nil {
		return err
	}
	if err := r.syncModelDeploymentOwnedChildren(ctx, md, objectIfPresent(rendered.ServiceAccount, wanted),
		func() ctrlcli.Object { return new(core.ServiceAccount) }, new(core.ServiceAccountList),
		alignModelDeploymentChildMetadata, modelDeploymentResourceNoteRouter, "serviceaccount"); err != nil {
		return err
	}
	if err := r.syncModelDeploymentOwnedChildren(ctx, md, objectIfPresent(rendered.Role, wanted),
		func() ctrlcli.Object { return new(rbac.Role) }, new(rbac.RoleList),
		func(actual, expected ctrlcli.Object) bool {
			role, want := actual.(*rbac.Role), expected.(*rbac.Role)
			changed := false
			if !kubemeta.DeepEqual(role.Rules, want.Rules) {
				role.Rules = want.Rules
				changed = true
			}
			return alignModelDeploymentChildMetadata(actual, expected) || changed
		}, modelDeploymentResourceNoteRouter, "role"); err != nil {
		return err
	}
	return r.syncModelDeploymentOwnedChildren(ctx, md, objectIfPresent(rendered.RoleBinding, wanted),
		func() ctrlcli.Object { return new(rbac.RoleBinding) }, new(rbac.RoleBindingList),
		func(actual, expected ctrlcli.Object) bool {
			binding, want := actual.(*rbac.RoleBinding), expected.(*rbac.RoleBinding)
			changed := false
			if !kubemeta.DeepEqual(binding.Subjects, want.Subjects) {
				binding.Subjects = want.Subjects
				changed = true
			}
			return alignModelDeploymentChildMetadata(actual, expected) || changed
		}, modelDeploymentResourceNoteRouter, "rolebinding")
}

func objectIfPresent(object ctrlcli.Object, present bool) []ctrlcli.Object {
	if !present {
		return nil
	}
	return []ctrlcli.Object{object}
}

// alignModelDeploymentRouterDeployment 使线上路由器 Deployment 与渲染结果保持一致。
//
// 该函数刻意不比较 selector，因为 selector 在 apps/v1 中是不可变的，一旦存在差异就会
// 导致后续所有 Update 失败。渲染出的 selector 嵌入了 `spec.router.name`，因此改变它的
// 唯一途径就是修改该字段——而准入控制会在创建后冻结该字段。不同部署之间该值可以不同，
// 但同一部署内部绝不会改变，因此忽略 selector 是安全可靠的，而非未经验证的妥协。
//
// 如果未来解除了该字段的冻结限制，问题将暴露在此函数中，且比较 selector 并非正确的修复
// 方式：届时必须删除并重建该对象，因为 API server 不会接受在已有对象上修改 selector。
func alignModelDeploymentRouterDeployment(actual, expected *app.Deployment) (changed bool) {
	if !kubemeta.DeepEqual(actual.Spec.Replicas, expected.Spec.Replicas) {
		actual.Spec.Replicas = expected.Spec.Replicas
		changed = true
	}
	actualPod, expectedPod := &actual.Spec.Template, &expected.Spec.Template
	if !kubemeta.DeepEqual(actualPod.Labels, expectedPod.Labels) {
		actualPod.Labels = expectedPod.Labels
		changed = true
	}
	if !kubemeta.DeepEqual(actualPod.Annotations, expectedPod.Annotations) {
		actualPod.Annotations = expectedPod.Annotations
		changed = true
	}
	if actualPod.Spec.ServiceAccountName != expectedPod.Spec.ServiceAccountName {
		actualPod.Spec.ServiceAccountName = expectedPod.Spec.ServiceAccountName
		changed = true
	}
	// 尽管下方的拉取策略位于容器定义内部，此处仍会比较该字段：在此添加或移除 Secret 会改变
	// 镜像拉取时的认证凭据，而一个渲染了却从不进行对齐的字段，在修改后只能通过手动重建
	// Deployment 才能生效。
	if !kubemeta.DeepEqual(actualPod.Spec.ImagePullSecrets, expectedPod.Spec.ImagePullSecrets) {
		actualPod.Spec.ImagePullSecrets = expectedPod.Spec.ImagePullSecrets
		changed = true
	}
	if !kubemeta.DeepEqual(actualPod.Spec.Volumes, expectedPod.Spec.Volumes) {
		actualPod.Spec.Volumes = expectedPod.Spec.Volumes
		changed = true
	}
	if len(actualPod.Spec.Containers) != len(expectedPod.Spec.Containers) {
		actualPod.Spec.Containers = expectedPod.Spec.Containers
		changed = true
	} else {
		for i := range expectedPod.Spec.Containers {
			if alignRenderedContainer(&actualPod.Spec.Containers[i], expectedPod.Spec.Containers[i]) {
				changed = true
			}
		}
	}

	return alignModelDeploymentChildMetadata(actual, expected) || changed
}

func alignModelDeploymentRouterConfigMap(actual, expected *core.ConfigMap) (changed bool) {
	if !maps.Equal(actual.Data, expected.Data) {
		actual.Data = expected.Data
		changed = true
	}
	return alignModelDeploymentChildMetadata(actual, expected) || changed
}

// alignModelDeploymentChildMetadata 将子对象的标签与资源备注收敛至渲染后的值。其中
// 备注部分并非表面装饰，而是关键依赖：它仅在 Create 时写入，而清理门控会跳过
// 备注为空的任何子对象——因此一旦备注被剥离，该对象将永久免于清理，这一机制
// 正是为了保证该门控的可靠性。
func alignModelDeploymentChildMetadata(actual, expected ctrlcli.Object) (changed bool) {
	if !maps.Equal(actual.GetLabels(), expected.GetLabels()) {
		actual.SetLabels(maps.Clone(expected.GetLabels()))
		changed = true
	}
	if !systemmeta.EqualResourceTypeAndNotes(expected, actual) {
		systemmeta.SyncResourceTypeAndNotes(expected, actual)
		changed = true
	}
	return changed
}
