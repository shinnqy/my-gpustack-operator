package worker

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	core "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/workqueue"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlevent "sigs.k8s.io/controller-runtime/pkg/event"
	ctrlhandler "sigs.k8s.io/controller-runtime/pkg/handler"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	ctrlpredicate "sigs.k8s.io/controller-runtime/pkg/predicate"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/modelartifact"
	"gpustack.ai/gpustack/pkg/modelstore"
	"gpustack.ai/gpustack/pkg/setting"
	"gpustack.ai/gpustack/pkg/utils/stringx"
	"gpustack.ai/gpustack/pkg/worker/settings"
)

// 当 deployment 的权重不可用时 WeightsReady 所携带的 reason。其中每一项都会阻止创建新的 Pod，
// 但均不会影响已在运行的 Pod。
const (
	modelWeightsReasonNotApplicable    = "NotApplicable"
	modelWeightsReasonArtifactNotFound = "ArtifactNotFound"
	modelWeightsReasonArtifactUnready  = "ArtifactNotResolved"
	modelWeightsReasonClaimNotBound    = "ClaimNotBound"
	modelWeightsReasonAccessModeClash  = "AccessModeConflict"
	modelWeightsReasonNotMounted       = "WeightsNotMounted"
	modelWeightsReasonDownloading      = "Downloading"
	modelWeightsReasonMounted          = "Mounted"
	modelWeightsReasonDownloaded       = "Downloaded"
	modelWeightsReasonNodeUnavailable  = "NodeDeliveryUnavailable"
	modelWeightsReasonFilterNeedsNode  = "FilterNeedsNodeDelivery"
	modelWeightsReasonAnchorNeedsNode  = "AnchorNeedsNodeDelivery"
	modelWeightsReasonMaterializing    = "Materializing"
	modelWeightsReasonMaterializeFail  = "MaterializationFailed"

	// modelArtifactNoProvisioner 是静态本地卷 StorageClass 所指定的 provisioner 名称。
	modelArtifactNoProvisioner = "kubernetes.io/no-provisioner"
)

// modelArtifactWeights 表示单次调谐中消费端权重的解析结果。
type modelArtifactWeights struct {
	// Render 描述权重如何交付给推理引擎。在 artifact 从未完成解析前为 nil，此时消费端的 Pod
	// 无法进行任何渲染。
	Render *ModelDeploymentArtifactRender
	// Affinity 是已绑定 PV 的强制节点亲和性，会在创建每个 Pod 时注入。
	Affinity *core.NodeSelector
	// Status 是回写到消费端 status 中的权重状态摘要，在 artifact 解析完成前为 nil。
	Status *workercore.ModelDeploymentModelStatus
	// KVIdentity 是 KV 存储键所使用的权重身份前缀；参见 modelArtifactKVIdentity。
	KVIdentity string
	// Blocked 表示因 Reason 和 Message 所述原因不得创建新的 Pod。
	Blocked bool
	Reason  string
	Message string
}

func blockedModelArtifactWeights(reason, message string) *modelArtifactWeights {
	return &modelArtifactWeights{Blocked: true, Reason: reason, Message: message}
}

// modelArtifactDeliveryMode 是 ModelDeployment 调谐时读取的交付模式 Setting。将其定义为变量，
// 以便测试无需 Settings 存储即可指定交付方式。
//
// 此处直接从 API server 而非 informer 缓存读取：该值是因交付策略而被阻塞的 deployment 解除阻塞
// 的唯一出口，若 informer 停滞不前，这一出口将被冻结——每次定期复查都会读到同一个过时的值。
var modelArtifactDeliveryMode = func(ctx context.Context) string {
	return settings.ModelArtifactDeliveryMode.ShouldValueFromRemote(ctx)
}

// resolveModelArtifactWeights 读取消费端引用的 artifact，并在来源为 claim 时确定该 claim 允许
// Pod 运行的位置。pods 表示同时挂载该权重的 Pod 数量，非共享访问模式的 claim 无法同时服务超过
// 一个 Pod。nodeOnly 表示消费端没有可自行下载 hub artifact 的推理引擎，因此无论交付 Setting 如何
// 配置，都只能使用节点交付模式。
//
// 此处没有任何拒绝操作。所有阻止创建新 Pod 的状态都会自行变化或随着用户创建相应资源而改变，
// 因此消费端只需等待并说明原因，待状态解除后的下一次调谐即可创建 Pod。
func resolveModelArtifactWeights(
	ctx context.Context, cli ctrlcli.Reader, namespace, name string, pods int, nodeOnly bool,
) (*modelArtifactWeights, error) {
	ma := new(workercore.ModelArtifact)
	if err := cli.Get(ctx, ctrlcli.ObjectKey{Namespace: namespace, Name: name}, ma); err != nil {
		if kerrors.IsNotFound(err) {
			return blockedModelArtifactWeights(modelWeightsReasonArtifactNotFound,
				fmt.Sprintf("ModelArtifact %q does not exist in this namespace; nothing is created until it does", name)), nil
		}
		return nil, fmt.Errorf("read model artifact %q: %w", name, err)
	}

	resolved := ma.Status.Resolved
	if resolved == nil {
		return blockedModelArtifactWeights(modelWeightsReasonArtifactUnready, fmt.Sprintf(
			"ModelArtifact %q has not resolved: %s", name, ModelArtifactConditionResolved.GetMessage(ma),
		)), nil
	}

	w := &modelArtifactWeights{}
	switch source := ma.Spec.Source; {
	case source.PersistentVolumeClaim != nil:
		w.Render = &ModelDeploymentArtifactRender{
			Delivery:  workercore.ModelDeploymentModelDeliveryPVC,
			ClaimName: source.PersistentVolumeClaim.ClaimName,
			Path:      source.PersistentVolumeClaim.Path,
		}
	case source.Image != nil:
		// 镜像由 kubelet 完整拉取交付，无论 Pod 调度到哪个节点：无需节点亲和性、无需插件、无需文件
		// 过滤，也无需计算容量大小。不可变 spec 中的引用即为唯一的内容标识，因此已解析状态采用与
		// claim 相同的形态，且 KV 身份回退为 artifact 的 UID。
		w.Render = &ModelDeploymentArtifactRender{
			Delivery:       workercore.ModelDeploymentModelDeliveryImage,
			ImageReference: source.Image.Reference,
		}
	case source.HuggingFace != nil, source.ModelScope != nil:
		// 两个 hub 仅在解析所用的端点以及引擎下载读取的环境变量上有所不同；交付方式、调度放置以及
		// 所有基于摘要的行为均为共用。
		hub, kind := source.HuggingFace, modelDeploymentHubHuggingFace
		endpoint := settings.ModelArtifactHuggingFaceEndpoint.ShouldValue(ctx)
		if hub == nil {
			hub, kind = source.ModelScope, modelDeploymentHubModelScope
			endpoint = settings.ModelArtifactModelScopeEndpoint.ShouldValue(ctx)
		}
		if nodeOnly || modelArtifactDeliveryMode(ctx) == settings.ModelArtifactDeliveryNode {
			w.Render = &ModelDeploymentArtifactRender{
				Delivery:       workercore.ModelDeploymentModelDeliveryNode,
				Hub:            kind,
				ArtifactName:   name,
				ArtifactUID:    string(ma.UID),
				ManifestDigest: resolved.ManifestDigest,
				Repository:     hub.Repository,
				Revision:       resolved.Revision,
				SizeBytes:      resolved.SizeBytes,
			}
		} else {
			w.Render = &ModelDeploymentArtifactRender{
				Delivery:   workercore.ModelDeploymentModelDeliveryEngine,
				Hub:        kind,
				Repository: hub.Repository,
				Revision:   resolved.Revision,
				SizeBytes:  resolved.SizeBytes,
				Endpoint:   endpoint,
				HTTPSProxy: settings.ModelArtifactHTTPSProxy.ShouldValue(ctx),
				NoProxy:    settings.ModelArtifactNoProxy.ShouldValue(ctx),
			}
		}
		if hub.SecretRef != nil {
			w.Render.SecretName = hub.SecretRef.Name
		}
	default:
		return blockedModelArtifactWeights(modelWeightsReasonArtifactUnready,
			fmt.Sprintf("ModelArtifact %q has a source this version does not deliver", name)), nil
	}
	w.KVIdentity = modelArtifactKVIdentity(ma)
	w.Status = &workercore.ModelDeploymentModelStatus{
		Artifact:       name,
		Revision:       resolved.Revision,
		ManifestDigest: resolved.ManifestDigest,
		Delivery:       w.Render.Delivery,
	}

	if !ModelArtifactConditionResolved.IsTrue(ma) {
		w.Blocked, w.Reason = true, modelWeightsReasonArtifactUnready
		w.Message = fmt.Sprintf("ModelArtifact %q is not resolved (%s): %s; running replicas are left alone",
			name, ModelArtifactConditionResolved.GetReason(ma), ModelArtifactConditionResolved.GetMessage(ma))
		return w, nil
	}

	switch w.Render.Delivery {
	case workercore.ModelDeploymentModelDeliveryPVC:
		if err := placeModelArtifactClaim(ctx, cli, namespace, w.Render.ClaimName, pods, w); err != nil {
			return nil, err
		}
	case workercore.ModelDeploymentModelDeliveryNode:
		// 节点交付模式依赖该 CSI 插件。若配置值来自环境变量预设或插件在之后被移除，准入阶段无法得知
		// 插件是否已安装这一运行时事实，因此消费端在此等待。
		switch err := cli.Get(ctx, ctrlcli.ObjectKey{Name: modelstore.DriverName}, new(storage.CSIDriver)); {
		case kerrors.IsNotFound(err):
			w.Blocked, w.Reason = true, modelWeightsReasonNodeUnavailable
			w.Message = fmt.Sprintf("ModelArtifact %q is delivered by the node, and the CSIDriver %s does not exist: "+
				"install the model-manager plugin (chart value modelManager.enabled), or set "+
				"model-artifact-delivery-mode to Engine for a ModelDeployment", name, modelstore.DriverName)
		case err != nil:
			return nil, fmt.Errorf("read the CSIDriver %s: %w", modelstore.DriverName, err)
		}
	case workercore.ModelDeploymentModelDeliveryEngine:
		// 推理引擎会自行决定下载哪些文件，且缓存容量也是按过滤后的总大小计算的，因此文件过滤条件只能由
		// 节点交付模式来履行。
		if len(ma.Spec.AllowPatterns) > 0 || len(ma.Spec.IgnorePatterns) > 0 {
			w.Blocked, w.Reason = true, modelWeightsReasonFilterNeedsNode
			w.Message = fmt.Sprintf("ModelArtifact %q selects files with allow or ignore patterns, which only node "+
				"delivery honors: set model-artifact-delivery-mode to Node", name)
			break
		}
		// 锚点是一项推理引擎无法保证的断言：引擎按仓库和版本修订号下载，无法针对摘要校验拉取的内容，
		// 且基于 ExpectedDigest 的身份根本没有可用于固定版本的 commit。
		if ma.Spec.ExpectedDigest != "" {
			w.Blocked, w.Reason = true, modelWeightsReasonAnchorNeedsNode
			message := fmt.Sprintf("ModelArtifact %q carries expectedDigest, and an engine downloads by "+
				"repository and revision, so it cannot anchor-verify what it fetched: set "+
				"model-artifact-delivery-mode to Node", name)
			if resolved.DigestSource == workercore.ModelArtifactDigestSourceExpected {
				message += "; the artifact resolved to its anchor without the hub and has no resolved commit"
			}
			w.Message = message
		}
	}

	return w, nil
}

// placeModelArtifactClaim 决定 claim 允许其 Pod 在何处运行，以及是否允许其运行。
//
// KUEUE 的拓扑感知调度（TAS）不会读取 Pod 的卷配置。实测表明，若不做此处理，绑定到带有节点
// 亲和性 PV 的 claim 会静默失败：TAS 会将 Pod 分配到其他节点，导致 Pod 永久处于 Pending，
// 其 Workload 则维持 Admitted 状态并持续占用配额，且没有任何信息说明原因。将 PV 的强制节点
// 亲和性添加到 Pod 后，TAS 便会遵守该约束；当该节点资源已满时，Workload 会在分配配额前
// 等待，并在出现空余资源后自行准入。
func placeModelArtifactClaim(
	ctx context.Context, cli ctrlcli.Reader, namespace, claimName string, pods int, w *modelArtifactWeights,
) error {
	pvc := new(core.PersistentVolumeClaim)
	if err := cli.Get(ctx, ctrlcli.ObjectKey{Namespace: namespace, Name: claimName}, pvc); err != nil {
		if kerrors.IsNotFound(err) {
			w.Blocked, w.Reason = true, modelWeightsReasonArtifactUnready
			w.Message = fmt.Sprintf("PersistentVolumeClaim %q does not exist in this namespace", claimName)
			return nil
		}
		return fmt.Errorf("read persistent volume claim %q: %w", claimName, err)
	}

	modes := pvc.Status.AccessModes
	if len(modes) == 0 {
		modes = pvc.Spec.AccessModes
	}
	if pods > 1 && !slices.Contains(modes, core.ReadOnlyMany) &&
		!slices.Contains(modes, core.ReadWriteMany) {
		w.Blocked, w.Reason = true, modelWeightsReasonAccessModeClash
		w.Message = fmt.Sprintf(
			"%d Pods would mount PersistentVolumeClaim %q, whose access modes %v allow neither ReadOnlyMany "+
				"nor ReadWriteMany: ReadWriteOnce would crowd every replica onto one node and "+
				"ReadWriteOncePod admits one; use a claim that can be shared", pods, claimName, modes,
		)
		return nil
	}

	switch pvc.Status.Phase {
	case core.ClaimBound:
		pv := new(core.PersistentVolume)
		if err := cli.Get(ctx, ctrlcli.ObjectKey{Name: pvc.Spec.VolumeName}, pv); err != nil {
			return fmt.Errorf("read persistent volume %q: %w", pvc.Spec.VolumeName, err)
		}
		if pv.Spec.NodeAffinity != nil && pv.Spec.NodeAffinity.Required != nil {
			w.Affinity = pv.Spec.NodeAffinity.Required.DeepCopy()
		}
		return nil
	case core.ClaimPending:
		dynamic, err := modelArtifactClaimBindsOnFirstPod(ctx, cli, pvc)
		if err != nil {
			return err
		}
		if dynamic {
			// provisioner 会在首个 Pod 被调度到的节点上创建卷，因此是由调度结果决定卷绑定，而非相反。
			return nil
		}
	}

	w.Blocked, w.Reason = true, modelWeightsReasonClaimNotBound
	w.Message = fmt.Sprintf(
		"PersistentVolumeClaim %q is %s and nothing binds it to the node a Pod is assigned: bind it to "+
			"its PersistentVolume first (spec.volumeName), so the Pods can be placed where that volume is",
		claimName, pvc.Status.Phase,
	)

	return nil
}

// modelArtifactClaimBindsOnFirstPod 报告处于 Pending 状态的 claim 是否会在首个消费端 Pod 被调度后
// 在对应节点上动态创建：即使用 WaitForFirstConsumer 且配有可创建卷的 provisioner。
//
// 没有 provisioner 的 StorageClass 通常是静态本地卷类，这里按未绑定处理：TAS 会先选定节点，
// 随后卷绑定器仅在该节点上查找匹配的 PV，因此若该节点上不存在可用的 PV，就会再次出现占用
// 配额却静默卡在 Pending 的问题。这一判断是从绑定顺序推断得出的，尚未经过实测验证。
func modelArtifactClaimBindsOnFirstPod(ctx context.Context, cli ctrlcli.Reader, pvc *core.PersistentVolumeClaim) (bool, error) {
	if pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName == "" {
		return false, nil
	}
	sc := new(storage.StorageClass)
	if err := cli.Get(ctx, ctrlcli.ObjectKey{Name: *pvc.Spec.StorageClassName}, sc); err != nil {
		if kerrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("read storage class %q: %w", *pvc.Spec.StorageClassName, err)
	}

	return sc.VolumeBindingMode != nil && *sc.VolumeBindingMode == storage.VolumeBindingWaitForFirstConsumer &&
		sc.Provisioner != modelArtifactNoProvisioner, nil
}

// injectModelArtifactAffinity 将 PV 的强制节点亲和性与 Pod 自身的节点亲和性以逻辑与（AND）合并：
// 对 Pod 的条件项和 PV 的条件项做笛卡尔积，使节点必须同时满足双方各自至少一个条件项才算匹配。
func injectModelArtifactAffinity(pod *core.Pod, required *core.NodeSelector) {
	if required == nil || len(required.NodeSelectorTerms) == 0 {
		return
	}
	if pod.Spec.Affinity == nil {
		pod.Spec.Affinity = &core.Affinity{}
	}
	if pod.Spec.Affinity.NodeAffinity == nil {
		pod.Spec.Affinity.NodeAffinity = &core.NodeAffinity{}
	}
	na := pod.Spec.Affinity.NodeAffinity
	existing := na.RequiredDuringSchedulingIgnoredDuringExecution
	if existing == nil || len(existing.NodeSelectorTerms) == 0 {
		na.RequiredDuringSchedulingIgnoredDuringExecution = required.DeepCopy()
		return
	}

	crossed := make([]core.NodeSelectorTerm, 0, len(existing.NodeSelectorTerms)*len(required.NodeSelectorTerms))
	for _, a := range existing.NodeSelectorTerms {
		for _, b := range required.NodeSelectorTerms {
			crossed = append(crossed, core.NodeSelectorTerm{
				MatchExpressions: slices.Concat(a.MatchExpressions, b.MatchExpressions),
				MatchFields:      slices.Concat(a.MatchFields, b.MatchFields),
			})
		}
	}
	na.RequiredDuringSchedulingIgnoredDuringExecution = &core.NodeSelector{NodeSelectorTerms: crossed}
}

// resolveModelDeploymentWeights 解析 spec.model.artifactRef 所指定的权重；若 deployment 未指定
// artifactRef，则返回 nil。
func (r *ModelDeploymentReconciler) resolveModelDeploymentWeights(
	ctx context.Context, md *workercore.ModelDeployment,
) (*modelArtifactWeights, error) {
	name := ModelDeploymentArtifactName(md)
	if name == "" {
		return nil, nil
	}

	// 每个角色的每个 Pod 都会挂载一个 claim，包含接管层级（take-over tier）。
	pods := 0
	for i := range md.Spec.Roles {
		pods += int(md.Spec.Roles[i].Replicas) * modelDeploymentRoleSize(&md.Spec.Roles[i])
	}

	return resolveModelArtifactWeights(ctx, r.Client, md.Namespace, name, pods, false)
}

// observeModelDeploymentWeights 写入 status.model 和 WeightsReady condition。nodeModels 按节点名记录
// 每个副本 Pod 所在节点针对该摘要上报的节点分发状态。
//
// 该 condition 仅根据 Pod 的 condition 与节点状态推导，绝不依赖 Event。对于 claim 或节点分发的制品，
// 一旦 kubelet 挂载完 Pod 的卷（由 PodReadyToStartContainers 上报），即表示权重已就绪；在此之前，
// 由节点上的 NodeModelStore 说明正在准备权重还是准备失败。对于引擎自行下载，只有当引擎开始提供服务时
// 才视为下载完成，因为引擎自身不会上报下载进度。
func observeModelDeploymentWeights(
	holder *workercore.ModelDeployment, pods []core.Pod, weights *modelArtifactWeights,
	nodeModels map[string]*workercore.NodeModelStoreModel,
) {
	if weights == nil {
		holder.Status.Model = nil
		ModelDeploymentConditionWeightsReady.True(holder, modelWeightsReasonNotApplicable,
			"the deployment names no model artifact")
		return
	}

	holder.Status.Model = weights.Status
	if weights.Blocked {
		ModelDeploymentConditionWeightsReady.False(holder, weights.Reason, weights.Message)
		return
	}

	engine := weights.Render.Delivery == workercore.ModelDeploymentModelDeliveryEngine
	node := weights.Render.Delivery == workercore.ModelDeploymentModelDeliveryNode
	waiting := 0
	var materializing int
	var failed *workercore.NodeModelStoreModel
	for i := range pods {
		pod := &pods[i]
		if pod.DeletionTimestamp != nil || pod.Status.Phase == core.PodSucceeded || pod.Status.Phase == core.PodFailed {
			continue
		}
		if engine && !modelDeploymentMainContainerReady(pod) || !engine && !modelDeploymentPodVolumesMounted(pod) {
			waiting++
			if m := nodeModels[pod.Spec.NodeName]; node && m != nil {
				switch m.State {
				case workercore.NodeModelStoreModelStateDownloading:
					materializing++
				case workercore.NodeModelStoreModelStateFailed:
					failed = m
				}
			}
		}
	}

	switch {
	case waiting > 0 && failed != nil:
		retry := ""
		if failed.RetryTime != nil {
			retry = "; the node tries again at " + failed.RetryTime.UTC().Format(time.RFC3339)
		}
		ModelDeploymentConditionWeightsReady.False(holder, modelWeightsReasonMaterializeFail, fmt.Sprintf(
			"a replica Pod's node failed to materialize %s, %s: %s%s", failed.Digest, failed.Reason, failed.Message, retry,
		))
	case waiting > 0 && materializing > 0:
		ModelDeploymentConditionWeightsReady.False(holder, modelWeightsReasonMaterializing, fmt.Sprintf(
			"%d replica Pods wait for their nodes to materialize %s; a Pod starts at kubelet's next mount "+
				"retry after that, up to about two minutes later", waiting, weights.Render.ManifestDigest,
		))
	case waiting > 0 && engine:
		ModelDeploymentConditionWeightsReady.False(holder, modelWeightsReasonDownloading, fmt.Sprintf(
			"%d replica Pods' engines are not ready yet; each downloads commit %s itself and reports no progress",
			waiting, weights.Render.Revision,
		))
	case waiting > 0:
		ModelDeploymentConditionWeightsReady.False(holder, modelWeightsReasonNotMounted, fmt.Sprintf(
			"%d replica Pods do not have their volumes mounted yet (PodReadyToStartContainers is not True)", waiting,
		))
	case engine:
		ModelDeploymentConditionWeightsReady.True(holder, modelWeightsReasonDownloaded,
			fmt.Sprintf("every replica Pod serves commit %s", weights.Render.Revision))
	case node:
		ModelDeploymentConditionWeightsReady.True(holder, modelWeightsReasonMounted,
			fmt.Sprintf("every replica Pod has %s mounted from its node", weights.Render.ManifestDigest))
	default:
		ModelDeploymentConditionWeightsReady.True(holder, modelWeightsReasonMounted,
			fmt.Sprintf("every replica Pod has PersistentVolumeClaim %q mounted", weights.Render.ClaimName))
	}
}

// modelArtifactNodeModels 按节点名读取每个 Pod 所在节点关于指定摘要的上报信息。没有 NodeModelStore
// 或未列出该摘要的节点不会出现在结果中。
func modelArtifactNodeModels(
	ctx context.Context, cli ctrlcli.Reader, pods []core.Pod, digest string,
) (map[string]*workercore.NodeModelStoreModel, error) {
	models := map[string]*workercore.NodeModelStoreModel{}
	for i := range pods {
		name := pods[i].Spec.NodeName
		if name == "" {
			continue
		}
		if _, seen := models[name]; seen {
			continue
		}
		nms := new(workercore.NodeModelStore)
		if err := cli.Get(ctx, ctrlcli.ObjectKey{Name: name}, nms); err != nil {
			if kerrors.IsNotFound(err) {
				models[name] = nil
				continue
			}
			return nil, fmt.Errorf("read node model store %q: %w", name, err)
		}
		models[name] = nil
		for j := range nms.Status.Models {
			if nms.Status.Models[j].Digest == digest {
				models[name] = &nms.Status.Models[j]
			}
		}
	}

	return models, nil
}

func modelDeploymentPodVolumesMounted(pod *core.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == core.PodReadyToStartContainers {
			return c.Status == core.ConditionTrue
		}
	}

	return false
}

// modelDeploymentMainContainerReady 检查引擎主容器而非整个 Pod 的就绪状态：Pod 的就绪状态还会等待
// 路由 sidecar，而该 sidecar 与权重下载进度无关。
func modelDeploymentMainContainerReady(pod *core.Pod) bool {
	for _, c := range pod.Status.ContainerStatuses {
		if c.Name == modelDeploymentMainContainerName {
			return c.Ready
		}
	}

	return false
}

// modelDeploymentEvictionNote 返回首个被驱逐的副本 Pod 中 kubelet 记录的原始消息，若无则返回 ""。
// 驱逐原因仅保留在 Pod 对象上，容器日志会随容器一并丢失。
func modelDeploymentEvictionNote(pods []core.Pod) string {
	for i := range pods {
		if pods[i].Status.Reason == "Evicted" && pods[i].Status.Message != "" {
			return fmt.Sprintf("replica Pod %s was evicted: %s", pods[i].Name, pods[i].Status.Message)
		}
	}

	return ""
}

// mapModelDeploymentArtifact 将该制品所在 Namespace 中引用它的 ModelDeployment 加入调谐队列。
func (r *ModelDeploymentReconciler) mapModelDeploymentArtifact(ctx context.Context, obj ctrlcli.Object) []ctrlreconcile.Request {
	return r.modelDeploymentsReferencing(ctx, obj.GetNamespace(), obj.GetName())
}

// mapModelDeploymentClaim 将制品指向该 claim 的 ModelDeployment 加入调谐队列。
func (r *ModelDeploymentReconciler) mapModelDeploymentClaim(ctx context.Context, obj ctrlcli.Object) []ctrlreconcile.Request {
	artifacts := new(workercore.ModelArtifactList)
	if err := r.Client.List(ctx, artifacts, ctrlcli.InNamespace(obj.GetNamespace())); err != nil {
		ctrllog.FromContext(ctx).Error(err, "list model artifacts for a claim", "claim", obj.GetName())
		return nil
	}
	var names []string
	for i := range artifacts.Items {
		if claim := artifacts.Items[i].Spec.Source.PersistentVolumeClaim; claim != nil && claim.ClaimName == obj.GetName() {
			names = append(names, artifacts.Items[i].Name)
		}
	}

	return r.modelDeploymentsReferencing(ctx, obj.GetNamespace(), names...)
}

// mapModelDeploymentNodeModelStore 将节点列出了对应摘要且采用节点分发的 ModelDeployment 加入调谐队列。
func (r *ModelDeploymentReconciler) mapModelDeploymentNodeModelStore(ctx context.Context, obj ctrlcli.Object) []ctrlreconcile.Request {
	nms, ok := obj.(*workercore.NodeModelStore)
	if !ok || len(nms.Status.Models) == 0 {
		return nil
	}
	digests := make(map[string]bool, len(nms.Status.Models))
	for i := range nms.Status.Models {
		digests[nms.Status.Models[i].Digest] = true
	}
	mds := new(workercore.ModelDeploymentList)
	if err := r.Client.List(ctx, mds); err != nil {
		ctrllog.FromContext(ctx).Error(err, "list model deployments for a node model store")
		return nil
	}
	var reqs []ctrlreconcile.Request
	for i := range mds.Items {
		m := mds.Items[i].Status.Model
		if m != nil && m.Delivery == workercore.ModelDeploymentModelDeliveryNode && digests[m.ManifestDigest] {
			reqs = append(reqs, ctrlreconcile.Request{NamespacedName: ctrlcli.ObjectKeyFromObject(&mds.Items[i])})
		}
	}

	return reqs
}

// nodeModelStoreModelsChanged 仅在 NodeModelStore 的 models 发生变化时才放行更新事件。
// ModelDeployment 不会从节点读取其他字段，而容量与 conditions 在每次下载过程中都会频繁变化，
// 但并不会影响任何 ModelDeployment 看到的结果。
func nodeModelStoreModelsChanged() ctrlpredicate.Predicate {
	// 节点每次写入阈值时下载进度都会更新，而没有任何 ModelDeployment 会读取该进度。
	return ctrlpredicate.Funcs{
		UpdateFunc: func(e ctrlevent.UpdateEvent) bool {
			o, okOld := e.ObjectOld.(*workercore.NodeModelStore)
			n, okNew := e.ObjectNew.(*workercore.NodeModelStore)
			return !okOld || !okNew || !kubemeta.DeepEqual(modelsWithoutProgress(o), modelsWithoutProgress(n))
		},
	}
}

// modelsWithoutProgress 返回节点的模型条目列表，并剔除其中的下载进度字段。
func modelsWithoutProgress(nms *workercore.NodeModelStore) []workercore.NodeModelStoreModel {
	models := make([]workercore.NodeModelStoreModel, len(nms.Status.Models))
	for i, m := range nms.Status.Models {
		m.DownloadedBytes = 0
		models[i] = m
	}

	return models
}

// mapModelDeploymentDelivery 将所有引用制品的 ModelDeployment 加入调谐队列。Hub 制品采用哪种分发方式
// 以及是否能够分发，取决于分发 Setting 与插件的 CSIDriver，而这两者都不会修改任何 ModelDeployment
// 自身的对象。若无此映射，因 NodeDeliveryUnavailable 或 FilterNeedsNodeDelivery 而阻塞的部署在
// 条件修复后会一直等待，切换分发方式也不会触发滚动更新，直到有其他无关事件唤醒它们。
//
// 此处选择返回 error 而非仅记录日志：该映射产生的唤醒是受阻部署收到的唯一信号，因此列表查询失败必须
// 传递给能够触发重试的调用方。
//
// 列表查询直接通过 API reader 而非缓存读取，原因与直接从 API server 读取分发配置相同：停滞的
// informer 会无报错地返回冻结的旧列表，导致重试逻辑无法区分“确实没有需要唤醒的对象”与“缓存过期”。
func (r *ModelDeploymentReconciler) mapModelDeploymentDelivery(ctx context.Context, _ ctrlcli.Object) ([]ctrlreconcile.Request, error) {
	mds := new(workercore.ModelDeploymentList)
	if err := r.APIReader.List(ctx, mds); err != nil {
		return nil, fmt.Errorf("list model deployments for a delivery change: %w", err)
	}
	var reqs []ctrlreconcile.Request
	for i := range mds.Items {
		if ModelDeploymentArtifactName(&mds.Items[i]) != "" {
			reqs = append(reqs, ctrlreconcile.Request{NamespacedName: ctrlcli.ObjectKeyFromObject(&mds.Items[i])})
		}
	}

	return reqs, nil
}

// modelDeploymentDeliveryRecheck 是因分发问题而受阻的 ModelDeployment 在无事件触发时重新检查的时间间隔。
const modelDeploymentDeliveryRecheck = time.Minute

// settingsReadDelay 是 Settings 变更后重新检查 ModelDeployment 的延迟时间：需超过 Settings 读缓存的
// 有效期，以确保该轮调谐读到新值而非缓存值。
const settingsReadDelay = setting.ReadCacheTTL + 5*time.Second

// deliveryChangeRetryDelay 是分发映射失败后重新执行映射的等待时间：列表查询失败绝不能丢弃唤醒信号，
// 而该等待时间可避免持续失败时出现空转。
const deliveryChangeRetryDelay = 10 * time.Second

// deliveryChangeMapFunc 将分发相关的变更（分发 Setting 或插件的 CSIDriver）映射到其可能解除阻塞的
// ModelDeployment。
type deliveryChangeMapFunc func(context.Context, ctrlcli.Object) ([]ctrlreconcile.Request, error)

// deliveryChangeHandler 在经过设定的延迟后，将映射函数返回的对象加入队列。映射失败时会重试而非丢弃：
// 该唤醒是受阻部署能收到的唯一信号。
type deliveryChangeHandler struct {
	mapFn deliveryChangeMapFunc
	delay time.Duration
	// retrying 用于合并并发失败的重试请求：无论触发的源对象是什么，映射都会覆盖全部 ModelDeployment，
	// 因此一个待执行的重试即可代表所有失败。
	retrying atomic.Bool
}

// enqueueDeliveryChange 在经过 delay 后，将 mapFn 针对分发变更返回的请求加入队列。
func enqueueDeliveryChange(mapFn deliveryChangeMapFunc, delay time.Duration) ctrlhandler.EventHandler {
	return &deliveryChangeHandler{mapFn: mapFn, delay: delay}
}

func (h *deliveryChangeHandler) add(ctx context.Context, obj ctrlcli.Object, q workqueue.TypedRateLimitingInterface[ctrlreconcile.Request]) {
	reqs, err := h.mapFn(ctx, obj)
	if err != nil {
		ctrllog.FromContext(ctx).Error(err, "map a delivery change")
		h.retryLater(ctx, obj, q)
		return
	}
	for _, req := range reqs {
		q.AddAfter(req, h.delay)
	}
}

// retryLater 在等待一段时间后重新执行映射，并立即将结果入队，因为延迟已在等待过程中消耗。
func (h *deliveryChangeHandler) retryLater(ctx context.Context, obj ctrlcli.Object, q workqueue.TypedRateLimitingInterface[ctrlreconcile.Request]) {
	if !h.retrying.CompareAndSwap(false, true) {
		return
	}
	time.AfterFunc(max(h.delay, deliveryChangeRetryDelay), func() {
		h.retrying.Store(false)
		if ctx.Err() != nil {
			return
		}
		reqs, err := h.mapFn(ctx, obj)
		if err != nil {
			ctrllog.FromContext(ctx).Error(err, "map a delivery change")
			h.retryLater(ctx, obj, q)
			return
		}
		for _, req := range reqs {
			q.Add(req)
		}
	})
}

func (h *deliveryChangeHandler) Create(ctx context.Context, e ctrlevent.CreateEvent, q workqueue.TypedRateLimitingInterface[ctrlreconcile.Request]) {
	h.add(ctx, e.Object, q)
}

func (h *deliveryChangeHandler) Update(ctx context.Context, e ctrlevent.UpdateEvent, q workqueue.TypedRateLimitingInterface[ctrlreconcile.Request]) {
	h.add(ctx, e.ObjectNew, q)
}

func (h *deliveryChangeHandler) Delete(ctx context.Context, e ctrlevent.DeleteEvent, q workqueue.TypedRateLimitingInterface[ctrlreconcile.Request]) {
	h.add(ctx, e.Object, q)
}

func (h *deliveryChangeHandler) Generic(
	ctx context.Context, e ctrlevent.GenericEvent, q workqueue.TypedRateLimitingInterface[ctrlreconcile.Request],
) {
	h.add(ctx, e.Object, q)
}

func (r *ModelDeploymentReconciler) modelDeploymentsReferencing(
	ctx context.Context, namespace string, artifacts ...string,
) []ctrlreconcile.Request {
	if len(artifacts) == 0 {
		return nil
	}
	mds := new(workercore.ModelDeploymentList)
	if err := r.Client.List(ctx, mds, ctrlcli.InNamespace(namespace)); err != nil {
		ctrllog.FromContext(ctx).Error(err, "list model deployments for a model artifact")
		return nil
	}
	var reqs []ctrlreconcile.Request
	for i := range mds.Items {
		if slices.Contains(artifacts, ModelDeploymentArtifactName(&mds.Items[i])) {
			reqs = append(reqs, ctrlreconcile.Request{NamespacedName: ctrlcli.ObjectKeyFromObject(&mds.Items[i])})
		}
	}

	return reqs
}

// modelArtifactKVIdentityDigits 指定标识中保留摘要的十六进制位数：共 128 位，远足以避免共享同一
// 存储租户的制品发生冲突，且每个键上占用 34 个字符。
const modelArtifactKVIdentityDigits = 32

// modelArtifactKVIdentity 生成部署的 KV 存储键所携带的权重标识：对于内容可寻址的 Hub 制品，格式为
// "m-" 加上 manifest 摘要的前缀位数；对于无内容地址的 claim，格式为 "m-" 加上制品 UID 的 SHA-256
// 前缀位数。标识相同的部署会共享块，标识不同的部署则永不共享。
//
// claim 的标识绑定的是 ModelArtifact 本身，而非 claim 中的实际内容；因此若在同一制品下替换文件，
// 标识不会变化，新部署可能会命中基于旧文件计算的块。文档中也明确说明，claim 上的新权重应创建新的
// ModelArtifact。
//
// 返回结果中不包含 "@"、"_" 或 ":"，这些字符是引擎用来拼接键的分隔符。
//
// 制品必须已解析完成：Hugging Face 制品在获得摘要之前没有标识，若回退到 UID 会生成错误的标识而非空值。
func modelArtifactKVIdentity(ma *workercore.ModelArtifact) string {
	hexDigits := strings.TrimPrefix(ma.Status.Resolved.ManifestDigest, modelartifact.DigestSHA256+":")
	if ma.Status.Resolved.ManifestDigest == "" {
		hexDigits = stringx.SumBySHA256(string(ma.UID))
	}
	if len(hexDigits) > modelArtifactKVIdentityDigits {
		hexDigits = hexDigits[:modelArtifactKVIdentityDigits]
	}

	return "m-" + hexDigits
}
