package worker

import (
	"context"
	"fmt"
	"slices"
	"strings"

	app "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	kueueworkload "sigs.k8s.io/kueue/pkg/workload"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubeapistatus"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/utils/ctrlclix"
	"gpustack.ai/gpustack/pkg/utils/strconvx"
	"gpustack.ai/gpustack/pkg/worker/kuberess"
)

const (
	// ModelDeploymentPhaseStarting 在副本仍在启动且尚无副本就绪时报告。
	ModelDeploymentPhaseStarting = "Starting"
	// ModelDeploymentPhaseReady 在每个角色的就绪副本数都等于期望副本数时报告。
	ModelDeploymentPhaseReady = "Ready"
	// ModelDeploymentPhaseDegraded 在至少有一个副本就绪、同时至少有一个副本未就绪时报告。它是独立于 Starting 的阶段，用于表示 deployment 在对外提供服务的同时处于容量不足的状态。
	ModelDeploymentPhaseDegraded = "Degraded"
	// ModelDeploymentPhaseDeleting 在副本正在被销毁时报告。
	ModelDeploymentPhaseDeleting = "Deleting"
)

// ModelDeployment 报告的 condition 类型，每个维度对应一个。它们彼此独立：“配额已预留但缓存未挂载”是一个真实且可操作的状态，这是单个阶段字符串无法表达的。
//
// DomainRegistered 声明在解析 Binding 的规则旁，CacheAttached 声明在判断它的读取逻辑旁，ReplicasUpToDate 声明在决定它的收敛逻辑旁，从而使每个 condition 的词汇定义都与真正能观测到它的代码放在一起。
const (
	ModelDeploymentConditionQuotaReserved kubeapistatus.ConditionType = "QuotaReserved"

	// ModelDeploymentConditionRoleKindsReady 报告本 deployment 声明的每一种角色类型是否都至少有一个就绪副本。
	//
	// 它不是副本完整性——尽管两者概念最接近，也最容易被误解为此意。“每个角色都拥有其请求的全部副本”已由阶段（phase）通过汇总各角色计数后判断并给出回答——而简单的总和无法表达本 condition 的语义：完全缺失某种角色类型的 deployment 与仅缺少一个副本的 deployment 会得到相同的总和，从而处于相同的阶段。
	//
	// 它也不等同于可用性，两者之间的差距比表面看起来更大。请求能否被成功处理，取决于引擎如何对待被告知为 producer 的角色，以及 Service 的端点是否能路由到已就绪的副本。这两点都无法从当前对象直接观测到。因此，本 condition 仅按角色类型报告就绪状态，不做超出此范围的断言；无论上述两个问题日后如何解答，这一语义都能保持正确。
	ModelDeploymentConditionRoleKindsReady kubeapistatus.ConditionType = "RoleKindsReady"

	// ModelDeploymentConditionRouterReady 报告受管 router 是否已完成渲染且拥有就绪副本。渲染失败会投影到此处而非直接返回：它属于 spec 问题，若因此中断整个状态更新轮次，会导致其他所有维度的状态被冻结，且无法指明具体原因。收敛路径仍会返回相同的 error，因此该轮次依然会重试。
	ModelDeploymentConditionRouterReady kubeapistatus.ConditionType = "RouterReady"

	// ModelDeploymentConditionWeightsReady 报告每个引擎角色的模型权重是否已可用：即声明挂载已就绪，或引擎自身下载已完成。对于未指定 artifact 的 deployment，其状态为 True，原因为 NotApplicable。
	ModelDeploymentConditionWeightsReady kubeapistatus.ConditionType = "WeightsReady"

	// ModelDeploymentConditionEndpointEligibility 报告 deployment 的端点是否已经过资格判定。状态为 Unknown 且原因为 NotObserved 表示尚无资格观测记录——这与 False 不同，后者表示 operator 取消了已观测端点的资格。status.roles[].endpoints 下按角色统计的合格数量在下一层级保留了同样的区分：nil 表示未观测，显式的 0 表示观测到的空集合。
	ModelDeploymentConditionEndpointEligibility kubeapistatus.ConditionType = "EndpointEligibility"
)

// modelDeploymentReasonNotObserved 是状态中已存在但尚无观测结果的维度所使用的 reason：能够回答该维度的读取逻辑尚未构建。它表示观测缺失而非观测到不存在，这正是“等待读取结果”与“针对空集合采取行动”之间的区别。
const modelDeploymentReasonNotObserved = "NotObserved"

// 整组健康谓词给出结果后 EndpointEligibility 携带的各类 reason。它们作为已有 condition 上的 reason 而不是新增字段存在，因为接口契约已冻结，且其中每一种状态都是该 condition 本身即可表达的。
//
// reason 是机器可读的分类，而 message 并非契约。这里最重要的区分在于 Revoked 与 Unsupported：前者表示本 operator 将端点移出了选择集合，后者表示无法将其恢复。无法区分这两者的读取方会误以为看到的是一个健康的 deployment。
const (
	// modelDeploymentReasonEndpointsQualified 表示所有副本的整组均已验证通过。
	modelDeploymentReasonEndpointsQualified = "Qualified"

	// modelDeploymentReasonEndpointsHeldUnverified 表示存在本 operator 无法评估的分支。
	// 资格状态保持挂起，且 message 会指明该分支。
	modelDeploymentReasonEndpointsHeldUnverified = "HeldUnverified"

	// modelDeploymentReasonEndpointsUnsupported 表示引擎形态完全没有可验证的组谓词。恢复操作在其上保持挂起，而非仅凭健康状态即予以放行。
	modelDeploymentReasonEndpointsUnsupported = "Unsupported"

	// modelDeploymentReasonEndpointsRevoked 表示明确的健康故障：有成员未就绪、成员集合数量不足，或副本正在被替换。撤回端点资格是无条件的。
	modelDeploymentReasonEndpointsRevoked = "Revoked"
)

// RoleKindsReady 携带的各类 reason。共有三个而非两个，因为“没有缺失的角色类型”和“尚无任何角色纳入统计”是两种不同的结果，若统一用 False 会将它们混为一谈。
const (
	modelDeploymentReasonAllKindsReady = "AllKindsReady"

	modelDeploymentReasonKindsNotReady = "KindsNotReady"

	// modelDeploymentReasonNoRoleStatuses 表示 Unknown 情况：本轮处理未统计到任何角色，
	// 因此无从判断。此处若报告 False，则意味着某种角色类型缺失，而本轮处理无法
	// 做出这一断言。
	modelDeploymentReasonNoRoleStatuses = "NoRoleStatuses"
)

// modelDeploymentReasonPodGroupIncomplete 是 QuotaReserved 在副本组数量不足其声明总数
// 时的原因。
//
// 之所以需要该原因，是因为处于此状态时根本没有 Workload（Kueue 不会为尚未完整出现的组
// 生成 Workload），因此从 Workload 侧来看，它与准入尚未发生的情况无法区分。这两者必须
// 加以区分：后者很快就会恢复正常，而前者则表现为部署一直停留在 Pod 受门控且
// `kubectl get workloads` 为空的状态，直到有组件创建出缺失的副本为止。
const modelDeploymentReasonPodGroupIncomplete = "PodGroupIncomplete"

// modelDeploymentReasonPreemptedInPart 是 QuotaReserved 在部署被更高优先级工作负载
// 抢占部分资源时的原因。
//
// 之所以单独设立此原因，是因为运维处理方式不同。该条件报告的其他所有等待状态要么会
// 自行恢复，要么会指明部署中需要修复的问题；而此状态只有当其他地方释放出容量时才会
// 解除，在此之前，存活的副本组仍占用着加速器，但部署实际拥有的资源已少于准入配额；
// 若被回收的恰好是某种角色类型的全部副本组，则意味着该部署已完全失去该角色。对于
// 其他情况而言，等待是合理的，但此处需要人工决策，因为等待时长取决于抢占方的行为，
// 而这超出了当前对象与本 operator 的控制范围。
//
// Reason 是机器可读的分类，而 Message 不是契约。若告警规则或运维手册需要区分
// “继续等待”与“排查配额被谁占用”，必须基于稳定的字段进行分支判断，而非对语句做子串匹配。
const modelDeploymentReasonPreemptedInPart = "PreemptedInPart"

// RouterReady 携带的原因。NotApplicable 为 True 的理由与 KVEventsPublishing 报告该原因
// 相同：条件缺失与条件不适用属于两种不同状态，不应让读取方根据前者去推断后者。
const (
	modelDeploymentReasonRouterReady          = "Ready"
	modelDeploymentReasonRouterNotApplicable  = "NotApplicable"
	modelDeploymentReasonRouterRenderFailed   = "RenderFailed"
	modelDeploymentReasonRouterNotDeployed    = "NotDeployed"
	modelDeploymentReasonRouterNoReadyReplica = "NoReadyReplicas"
)

// 副本组服务状态结果携带的原因。
//
// 它们以副本组为主体，而部署自身的原因则以部署为主体：两者是同一缺陷在两个层级上的
// 体现，查看副本组条目的读取方关注的是该副本组本身。
const (
	// modelDeploymentReasonStaleGroupServing 表示该副本组的 Router 视图在状态发布前已过期。
	modelDeploymentReasonStaleGroupServing = "the collected view of this group is stale by the time " +
		"this status was written"
	// modelDeploymentReasonGroupUnobserved 表示 Router 返回结果中未包含该副本组。
	modelDeploymentReasonGroupUnobserved = "the Router's answer does not carry this group"
	// modelDeploymentReasonCollectiveNotEvaluated 表示弹性集合体（elastic collective），
	// operator 从未将其作为单一副本组进行资格判定。
	//
	// master 的判定结果不代表整个集合体。现有的资格判定仅针对 master 本身，而 master 只是
	// 集合体中的一个成员，并非集合体本身；若直接发布 master 的状态，就相当于发布了针对
	// 另一个不同组别的判定结果。
	modelDeploymentReasonCollectiveNotEvaluated = "the whole elastic collective has not been " +
		"evaluated as one group"
)

// syncModelDeploymentStatus 根据本轮观察到的状态重建 status，且仅当其与已存储状态不同时
// 才执行写入。
//
// 状态采用重建而非局部 patch 的方式，因此过期字段不会在与 Pod 实际情况不一致时残留：
// 曾处于 Ready 但现已不再 Ready 的角色，会报告刚刚实测到的计数，而非上次变更时的旧值。
// 出于同样的原因，后续任务新增的任何字段都必须汇总到此函数中，若存在第二个写入点，
// 就可能遗留旧字段。
// domain 是本轮对所引用 Binding 的观察结果；若本轮未检查 Binding，则为 nil。传入 nil 时
// 会完整保留原有的 domain 投影及其 condition，因为若在销毁阶段报告“未观察到”，
// 会被误读为 domain 已消失。
//
// rollout 记录在相同场景下同样为 nil，但处理方式恰好相反，这是在新增调用方前唯一
// 需要注意的不对称性。当 rollout 记录为 nil 或未统计到任何副本时，系统会报告 Unknown，
// 而非保留已存储的 condition。差异在于对象归属：Binding 属于其他组件，未读取它的一轮
// 处理无需给出结论；而副本归本控制器自身管理，未统计到任何副本时控制器必须给出明确
// 结论，即“无法判断”。若改为保留旧值，则处于稳定状态的部署其 True 状态会在删除全部
// 副本的那一轮处理中错误地留存下来。
func (r *ModelDeploymentReconciler) syncModelDeploymentStatus(
	ctx context.Context, md *workercore.ModelDeployment, pods []core.Pod, domain *modelDeploymentDomain,
	rollout *modelDeploymentRollout, weights *modelArtifactWeights,
	observation modelDeploymentPassObservation,
) error {
	// 此处使用的是本轮收集到的服务状态结果，本轮也会将同一结果传递给资格写入逻辑：
	// 它是对当前实际存在的 Router 的观察快照，其中的任何变化都必须像其他观察结果的变化
	// 一样独立反映到 status 中。过程中的所有失败均已编码为结果中的状态，绝不作为
	// reconcile error 返回。
	serving := r.modelDeploymentServingForStatus(md, observation)

	desired, err := r.computeModelDeploymentStatusWithQualifications(
		ctx, md, pods, domain, rollout, weights, observation.Qualifications,
	)
	if err != nil {
		return err
	}
	applyModelDeploymentServing(desired, serving)

	// 副本组条目在此处附加，而非在计数逻辑内部附加，以便将集合体的成员信息传递至发布结果：
	// 计数逻辑仅读取传入的 Pod，而弹性部署的副本组并不对应计数中的某一个副本。
	//
	// 集合体成员列表随本轮载体传入，对于没有集合体的形态该列表为空，因此非集合体角色
	// 仍保留由副本推导出的副本组。
	collective := applyModelDeploymentGroupStatuses(
		md, desired.Roles, observation.Qualifications, pods, observation.CollectiveMembers,
	)

	// 副本组状态与计数均基于同一份观察结果解析，从而避免副本组与其所属角色描述同一批对象
	// 在两个不同时刻的状态。
	r.applyModelDeploymentGroupAnswer(desired, observation, md, collective)

	// 时间点依据此前已发布的状态进行结算，使持续具备资格的副本组保留其最初获得资格的
	// 时间点，而非更新为最新一轮处理的时间点。
	applyModelDeploymentGroupQualificationTimes(
		desired, md.Status.Roles, meta.NewTime(r.modelDeploymentNow()), collective,
	)

	// kubemeta.DeepEqual 也会比较 LastTransitionTime，这并非巧合而是有意为之：
	// condition 访问器仅在条件值发生变化时才会更新该时间戳，因此状态未变的一轮处理
	// 会保持其不变，使比较结果为相等。
	if kubemeta.DeepEqual(*desired, md.Status) {
		return nil
	}

	md.Status = *desired

	return r.Client.Status().Update(ctx, md)
}

// modelDeploymentServingForStatus 返回将发布到 status 中的服务状态结果。
//
// 未携带结果的载体表示该路径未进行观察，此时会明确表达该语义，而不是发布空字符串
// （空字符串并非有效的服务状态）。
//
// 已过期的确认结果不会作为当前确认结果发布。虽然结果在收集时是新鲜的，但从收集到
// 本次发布之间的写入耗时并无上限，若直接发布，几分钟前确认的计数就会被当成此刻确认的
// 计数对外公布。
func (r *ModelDeploymentReconciler) modelDeploymentServingForStatus(
	md *workercore.ModelDeployment, observation modelDeploymentPassObservation,
) ServingAnswer {
	serving := observation.Serving
	if serving.State == "" {
		serving = modelDeploymentServingUnobserved(md, "this pass observed no router")
	}
	if serving.State != workercore.ModelDeploymentServingStateConfirmed {
		return serving
	}
	if r.modelDeploymentServingFreshNow(serving) {
		return serving
	}

	return ServingAnswer{
		State:  workercore.ModelDeploymentServingStateUnknown,
		Reason: "the collected view is stale by the time this status was written",
	}
}

// computeModelDeploymentStatusWithQualifications 为已完成整组健康谓词评估的一轮处理
// 计算 status，使发布到状态中的端点结果与本轮早些时候执行的资格写入均来自同一组
// 资格判定，而非对同一批 Pod 进行两次分组。
//
// 未执行谓词评估的处理轮次不会传入任何资格判定结果，从而使端点结果保持“未观察”状态，
// 而非伪造一个本轮从未做出的判定。
func (r *ModelDeploymentReconciler) computeModelDeploymentStatusWithQualifications(
	ctx context.Context, md *workercore.ModelDeployment, pods []core.Pod, domain *modelDeploymentDomain,
	rollout *modelDeploymentRollout, weights *modelArtifactWeights,
	qualifications []modelDeploymentInstanceQualification,
) (*workercore.ModelDeploymentStatus, error) {
	// condition 访问器会直接修改传入的对象，因此这里基于已观察 status 的深拷贝进行操作：
	// condition 中包含 LastTransitionTime，不能在每一轮都重置，只有访问器自己知道
	// 何时应当更新该时间戳。
	holder := &workercore.ModelDeployment{Status: *md.Status.DeepCopy()}

	// 一次性获取该部署的所有 Workload：各角色的 flavor 结果和配额判定均源自这一次读取。
	// 若分两次读取，status 的两部分就可能描述两个不同时刻的状态。
	wls, err := r.findModelDeploymentGroupWorkloads(ctx, md, pods)
	if err != nil {
		return nil, err
	}
	// 一次性解析每个副本各自的 Workload。当每个副本各自对应一个 Workload 时，便不存在所谓的
	// “部署级 Workload”；若按角色视图直接取排序最前的 Workload 作为代表，则会把存活副本
	// 的状态误报给自身 Workload 缺失的副本。
	wlByReplica := modelDeploymentReplicaWorkloads(pods, wls)

	// 此处基于与资格写入逻辑相同的资格判定结果一次性计算合格计数。若从同一批 Pod 重新
	// 推导一次，就意味着二次分组，而两次分组正是导致两个统计数字对副本界定不一致的根源。
	qualified := modelDeploymentRoleQualified(qualifications)
	holder.Status.Roles = modelDeploymentRoleStatuses(md, pods, wlByReplica, qualified)

	if md.Spec.Router == nil {
		holder.Status.Endpoint = modelDeploymentEndpoint(md)
	} else {
		holder.Status.Endpoint = ""
	}

	observeModelDeploymentDomain(holder, domain)

	observeModelDeploymentQuota(md, pods, wls, wlByReplica, holder)

	r.observeModelDeploymentCache(ctx, md, pods, domain, holder)

	observeModelDeploymentRollout(holder, md, pods, wlByReplica, rollout)

	observeModelDeploymentRoleKinds(holder)

	// 资格视图会与相应的计数结果一同上报，使仅查看 condition 的读取方也能区分“未观察到
	// 端点集合”与“已观察到但结果为空”。
	observeModelDeploymentEndpointEligibility(holder, qualifications)

	var nodeModels map[string]*workercore.NodeModelStoreModel
	if weights != nil && weights.Render != nil && weights.Render.Delivery == workercore.ModelDeploymentModelDeliveryNode {
		if nodeModels, err = modelArtifactNodeModels(ctx, r.Client, pods, weights.Render.ManifestDigest); err != nil {
			return nil, err
		}
	}
	observeModelDeploymentWeights(holder, pods, weights, nodeModels)

	// 为两个路由路径观察器统一解析一次；对于未路由的部署，由于不存在读取厂商信息的逻辑，
	// 此处为 nil。
	manufacturers := r.modelDeploymentRoleManufacturers(ctx, md)

	observeModelDeploymentKVEvents(md, holder, manufacturers)

	routerReady, err := r.observeModelDeploymentRouter(ctx, md, domain, holder, manufacturers)
	if err != nil {
		return nil, err
	}
	holder.Status.RoleSummary = modelDeploymentRoleSummary(md.Spec.Router, routerReady, holder.Status.Roles)

	deriveModelDeploymentPhase(md, holder, routerReady > 0)
	annotateModelDeploymentPhase(holder, pods)

	return &holder.Status, nil
}

func modelDeploymentRoleSummary(
	router *workercore.ModelDeploymentRouter, routerReady int32,
	roles []workercore.ModelDeploymentRoleStatus,
) string {
	var summary strings.Builder
	if router != nil {
		fmt.Fprintf(&summary, "%dR", routerReady)
	}

	counts := map[workercore.ModelDeploymentRoleKind]int32{}
	var other strings.Builder
	for _, role := range roles {
		switch role.Kind {
		case workercore.ModelDeploymentRoleKindServer,
			workercore.ModelDeploymentRoleKindEncode,
			workercore.ModelDeploymentRoleKindPrefill,
			workercore.ModelDeploymentRoleKindDecode:
			counts[role.Kind] += role.Ready
		default:
			fmt.Fprintf(&other, "%d%s", role.Ready, strings.ToUpper(string(role.Kind)))
		}
	}
	// 顺序是固定的，而非按角色声明顺序。摘要信息需要一眼读懂并与其他摘要快速对比，因此
	// 无论部署中将 encoder 写在前面还是后面，渲染出的字母位置都必须保持一致。
	//
	// server 对应的字母保持在最前。server 与 encoder 不会同时出现在同一个可准入部署中
	// （准入阶段会拒绝该组合），因此两者的相对位置在任何合法部署上都不可见；将其保留在
	// 所有不含 encoder 的部署原本所在的位置，即可确保原有展示形式不受任何影响。
	for _, kind := range []struct {
		name workercore.ModelDeploymentRoleKind
		code string
	}{
		{workercore.ModelDeploymentRoleKindServer, "S"},
		{workercore.ModelDeploymentRoleKindEncode, "E"},
		{workercore.ModelDeploymentRoleKindPrefill, "P"},
		{workercore.ModelDeploymentRoleKindDecode, "D"},
	} {
		if count, declared := counts[kind.name]; declared {
			fmt.Fprintf(&summary, "%d%s", count, kind.code)
		}
	}
	summary.WriteString(other.String())

	return summary.String()
}

func (r *ModelDeploymentReconciler) observeModelDeploymentRouter(
	ctx context.Context, md *workercore.ModelDeployment, domain *modelDeploymentDomain,
	holder *workercore.ModelDeployment, manufacturers map[string]string,
) (int32, error) {
	if md.Spec.Router == nil {
		holder.Status.Router = nil
		ModelDeploymentConditionRouterReady.True(holder,
			modelDeploymentReasonRouterNotApplicable, "the deployment declares no router")
		return 0, nil
	}

	objects, err := renderModelDeploymentRouterObjects(ctx, md, manufacturers, false)
	if err != nil {
		// 渲染拒绝属于 spec 问题而非集群问题：此处将其投影为 condition 并继续计算其余 status
		// 字段，因为在此直接返回会冻结其他所有字段而不更新其状态。收敛路径会返回相同的 error，
		// 因此本轮处理仍会被重试。
		ModelDeploymentConditionRouterReady.False(holder,
			modelDeploymentReasonRouterRenderFailed, err.Error())
		return 0, nil
	}
	poolEndpoint := ""
	if domain != nil && domain.KVCache != nil {
		pool := new(workercore.KVCachePool)
		err = r.Client.Get(ctx, ctrlcli.ObjectKey{Name: domain.KVCache.Pool}, pool)
		if err != nil && !kerrors.IsNotFound(err) {
			return 0, err
		}
		if err == nil {
			poolEndpoint = pool.Status.ClientEndpoint
		}
	}
	status := projectModelDeploymentRouterStatus(&objects.Contract, poolEndpoint)
	holder.Status.Router = status

	deployment := new(app.Deployment)
	err = r.Client.Get(ctx, ctrlcli.ObjectKey{Namespace: md.Namespace, Name: objects.Deployment.Name}, deployment)
	if kerrors.IsNotFound(err) {
		ModelDeploymentConditionRouterReady.Unknown(holder,
			modelDeploymentReasonRouterNotDeployed, "the router's Deployment does not exist yet")
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if deployment.Status.ReadyReplicas == 0 {
		ModelDeploymentConditionRouterReady.False(holder,
			modelDeploymentReasonRouterNoReadyReplica,
			fmt.Sprintf("router %q has no ready replicas", md.Spec.Router.Name))
		return 0, nil
	}

	ModelDeploymentConditionRouterReady.True(holder,
		modelDeploymentReasonRouterReady,
		fmt.Sprintf("router %q has %d ready replicas", md.Spec.Router.Name, deployment.Status.ReadyReplicas))
	holder.Status.Endpoint = status.Endpoint
	return deployment.Status.ReadyReplicas, nil
}

func projectModelDeploymentRouterStatus(
	contract *workercore.ModelDeploymentRouterStatus, poolEndpoint string,
) *workercore.ModelDeploymentRouterStatus {
	status := contract.DeepCopy()
	status.PoolEndpoint = poolEndpoint

	return status
}

// modelDeploymentRoleStatuses 按角色统计 spec 请求的副本数、处于 Ready 状态的副本数、
// 持有配额预留（quota reservation）的副本数，以及已分配副本所落入的 flavor 集合。
//
// 角色下的副本通过 Pod 的资源注解识别，而非解析其名称，因为名称只是渲染产物，而注解
// 才记录了渲染器写入的元信息。
//
// 计数与 flavor 集合仅针对保留中的副本统计。正在退出的副本仍持有其 Workload 与配额，
// 若将其计入，就会让即将淘汰的副本掩盖实际的副本缺口；其 flavor 也会随之退出，若仍
// 将其列出，在回答“该角色落在何处”时就会给出角色正在迁出的位置。
//
// 配额计数与 flavor 集合基于同一解析结果，但回答的问题不同，因此后者不以先决条件
// 依赖前者：在伴随的预留已消失后，Workload 仍保留其分配信息——正在被抢占的副本表现为
// QuotaReserved 为 false，但准入信息中仍指明了 flavor——而此时“该副本落在了哪种
// 型号上”依然有明确答案。两者绝不能在“统计了哪些副本”这一点上出现分歧。
func modelDeploymentRoleStatuses(
	md *workercore.ModelDeployment, pods []core.Pod,
	wlByReplica map[types.UID]*kueue.Workload, qualified map[string]*int32,
) []workercore.ModelDeploymentRoleStatus {
	// 此处的每一项统计都以副本为单位，因此必须先对 Pod 进行分组。一旦单个副本可能包含多个
	// Pod，按 Pod 计数就会出错，且两项统计的错误方向截然不同：Ready 会将半启动状态的
	// 副本误报为部分提供服务，而实际上成员未全部就绪的副本组根本无法提供服务；由于同一
	// 副本的各成员共享同一个 Workload，QuotaReserved 会按成员重复计算同一份预留，导致
	// 包含 2 个副本（每副本 4 个 Pod）的角色在期望数为 2 的情况下报出 8 份预留。当每个
	// 副本只有一个成员时，分组即为恒等映射，因此其结果与此前单 Pod 形态完全一致。
	//
	ready := make(map[string]int32, len(md.Spec.Roles))
	unmanaged := make(map[string]bool, len(md.Spec.Roles))
	reserved := make(map[string]int32, len(md.Spec.Roles))
	flavors := make(map[string]sets.Set[string], len(md.Spec.Roles))
	for _, view := range modelDeploymentGroupPodsByReplica(pods) {
		role := view.Role

		// 运行命令行是否由 operator 构建，需从副本而非角色中读取，只要存在一个 operator
		// 未参与构建命令行的副本即可判定。
		if execution, _ := modelDeploymentDeployedReplicaExecution(view.Members); execution ==
			modelDeploymentExecutionTakeover {
			unmanaged[role] = true
		}

		// 当一个副本集齐了其声明的全部成员且所有成员均处于 Ready 状态时，该副本才算 Ready。
		// 完整性检查与就绪检查并非冗余：缺少成员的副本，其已存在的每个成员都可能报告就绪，
		// 但 Kueue 并未为其生成 Workload，它也未获得任何准入。
		//
		// 成员总数取自副本组自身声明的总数，因此处于编辑中的角色仍会统计其正在提供服务的副本。
		allReady := modelDeploymentDeployedReplicaShape(view).modelDeploymentReplicaIsWhole()
		for _, pod := range view.Members {
			allReady = allReady && podIsReady(pod)
		}
		if allReady {
			ready[role]++
		}

		// 使用副本自身的 Workload，而非按角色解析的 Workload：每个副本都会生成各自的 Workload，
		// 若采用更粗粒度的视图，缺失 Workload 与未分配 Workload 将无法区分。同一副本的所有成员
		// 都解析到同一个 Workload，因此每个副本只需读取一次——按成员读取会导致计数乘以副本规模。
		var wl *kueue.Workload
		for _, pod := range view.Members {
			if wl = wlByReplica[pod.UID]; wl != nil {
				break
			}
		}
		if wl == nil {
			continue
		}
		if kubeapistatus.ConditionType(kueue.WorkloadQuotaReserved).IsTrue(wl) {
			reserved[role]++
		}
		if flavor := assignedFlavor(wl, kueue.PodSetReference(role)); flavor != "" {
			if flavors[role] == nil {
				flavors[role] = sets.New[string]()
			}
			flavors[role].Insert(string(flavor))
		}
	}

	statuses := make([]workercore.ModelDeploymentRoleStatus, 0, len(md.Spec.Roles))
	for i := range md.Spec.Roles {
		role := &md.Spec.Roles[i]
		statuses = append(statuses, workercore.ModelDeploymentRoleStatus{
			Name: role.Name,
			// 此处回显 kind 字段，使仅查看 status 的运维人员也能分清各个角色。该字段通过解析而非直接
			// 拷贝得到：由于它是必填枚举字段，若直接透传未设置的 spec kind 会被校验拒绝，且
			// API server 会拒绝整个 status 写入，而不仅仅是这一个字段。
			Kind:          ModelDeploymentEffectiveRoleKind(role),
			Desired:       role.Replicas,
			Ready:         ready[role.Name],
			QuotaReserved: reserved[role.Name],
			// 替换了整条命令行的副本并未注入缓存客户端，因此此处不能声称其挂载了缓存客户端。
			Unmanaged:       unmanaged[role.Name],
			AssignedFlavors: modelDeploymentAssignedFlavors(flavors[role.Name]),
			Parallelism:     ReadModelDeploymentRoleParallelism(md.Spec.Engine.Name, role),
			Endpoints:       modelDeploymentRoleEndpoints(md.Spec.Router != nil, qualified[role.Name]),
		})
	}

	return statuses
}

// modelDeploymentAssignedFlavors 返回 Kueue 为该角色各副本分配的 flavor 有序集合；
// 若没有已分配副本指定 flavor，则返回 nil。
//
// 返回 nil 而非空切片：“没有副本持有分配”是一个明确事实，而存在但为空的列表会被
// 误读为“分配到了一个无名 flavor”——这与该字段早期标量形式通过指针避免的语义混淆
// 如出一辙。
func modelDeploymentAssignedFlavors(flavors sets.Set[string]) []string {
	if flavors.Len() == 0 {
		return nil
	}

	return sets.List(flavors)
}

// modelDeploymentRoleEndpoints 返回单个角色的端点状态结果：有多少个副本通过了整组
// 健康谓词的资格判定，以及基于这些端点获取的服务确认状态。
//
// 只要列表不完整，合格计数就必须为 nil。对于有三个合格副本和一个待定（held）副本的角色，
// 不能报告三个可用端点：此时不应报告无法保证准确的数字，否则读取方会看到数字并误以为
// 该角色已完全合格。显式的 0 仍然是一个真实有效的答案，它代表该角色所有副本都已知且
// 没有任何一个符合资格。
func modelDeploymentRoleEndpoints(
	routed bool, eligible *int32,
) workercore.ModelDeploymentRoleEndpointsStatus {
	serving := workercore.ModelDeploymentServingStateUnknown
	if !routed {
		serving = workercore.ModelDeploymentServingStateNotConfigured
	}

	return workercore.ModelDeploymentRoleEndpointsStatus{
		Eligible: eligible,
		Serving:  workercore.ModelDeploymentServingStatus{State: serving},
	}
}

// 副本组投影。
//
// 副本组条目是按副本粒度的状态结果，上方的角色计数是由此衍生出的聚合视图。当读取方
// 需要知道具体是哪个副本失败或 Router 停止选择哪个副本时，必须查看该条目，而无法
// 从计数中获取。
//
// 副本组状态（state）与服务确认（serving confirmation）是两个独立的结果，绝不能合并为一：
// 状态指明阻碍该副本组的原因，而服务确认则反映 Router 对它的反馈。

// applyModelDeploymentGroupStatuses 为 status 看到的每个副本组附加一条待发布条目。
//
// 每个副本组一条条目，而非每个成员一条：包含三个成员的副本是一个列出其三个成员的
// 副本组，按成员拆分的列表只会回答一个无人关心的问题。
//
// 副本组来自本 status 读取的 Pod 列表，因此本轮刚创建的副本在本轮就会被发布。
// 成员信息随之附带：尚未获得资格判定的副本组依然拥有成员，缺少资格判定不代表没有成员。
//
// 弹性部署的副本组即其集合体（collective），而集合体的成员是当前完整的 worker 集合，
// 而非单个序号（ordinal）下的成员。
//
// 计数与副本组是两个不同的视图，且在弹性形态下它们描述的是不同的对象，这一点必须明确
// 告知读取方而非让其自行摸索。计数是角色的副本计数：角色声明了一个副本，计数说明该
// 副本是否已启动。副本组则列出了构成该集合体的所有 worker 成员。计数依据弹性路径
// 读取的 master 计算（因为 Router 观察的副本是 master），而副本组依据完整的 worker 集合
// 计算（因为当某个 rank 失败时，读取方关心的是整个集合体）。
//
// 因此在该形态下，计数与副本组绝不描述同一批成员，且副本组自身的原因会表明
// operator 未对整体做出判定。
//
// 当本轮已有判定结果时，状态取本轮的判定；否则取从当前视图推导出的 Pending 状态。
// 对于成员尚未上报状态的副本组，两者的结论一致，从而确保创建副本的那一轮处理与
// 紧随其后的下一轮处理不会对同一副本发布两种不同的结果。
//
// 条目按序号（ordinal）排序，以便读取方无需搜索即可定位副本，且排序结果不依赖于
// 集群返回 Pod 列表的顺序。
//
// 函数返回已发布的集合体条目，因为 Router 的反馈不能被误读为针对整个集合体的反馈：
// Router 观察的是 master，而 master 只是集合体中的一个成员。
func applyModelDeploymentGroupStatuses(
	md *workercore.ModelDeployment, roles []workercore.ModelDeploymentRoleStatus,
	qualifications []modelDeploymentInstanceQualification, pods []core.Pod,
	collectiveMembers []core.Pod,
) *modelDeploymentReplicaView {
	byRole := make(map[string][]modelDeploymentReplicaView, len(roles))
	for _, view := range modelDeploymentGroupPodsByReplica(pods) {
		byRole[view.Role] = append(byRole[view.Role], view)
	}

	// collective 是弹性角色唯一的逻辑副本，因此其条目会替换由副本派生的条目，
	// 而不是加入其中。
	//
	// 成员为空时完全不发布任何组，而不是发布由 master 派生的副本组。
	// master 组回答的是该角色单个副本的问题，而 collective 则是从整体视角看待的
	// 该角色唯一副本：如果读取方在应属于 collective 的位置读到了 master 组，
	// 就会读到本 operator 从未评估过的组的判定结论。
	collective := modelDeploymentElasticCollectiveView(md, collectiveMembers)

	for i := range roles {
		if ModelDeploymentElasticRole(md) != nil && roles[i].Name == ModelDeploymentElasticRole(md).Name {
			if collective == nil {
				continue
			}
			roles[i].Groups = []workercore.ModelDeploymentGroupStatus{
				modelDeploymentGroupCollectiveStatus(md, *collective),
			}
			continue
		}

		groupViews := byRole[roles[i].Name]
		if len(groupViews) == 0 {
			continue
		}
		// ordinal 决定主要顺序，generation 用于打破平局，因为只有当其中一个组完全未占用槽位时，
		// 两个组才会持有相同的 ordinal。generation 正是区分这两者的依据，因此排序由 generation
		// 决定，而不依赖集群碰巧列出 Pod 的顺序。
		slices.SortStableFunc(groupViews, func(a, b modelDeploymentReplicaView) int {
			if a.Ordinal != b.Ordinal {
				return a.Ordinal - b.Ordinal
			}

			return strings.Compare(
				string(modelDeploymentGenerationOf(a)), string(modelDeploymentGenerationOf(b)),
			)
		})

		roles[i].Groups = make([]workercore.ModelDeploymentGroupStatus, 0, len(groupViews))
		for _, view := range groupViews {
			qualification, found := modelDeploymentGroupQualificationOf(
				qualifications, view.Role, view.Ordinal, string(modelDeploymentGenerationOf(view)),
			)
			roles[i].Groups = append(roles[i].Groups,
				modelDeploymentGroupStatus(md, view, qualification, found))
		}
	}

	return collective
}

// modelDeploymentElasticCollectiveView 返回弹性部署的逻辑组视图。
//
// collective 的成员是当前弹性角色的 GPU worker，包含 API/DP master，但不包含辅助的
// Ray head：head 不是 worker，若将 head 纳入 collective 成员中，就会把一个不服务该组的
// 对象列为成员。
//
// 正在退出的 worker 不算作成员，因为它正在离开 collective，将其发布会导致每次退出时
// 成员列表及其 generation 发生抖动。
//
// 没有 worker 的 collective 不构成组，因为没有可列出的成员。master 本身就是 worker 之一，
// 因此没有 worker 的 collective 也意味着部署当前没有 master。
func modelDeploymentElasticCollectiveView(
	md *workercore.ModelDeployment, members []core.Pod,
) *modelDeploymentReplicaView {
	role := ModelDeploymentElasticRole(md)
	if role == nil {
		return nil
	}

	workers := make([]*core.Pod, 0, len(members))
	for i := range members {
		member := &members[i]
		if member.DeletionTimestamp != nil {
			continue
		}
		if modelDeploymentPodRole(member) != role.Name {
			continue
		}
		workers = append(workers, member)
	}
	if len(workers) == 0 {
		return nil
	}

	return &modelDeploymentReplicaView{
		Role:    role.Name,
		Ordinal: 0,
		Seated:  true,
		Members: workers,
	}
}

// modelDeploymentGroupCollectiveStatus 返回弹性 collective 的已发布状态条目。
//
// 其状态为 Unknown，因为 operator 从未将 collective 作为一个整体组进行资格评估：
// 现有的资格评估仅覆盖 master 本身。发布一个未经评估的状态等于作出 operator 无法证实的声明，
// 而发布 master 的状态则是对另一个不同组作出的声明。
//
// 服务状态（serving answer）取默认值且不携带计数，因为只有确认状态才携带计数，
// 且 Router 的响应反映的是 master 的事实而非 collective 的事实。
//
// 此处不生成时间戳，因为时间戳属于转换到已发布 Eligible 状态的过程，而 collective 永远不会
// 处于 Eligible 状态。
func modelDeploymentGroupCollectiveStatus(
	md *workercore.ModelDeployment, collective modelDeploymentReplicaView,
) workercore.ModelDeploymentGroupStatus {
	return workercore.ModelDeploymentGroupStatus{
		Role:       collective.Role,
		Ordinal:    int32(collective.Ordinal),
		Generation: string(modelDeploymentGenerationOf(collective)),
		State:      workercore.ModelDeploymentGroupStateUnknown,
		Reason:     modelDeploymentReasonCollectiveNotEvaluated,
		Members:    modelDeploymentCollectiveMembers(collective.Members),
		Serving:    modelDeploymentGroupServingDefault(md),
	}
}

// modelDeploymentGroupStatus 返回单个组的已发布状态条目。
//
// 当本轮调谐已有资格评估结论时，状态取自该评估自身的判定；否则取自基于视图推导出的待定状态。
func modelDeploymentGroupStatus(
	md *workercore.ModelDeployment, view modelDeploymentReplicaView,
	qualification modelDeploymentInstanceQualification, found bool,
) workercore.ModelDeploymentGroupStatus {
	var state workercore.ModelDeploymentGroupState
	var reason string
	if found {
		state = workercore.ModelDeploymentGroupState(qualification.GroupState())
		reason = qualification.Reason()
	} else {
		state, reason = modelDeploymentPendingGroupState(md, view)
	}

	return workercore.ModelDeploymentGroupStatus{
		Role:       view.Role,
		Ordinal:    int32(view.Ordinal),
		Generation: string(modelDeploymentGenerationOf(view)),
		State:      state,
		Reason:     reason,
		Members:    modelDeploymentGroupMembers(view.Members),
		Serving:    modelDeploymentGroupServingDefault(md),
	}
}

// modelDeploymentPendingGroupState 返回本轮尚未评估的组的状态。
//
// 该状态直接取自各成员判定环节（member legs）自身的结论，并与资格评估使用同一辅助函数，
// 从而保证待定状态与已评估状态属于同一套分类体系：对于成员尚未上报的副本，无论本轮是否对其
// 进行过资格评估，呈现的状态都一致；若同一副本在不同路径下给出两种状态，就会在没有任何变更的
// 轮次中导致状态抖动。
//
// 此处无法获取前向探测环节（forward leg）的结果，因为读取它正是本轮未执行的探测。
// 因此，即使所有成员都已就绪，该组也不能标记为 Eligible：目前尚无法得出该分类结论，
// 状态会如实反映这一点，而不会伪称通过了从未进行的资格评估。
func modelDeploymentPendingGroupState(
	md *workercore.ModelDeployment, view modelDeploymentReplicaView,
) (workercore.ModelDeploymentGroupState, string) {
	role := modelDeploymentRoleByName(md, view.Role)
	if role == nil {
		return workercore.ModelDeploymentGroupStateUnknown,
			"the role this replica belongs to is not in the spec"
	}

	local := newModelDeploymentReplicaLocal(view, role)
	complete := local.membersComplete
	ready := local.membersReady
	switch {
	case complete.Verdict == modelDeploymentLegFailed:
		return workercore.ModelDeploymentGroupStateWithdrawn, complete.Reason
	case ready.Verdict == modelDeploymentLegFailed:
		return workercore.ModelDeploymentGroupStateWithdrawn, ready.Reason
	case complete.Verdict != modelDeploymentLegVerified:
		return workercore.ModelDeploymentGroupStateAwaitingMembers, complete.Reason
	case ready.Verdict != modelDeploymentLegVerified:
		return workercore.ModelDeploymentGroupStateAwaitingReady, ready.Reason
	default:
		return workercore.ModelDeploymentGroupStateUnknown,
			"the pass has not evaluated this group yet"
	}
}

// modelDeploymentGroupMembers 生成并发布单个组的成员列表。
//
// 成员按 seat 排序，因为 seat 是区分成员的依据；未声明 seat 的成员排在最后：它无法被定位，
// 若将其排在中间，就会宣称一种该组实际并不存在的顺序。
//
// 未声明 seat 或无法读取 seat 的成员，在发布时完全不设置 seat，而不会回退为 leader 的 seat：
// 其他地方的 seat 辅助函数所使用的默认值在那里是正确的，但在此处并不适用。
func modelDeploymentGroupMembers(members []*core.Pod) []workercore.ModelDeploymentGroupMemberStatus {
	published := make([]workercore.ModelDeploymentGroupMemberStatus, 0, len(members))
	for _, member := range members {
		status := modelDeploymentGroupMemberStatusOf(member)
		if seat, state := modelDeploymentMemberSeat(member); state == modelDeploymentSeatValid {
			claimed := int32(seat)
			status.Seat = &claimed
		}
		published = append(published, status)
	}

	slices.SortStableFunc(published, modelDeploymentGroupMemberOrder)

	return published
}

// modelDeploymentCollectiveMembers 生成并发布弹性 collective 的成员列表。
//
// 此处成员的 seat 是其准入序号（admission ordinal），而非它在所属副本成员中声明的 seat：
// collective 的成员是一个个完整的副本，它们在单个副本内部声明的 seat 并非 collective 的 seat，
// 若直接发布会导致所有成员的 seat 都变成 0。
//
// 该 ordinal 是 operator 自身记录的事实，而非推理引擎的运行时 rank：哪个成员持有哪个 rank
// 由引擎决定，operator 的编号从不隐含这一点。
//
// 无法读取 ordinal 的 worker 在发布时不设置 seat，处理方式与任何其他未声明 seat 的成员相同。
func modelDeploymentCollectiveMembers(members []*core.Pod) []workercore.ModelDeploymentGroupMemberStatus {
	published := make([]workercore.ModelDeploymentGroupMemberStatus, 0, len(members))
	for _, member := range members {
		status := modelDeploymentGroupMemberStatusOf(member)
		if ordinal, ok := modelDeploymentPodOrdinal(member); ok {
			seat := int32(ordinal)
			status.Seat = &seat
		}
		published = append(published, status)
	}

	slices.SortStableFunc(published, modelDeploymentGroupMemberOrder)

	return published
}

// modelDeploymentGroupMemberStatusOf 返回单个成员要发布的身份标识与就绪状态。
//
// 此处不设置 seat，因为成员声明的 seat 取决于它所属组的形态。
func modelDeploymentGroupMemberStatusOf(member *core.Pod) workercore.ModelDeploymentGroupMemberStatus {
	return workercore.ModelDeploymentGroupMemberStatus{
		Name:  member.Name,
		UID:   string(member.UID),
		Ready: modelDeploymentGroupMemberReady(member),
	}
}

// modelDeploymentGroupMemberOrder 先按 seat、再按身份标识对已发布的成员排序。
//
// seat 优先，因为它代表成员在组中的位置；当成员共享同一 seat 时，由身份标识决定顺序，
// 从而确保对同一成员集合执行两次遍历时发布的列表顺序一致。
func modelDeploymentGroupMemberOrder(a, b workercore.ModelDeploymentGroupMemberStatus) int {
	switch {
	case a.Seat != nil && b.Seat != nil:
		if *a.Seat != *b.Seat {
			if *a.Seat < *b.Seat {
				return -1
			}

			return 1
		}
	case a.Seat != nil:
		return -1
	case b.Seat != nil:
		return 1
	}

	return strings.Compare(a.UID, b.UID)
}

// modelDeploymentGroupMemberReady 返回单个成员发布的就绪状态。
//
// 它是三态值，且第三种状态并非“缺失”：从未上报过 Ready 条件的成员状态为 Unknown，这与
// 已被观测到但处于未就绪状态的成员是两种不同的事实。若状态无法区分这两者，就无法说明究竟
// 发生了哪种情况。
func modelDeploymentGroupMemberReady(pod *core.Pod) meta.ConditionStatus {
	for _, condition := range pod.Status.Conditions {
		if condition.Type != core.PodReady {
			continue
		}
		switch condition.Status {
		case core.ConditionTrue:
			return meta.ConditionTrue
		case core.ConditionFalse:
			return meta.ConditionFalse
		}

		return meta.ConditionUnknown
	}

	return meta.ConditionUnknown
}

// modelDeploymentGroupServingDefault 返回在应用 Router 响应之前，组默认携带的服务状态。
//
// 未配置 Router 的部署没有任何需要确认的对象，而配置了 Router 的部署则是尚未读取到结果。
// 这两者对应不同的状态，且都不是空字符串（空字符串根本不是有效的服务状态）。
func modelDeploymentGroupServingDefault(md *workercore.ModelDeployment) workercore.ModelDeploymentServingStatus {
	if md.Spec.Router == nil {
		return workercore.ModelDeploymentServingStatus{
			State: workercore.ModelDeploymentServingStateNotConfigured,
		}
	}

	return workercore.ModelDeploymentServingStatus{
		State: workercore.ModelDeploymentServingStateUnknown,
	}
}

// applyModelDeploymentGroupAnswer 为每个已发布的组解析 Router 的响应，并暂留因观测条件
// 未满足而需要等待的组。
//
// 该响应是本轮调谐的状态载体，恢复屏障（restoration）也会读取同一份响应，从而确保状态与标签
// 均基于同一时刻的同一份响应写入。
//
// 是否暂留由恢复屏障逐组判断：受保护的组若观测缺失或无效，则会等待有效观测；允许保留的组则
// 维持现有状态。
func (r *ModelDeploymentReconciler) applyModelDeploymentGroupAnswer(
	status *workercore.ModelDeploymentStatus,
	observation modelDeploymentPassObservation,
	md *workercore.ModelDeployment,
	collective *modelDeploymentReplicaView,
) {
	fresh := r.modelDeploymentServingFreshNow(observation.Serving)
	for i := range status.Roles {
		for j := range status.Roles[i].Groups {
			group := &status.Roles[i].Groups[j]

			// collective 并非 Router 所观测的组。Router 的响应承载的是 master 的事实，而 master 只是
			// collective 的一个成员而非 collective 本身，因此该事实绝不能被解读为关于 collective 的响应——
			// 即使当 collective 只有一个成员且其 generation 与 master 自身的 generation 相同时也是如此。
			if modelDeploymentGroupIsCollective(group, collective) {
				group.Serving, group.ServingReason = modelDeploymentGroupCollectiveServing(md)
				continue
			}

			fact, observed := observation.Serving.Groups[modelDeploymentGroupKeyOf(group)]

			group.Serving, group.ServingReason = modelDeploymentGroupServingAnswer(
				observation.Serving, fact, observed, fresh, md,
			)

			r.holdModelDeploymentGroup(group, observation, md, fresh)
		}
	}
}

// modelDeploymentGroupIsCollective 报告已发布的组是否为弹性 collective。
//
// 通过组自身的槽位判断：collective 是弹性角色的唯一逻辑副本，固定发布在 ordinal 0 处。
func modelDeploymentGroupIsCollective(
	group *workercore.ModelDeploymentGroupStatus, collective *modelDeploymentReplicaView,
) bool {
	if collective == nil {
		return false
	}

	return group.Role == collective.Role && group.Ordinal == int32(collective.Ordinal)
}

// modelDeploymentGroupCollectiveServing 返回弹性 collective 发布的服务状态。
//
// 它取默认值，因为 Router 的响应针对的是 master 而非 collective；同时它不携带计数：只有确认状态
// 才携带计数，而 collective 永远不会被确认。
//
// 配置了 Router 时，reason 会指明缺失的观测，以便读取到 Unknown 服务状态的调用方能够将其与
// “Router 已观测 collective 但未上报任何内容”区分开来。
func modelDeploymentGroupCollectiveServing(
	md *workercore.ModelDeployment,
) (workercore.ModelDeploymentServingStatus, string) {
	if md.Spec.Router == nil {
		return modelDeploymentGroupServingDefault(md), ""
	}

	return modelDeploymentGroupServingDefault(md), modelDeploymentReasonGroupUnobserved
}

// holdModelDeploymentGroup 暂留因 Router 观测原因需要等待的组。
//
// 已被资格评估暂留的组保留其自身的状态与原因：观测原因绝不会覆盖导致该组被暂留的评估环节。
//
// 不受保护的组不会被暂留：兼容类型保留其原有行为，且普通的单成员 Server 没有需要保护的组。
func (r *ModelDeploymentReconciler) holdModelDeploymentGroup(
	group *workercore.ModelDeploymentGroupStatus,
	observation modelDeploymentPassObservation,
	md *workercore.ModelDeployment,
	fresh bool,
) {
	if group.State != workercore.ModelDeploymentGroupStateEligible {
		return
	}
	if md.Spec.Router == nil {
		return
	}
	role := modelDeploymentRoleByName(md, group.Role)
	if role == nil {
		return
	}
	qualification, found := modelDeploymentGroupQualificationOf(
		observation.Qualifications, group.Role, int(group.Ordinal), group.Generation,
	)
	if !found {
		return
	}
	if modelDeploymentGroupProtectionOf(md, role, qualification.View) != modelDeploymentGroupProtected {
		return
	}

	fact, observed := observation.Serving.Groups[modelDeploymentGroupKeyOf(group)]
	if modelDeploymentGroupRestores(observed, fresh, fact) {
		return
	}

	// 若组内有新近成员尚未就绪，则由该成员暂留该组，且状态应标明对应的环节。
	// AwaitingObservation 表示因 Router 观测而暂留的组，但暂留本组的并非观测而是新近成员状态。
	// reason 中已经指明了该成员，而 Router 自身的原因则保留在其对应的服务响应中。
	if observed && fact.MemberNotReady {
		group.State = workercore.ModelDeploymentGroupStateAwaitingReady
		group.Reason = modelDeploymentGroupObservationReason(observation.Serving, fact, observed)

		return
	}

	group.State = workercore.ModelDeploymentGroupStateAwaitingObservation
	group.Reason = modelDeploymentGroupObservationReason(observation.Serving, fact, observed)
}

// modelDeploymentGroupRestores 报告 Router 的响应是否允许该组继续提供服务。
//
// 这是在组级别评估的恢复屏障判定：当组拥有针对自身的最新、完整且一致的观测时即可恢复；
// 当观测丢失时，处于 Eligible 且未发生变更的组可维持现有状态。
func modelDeploymentGroupRestores(observed, fresh bool, fact modelDeploymentGroupServing) bool {
	if observed && fresh && fact.SnapshotValid {
		return true
	}

	return observed && fact.Retained
}

// modelDeploymentGroupServingAnswer 返回单个组发布的服务状态响应。
//
// 组自身的观测事实即为其响应，且只有最新的事实才能用于确认：已经过期的视图不能作为确认依据，
// 发布其中携带的计数将等于上报一个 operator 已不再拥有的观测。
//
// 响应中未包含的组不能由部署级的响应来确认：聚合结果仅代表其实际统计到的端点，对未提及的组
// 不作任何说明。此类组的状态为 Unknown，或者在没有配置 Router 的情况下为 NotConfigured。
func modelDeploymentGroupServingAnswer(
	answer ServingAnswer, fact modelDeploymentGroupServing, observed, fresh bool,
	md *workercore.ModelDeployment,
) (workercore.ModelDeploymentServingStatus, string) {
	switch {
	case observed && !fresh:
		return workercore.ModelDeploymentServingStatus{
			State: workercore.ModelDeploymentServingStateUnknown,
		}, modelDeploymentReasonStaleGroupServing
	case observed:
		serving := workercore.ModelDeploymentServingStatus{State: fact.State}
		if serving.State == "" {
			serving.State = workercore.ModelDeploymentServingStateUnknown
		}
		// 只有确认状态才携带计数。其他状态表示未能确立计数，API 中将其表示为缺失值（nil）而非零。
		if serving.State == workercore.ModelDeploymentServingStateConfirmed {
			endpoints := fact.Endpoints
			serving.Value = &endpoints
		}

		return serving, fact.InvalidReason
	case md.Spec.Router == nil:
		return workercore.ModelDeploymentServingStatus{
			State: workercore.ModelDeploymentServingStateNotConfigured,
		}, ""
	default:
		return workercore.ModelDeploymentServingStatus{
			State: workercore.ModelDeploymentServingStateUnknown,
		}, modelDeploymentGroupObservationReason(answer, fact, observed)
	}
}

// modelDeploymentGroupObservationReason 指明 Router 关于单个组的响应中缺失的内容。
//
// 组自身的事实指明其自身的缺陷，部署级响应则指明整体的缺陷。两者均不能为空：若读取方发现
// 存在服务状态却没有原因，就无法区分是未观测到 Router，还是已观测到但 Router 未上报任何内容。
func modelDeploymentGroupObservationReason(
	answer ServingAnswer, fact modelDeploymentGroupServing, observed bool,
) string {
	if observed && fact.InvalidReason != "" {
		return fact.InvalidReason
	}
	if answer.Reason != "" {
		return answer.Reason
	}

	return modelDeploymentReasonGroupUnobserved
}

// modelDeploymentGroupKeyOf 返回用于索引 Router 逐组事实的键。
//
// 该键代表组自身的身份，归档在其他成员集合名下的事实不属于当前组的响应：成员已经变更，
// 因此先前的观测描述的是不再属于当前组的 Pod。
func modelDeploymentGroupKeyOf(group *workercore.ModelDeploymentGroupStatus) modelDeploymentGroupKey {
	return modelDeploymentGroupKey{
		Role:       group.Role,
		Ordinal:    int(group.Ordinal),
		Generation: modelDeploymentMembershipGeneration(group.Generation),
	}
}

// modelDeploymentGroupQualificationOf 查找本轮对指定组得出的资格评估结果。
//
// 匹配依据是组自身的槽位（role 与 ordinal）以及 generation：因为关于某个组的结论只针对该组
// 本身，而非碰巧共享同一 role 的其他组；同时，基于已变更的成员集合所做的资格评估描述的是
// 不再属于当前组的 Pod。
func modelDeploymentGroupQualificationOf(
	qualifications []modelDeploymentInstanceQualification,
	role string, ordinal int, generation string,
) (modelDeploymentInstanceQualification, bool) {
	for _, qualification := range qualifications {
		if qualification.View.Role != role {
			continue
		}
		if qualification.View.Ordinal != ordinal {
			continue
		}
		if string(qualification.Generation) != generation {
			continue
		}

		return qualification, true
	}

	return modelDeploymentInstanceQualification{}, false
}

// modelDeploymentRoleByName 查找状态中指定名称对应的 role。
func modelDeploymentRoleByName(md *workercore.ModelDeployment, name string) *workercore.ModelDeploymentRole {
	for i := range md.Spec.Roles {
		if md.Spec.Roles[i].Name == name {
			return &md.Spec.Roles[i]
		}
	}

	return nil
}

// applyModelDeploymentGroupQualificationTimes 确定每个组最近一次变为 Eligible 的时间点。
//
// 该时间点仅记录转换到“已发布 Eligible”的时刻，别无其他含义。因 Router 观测而被暂留的组尚未
// 作为 Eligible 发布，因此不会生成自己的时间点：否则该时间点将指向一次从未发生过的状态转换。
//
// 被暂留的组会保留此前已获得的时间点，因为暂留源于 Router 观测，而此前获得该时间点的资格评估
// 确实发生过，历史记录应予以保留。
//
// 重新回到已发布 Eligible 状态的组会记录新的时间点，因为该时间点标识的是组作为 Eligible 发布的
// 时刻，而这一时刻已经更新。
//
// 新的 generation 不继承任何时间点：它所对应的成员集合从未通过资格评估，继承旧时间点会产生
// 误导。
//
// 该时间点在组首次通过资格评估时立即记录。推迟到后续轮次记录会导致状态在首次通过后的下一轮发生
// 变动，而在 spec 未变的情况下变动状态会触发多余的状态写入。
func applyModelDeploymentGroupQualificationTimes(
	status *workercore.ModelDeploymentStatus,
	previous []workercore.ModelDeploymentRoleStatus,
	now meta.Time,
	collective *modelDeploymentReplicaView,
) {
	for i := range status.Roles {
		for j := range status.Roles[i].Groups {
			group := &status.Roles[i].Groups[j]

			// collective 不继承其所占槽位上的任何历史记录。此前发布在该槽位上的条目是该角色的一个副本
			// （即 master 自身的组），其时间点属于那个组。当只有一个 worker 时两者共享同一个 generation，
			// 结算逻辑若按槽位查找先前的条目并复制时间点，就会把曾经 Eligible 的组的历史赋予从未 Eligible 的
			// collective。
			if modelDeploymentGroupIsCollective(group, collective) {
				group.LastQualificationTime = nil
				continue
			}

			prior := modelDeploymentGroupStatusOf(previous, group)
			switch {
			case prior == nil:
				// 该组首次发布。
				if group.State == workercore.ModelDeploymentGroupStateEligible {
					group.LastQualificationTime = &now
				}
			case prior.Generation != group.Generation:
				// 新的成员集合独立生成自己的时间点，不继承任何历史时间点。
				if group.State == workercore.ModelDeploymentGroupStateEligible {
					group.LastQualificationTime = &now
				}
			case group.State == workercore.ModelDeploymentGroupStateEligible &&
				prior.State != workercore.ModelDeploymentGroupStateEligible:
				// 该组重新回到已发布 Eligible 状态。
				group.LastQualificationTime = &now
			case prior.LastQualificationTime != nil:
				// 在暂留和撤销期间保留历史记录。
				group.LastQualificationTime = prior.LastQualificationTime.DeepCopy()
			case group.State == workercore.ModelDeploymentGroupStateEligible:
				// 该组处于 Eligible 状态，且其时间点此前从未被记录。
				group.LastQualificationTime = &now
			}
		}
	}
}

// modelDeploymentGroupStatusOf 查找某个组此前已发布的条目。
//
// 匹配依据是组自身的槽位（role 与 ordinal），而不是 generation：generation 是用于与找到的条目
// 进行比对的字段，若按 generation 查找，将永远找不到成员已发生变更的组条目。
func modelDeploymentGroupStatusOf(
	previous []workercore.ModelDeploymentRoleStatus,
	group *workercore.ModelDeploymentGroupStatus,
) *workercore.ModelDeploymentGroupStatus {
	for i := range previous {
		if previous[i].Name != group.Role {
			continue
		}
		for j := range previous[i].Groups {
			if previous[i].Groups[j].Ordinal == group.Ordinal {
				return &previous[i].Groups[j]
			}
		}
	}

	return nil
}

// observeModelDeploymentEndpointEligibility 在部署级别上报本轮所见副本的整组健康谓词结论。
//
// 没有任何资格评估结果意味着尚无观测，此时应保持为 Unknown 而非变为 False：False 会被解读为
// operator 作出了取消资格的判定，而实际上没有任何轮次作出过该判定。为 nil 的各 role 计数与该条件
// 分别在两个层级表达相同的事实。
//
// 优先级规则保证了该条件的可信度。明确的吊销（revocation）优先于缺失观测，因为 operator 只要能
// 观测到异常组就必须明确指出，即便此时无法观测到正常组；Unsupported 优先于普通暂留，因为
// 完全没有组谓词的引擎形态与本轮某个环节暂时无法读取是两种不同的情况，调用方后续采取的措施也
// 截然不同。
func observeModelDeploymentEndpointEligibility(
	holder *workercore.ModelDeployment, qualifications []modelDeploymentInstanceQualification,
) {
	revoked, unsupported, held, qualified, observed := 0, 0, 0, 0, 0
	heldLegs := map[modelDeploymentQualificationLegName]bool{}
	for _, qualification := range qualifications {
		// 尚未有任何上报的副本不计入此结论。这是部署在创建副本后、首次收到就绪报告前所处的状态，将其计入
		// 会发布一个 operator 尚未作出的暂留判定。排除这类副本也能避免创建后的下一轮调谐变动该条件——
		// 若在 spec 未变的情况下变动此值，每一轮都会触发一次状态写入。
		if !qualification.Observed {
			continue
		}
		observed++
		// 首先检查 Eligible，因为它是四者中唯一不会由任何失败或缺失观测产生的结果，且所有评估环节均已
		// 验证通过的副本应当被认定为合格而非暂留——一个已激活且没有任何异常的副本并不在等待任何条件。
		//
		// 未激活的副本按原因进一步区分。Unsupported 表示当前形态不存在可用的观测机制，这是部署的固有
		// 事实；Unknown 表示本轮尝试了观测但未能完成绑定，这是本轮的临时事实，下一轮可能会有所变化。
		// 将两者混为一谈会向操作者错误地传递“该组永久无法验证”的信号，而实际上它片刻之后就可能恢复响应。
		switch {
		case qualification.Eligible():
			qualified++
		case qualification.HasFailure():
			revoked++
		case qualification.GroupForward.State == modelDeploymentGroupForwardUnsupported:
			unsupported++
		default:
			held++
			for _, leg := range qualification.HeldLegs() {
				heldLegs[leg] = true
			}
		}
	}

	// 同一状态只用同一条消息。尚无副本的部署和副本尚未上报的部署对应同一结果——尚无观测数据——因此这里有意
	// 使用相同的措辞：condition 访问器会在消息变化时更新 LastTransitionTime，如果对同一状态使用两种表述，
	// 即使部署从未发生变化，每次处理也会不断改写状态。
	if observed == 0 {
		ModelDeploymentConditionEndpointEligibility.Unknown(holder,
			modelDeploymentReasonNotObserved,
			"no replica has reported its readiness, so no endpoint has been qualified")

		return
	}

	switch {
	case revoked > 0:
		ModelDeploymentConditionEndpointEligibility.False(holder,
			modelDeploymentReasonEndpointsRevoked,
			strconvx.Itoa(revoked)+" of "+strconvx.Itoa(observed)+
				" observed replicas fail a whole-group health leg and have been disqualified")
	case unsupported > 0:
		ModelDeploymentConditionEndpointEligibility.Unknown(holder,
			modelDeploymentReasonEndpointsUnsupported,
			strconvx.Itoa(unsupported)+" of "+strconvx.Itoa(observed)+
				" observed replicas have no verifiable engine group-forward predicate; their eligibility is held")
	case held > 0:
		ModelDeploymentConditionEndpointEligibility.Unknown(holder,
			modelDeploymentReasonEndpointsHeldUnverified,
			strconvx.Itoa(held)+" of "+strconvx.Itoa(observed)+
				" observed replicas are held on unverified legs "+heldLegNames(heldLegs))
	default:
		ModelDeploymentConditionEndpointEligibility.True(holder,
			modelDeploymentReasonEndpointsQualified,
			"every observed replica's whole group is qualified")
	}
}

// modelDeploymentQualificationsDecided 在同一组副本上遵循与
// observeModelDeploymentEndpointEligibility 相同的优先级：明确撤销判定为 False，全部合格的轮次
// 判定为 True，而 Unsupported、暂缓以及无观测结果均不做判定。status holder 只是一个拷贝，因此
// 收敛路径从这里读取本轮的判定，而非读取尚未持久化的 condition。
func modelDeploymentQualificationsDecided(
	qualifications []modelDeploymentInstanceQualification,
) bool {
	revoked, qualified, observed := 0, 0, 0
	for _, qualification := range qualifications {
		if !qualification.Observed {
			continue
		}
		observed++
		switch {
		case qualification.Eligible():
			qualified++
		case qualification.HasFailure():
			revoked++
		}
	}

	// 明确失败时判定为 False。否则，本轮仅在所有已观测到的副本都合格时才作出判定，这一标准与记录
	// Unknown condition 时所使用的门槛一致。
	return revoked > 0 || (observed > 0 && qualified == observed)
}

// heldLegNames 将未验证的环节名称格式化为 condition 消息，并排序以确保同一受阻组在连续两轮
// 处理中报告相同的理由。
func heldLegNames(held map[modelDeploymentQualificationLegName]bool) string {
	names := make([]string, 0, len(held))
	for name := range held {
		names = append(names, string(name))
	}
	slices.Sort(names)
	if len(names) == 0 {
		return "(none)"
	}

	return strings.Join(names, ", ")
}

// modelDeploymentPodRole 读取副本所属的角色。
func modelDeploymentPodRole(pod *core.Pod) string {
	return pod.Labels[modelDeploymentLabelKeyComponent]
}

// observeModelDeploymentQuota 报告部署的每一个副本是否都持有配额。
//
// 每个副本都有自己的 Workload 并独立预留资源，因此 `True` 是针对整个集合而非排在最前面的某个
// Workload 的回答，下面的各个分支正是为了保证这一点：半个部署持有配额并不等于整个部署持有配额，
// 此前集群中就曾发现过将幸存副本的结果误报给自身预留已丢失副本的缺陷。
//
// 它直接读取这些 Workload 自身的 condition，而不是查询准入网关（admission gate）。Workload 一旦
// 被准入，网关就会停止对其评估，因此任何来自网关的结果都只反映准入那一刻的状态而不再更新——之后
// 被抢占的 Workload 仍会被读作已预留。
//
// 分支的顺序至关重要，PodGroupIncomplete 必须排在“尚无 workload”之前。缺员的副本从构造上就不
// 会组合出任何 Workload——Kueue 会拒绝为其组合，并在 Pod 上给出不可重试的说明——因此这两种状态
// 在这里看起来完全相同，实际含义却截然相反：前者片刻后就会自行解决，后者则会让部署带着被阻塞的
// Pod 和空空如也的 `kubectl get workloads` 永远停在那里。设置这个原因正是为了避免把后者误报成前者。
func observeModelDeploymentQuota(
	md *workercore.ModelDeployment, pods []core.Pod, wls []*kueue.Workload,
	wlByReplica map[types.UID]*kueue.Workload, holder *workercore.ModelDeployment,
) {
	// 优先报告已驻停（Parked）的部署，因为下面其他所有答案都会错误地描述它。它的各个组是完整的，且其
	// Workload 已被停用（deactivated），而这里的其余逻辑会把这种状态误读为“等待准入”——一旦上限
	// 触发，这与事实恰好相反，并且会让运维人员白白等待一个永远不会发生的结果。
	//
	// 该状态在此处通过观测得出，而不是由将其驻停的控制器直接写入。Status 在每轮协调中都完全由这一个
	// 函数根据观测状态重建，若存在第二个写入方，一旦二者意见不一致就会留下过时字段。上限是在 Workload
	// 上度量的；这里只读取该度量在 Workload 上留下的结果。
	if parked := parkedModelDeploymentWorkloads(wls); len(parked) > 0 {
		ModelDeploymentConditionQuotaReserved.False(holder, "Parked", fmt.Sprintf(
			"the deployment's groups could not all be placed, so %d of its workloads are deactivated "+
				"and no longer ask for quota: %s. An identical re-apply does not clear this — free the "+
				"capacity the deployment needs and reactivate them, or delete the deployment and "+
				"create it again",
			len(parked), strings.Join(parked, ", "),
		))

		return
	}

	// 部分被抢占的部署必须在任何其他等待状态之前报告，它反映的是与后者完全不同的事实：其中一部分仍处于
	// 准入并服务状态，为一个不再完整的部署占用着加速器，而其余部分则在等待已被夺走的配额。
	//
	// 其他所有分支都会错误地描述这种状态，其中一个甚至会给出完全错误的结论。下面的多组等待分支最后会写
	// “在整套副本都能运行之前没有任何角色被准入”；而在这里确实有角色处于准入状态，这正是该状态重要
	// 的原因。完整性分支则会把被抢占的组仅仅报告为副本数量不足，误导运维人员去排查调度问题，而不是去
	// 查找抢走配额的更高优先级 Workload。
	//
	// 分类信息由 Reason 而非 Message 承载。区分当前状态是部分被抢占还是普通等待，决定了运维人员的
	// 后续动作——是继续等待，还是去排查是什么抢占了它——如果调用方必须通过子串匹配消息来做此判断，
	// 那它依赖的就不是稳定的契约。
	//
	// Workload 在抢占后依然保留，这是整个状态可被读取的前提，且该行为是从 Kueue 实现中确认而非凭空
	// 假设的：Kueue 的抢占路径会以 Preempted reason 调用 workload.Evict，并在准备步骤中设置
	// Preempted condition，而 Evict 只会 patch status——该路径上没有任何操作会删除对象。因此
	// 两个 condition 都会保留在对象上供读取。
	//
	// 唯一仍需假设的是副本本身后续会发生什么，而将该分支放在完整性检查之前，正是为了确保无论答案如何
	// 都不会影响报告结果。如果被抢占服务组的 Pod 被移除，这一顺序可以避免该组被误报为仅仅缺少副本；
	// 如果这些 Pod 保留下来，这一顺序则不会产生额外影响。无论哪种情况，报告都是正确的，该假设只决定
	// 这一顺序是否是必要约束。
	lost, kept, lostReplicas := modelDeploymentPreemptedInPart(md, pods, wlByReplica)
	// 其他异常情况仍然会被报告，并且抢占信息会一并向下传递。当某个角色被抢占且没有其他兄弟角色幸存时，
	// 并不属于 PreemptedInPart 所指的状态——此时没有任何资源被继续占用——但“有其他任务夺走了某个
	// 副本的配额”这一事实若被丢弃，结果就会误导读者去排查错误的方向。
	//
	// 在该部署存在副本被抢占时可能给出的每一种结果都会附带该信息，下面列出不附带的几种情况及其原因，
	// 因为“有意不附带”和“遗漏”在代码里看起来是一样的：
	//
	//   - NoReplicas 在观测到抢占时不可能到达。副本的 Workload 是通过该副本的所有权关系解析的，
	//     因此没有 Pod 的部署无法解析出任何 Workload，也就无从观测到抢占。
	//   - NoQueueInReservedNamespace 从另一个角度同样不可能：保留 Namespace 中没有
	//     LocalQueue，因此其中的部署从未被调度，也从未持有配额，自然无从失去配额。
	//   - Parked 则是覆盖而非遗漏。这些 Workload 已被停用，不再申请任何配额，因此此前是谁夺走了
	//     部分配额已不再影响读者接下来的操作。并且它在计算该信息之前就已返回，所以看起来像未附带。
	//
	// AllReplicasTerminating 确实会附带该信息，它看似不可达但实际上可达：无论副本是否正在终止，
	// 其 Workload 都会被解析，因此 Pod 正在退出的被抢占副本正好会落入这一分支。
	taken := modelDeploymentPreemptionNote(lost)
	if len(lost) > 0 && len(kept) > 0 {
		// 损失的具体影响取决于部署形态，因此需要通过计算而非直接断言。每种角色各损失部分副本只会削减容量；
		// 而某一种角色失去全部副本则会彻底丧失该角色的功能，运维人员针对这两种情况的下一步操作并不相同。
		//
		// 两种措辞都不应说部署已经停止服务，之前的旧表述正是错在这里。prefill 角色提供入口吞吐能力，而
		// decode 角色的节点可以独立完成请求，因此保留了 decode 的部署只是处于降级状态，仍在提供服务。
		// 哪种损失会让部署丧失应答能力取决于各角色及其前端路由，而非该分支仅凭配额就能断定的问题——并且
		// phase 已经通过跨角色汇总副本数而不区分类型来反映可用性了。
		//
		// 措辞不能暗示该类型曾经被准入过。只要某一类型没有任何副本处于准入状态就会被计入此处，而一个从未
		// 被准入过的角色——例如副本数仍低于声明值、Kueue 尚未为其缺失副本组合 Workload 的角色——也会
		// 走到这个分支，因为对兄弟类型副本抢占的判断排在完整性检查之前。
		effect := "the deployment still serves, without the capacity those replicas provided"
		if unserved := modelDeploymentKindsWithoutAdmittedReplica(md, pods, wlByReplica); len(unserved) > 0 {
			effect = fmt.Sprintf(
				"the deployment has no admitted replica of role kind %s at all, which is the loss of "+
					"that role rather than of capacity",
				strings.Join(unserved, ", "),
			)
		}

		ModelDeploymentConditionQuotaReserved.False(holder, modelDeploymentReasonPreemptedInPart,
			fmt.Sprintf(
				"a higher-priority workload reclaimed the quota of %d of this deployment's %d replicas: "+
					"the replicas of roles %s. The replicas of roles %s are still admitted and hold "+
					"their accelerators, and %s. They are released only by deleting the deployment "+
					"or by the reclaimed replicas being admitted again once the capacity returns",
				lostReplicas, modelDeploymentDeclaredReplicas(md), strings.Join(lost, ", "),
				strings.Join(kept, ", "), effect,
			))

		return
	}

	if len(pods) == 0 {
		ModelDeploymentConditionQuotaReserved.Unknown(holder, "NoReplicas",
			"no replica has been created yet")

		return
	}

	// 以副本（replica）而非 Pod 为单位，因为下面每条消息都将该数值称为“replicas”，并与同样以副本为
	// 单位的声明数量进行比较。只有当一个副本对应一个 Pod 时，两者才相等。
	live := len(modelDeploymentGroupPodsByReplica(pods))
	all := len(modelDeploymentGroupPodsIncludingDeparting(pods))

	// 所有副本都在终止并不等同于“全部已预留”。上面的提前返回防护的是空列表，而这里防护的是过滤后的
	// 空结果，两者是不同的“空”。重建发布（recreate rollout）或组重建会传给本函数一个非空切片，但
	// 其中的每个成员都会被跳过；如果直接对剩余集合下结论，就会对一个刚刚被清空的集合做出错误保证。
	//
	// 这里设为 Unknown 而非 False：虽然没有副本持有配额，但也没有任何副本的配额申请被拒绝。
	if live == 0 {
		// 这里同样需要附带抢占信息，而且这是最关键的一个分支：从 Pod 侧来看，被回收的组正表现为所有副本
		// 都在退出，如果只说“所有副本都在终止”而不说明原因，就会误导读者去排查是谁删除了它们。
		ModelDeploymentConditionQuotaReserved.Unknown(holder, "AllReplicasTerminating", fmt.Sprintf(
			"all %d replicas are terminating, so none holds quota to report on%s", all, taken,
		))

		return
	}

	// 计数按角色分别进行，消息中指出的队列也是如此。一旦单副本组被观测到，每个副本就会组合出各自的
	// Workload，因此某个副本数不足声明值的角色中会存在 Kueue 尚未组合任何 Workload 的副本，而
	// 它的兄弟角色可能已经全部准入；比较整个部署的总和无法准确反映任何一方：只要有一个兄弟角色缺员，
	// 完整的角色也会被误判为不足，还会报告一个并非真正阻塞任务的队列。
	//
	// ClusterQueue 以 InstanceType 命名，因此拒绝信息指向的队列可以直接从 spec 读取，而无需再行
	// 解析：入口标签指定的 LocalQueue 正是由它派生而来。消息中会在该队列旁同时写出角色名，因为队列
	// 与角色在标识上并不等价：两个角色可能使用同一个 InstanceType，此时队列代表两个角色共享的资源池，
	// 无法区分具体是哪一个角色，而角色名能精确指出究竟是哪个角色缺员。
	// 副本通过本 status 中其他所有统计字段所使用的同一标签来归属到对应角色，因此不会出现某个 Pod 在
	// 一处被计入而在另一处不被计入的情况——早于该标签创建或仍在构建中的副本也会归入其所属角色，而不会
	// 遗漏不计。
	// 统计的是完整副本数而非 Pod 数，这一区分保证了一个副本包含多个成员时该分支仍能正常工作。
	// group.TotalCount 是副本数量，若将 Pod 总数与之比较，会把包含两个四 Pod 副本的角色算成“2 个
	// 中有 8 个”——该 condition 永远不会触发，它本应标识的状态也恰恰在最可能出现的地方被漏报：缺少
	// 成员的组正是 Kueue 不会为其组合 Workload 的情况。
	views := modelDeploymentGroupPodsByReplica(pods)
	groups := modelDeploymentPodGroups(md)
	for _, group := range groups {
		var alive int
		for _, view := range views {
			// 使用副本自身的组总数，这样正在被编辑的角色依然能够计入当前正在提供服务的副本。
			if view.Role == group.Role &&
				modelDeploymentDeployedReplicaShape(view).modelDeploymentReplicaIsWhole() {
				alive++
			}
		}

		if want := int(group.TotalCount); alive < want {
			ModelDeploymentConditionQuotaReserved.False(holder, modelDeploymentReasonPodGroupIncomplete,
				fmt.Sprintf(
					"%d of %d replicas role %q declares exist, so Kueue composes no workload for the "+
						"missing ones at all and there is nothing in cluster queue %q to hold quota "+
						"for them%s",
					alive, want, group.Role, group.InstanceType, taken,
				))

			return
		}
	}

	// 每个角色都已达到声明的副本数，因此接下来只需报告是否每个副本都持有配额。
	//
	// 结论针对所有副本，而非第一个 Workload。跨越多个 InstanceType 的部署为每个副本创建一个
	// Workload，若按排序最靠前的那个来代表整个角色，就会在部署一半没有配额的情况下仍报告已持有配额。
	// 集群实测中曾出现过：一个组已预留配额，其兄弟组的队列仍被阻塞，但该 condition 却显示 Reserved，
	// 且消息中写着部署的总副本数——结果在两个方面同时出错。
	// 单角色部署只有一个队列，消息措辞也明确体现这一点；多角色部署没有单一队列可指代，把第一个角色的
	// 队列说成整个部署的队列是不准确的。因此，仅在分支已确认只有单一角色时才读取队列，而多角色分支则
	// 直接指明其实际涉及的角色。
	queue := md.Spec.Roles[0].InstanceType

	if uncomposedRoles, uncomposed := modelDeploymentRolesWithUncomposedReplicas(md, pods, wlByReplica); uncomposed > 0 {
		if kuberess.IsReservedNamespace(md.Namespace) {
			ModelDeploymentConditionQuotaReserved.False(holder, "NoQueueInReservedNamespace", fmt.Sprintf(
				"the deployment is in reserved namespace %q and will never be scheduled", md.Namespace,
			))

			return
		}
		if len(groups) == 1 {
			ModelDeploymentConditionQuotaReserved.Unknown(holder, "AdmissionInFlight", fmt.Sprintf(
				"%d of this deployment's %d replicas have no workload yet in cluster queue %q%s",
				uncomposed, modelDeploymentDeclaredReplicas(md), queue, taken,
			))

			return
		}
		ModelDeploymentConditionQuotaReserved.Unknown(holder, "AdmissionInFlight", fmt.Sprintf(
			"%d of this deployment's %d replicas have no workload yet: the replicas of roles %s%s",
			uncomposed, modelDeploymentDeclaredReplicas(md),
			strings.Join(uncomposedRoles, ", "), taken,
		))

		return
	}

	waitingRoles, waiting := modelDeploymentRolesWithUnquotaedReplicas(md, pods, wlByReplica)
	if waiting == 0 {
		// 单角色场景的措辞尽量与当前运维人员熟悉的该 condition 文本保持一致，因为在单角色下这本就是
		// 完全准确的。
		if len(groups) == 1 {
			ModelDeploymentConditionQuotaReserved.True(holder, "Reserved", fmt.Sprintf(
				"all %d replicas have quota reserved in cluster queue %q", live, queue,
			))

			return
		}
		ModelDeploymentConditionQuotaReserved.True(holder, "Reserved", fmt.Sprintf(
			"all %d of this deployment's replicas have quota reserved", live,
		))

		return
	}
	downstream := modelDeploymentInadmissibleWorkloadDetails(pods, wlByReplica)

	if len(groups) == 1 {
		ModelDeploymentConditionQuotaReserved.False(holder, "Pending", fmt.Sprintf(
			"%d of this deployment's %d replicas are waiting for quota in cluster queue %q%s%s",
			waiting, modelDeploymentDeclaredReplicas(md), queue, taken, downstream,
		))

		return
	}
	// 阻塞说明针对的是仍在等待的副本，而非整个部署。扩容操作走到这一分支时，原本运行中的副本仍处于
	// 准入状态，只有新增副本在排队，因此如果说“没有任何副本被准入”，不仅与旁边的计数相矛盾，还会在
	// 部署实际仍在服务时误导运维人员以为没有任何副本在服务。
	ModelDeploymentConditionQuotaReserved.False(holder, "Pending", fmt.Sprintf(
		"%d of this deployment's %d replicas are waiting for quota: the replicas of roles %s. Those "+
			"replicas are admitted only once the whole set can run%s%s",
		waiting, modelDeploymentDeclaredReplicas(md), strings.Join(waitingRoles, ", "), taken, downstream,
	))
}

// modelDeploymentInadmissibleWorkloadDetails 在保留 Kueue 原始说明的同时，补充了运维人员定位
// 受影响组所需的角色和副本标识。Kueue 的 reason 和 message 属于下游描述文本，并非稳定的
// ModelDeployment reason 词汇表，因此仅做展示，绝不解析或提升为新的 condition reason。
func modelDeploymentInadmissibleWorkloadDetails(
	pods []core.Pod, wlByReplica map[types.UID]*kueue.Workload,
) string {
	type detail struct {
		role, workload, message string
		ordinal                 int
	}
	var details []detail
	for _, view := range modelDeploymentGroupPodsByReplica(pods) {
		if !view.Seated {
			continue
		}
		wl := modelDeploymentReplicaWorkload(view, wlByReplica)
		if wl == nil || kubeapistatus.ConditionType(kueue.WorkloadQuotaReserved).IsTrue(wl) {
			continue
		}
		for _, condition := range wl.Status.Conditions {
			if condition.Type == kueue.WorkloadQuotaReserved && condition.Message != "" {
				details = append(details, detail{
					role: view.Role, ordinal: view.Ordinal, workload: wl.Name, message: condition.Message,
				})
				break
			}
		}
	}
	slices.SortFunc(details, func(a, b detail) int {
		if byRole := strings.Compare(a.role, b.role); byRole != 0 {
			return byRole
		}
		if a.ordinal != b.ordinal {
			return a.ordinal - b.ordinal
		}
		return strings.Compare(a.workload, b.workload)
	})
	if len(details) == 0 {
		return ""
	}
	parts := make([]string, 0, len(details))
	for _, item := range details {
		parts = append(parts, fmt.Sprintf("role %q replica %d workload %q: %s",
			item.role, item.ordinal, item.workload, item.message))
	}
	return "; downstream admission: " + strings.Join(parts, "; ")
}

// modelDeploymentReplicaWorkloads 为本轮观测到的每个副本解析唯一拥有它的 Workload。
//
// 仅通过所有权关系读取 Workload，不依赖任何其他途径。本 operator 为组生成的名称会随 spec 变化，
// 而组合 Workload 的组件也会按自身规则为其命名——目前是 Kueue，若底层以后更换也可能是其他组件——
// 因此靠重新拼接名称来查找的做法在两者规则分叉的那天不会报错，而是直接找不到对象并报告空状态。
// 所有权是双方都会维护的唯一关系，因此是这里唯一使用的匹配依据。
//
// 按副本而非按角色建立条目，因为现在角色下的每个副本各自对应一个 Workload，而 status 要回答的
// 每一个问题——有多少副本持有配额、落在了哪些 flavor 上、哪个角色的哪个槽位空缺——都是针对副本的。
// 如果按角色视图从拥有其任意 Pod 的 Workload 中取值，就无法区分丢失了一个副本 Workload 的角色
// 与保留全部 Workload 的角色：两种情况都会命中查找，进而把幸存副本的结果套用到缺失副本上。
//
// 该索引覆盖所有 Pod（包括正在终止的 Pod），因为持有退出中副本的 Workload 仍然持有其配额和
// finalizer，不应计入退出中副本的读取方会在各自逻辑分支中跳过它，而不是由索引预先替它们决定。
//
// 传入的 Workload 按名称排序，因此重建期间短暂同时被两个 Workload 拥有的副本在每次处理中都会
// 解析到同一个 Workload，不会在旧对象排空期间来回切换。
//
// 所有权关系只做一次反向索引，而不是按 Pod 逐个搜索。Workload 自身记录了成员列表，正向读取这些
// 引用即可在一次遍历中构建完整映射；若对每个 Pod 逐一查询哪个 Workload 认领了它，则每个 Pod
// 都要重走一遍所有 Workload 的引用，而部署的 Pod 数和 Workload 数都会随规模同步增长。只保留
// 第一个认领该 UID 的 Workload 正是为了维持上述按名称排序的确定性：遍历本身按该顺序访问 Workload，
// 因此存在归属竞争的副本所落定的条目与逐 Pod 搜索最先停下的那个完全一致。
func modelDeploymentReplicaWorkloads(
	pods []core.Pod, wls []*kueue.Workload,
) map[types.UID]*kueue.Workload {
	owners := make(map[types.UID]*kueue.Workload, len(pods))
	for _, wl := range wls {
		for _, ref := range wl.OwnerReferences {
			if !modelDeploymentOwnerRefNamesAPod(ref) {
				continue
			}
			if _, claimed := owners[ref.UID]; !claimed {
				owners[ref.UID] = wl
			}
		}
	}

	byReplica := make(map[types.UID]*kueue.Workload, len(pods))
	for i := range pods {
		if wl := owners[pods[i].UID]; wl != nil {
			byReplica[pods[i].UID] = wl
		}
	}

	return byReplica
}

// modelDeploymentPreemptedInPart 在以下两个列表均非空时，分别列出存在配额被高优先级
// Workload 回收的副本的角色、以及仍处于已准入（admitted）状态的副本的角色，并统计被回收的副本数。
//
// 第二个列表是整个判定的核心。若某个部署的所有副本都被抢占，这属于常规状态：它不占用任何资源、
// 不提供服务，与从未启动过的部署一样只是在等待容量。本状态的不同之处在于，部署中仍有部分副本
// 保留了配额并在运行——这意味着一个已不再完整的部署仍在占用加速器资源，而仅凭其中任何一半的
// 状态都无法看出全貌。其余副本是否仍在提供服务取决于被回收副本所承载的 kind，该问题会单独判定，
// 绝不做假设。如果只检查“是否有副本被抢占”，对这两种情况会得出相同结论，而本函数正是为区分
// 它们而设计。
//
// 没有关联 Workload 的副本既不计入被抢占列表也不计入存活列表，这种不对称是有意为之。Kueue
// 不会为尚不存在的副本生成 Workload，因此这类副本不占用任何资源，也无状态可读：它既不能作为
// 占用加速器的存活副本，也无法证明其曾被抢占。由此带来两个不同的结果。
//
// 只要存在存活副本，报告就会以抢占状态为准，即使此时仍有其他副本正在组装中。毕竟有其他任务
// 抢走了本部署已获准入的容量，这是运维人员唯一需要采取行动的事实，而正在组装的副本最终会自行
// 就绪。
//
// 若不存在存活副本，则说明没有资源被空占，因此不属于该 reason 所指代的状态，报告会继续向下
// 走入描述其他异常原因的分支。该分支过去完全不提及抢占事实——当某个副本的配额被抢走时，它会
// 引导排查人员去调查缺失副本——因此现在会将抢占事实向下传递，而不是扩大该 reason 的适用范围
// 去覆盖并不存在资源碎片危害的状态。
//
// 如今同一个角色可以同时出现在两个列表中，因为它的每个副本都有各自独立的 Workload：同一角色
// 下一个副本被回收而另一个同级副本仍处于已准入状态，正是该 reason 所描述的碎片化场景；若强行
// 将该角色归入单一列表，反而会丢失其中一半的事实。
//
// 抢占状态直接向 Kueue 查询，而非自行推断。Kueue 会明确标出抢占：被驱逐的 Workload 带有
// 原因字段，Preempted 与 PodsReadyTimeout、AdmissionCheck 及队列停止等原因并列。若仅凭
// “失去预留配额”来推断，就会把上述所有情况混为一谈，而它们各自需要完全不同的处理措施。
func modelDeploymentPreemptedInPart(
	md *workercore.ModelDeployment, pods []core.Pod, wlByReplica map[types.UID]*kueue.Workload,
) (lost, kept []string, lostReplicas int) {
	// 与上述统计不同，此处分组时保留了正在退出的副本：从 Pod 侧来看，被回收的组正处于退出过程中，
	// 若在此跳过它们，就会掩盖本函数旨在识别的抢占状态。
	views := modelDeploymentGroupPodsIncludingDeparting(pods)
	for i := range md.Spec.Roles {
		role := &md.Spec.Roles[i]
		var lostRole, keptRole bool
		for _, view := range views {
			if view.Role != role.Name {
				continue
			}
			wl := modelDeploymentReplicaWorkload(view, wlByReplica)
			if wl == nil {
				continue
			}
			switch {
			case modelDeploymentWorkloadPreempted(wl):
				lostRole = true
				lostReplicas++
			case kubeapistatus.ConditionType(kueue.WorkloadAdmitted).IsTrue(wl):
				keptRole = true
			}
		}
		if lostRole {
			lost = append(lost, role.Name)
		}
		if keptRole {
			kept = append(kept, role.Name)
		}
	}

	return lost, kept, lostReplicas
}

// modelDeploymentWorkloadPreempted 报告 Kueue 是否因高优先级任务而收回了该 Workload 的配额。
//
// 这里会同时检查两个条件，因为它们在不同时间点写入，且任意一个都可能仍保留在状态中：Kueue
// 在做出抢占决策时设置 Preempted，在执行驱逐时设置带 Preempted 原因的 Evicted。只读其中
// 一个条件会因读写时机差异而得出不同结论。
func modelDeploymentWorkloadPreempted(wl *kueue.Workload) bool {
	if kubeapistatus.ConditionType(kueue.WorkloadPreempted).IsTrue(wl) {
		return true
	}

	for i := range wl.Status.Conditions {
		c := &wl.Status.Conditions[i]
		if c.Type == kueue.WorkloadEvicted && c.Status == meta.ConditionTrue &&
			c.Reason == kueue.WorkloadEvictedByPreemption {
			return true
		}
	}

	return false
}

// modelDeploymentKindsWithoutAdmittedReplica 按角色首次声明的顺序，返回没有任何已准入副本
// 的角色 kind 列表。
//
// 该结果用于判断部分被抢占的部署是否仍在提供服务，因此必须按 kind 而非按角色维度查询。两个
// 角色可以共享同一种 kind，若其中一个角色的所有副本均丢失而同 kind 的兄弟角色副本仍在运行，
// 部署依然能够维持该部分服务；仅统计角色数无法回答这一问题。
//
// 若某角色的副本没有对应的 Workload，则视为未准入。Kueue 不会为不存在的副本生成 Workload，
// 而未准入的副本无论出于何种原因都未在提供服务。这与相邻抢占判定函数的语义不同：在此处这是
// 准确的状态解读，而在抢占判定中，相同状态仅表示“该副本无可报告信息”，而非“该副本已丢失”。
func modelDeploymentKindsWithoutAdmittedReplica(
	md *workercore.ModelDeployment, pods []core.Pod, wlByReplica map[types.UID]*kueue.Workload,
) []string {
	order := make([]string, 0, len(md.Spec.Roles))
	served := make(map[string]bool, len(md.Spec.Roles))
	for i := range md.Spec.Roles {
		role := &md.Spec.Roles[i]
		kind := string(ModelDeploymentEffectiveRoleKind(role))
		if _, seen := served[kind]; !seen {
			order = append(order, kind)
			served[kind] = false
		}

		for j := range pods {
			if modelDeploymentPodRole(&pods[j]) != role.Name {
				continue
			}
			if wl := wlByReplica[pods[j].UID]; wl != nil &&
				kubeapistatus.ConditionType(kueue.WorkloadAdmitted).IsTrue(wl) {
				served[kind] = true

				break
			}
		}
	}

	var unserved []string
	for _, kind := range order {
		if !served[kind] {
			unserved = append(unserved, kind)
		}
	}

	return unserved
}

// modelDeploymentDeclaredReplicas 汇总所有角色声明的副本总数，作为按副本统计各项指标时的分母。
func modelDeploymentDeclaredReplicas(md *workercore.ModelDeployment) int {
	declared := 0
	for i := range md.Spec.Roles {
		declared += int(md.Spec.Roles[i].Replicas)
	}

	return declared
}

// modelDeploymentRolesWithUncomposedReplicas 列出包含 Kueue 尚未生成 Workload 的活跃副本的
// 角色，并统计此类副本的数量。处于该状态的活跃副本正处于准入流程中；而缺失副本由上方的完整性
// 分支负责处理，因此两者的角色列表并不相同。
//
// 此处不统计正在退出的副本，原因与旁边其他统计口径一致：正在退出的副本并不在等待生成 Workload，
// 若将其计入会导致状态条件持续卡在部署正在缩容或清理的成员上。
func modelDeploymentRolesWithUncomposedReplicas(
	md *workercore.ModelDeployment, pods []core.Pod, wlByReplica map[types.UID]*kueue.Workload,
) (roles []string, uncomposed int) {
	views := modelDeploymentGroupPodsByReplica(pods)
	for i := range md.Spec.Roles {
		role := &md.Spec.Roles[i]
		var short int
		for _, view := range views {
			if view.Role != role.Name {
				continue
			}
			if modelDeploymentReplicaWorkload(view, wlByReplica) == nil {
				short++
			}
		}
		if short > 0 {
			roles = append(roles, role.Name)
			uncomposed += short
		}
	}

	return roles, uncomposed
}

// 本文件发布的所有指标均以“副本”为单位，因为 `desired` 的单位是副本，所有状态消息也以副本
// 计数。若按 Pod 循环统计，得出的结果是以 Pod 为单位的数值——只有当每个副本仅包含 1 个 Pod 时
// 两者才相等：对于包含 2 个副本、每副本 2 个 Pod 的角色，按 Pod 统计会报告“该部署的 2 个副本
// 中有 4 个正在等待”，这描述的是一个不可能存在的部署状态。
//
// 同一副本内的成员共享同一个 Workload，因此按 Pod 遍历未预留配额的副本还会把同一个 Workload
// 按成员数重复计数——这种错误计数会按 `size` 倍数放大，并恰好随着为此模型引入的字段一同增长。

// modelDeploymentReplicaWorkload 返回同一副本成员共享的 Workload；若 Kueue 尚未生成，则
// 返回 nil——这正是不完整副本组的表现，因为 Kueue 在副本组集齐所有声明成员之前不会生成 Workload。
//
// 函数从首个能找到对应关系的成员读取 Workload，而非固定读取某个特定成员：即使副本缺少主节点
// （leader），其存活成员仍关联着已加入的 Workload。
func modelDeploymentReplicaWorkload(
	view modelDeploymentReplicaView, wlByReplica map[types.UID]*kueue.Workload,
) *kueue.Workload {
	for _, pod := range view.Members {
		if wl := wlByReplica[pod.UID]; wl != nil {
			return wl
		}
	}

	return nil
}

// modelDeploymentRolesWithUnquotaedReplicas 列出包含尚未持有配额预留（quota reservation）
// 的 Workload 的活跃副本的角色，并统计此类副本的数量。
//
// 每个副本的状态由其所属的 Workload 决定。如果整个角色只用单个 Workload 来判断，就无法区分
// “整组均已预留”与“其中某一个副本的预留已丢失”——这是每角色单 Workload 模型根本无法表达的
// 状态——而这两种状态恰恰区分了“即将运行的部署”和“仅部分被占用的部署”。
func modelDeploymentRolesWithUnquotaedReplicas(
	md *workercore.ModelDeployment, pods []core.Pod, wlByReplica map[types.UID]*kueue.Workload,
) (roles []string, waiting int) {
	views := modelDeploymentGroupPodsByReplica(pods)
	for i := range md.Spec.Roles {
		role := &md.Spec.Roles[i]
		var short int
		for _, view := range views {
			if view.Role != role.Name {
				continue
			}
			if wl := modelDeploymentReplicaWorkload(view, wlByReplica); wl == nil ||
				!kubeapistatus.ConditionType(kueue.WorkloadQuotaReserved).IsTrue(wl) {
				short++
			}
		}
		if short > 0 {
			roles = append(roles, role.Name)
			waiting += short
		}
	}

	return roles, waiting
}

// findModelDeploymentGroupWorkloads 按名称顺序返回拥有任意这些 Pod 的所有 Workload。
//
// 此处故意不提供仅返回单个 Workload 的方法。此前曾有返回首个 Workload 的实现且已无生产调用方：
// 每个查询都针对单个副本，而哪个 Workload 对应哪个副本由各自携带的所有权关系
// （modelDeploymentReplicaWorkloads）决定，而非排序顺序。一个书写正确但仅服务于本包已废弃
// 模型的访问器并非单纯的冗余代码，它还会诱使后续代码重新引入该模型下的缺陷——并且因为代码
// 本身没错而在评审时难以察觉。
//
// 此处匹配的是普通 OwnerReference，而非 ControllerReference，这并非放宽条件，而是能否找到
// Workload 的关键区别。Kueue 基于单个 Pod 构建 Workload 时会设置 ControllerReference，而基于
// Pod 组构建 Workload 时会为每个成员设置普通 OwnerReference（对每个 Pod 调用 `SetOwnerReference`，
// 而非 `SetControllerReference`）。因此一旦副本成为多 Pod 组，ControllerReference 过滤器就会
// 完全匹配不到对象，导致所有部署在正常准入的情况下却永远报告“尚无 Workload”。
//
// 匹配时同时校验 APIVersion 和 Kind：“Pod”并非保留字，其他 API 组中也可能存在同名资源，
// 仅凭 Kind 会产生误匹配。
//
// 重建过程可能会短暂留下仍拥有正在退出 Pod 的旧组 Workload。本函数仅扫描本轮观测到的 Pod，
// 并按名称排序返回结果，从而防止在旧组排水期间因存在两个候选 Workload 而导致上报状态在各轮
// 之间来回闪烁。若调用方只取第一个 Workload，剩余的 Workload 会被遗留下来——而它们各自仍在
// 其对应的副本上保留着 Kueue 的 finalizer，这会导致副本永远无法退出，表现为部署一直卡在
// Deleting 状态且没有任何报错。
//
// 调用方可通过限定传入的 Pod 范围来限定返回结果：传入单个组的副本即可只获取该组对应的 Workload，
// 重建逻辑正是借此精确定位到正在重建的副本组。
func (r *ModelDeploymentReconciler) findModelDeploymentGroupWorkloads(
	ctx context.Context, md *workercore.ModelDeployment, pods []core.Pod,
) ([]*kueue.Workload, error) {
	if len(pods) == 0 {
		return nil, nil
	}

	ours := sets.New[types.UID]()
	for i := range pods {
		ours.Insert(pods[i].UID)
	}

	wlList := new(kueue.WorkloadList)
	err := r.Client.List(ctx, wlList, ctrlcli.InNamespace(md.Namespace), ctrlclix.WithoutQuorum)
	if err != nil {
		return nil, fmt.Errorf("list workloads: %w", err)
	}

	var found []*kueue.Workload
	for i := range wlList.Items {
		wl := &wlList.Items[i]
		if modelDeploymentWorkloadOwnsAny(wl, ours) {
			found = append(found, wl)
		}
	}
	slices.SortFunc(found, func(a, b *kueue.Workload) int { return strings.Compare(a.Name, b.Name) })

	return found, nil
}

// modelDeploymentWorkloadOwnsAny 报告该 Workload 是否将传入的任意 Pod 列为所有者（owner）。
func modelDeploymentWorkloadOwnsAny(wl *kueue.Workload, pods sets.Set[types.UID]) bool {
	for _, ref := range wl.OwnerReferences {
		if modelDeploymentOwnerRefNamesAPod(ref) && pods.Has(ref.UID) {
			return true
		}
	}

	return false
}

// modelDeploymentOwnerRefNamesAPod 报告某个 OwnerReference 是否指向 Pod，这是副本 Workload
// 唯一认可的所有者类型。
//
// 必须校验 Kind，因为仅凭 UID 并不足以确立所有权关系。Workload 会保留其创建者赋予的任何
// 所有者引用，若某个其他类型的对象碰巧与某个 Pod 拥有相同的 UID，就会导致无关对象被当作副本
// 的 Workload。所有读取该关系的地方都统一经由本函数——包括将 Workload 匹配到副本的状态计算
// 与发布守卫，以及根据 Workload 反查所属部署的 Workload Watch 与联合准入屏障——以确保各方
// 对“拥有副本”的定义完全一致。
func modelDeploymentOwnerRefNamesAPod(ref meta.OwnerReference) bool {
	return ref.Kind == "Pod" && ref.APIVersion == "v1"
}

// observeModelDeploymentRoleKinds 报告是否每种角色 kind 都至少有一个就绪副本。
//
// 该函数读取本轮刚计算出的角色状态而非 spec，从而保证其结论不会与旁边的计数结果矛盾。如果
// 直接读取 spec，状态条件就可能对本轮未能统计到副本的角色给出错误判断。
func observeModelDeploymentRoleKinds(holder *workercore.ModelDeployment) {
	roles := holder.Status.Roles
	if len(roles) == 0 {
		ModelDeploymentConditionRoleKindsReady.Unknown(holder, modelDeploymentReasonNoRoleStatuses,
			"no role has been accounted for yet")

		return
	}

	missing := modelDeploymentKindsWithoutReady(roles)
	if len(missing) == 0 {
		ModelDeploymentConditionRoleKindsReady.True(holder, modelDeploymentReasonAllKindsReady,
			"every role kind of this deployment has at least one ready replica")

		return
	}

	ModelDeploymentConditionRoleKindsReady.False(holder, modelDeploymentReasonKindsNotReady,
		fmt.Sprintf("no replica is ready for role kinds %s", strings.Join(missing, ", ")))
}

// modelDeploymentKindsWithoutReady 按角色首次声明的顺序，返回没有任何就绪副本的角色 kind 列表。
//
// 该函数是纯函数：输入相同的状态即输出相同的结果，不依赖客户端也不依赖时钟。
//
// 阈值是一个就绪副本，而非全部就绪。“全部就绪”属于副本完整性判定，phase 字段已经推导了该
// 语义；若在此重复判定，就会让两个字段表达同一问题，一旦修改其中之一便会产生矛盾。只要求一个
// 就绪副本，才能使该问题聚焦于部署的“形态（shape）”：即分离式部署的各个组成部分是否均已就位。
//
// 列表顺序严格遵循角色声明顺序（以首次出现为准），确保在状态未变时连续两轮生成完全一致的
// 消息。map 的遍历顺序无法保证这一点，而该列表会被渲染进状态条件消息中，并与已存储的消息
// 比对以决定是否需要写回更新。
//
// 非托管角色（unmanaged role）的就绪副本也会计入统计，这是有意的设计决策而非疏漏。此类角色
// 替换了完整的启动命令行，渲染器未向其附加探针，因此只要容器启动其 Pod 即为 Ready——这比带
// 探针角色的 Ready 语义更弱。但它仍需计入，因为 Operator 并未构建该命令行、没有更可靠的判断依据，
// 且若忽略它会导致所有角色都采用自定义命令的部署永远报错缺少 kind。其代价在于，对于此类 kind，
// 该条件为 True 仅基于 kubelet 的容器启动信号；由于每个角色的 status.roles[].Unmanaged 字段
// 均会公开，需要更严格判定的读取方可以自行据此过滤，无需由本字段代为收紧。
func modelDeploymentKindsWithoutReady(roles []workercore.ModelDeploymentRoleStatus) []string {
	order := make([]string, 0, len(roles))
	ready := make(map[string]bool, len(roles))

	for i := range roles {
		kind := string(roles[i].Kind)
		if _, seen := ready[kind]; !seen {
			order = append(order, kind)
			ready[kind] = false
		}
		if roles[i].Ready > 0 {
			ready[kind] = true
		}
	}

	var missing []string
	for _, kind := range order {
		if !ready[kind] {
			missing = append(missing, kind)
		}
	}

	return missing
}

// deriveModelDeploymentPhase 将各项计数汇总为人工排查时最先查看的 phase 字段。
//
// Degraded 表示“部分”副本就绪而另一部分未就绪，这是最值得区分的状态：部署仍在提供服务，
// 但低于预期容量。没有任何副本就绪的部署无论原因为何统一归为 Starting——对查看者而言，尚未
// 准入的副本与尚未加载完权重的副本处于同一阶段，具体区别由 phaseMessage 说明。
func deriveModelDeploymentPhase(md, holder *workercore.ModelDeployment, routerReady bool) {
	if md.DeletionTimestamp != nil {
		holder.Status.Phase = ModelDeploymentPhaseDeleting
		holder.Status.PhaseMessage = "the replicas are being torn down"

		return
	}

	var desired, ready int32
	for _, rs := range holder.Status.Roles {
		desired += rs.Desired
		ready += rs.Ready
	}

	switch {
	case ready == desired && md.Spec.Router != nil && !routerReady:
		holder.Status.Phase = ModelDeploymentPhaseDegraded
		// 观测器在所有报告未就绪的路径上都会设置该条件，且其消息更准确：例如能够指明路由器渲染失败，
		// 这是单纯的“没有就绪副本”所无法表达的。
		holder.Status.PhaseMessage = ModelDeploymentConditionRouterReady.GetMessage(holder)
	case ready == desired:
		holder.Status.Phase = ModelDeploymentPhaseReady
		holder.Status.PhaseMessage = ""
	case ready > 0:
		holder.Status.Phase = ModelDeploymentPhaseDegraded
		holder.Status.PhaseMessage = fmt.Sprintf("%d of %d replicas are ready", ready, desired)
	default:
		holder.Status.Phase = ModelDeploymentPhaseStarting
		holder.Status.PhaseMessage = ModelDeploymentConditionQuotaReserved.GetMessage(holder)
	}
}

// annotateModelDeploymentPhase 在 phaseMessage 中补充仅凭 phase 无法表达的信息：为何等待
// 权重的部署没有任何副本，以及哪个副本被驱逐了。优先检查权重等待状态，因为在该状态持续期间
// 不会创建任何副本，它就是全部原因。
func annotateModelDeploymentPhase(holder *workercore.ModelDeployment, pods []core.Pod) {
	if holder.Status.Phase == ModelDeploymentPhaseReady || holder.Status.Phase == ModelDeploymentPhaseDeleting {
		return
	}
	if ModelDeploymentConditionWeightsReady.IsFalse(holder) && len(pods) == 0 {
		holder.Status.PhaseMessage = ModelDeploymentConditionWeightsReady.GetMessage(holder)
		return
	}
	if note := modelDeploymentEvictionNote(pods); note != "" {
		if holder.Status.PhaseMessage == "" {
			holder.Status.PhaseMessage = note
			return
		}
		holder.Status.PhaseMessage += "; " + note
	}
}

// parkedModelDeploymentWorkloads 返回部署中被联合准入屏障（joint barrier）停用的 Workload 名称列表。
//
// spec.active 是屏障在放弃继续维持一组 Workload 时写入的字段，也是调度器读取的持久化状态：
// 当其为 false 时，该 Workload 不会被调度器考虑，其对应的副本组也不会在后台重建。
//
// 但仅凭该标志本身无法说明是谁写入的。Kueue 会自行停用 Workload，运维人员也可以手动暂停某组
// 任务。若将这两种情况都报告为“因不可行而暂停（parked for infeasibility）”，就会误导查看者
// 以为部署的副本组无法放置，进而指示其释放容量或删除重建——针对别人主动选择的状态给出了答非
// 所问的指导。因此，只有当屏障自身的 AdmissionCheck 在同一个 Workload 上带有相应裁决结果时，
// 才将其认定为屏障触发的停用。
func parkedModelDeploymentWorkloads(wls []*kueue.Workload) []string {
	var parked []string
	for _, wl := range wls {
		if !kueueworkload.IsActive(wl) && jointBarrierParked(wl) {
			parked = append(parked, wl.Name)
		}
	}

	return parked
}

// jointBarrierParked 通过读取联合准入检查控制器留下的裁决结果，报告该 Workload 是否因联合
// 准入检查而处于非激活状态。
func jointBarrierParked(wl *kueue.Workload) bool {
	acs := jointCheckEntry(wl)

	return acs != nil && strings.Contains(acs.Message, _JointAdmissionParkedMarker)
}

// modelDeploymentPreemptionNote 生成附加在其他配额说明之后的补充子句，用于在部署的某个组被
// 抢占、但整体又不属于 PreemptedInPart 状态时说明情况。
//
// 它作为后缀而非独立的 reason 存在，因为它并非该状态条件的核心主题：当前所处的分支已经说明了
// 问题所在，此处仅补充是哪个对象占走了某个组的配额。若将其设为独立 reason，会导致同一状态存在
// 两种分类，而真正能指导排查人员下一步操作的仍然是当前分支本身的 reason。
func modelDeploymentPreemptionNote(lost []string) string {
	if len(lost) == 0 {
		return ""
	}

	return fmt.Sprintf(
		". A higher-priority workload has also reclaimed the quota of the replicas of roles %s",
		strings.Join(lost, ", "),
	)
}
