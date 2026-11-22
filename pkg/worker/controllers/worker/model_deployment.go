package worker

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	app "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	node "k8s.io/api/node/v1"
	rbac "k8s.io/api/rbac/v1"
	storage "k8s.io/api/storage/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	kmeta "k8s.io/apimachinery/pkg/api/meta"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	ctrlrecord "k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlbuilder "sigs.k8s.io/controller-runtime/pkg/builder"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlevent "sigs.k8s.io/controller-runtime/pkg/event"
	ctrlhandler "sigs.k8s.io/controller-runtime/pkg/handler"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	ctrlpredicate "sigs.k8s.io/controller-runtime/pkg/predicate"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	kueuepodconst "sigs.k8s.io/kueue/pkg/controller/jobs/pod/constants"
	kueueworkload "sigs.k8s.io/kueue/pkg/workload"

	worker "gpustack.ai/gpustack/api/worker/v1"
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/controller"
	kubeapistatus "gpustack.ai/gpustack/pkg/kubeapistatus"
	"gpustack.ai/gpustack/pkg/kubediscovery"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/modelstore"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/setting"
	"gpustack.ai/gpustack/pkg/system"
	"gpustack.ai/gpustack/pkg/systemmeta"
	"gpustack.ai/gpustack/pkg/utils/ctrlclix"
	"gpustack.ai/gpustack/pkg/worker/elasticengine"
	"gpustack.ai/gpustack/pkg/worker/kvcache/inject"
	"gpustack.ai/gpustack/pkg/worker/settings"
)

// ModelDeploymentReconciler 调谐 v1alpha1.ModelDeployment 对象，以完成以下任务：
//   - 为每个 (role, ordinal) 渲染一个 Kubernetes Pod，归属于该 ModelDeployment，并携带将其路由到
//     对应角色池的入口标签以及该副本所属的 Kueue 组元数据。每个副本由 API server 自动命名，因此
//     替换的新副本绝不会沿用被替换副本的名称。
//   - 持续收敛该副本集合：当 API server 上某序号对应的副本为空时补齐角色缺失的序号；移除角色不再
//     声明的序号及其各自的 Workload；当所有已声明副本都持有已准入（admitted）的 Workload 时，替换
//     spec 已与其构建来源不一致的副本。
//
// 该调谐器不创建 Instance。一个 Instance 仅渲染一个 Pod 且其 spec 在创建后不可变，若通过 Instance
// 路由副本，不仅无法表达“单副本多 Pod”的模型，还会让每次发布退化为全量重建。准入链路以 Pod 为核心，
// 普通 Pod 本身就是其中的一等公民。
type ModelDeploymentReconciler struct {
	Client    ctrlcli.Client
	APIReader ctrlcli.Reader
	Recorder  ctrlrecord.EventRecorder

	// CacheScraper 读取每个副本自身报告的缓存客户端状态。它被设计为接口而非由调谐器直接发起网络连接，
	// 因为其所支撑的 condition 需要准确处理的各种场景均属于故障态，而真实连接无法按需模拟失败。
	//
	// 为 nil 时表示所有副本均不可读，对应 condition 会上报为 Unknown。当前该字段为 nil：针对具体引擎的
	// 读取实现尚未编写，而凭空假定指标名称正是设置该 condition 所要规避的行为。
	CacheScraper ModelDeploymentCacheScraper

	// servingViewFetch 读取单个 Router Pod 的观测视图。它被设计为函数而非由调谐器直接发起网络连接，原因与
	// CacheScraper 相同：服务状态判定必须覆盖各类故障情况，而真实连接无法按需模拟失败。为 nil 时表示使用
	// 生产环境传输实现。
	servingViewFetch routerViewFetch

	// groupForwardFetch 向单个成员的引擎查询该副本的前向路径是否正在承载请求。为 nil 时表示使用生产环境
	// 传输实现；无法查询的成员状态记为 Unknown 并保持阻断：该探测仅用于解除挂起，因此无法响应的传输通道
	// 会使该组维持原有的挂起状态。
	groupForwardFetch modelDeploymentGroupForwardFetch

	// groupForwardPairFetch 向一对 prefill 组和 decode 组查询两者之间的传输路径是否正在承载请求。为 nil
	// 时表示使用已接入的生产环境传输实现；当调用方完全未提供成对传输实现时，由限定器返回 Unsupported。
	groupForwardPairFetch modelDeploymentGroupForwardPairFetch

	// drainReader 为退场协议读取单个成员引擎的在途请求活动。为 nil 时表示使用生产环境传输实现，在底层
	// 采集器就位前该实现会拒绝放行：协议将停留在 Draining 状态且不执行任何删除操作，对于尚不存在的
	// 观测指标，这是安全保守的默认方向。
	drainReader     modelDeploymentDrainReader
	elasticObserver modelDeploymentElasticRayReader
	elasticClient   func(*core.Pod) (*elasticengine.Client, error)

	// clock 用于读取退场协议的超时预算。将其作为字段而非直接调用 time.Now，是为了让测试可以自由指定
	// 阶段的开始和过期时刻而无需实际等待，这是验证预算逻辑的唯一可行方式。
	clock func() time.Time
}

var _ ctrlreconcile.Reconciler = (*ModelDeploymentReconciler)(nil)

func (r *ModelDeploymentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := ctrllog.FromContext(ctx)

	// 获取对象。
	md := new(workercore.ModelDeployment)
	err := r.Client.Get(ctx, req.NamespacedName, md)
	if err != nil {
		if !kerrors.IsNotFound(err) {
			logger.Error(err, "fetch model deployment")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// 如果 ModelDeployment 已标记删除，则执行清理。
	if md.DeletionTimestamp != nil {
		return r.teardownModelDeployment(ctx, md)
	}

	// 加锁。
	if !systemmeta.Lock(md) {
		err = r.Client.Update(ctx, md)
		if err != nil {
			logger.Error(err, "lock model deployment")
			return ctrl.Result{}, err
		}
	}

	return r.convergeModelDeployment(ctx, md)
}

// teardownModelDeployment 删除所有副本，并在副本全部消失后释放 finalizer。
//
// finalizer 会一直保留到最后一个 Pod 真正退出，而非在发出删除请求后立即移除，因为比所属对象存活
// 更久的副本会继续占用加速器资源，而准入账本此时已停止对其计账。
func (r *ModelDeploymentReconciler) teardownModelDeployment(
	ctx context.Context, md *workercore.ModelDeployment,
) (ctrl.Result, error) {
	logger := ctrllog.FromContext(ctx)

	if systemmeta.IsLocked(md) {
		pods, err := r.listModelDeploymentPods(ctx, md)
		if err != nil {
			logger.Error(err, "list replicas")
			return ctrl.Result{}, err
		}

		if len(pods) > 0 {
			// 在发出删除请求前先上报阶段状态，以便监听该对象的操作方看到 Deleting，而不是此前最后一次达到的
			// Ready。此处刻意不重新读取 Binding：清理阶段无需向其查询信息，仍在写入的副本所用的域即为上次
			// 观测到的域。
			// 清理阶段不评估任何健康判定条件，因此不传入资格校验结果，使端点状态保持未观测状态，而非给出
			// 本轮并未做出的结论。
			if err = r.syncModelDeploymentStatus(
				ctx, md, pods, nil, nil, nil, modelDeploymentPassObservation{
					Serving: modelDeploymentServingUnobserved(md, "the teardown pass observed no router"),
				},
			); err != nil {
				// 事件过滤器会丢弃仅变更 status 或 metadata 的更新，因此导致冲突的变更可能不会触发新事件：应直接
				// 重新入队，而非等待事件。
				return objectWriteResult(logger, err, "update model deployment status to deleting",
					_requeueAfterConflict)
			}

			for i := range pods {
				if pods[i].DeletionTimestamp != nil {
					continue
				}
				// 清理流程刻意不被退场预留（retirement reservation）拦截，此处明确记录该设计决策，以免被误认为
				// 遗漏了路径。预留机制保留副本是为了让退场协议能够平稳下线它，而一旦所属对象不再需要该部署存在，
				// 这一前提便随之失效：DeletionTimestamp 意味着清理全部资源，而非要求保留某个副本继续服务。
				// 此时若继续占用容量不仅逻辑有误，还会导致死锁——因为下方的 finalizer 只有等最后一个副本离开后
				// 才会释放，拒绝放行的预留会让对象永远停留在 Deleting 状态且没有任何报错。
				//
				// preStop 钩子仍作为删除后的最后一道防线，承担优雅退出的职责：正在清理的副本在退出过程中仍会自行
				// 排空引擎请求。对于正在被整体删除的部署，放弃的只是控制器侧的退场撤离协议。
				if err = r.Client.Delete(ctx, &pods[i]); err != nil && !kerrors.IsNotFound(err) {
					logger.Error(err, "delete replica", "pod", pods[i].Name)
					return ctrl.Result{}, err
				}
			}

			// 仅删除副本不足以让 Pod 真正退出。Kueue 会为其管理的组内每个 Pod 加上 finalizer，而本
			// operator 构建的组带有 SERVING 注解——Kueue 将其定义为永不结束的组，因此处于终止态的副本会被
			// 视为正在等待替换，其 finalizer 永远不会被释放。唯一能触发释放它们的路径是删除对应的 Workload。
			//
			// 没有其他机制能打破这个循环。该 Workload 唯一的属主正是这些无法退出的 Pod，且它们通过非
			// controller 引用持有它，因此垃圾回收会等待所有 Pod 消失，也没有任何替换超时可以兜底。若不处理，
			// 该部署将永远卡在 Deleting 状态，这也是加入此删除逻辑前实际运行中观测到的现象。
			if err = r.deleteModelDeploymentGroupWorkload(ctx, md, pods); err != nil {
				logger.Error(err, "delete group workload")
				return ctrl.Result{}, err
			}

			logger.V(3).Info("replica deletion in progress; requeue in 2s", "replicas", len(pods))

			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
		}

		// 最后一个副本已经退出，因此可以释放对 Binding 的占用声明。若提前释放，授权可能会在仍有进程通过它
		// 写入数据时被删除。
		if err := r.releaseModelDeploymentBinding(ctx, md); err != nil {
			// Binding 的监听器会传递其所有变更，因此引发冲突的那次更新会再次唤醒该 ModelDeployment。
			return objectWriteResult(logger, err, "release kv cache pool binding", ctrl.Result{})
		}
	}

	// 解锁。
	if systemmeta.Unlock(md) {
		logger.V(3).Info("skip deleted model deployment")
		return ctrl.Result{}, nil
	}

	err := r.Client.Update(ctx, md)
	if err != nil {
		logger.Error(err, "unlock model deployment")
	}

	return ctrl.Result{}, ctrlcli.IgnoreNotFound(err)
}

// convergeModelDeployment 使渲染出的副本与 spec 保持一致。
//
// 该过程基于电平触发（level-based）：根据 spec 渲染出期望存在的副本，将其与当前实际状态比对，
// 并仅下发差异部分。当 spec 未变化时，一轮调谐不会产生任何写操作，这确保了在每个 Pod 事件上
// 都会触发的控制器不会反复重写所有资源。
func (r *ModelDeploymentReconciler) convergeModelDeployment(
	ctx context.Context, md *workercore.ModelDeployment,
) (ctrl.Result, error) {
	logger := ctrllog.FromContext(ctx)

	// 优先解析，但其判定结果刻意不作为下方收敛逻辑的前置门控。当 Binding 缺失或短暂不可用时（例如
	// 存储主节点重启会使所有 Binding 在数十秒内处于非 Ready 状态），现有副本仍应继续提供服务，仅通过
	// condition 上报状态即可。若在此处拒绝收敛，会把存储的常规升级演变为其上所有部署的服务中断。
	domain, err := r.resolveModelDeploymentDomain(ctx, md)
	if err != nil {
		logger.Error(err, "resolve kv cache pool binding")
		return ctrl.Result{}, err
	}

	// 只要能读取到 Binding，无论其判定结果如何都必须进行占用声明：该声明用于阻止管理员删除仍在写入的
	// 部署所依赖的 Binding，如果声明随就绪状态时有时无，恰恰会暴露出误删的时间窗口。
	if err = r.claimModelDeploymentBinding(ctx, md); err != nil {
		// Binding 的监听器会传递其所有变更，因此引发冲突的那次更新会再次唤醒该 ModelDeployment。
		return objectWriteResult(logger, err, "claim kv cache pool binding", ctrl.Result{})
	}

	// 每轮调谐仅解析一次，而非按角色分别解析：端点与传输方式属于整个池及其后端。返回 nil 表示
	// 当前尚无可用连接目标，此时渲染副本时不配置连接器，而不会配置残缺的连接器。
	connection, err := r.resolveModelDeploymentConnection(ctx, domain)
	if err != nil {
		logger.Error(err, "resolve kv cache connection")
		return ctrl.Result{}, err
	}
	var interfaceProtocols []string
	if connection != nil {
		interfaceProtocols = connection.Protocols
	} else if modelDeploymentRequestsInterfaces(md) && md.Spec.KVCache != nil {
		interfaceProtocols, err = r.resolveModelDeploymentInterfaceProtocols(ctx, md)
		if err != nil {
			return ctrl.Result{}, err
		}
	}

	// 在渲染任何内容之前先解析权重，因为从未解析完成的制品没有任何可渲染的信息：既无提交版本可固定，
	// 也无 claim 可挂载。该轮调谐完全不会操作任何副本——等待首次解析的部署尚无副本，而制品被删的
	// 部署受制品 finalizer 保护——仅在状态中说明等待原因。
	weights, err := r.resolveModelDeploymentWeights(ctx, md)
	if err != nil {
		logger.Error(err, "resolve model artifact")
		return ctrl.Result{}, err
	}
	if weights != nil && weights.Render == nil {
		pods, listErr := r.listModelDeploymentPods(ctx, md)
		if listErr != nil {
			return ctrl.Result{}, listErr
		}

		// 权重仍在解析中，因此本轮调谐尚未触及副本，也不做任何资格判定；端点状态将保持未观测状态，
		// 直到后续真正到达副本检查的调谐轮次为止。
		if err = r.syncModelDeploymentStatus(
			ctx, md, pods, domain, nil, weights, modelDeploymentPassObservation{
				Serving: modelDeploymentServingUnobserved(md, "the weights are still resolving"),
			},
		); err != nil {
			// 事件过滤器会丢弃仅变更 status 的更新，因此导致冲突的变更可能不会触发新事件：应直接重新入队，
			// 而非等待事件。
			return objectWriteResult(logger, err, "sync status while the weights resolve", _requeueAfterConflict)
		}

		return ctrl.Result{}, nil
	}

	desired, err := r.renderModelDeploymentPods(ctx, md, connection, interfaceProtocols, weights)
	if err != nil {
		// 渲染失败通常意味着 InstanceType 尚未就绪，或渲染器无法为某个角色构建容器。本轮调谐会在写入
		// 任何状态前中止，因此 Event 是读者获知失败原因的唯一途径：此时 conditions 仍显示“尚未创建
		// 任何副本”，对慢启动与永久性失败的描述并无二致。
		// 此处的空值保护与本文件中另外两处事件发送路径一致：Recorder 仅由 SetupController 注入，
		// 因此直接构造的调谐器（所有单元测试均如此）完全可能不携带 Recorder。
		if r.Recorder != nil {
			r.Recorder.Event(md, core.EventTypeWarning, modelDeploymentEventRenderFailed, err.Error())
		}
		logger.Error(err, "render replicas")

		return ctrl.Result{}, err
	}

	actual, err := r.listModelDeploymentPods(ctx, md)
	if err != nil {
		logger.Error(err, "list replicas")
		return ctrl.Result{}, err
	}

	// 退场预留在此处、于任何删除决策之前统一评估一次，下方所有路径均只查阅该计划而非直接读取预留字段。
	// 若某条路径直接读取 md.Status.Retirement，就会重新推导协议已经给出的结论，且两次结果可能不一致：
	// 这里的计划已经推进了状态机、应用了超时预算并将目标解析为存活成员，而直接读取看到的则是上一轮
	// 调谐结束时写入的旧状态。
	//
	// 当没有正在进行的预留时，此处生成空计划，下方所有路径的行为将与引入该协议前完全一致。整个特性
	// 正是建立在这一性质之上，各路径对应的测试会逐一断言该行为，此处不再赘述。
	retirement := r.planModelDeploymentRetirement(ctx, md, actual)
	// 本轮调谐到达的状态在此处、任何删除决策之前持久化，使本轮后续逻辑以及对象自身的状态同步都能看到
	// 当前最新的操作进度。这里单独执行一次写入，而非直接在内存对象上赋值，是因为本轮的状态同步在写入
	// 前会对比推导状态与已观测状态，若原地修改字段会导致对比结果相等而跳过写入。
	if err = r.persistModelDeploymentRetirement(ctx, md, md.Status.Retirement, retirement); err != nil {
		// 事件过滤器会丢弃仅变更 status 的更新，因此导致冲突的变更可能不会触发新事件：应直接重新入队，
		// 而非等待事件。
		return objectWriteResult(logger, err, "persist the retirement reservation", _requeueAfterConflict)
	}

	// 每个副本本身就是一个独立的调度单元：其 Kueue 组是由 (role, ordinal) 派生的单成员组，因此
	// 变更某个 role 的副本数不会改变任何运行中 Pod 所携带的总数。该变更仅会引入 spec
	// 新声明的 ordinal 或不再声明旧的 ordinal，两者都不需要存量副本在 Kueue 组合任何资源之前
	// 先达成一致——此前按 role 分组所需的整组重建已随之消除，并由下面按 ordinal 进行的
	// 计算取而代之。
	//
	// 正在离开的副本绝不会自行释放其槽位（无论其处于排空过程的哪个阶段），因此替换副本
	// 必须等待该 Pod 彻底消失，没有更短的捷径。基于本项目当前运行版本的实测：一个被删除的
	// 服务组成员在排空期间会保持 Running，随后转为 Succeeded（Kueue 仍将其计为活跃阶段），
	// 并带着 finalizer 继续留在记录中，其 Workload 也仍处于 admitted 状态；只有删除该
	// Workload 才会让 Pod 真正消失，而等待时长就是排空本身的时长，而非某个固定常量。因此
	// 下面的创建门控会直接检查该 ordinal 上是否仍存在 Pod，而不用超时时间来替代这一判断。
	//
	// 真正牵制住离开中副本的是它自己的 Workload。服务组中的 Pod 会一直携带 Kueue 的
	// finalizer，直到该组的 Workload 被删除，除此之外没有任何机制会释放它。本轮移除的
	// 副本——无论是永久移除还是被替换——都会在下方一并带走其 Workload：仅删除 Pod 会导致
	// Workload 一直占用配额、finalizer 一直卡住 Pod，且不会产生任何错误。对于被替换的
	// 副本而言，删除 Workload 具有双重关键作用：既清空替换副本的创建门控所观察的 ordinal，
	// 也让替换副本作为一次全新准入去竞争它所释放的配额。
	//
	// 删除操作与创建操作之间没有额外的抑制集合，原因在于决策本身的结构，而非旁设的守卫。
	// 下方的每一项计数都基于本轮开始时的成员列表，因此本轮刚从中移除成员的 role 在该列表中
	// 仍保持声明数量，不会触发创建；而低于声明数量的 role 其成员均被保留，本轮不会从其中
	// 删除。同一个 role 不会在同一轮中既收到创建又收到删除，而在即将离开的成员旁边执行的
	// 创建会直接等待该成员从 API server 消失，而无需对其发起任何额外询问。

	// 记录本轮对携带旧 spec 副本的决策，供汇报该状态的 condition 使用。清理（teardown）轮次
	// 不做任何记录：它会直接删除所有副本而不比较哈希，因此无法给出统计结果，下方会传入 nil
	// 而非零值。
	var rollout modelDeploymentRollout

	// 先按 role 收集存活成员，因为本次收敛是按 role 逐一对比的：该 role 的 Pod 占据了哪些
	// ordinal、每个 Pod 是否与其所在 ordinal 渲染出的结果匹配，以及 spec 声明了多少个。
	// 已经在离开途中的 Pod 不会被任何一方计入——它仍属于自己的组，但不再是 spec 可保留的成员。
	liveByRole := make(map[string][]*core.Pod, len(md.Spec.Roles))

	// 弹性部署在此处提前退出，不会进入下方固定期望集合的对比逻辑，因为该对比解决的问题
	// 在弹性部署中并不存在。固定路径会读取 spec 声明的副本数和尺寸计数，并删除 spec 不再
	// 命名的所有成员；而弹性成员并非某个单一声明计数的副本，宽度缩减也不应被解读为缩容掉
	// 某个副本。若让固定路径处理它们，就会使单成员组变得可按计数删除，而这正是本切片明确
	// 不负责的撤回操作。
	//
	// 固定规格的部署不受此分支影响，继续按下方原有路径处理。
	if elastic, observation, err := r.convergeModelDeploymentElastic(
		ctx, md, actual, desired, weights,
	); err != nil {
		return ctrl.Result{}, err
	} else if elastic {
		actual, err = r.listModelDeploymentPods(ctx, md)
		if err != nil {
			return ctrl.Result{}, err
		}
		masters := modelDeploymentElasticMasters(md, actual)
		// status 发布的是本轮自身的观测结果以及本轮自身的资质判定。如果在此处再做第二次
		// 资质判定，就会针对同一批 Pod 的另一种分组产生第二个裁决，从而导致 status 发布的裁决
		// 与写入标签所依据的事实不一致。
		if err = r.syncModelDeploymentStatus(ctx, md, masters, domain, nil, weights, observation); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	// spec 不再命名的 role 的副本会在此处连同其 Workload 一起删除：该 role 已被缩容至零或
	// 被重命名，无论哪种情况，这次离开都是永久性的，而非需要替换副本填补的空缺。
	departed := make([]core.Pod, 0, len(actual))
	for i := range actual {
		pod := &actual[i]
		if pod.DeletionTimestamp != nil {
			// 已在离开途中；此时发起创建会与删除产生竞争并被拒绝。
			continue
		}

		role := modelDeploymentPodRole(pod)
		if _, wanted := desired[role]; !wanted {
			// 已被缩容移除，或因 role 重命名而不再保留。此处的移除交由退出协议处理而非直接下发删除，
			// 辅助函数要么在本轮启动该协议，要么因已有操作正在进行而拒绝移除。无论哪种情况，该副本
			// 都会暂时保留并继续计入数量：缩容移除正是值得付出撤回与排空成本的时机，而直接在此删除
			// Pod 恰恰是这些机制所要避免的硬切断。
			if r.refuseModelDeploymentRemoval(ctx, md, retirement, pod, actual, nil) {
				logger.V(3).Info("holding a replica of a role no longer in the spec for the retirement",
					"pod", pod.Name)
				liveByRole[role] = append(liveByRole[role], pod)

				continue
			}

			logger.Info("removing replica of a role no longer in the spec", "pod", pod.Name)
			if err = r.Client.Delete(ctx, pod); err != nil && !kerrors.IsNotFound(err) {
				logger.Error(err, "delete replica", "pod", pod.Name)
				return ctrl.Result{}, err
			}
			departed = append(departed, *pod)

			continue
		}

		// 处于终态的 Pod 代表已经离开的副本，尽管并未有操作去删除它：kubelet 在节点压力下
		// 驱逐它或在准入阶段拒绝它，都会使其进入 Failed 且没有删除时间戳，而 Failed 的 Pod
		// 永远不会再次运行。Succeeded 则是通过干净退出完成的同样离开——引擎以退出码 0 响应
		// 驱逐发出的 SIGTERM，而在 RestartPolicy 为 Always 时，健康的副本绝不会进入终态，因此
		// 服务 Pod 上的 Succeeded 只能来自 kubelet 强制触发的终止。如果仍将其计为存活，它就会
		// 继续占据其 ordinal 并保留当前渲染版本的哈希，导致本轮既不会为该槽位创建新副本，又会
		// 报告该副本为最新。因此它在此处被删除，其 Workload 也在下方一并删除，处理方式与本轮
		// 主动发起的离开相同：Workload 会释放 Pod 上的 Kueue finalizer，并在 ordinal 变空后
		// 触发一次全新准入作为替换。多 Member 副本中的某个 Member 也会按同样方式处理，其存留的
		// Member 会因此出现缺员，进而由下方的滚动更新将其作为整体轮换。
		if pod.Status.Phase == core.PodFailed || pod.Status.Phase == core.PodSucceeded {
			// 处于终态的 Pod 是非本 operator 发起的离开，也是退出协议无法挽留的：引擎已经退出，
			// 预留的目标也不再可解析。此处不会查询保留状态，因为已无可保留的对象；退出协议会在
			// 同一轮中感知到这一情况，并将其记录为外部终止，而非预算耗尽时应记的正常中止。
			logger.Info("removing replica in a terminal phase",
				"pod", pod.Name, "phase", pod.Status.Phase, "reason", pod.Status.Reason)
			if err = r.Client.Delete(ctx, pod); err != nil && !kerrors.IsNotFound(err) {
				logger.Error(err, "delete terminal replica", "pod", pod.Name)
				return ctrl.Result{}, err
			}
			departed = append(departed, *pod)

			continue
		}

		liveByRole[role] = append(liveByRole[role], pod)
	}

	// 本轮发出的删除不会就此结束流程：下方的观测逻辑无论如何都会执行。如果在此直接返回，
	// 就会导致在最需要记录离开事件的时间窗口内——即副本正在离开的过程中——完全没有写入任何
	// 事件和 status。
	var requeue bool
	if len(departed) > 0 {
		// 在删除之后执行，而非替代删除：存留成员在此处由我们主动删除，而不是依赖 Workload
		// 消失时 Kueue 执行的停止操作，因为那种停止只是另一个控制器对我们删除行为的响应。移除
		// Workload 的作用是释放每个已离开副本上的 Kueue finalizer——现在是每个副本对应一个
		// Workload，因此被移除的 role 会精确释放其副本所持有的配额。
		if err = r.deleteModelDeploymentGroupWorkload(ctx, md, departed); err != nil {
			logger.Error(err, "delete departed replicas' workloads")
			return ctrl.Result{}, err
		}
	}

	// 本轮开始时就已经处于离开状态的副本，是由本 operator 之外的因素送走的：人工删除 Pod、
	// 节点排空或驱逐。这些操作都不会删除副本的 Workload，而服务组的 finalizer 又没有其他
	// 释放途径——因此如果没有这一步，该 ordinal 就会被一个永远无法离开的 Pod 占住，下方的
	// 创建门控也永远不会为其打开。上方的删除逻辑只覆盖本轮自己发出的删除。
	// 被预留的成员永远不会处于离开状态，因此清扫逻辑自身的规则已经保护了它：只要组内仍有
	// 成员存留，其 Workload 就会保留，而预留目标按定义就是存留的。这条规则本身就是全部
	// 保护，而且已经足够：若被预留成员从外部被终止，该终止会作为外部终止传达给预留状态机
	// 并由其观测和记录，因此本清扫逻辑无需借助预留信息来区分这两种情况。
	if err = r.releaseModelDeploymentStrandedWorkloads(ctx, md, actual); err != nil {
		logger.Error(err, "release the workloads of replicas nothing here sent away")
		return ctrl.Result{}, err
	}

	// 退出协议自身的删除操作在此处下发，位于上述受保护路径之后、按 role 收敛之前，这样
	// 操作决定移除的副本就会在做出提交的这一轮立即离开，而不必等到下一轮。随后的收敛逻辑
	// 会遍历同一个持有这些 Pod 的 `retirement`，并出于与其他路径相同的原因跳过它们。
	if err = r.commitModelDeploymentRetirement(ctx, md, retirement, actual); err != nil {
		logger.Error(err, "delete the retired replica")
		return ctrl.Result{}, err
	}
	// 收敛逻辑本身按 role 逐一执行，且数量调整优先于版本更新：低于声明数量的 role 不会
	// 在同一轮中滚动更新过期成员，因为从本就缺员的集合中删除副本会进一步扩大下方创建门控
	// 正在等待填补的缺口——而且下方的已准入计数守卫也会自行拒绝缺员的 role，因为缺失的
	// 准入就意味着已经有一个准入正在进行中。在数量正确之前，role 的版本新旧毫无意义。
	// 每轮只拉取一次 Workload 列表，且仅当有 role 需要时才拉取。下方的滚动更新守卫需要读取
	// Kueue 的 Admitted 裁决，因此该读取必须绕过缓存——informer 会滞后于写入方，否则守卫
	// 就会依据上一轮的结果做决策。但不合理的是为每个滚动中的 role 都拉取一次：该列表是整个
	// namespace 维度的，对每个 role 的答案都相同，而受阻的滚动更新在等待期间每隔几秒就会
	// 重新入队一次。将其延迟到首次需要时再执行，可避免仅在扩缩容或已稳定的部署为无人查看
	// 的读取付出代价。
	listWorkloads := sync.OnceValues(func() ([]kueue.Workload, error) {
		wlList := new(kueue.WorkloadList)
		if err := r.APIReader.List(ctx, wlList, ctrlcli.InNamespace(md.Namespace)); err != nil {
			return nil, fmt.Errorf("list workloads: %w", err)
		}

		return wlList.Items, nil
	})

	createOrdinals := make(map[string][]int, len(md.Spec.Roles))
	// partialCreates 是填入替换槽位正在构建的组中的成员，收集自下方同样认定部分创建
	// 具备可填充性的同一决策。
	partialCreates := make(map[string][]*core.Pod, len(md.Spec.Roles))
	// 本轮选定待替换的 ordinal 收集于此，使整组健康度谓词能够直接基于滚动更新做出的同一
	// 决策来回答“该副本是否正在离开”，而不必再做第二次可能得出不同结论的比较。
	//
	// 只有本轮明确选中的副本才会被记录。未被选中的过期副本仍在提供服务，若判定其不合格，
	// 会在替换副本尚未出现之前就撤回其端点。
	replacedByRole := make(map[string]map[int]bool, len(md.Spec.Roles))
	// holdingRoles 是替换副本尚未获得准入的 role。它们参与下方的每一项决策，唯独不会
	// 再挑选下一个健康副本进行替换。
	holdingRoles := make(map[string]modelDeploymentReplacementSlot, len(md.Spec.Roles))
	slots := modelDeploymentReplacementSlotsOf(md)
	minRenderVersion := modelDeploymentEffectiveMinRenderVersion(md)
	for i := range md.Spec.Roles {
		role := &md.Spec.Roles[i]
		live := liveByRole[role.Name]
		declared := int(role.Replicas)
		want := desired[role.Name]

		// 已经在替换一个副本的 role 在该替换获得准入前不会再选择另一个副本。处于排队中的
		// 替换尚未持有任何资源，因此在其身后继续消耗第二个健康副本只会导致每轮运行时该 role
		// 的已准入容量进一步减少。
		if slot, held := slots.slotFor(role.Name); held {
			resolved, resolveErr := r.resolveReplacementSlot(ctx, md, slot, role, want[slot.Ordinal])
			if resolveErr != nil {
				return objectWriteResult(
					logger, resolveErr, "resolve the replacement slot", _requeueAfterConflict,
				)
			}
			if resolved.cleared {
				// 清除操作必须持久化，而不能只从本轮视图中丢弃：留在对象上的槽位会永远卡住该 role，
				// 因为下一轮还会读到本轮刚刚解决的同一个未决替换记录。
				if err = r.endReplacementSlot(ctx, md, role.Name); err != nil {
					return objectWriteResult(
						logger, err, "clear the replacement slot", _requeueAfterConflict,
					)
				}
				slots = slots.clearRole(role.Name)
			} else {
				slots = slots.setSlot(resolved.slot)
				holdingRoles[role.Name] = resolved.slot
			}
			if resolved.requeue {
				requeue = true
			}
		}

		// spec 仍声明的 ordinal 会保留其 Pod，不再声明的 ordinal 则按从高到低的顺序离开，
		// 这样缩容时会优先撤掉最新的槽位，使 ordinal 从零开始保持紧凑。每个离开的副本都会带走
		// 自己的 Workload：每个副本独立成组，因此不会影响其他副本的准入；而如果不删除 Workload，
		// Pod 就会永远挂着 Kueue 的 finalizer 并持续占用配额。
		//
		// 没有 ordinal 的 Pod 不占任何槽位。它是在按副本分组机制存在之前渲染出来的；目前先保留，
		// 并在下方参与判定——由于本轮计算出的任何哈希都不可能与它匹配，它会按正常滚动更新节奏
		// 被替换，而不是立刻被强行拆除。
		// 下方的两个计数使用不同的单位，将它们区分开正是让多 Member 副本构成的 role 与单 Member
		// 副本构成的 role 在算术上保持一致的关键。
		// `declared` 统计的是副本数，这正是 spec 所声明且扩缩容所调整的维度。
		// 期望的成员尺寸完全不用于衡量存活集合：它描述的是将来构建替换副本时的规模，而运行中的
		// 副本则是按其创建时 spec 所指定的规模构建的。
		removed := make([]*core.Pod, 0)
		kept := make([]*core.Pod, 0, len(live))
		occupied := make(map[int]bool, declared)
		orphans := 0
		for _, pod := range live {
			// 本轮不允许移除的副本会在任何计数计算之前先行保留，而且是直接放入 `kept`，而不仅仅是
			// 不纳入移除。这两者并不相同：`kept` 是后续读取 ordinal 和统计数量的依据，因此如果一个
			// 副本只是被排除在 `removed` 之外，就会在下方空出槽位，导致创建门控在退出协议即将接管的
			// ordinal 上再建出第二个副本。这也正是缩容至零时能够保持容量的原因：spec 不再声明任何
			// 副本，ordinal 已超出声明数量，协议被准许接管，容量也因此原封不动地保留下来。
			//
			// 该移除交由协议处理而非直接下发，从而使本轮触发的缩容成为一次有序退出的起点，而不是对
			// 运行中服务引擎的瞬间切断。
			ordinal, ok := modelDeploymentPodOrdinal(pod)
			keepHeld := func() {
				kept = append(kept, pod)
				if ok {
					occupied[ordinal] = true
				}
			}

			if retirement.holds(pod.UID) {
				keepHeld()

				continue
			}
			// spec 在此处发起的移除会交由协议处理而非直接下发，从而使本轮触发的缩容成为一次有序退出
			// 的起点，而不是对运行中服务引擎的瞬间切断。只有当某个 ordinal 不再被该 role 声明时才会
			// 移交协议；下方其余情况均走常规分类逻辑。
			if ok && ordinal >= declared &&
				r.refuseModelDeploymentRemoval(ctx, md, retirement, pod, actual, nil) {
				keepHeld()

				continue
			}

			switch {
			case !ok:
				kept = append(kept, pod)
				orphans++
			case ordinal >= declared:
				removed = append(removed, pod)
			default:
				occupied[ordinal] = true
				kept = append(kept, pod)
			}
		}

		// 数量约束在这里依然生效，用于处理仅靠 ordinal 算术无法表述的状态：存活 Pod 数多于
		// role 声明的数量，且它们全都落在 spec 仍保留的槽位上——即同一个 ordinal 上存在重复，
		// 或者存在完全没有 ordinal 的 Pod。多余部分按槽位而非按年龄清理：没有 ordinal 的 Pod
		// 不占任何席位，优先于所有占位 Pod 离开；而承载多个成员的槽位则保留与当前渲染结果一致的
		// 成员（反正替换副本也会按此构建，清理它只会把同样的震荡推迟到下一轮），在无法区分的
		// 成员之间则按名称最大者胜出，这只为保证确定性而无业务含义。每个离开的副本都会带走其
		// Workload：让多余成员继续留在原地会导致 Kueue 在每一轮都自行驱逐一个成员。
		if surplus := modelDeploymentSurplusCount(kept); surplus > 0 {
			shedSurplus := modelDeploymentSurplusReplicas(kept, want, surplus, minRenderVersion)
			// 这次清理是不可分割的单一决策，不会部分执行。协议接收的是整个集合而非其中的第一个成员，
			// 因为副本是作为一个整体被移除的：如果先清理掉协议即将整体接管的集合中的某个成员，就会
			// 导致该组缺员，而 Kueue 对缺员组根本不会组合 Workload。没有 ordinal 的成员既未获得
			// 准入也不受保留，直接交由下方的滚动更新处理。
			if _, seated := modelDeploymentRetirementShedSeat(shedSurplus); seated &&
				r.refuseModelDeploymentRemoval(ctx, md, retirement, shedSurplus[0], actual,
					modelDeploymentShedMembers(shedSurplus)) {
				logger.V(3).Info("holding a surplus shed for the retirement", "role", role.Name,
					"members", len(shedSurplus))
				occupied[modelDeploymentOrdinalOrFloor(shedSurplus[0])] = true
			} else {
				removed = append(removed, shedSurplus...)
				kept = slices.DeleteFunc(kept, func(pod *core.Pod) bool {
					return slices.Contains(shedSurplus, pod)
				})
			}
		}

		// 按 ordinal 从高到低排序，使离开操作按自顶向下的顺序记录并下发，且对同一状态的多轮
		// 扫描会按相同顺序执行删除。没有 ordinal 的 Pod 排在最后：它不占任何席位，因此所有
		// 占位的 Pod 都排在它前面，槽位无法决定的顺序则由名称决定。
		slices.SortFunc(removed, func(a, b *core.Pod) int {
			if c := modelDeploymentOrdinalOrFloor(b) - modelDeploymentOrdinalOrFloor(a); c != 0 {
				return c
			}

			return strings.Compare(b.Name, a.Name)
		})

		var shed bool
		if len(removed) > 0 {
			for _, pod := range removed {
				logger.Info("removing replica the role no longer declares",
					"pod", pod.Name, "role", role.Name)
				if err = r.Client.Delete(ctx, pod); err != nil && !kerrors.IsNotFound(err) {
					logger.Error(err, "delete removed replica", "pod", pod.Name)
					return ctrl.Result{}, err
				}
			}
			removedPods := make([]core.Pod, 0, len(removed))
			for _, pod := range removed {
				removedPods = append(removedPods, *pod)
			}
			if err = r.deleteModelDeploymentGroupWorkload(ctx, md, removedPods); err != nil {
				logger.Error(err, "delete removed replicas' workloads", "role", role.Name)
				return ctrl.Result{}, err
			}
			shed = true
			requeue = true
		}

		// 在比较之前而非之后进行计数：到达这一行才说明本轮具备给出完整答案的能力，如果一轮
		// 检查尚未走到此处就回答“没有过期副本”，它汇报的只是它查看过，而不是实际发现了什么。
		//
		// 哈希归席位自身所有，因为组名——以及随之而来的指纹——都是按 (role, ordinal) 派生的：
		// 一个 Pod 只有与其自身席位的渲染结果匹配才算最新，这正是扩容不会触发存量副本滚动、
		// 槽位互换也不会悄然蒙混过关的原因。
		//
		// 裁决的对象是副本而非单个 Pod，这两者并非同一答案的不同粒度。同一副本的所有成员共享
		// 同一个 Kueue 组并作为整体准入；Kueue 会一直扣留已准入组中被删除的成员，直到该组的
		// Workload 消失，而删除该 Workload 无论如何都会停止其余存活成员。因此，替换一个副本中的
		// 单个成员并不比替换整个副本更省事——它只是同样的操作，却在中间多出一段服务组无法修复的
		// 窗口期。因此只要有一个成员被判定为过期，就会连带判定其所属副本需要替换，而据此汇总的
		// 计数也以副本为单位，从而一个包含 2 个副本、每个副本 4 个 Pod 的 role 会报告有 2 个
		// 可滚动对象，而非 8 个。
		outdatedByOrdinal := make(map[int]bool, len(kept))
		orphaned := make([]*core.Pod, 0, orphans)
		for _, pod := range kept {
			ordinal, ok := modelDeploymentPodOrdinal(pod)
			if !ok {
				// 未声明 ordinal 的 Pod 不属于任何副本，因此它自身就是一个独立的滚动更新单元——这与
				// 副本引入成员概念之前的行为完全一致。
				rollout.accounted++
				rollout.outdated++
				orphaned = append(orphaned, pod)

				continue
			}

			member := modelDeploymentPodMemberIndex(pod)
			members := want[ordinal]
			if member < len(members) &&
				modelDeploymentPodMatchesCurrency(pod, members[member], minRenderVersion) {
				continue
			}

			// 无论其指纹是否匹配，被保留的副本一律视为最新。它已经在按协议自身的节奏退出，若在此处
			// 也将其判为过期，就会导致滚动更新与协议争夺同一个副本：本处选择会挑出最高的 ordinal，
			// 于是被保留的 ordinal 会被先运行的一方删掉，使协议精心安排的撤回流程白白浪费在滚动更新
			// 已经接手的 Pod 上。将其视为最新没有任何代价——其他所有副本仍会正常比较指纹——并且能使
			// 两条路径互不交叉。
			if retirement.holds(pod.UID) {
				continue
			}

			outdatedByOrdinal[ordinal] = true
		}

		// 缺少声明成员数的副本同样视为过期，而上面的逻辑不会发现这一点：已存在的每个 Pod
		// 都可能携带着最新指纹，但该副本依然缺员，而 Kueue 无法为其组合 Workload 的副本也不可能
		// 是部署正在依赖的可用服务实例。因此它会按与携带旧渲染结果的副本相同的规则被判待替换，
		// 从而使修复路径保持单一。
		//
		// 用于比较的总数是成员自身携带的总数，绝不是 role 的期望尺寸：当一个 role 从 4 个成员
		// 编辑为 2 个成员时，每个运行中的副本在 4 个成员时仍是完整的，若将其视为缺员就会导致
		// 它们在同一时刻全部被判待替换。
		incompleteByOrdinal := make(map[int]bool, len(occupied))
		for _, view := range modelDeploymentGroupPods(modelDeploymentPodValues(kept), true) {
			if !view.Seated || !occupied[view.Ordinal] {
				continue
			}
			// 属于退役协议管辖的序号不归此规则处理。滚动更新会淘汰成员与 spec 不一致的副本，而在预留
			// 操作进行期间一致性尚未确定——协议正在移除双份序号中的一个成员，这会在操作持续期间让该
			// 序号在此规则看来像是超额完整。如果在这里淘汰它，就会删掉整个序号，连同盈余规则旨在保留
			// 的成员一起删掉：滚动更新会用更粗暴的选择推翻盈余规则对已解决状态的谨慎抉择。
			if retirement.names(role.Name, view.Ordinal) {
				continue
			}
			if modelDeploymentDeployedReplicaShape(view).State == modelDeploymentReplicaShapeIncomplete {
				outdatedByOrdinal[view.Ordinal] = true
				incompleteByOrdinal[view.Ordinal] = true
				continue
			}
			if !modelDeploymentReplicaRenderVersionUniform(view.Members) {
				outdatedByOrdinal[view.Ordinal] = true
			}
		}

		// 部分创建仅在槽位正在构建的组中补齐，且仅当该组属于当前渲染版本且未持有任何准入时才执行。
		// 失败的创建会使替换副本缺少一个成员而无法达到总数，而 Kueue 不会为不完整的组生成 Workload，
		// 因此若不补齐，槽位就会一直等待一个永远无法获得任何准入的组。
		//
		// 绝不在运行旧配置的组旁边补齐。数量相等不代表兼容：将当前 spec 渲染的成员与上一版本渲染的
		// 成员放在同一个组中，会导致成员对自身定义产生分歧，这是本 operator 无法渲染、准入或核算
		// 的状态。因此，在旧渲染版本下丢失成员的副本不会在此补齐——它会在下方槽位之后等待并被整体替换。
		//
		// 成员仅限于该槽位自身序号且不得有其他序号，且该组不得持有 Workload：挂着 Workload 并不能
		// 证明已获准入，而带有 Workload 的组已经是 Kueue 组合好的组，对其补齐属于 Kueue 的职责
		// 而非本 operator 的职责。
		if slot, holding := holdingRoles[role.Name]; holding {
			partial, fillable, err := r.modelDeploymentPartialGroupMembers(ctx, md, slot, want[slot.Ordinal])
			if err != nil {
				logger.Error(err, "read the replacing replica's group", "role", role.Name,
					"ordinal", slot.Ordinal)

				return ctrl.Result{}, err
			}
			if fillable {
				partialCreates[role.Name] = append(partialCreates[role.Name], partial...)
			}
		}

		outdated := orphaned
		for ordinal := range occupied {
			rollout.accounted++
			if outdatedByOrdinal[ordinal] {
				rollout.outdated++
				outdated = append(outdated, modelDeploymentReplicaMembers(kept, ordinal)...)
				continue
			}
			if members := modelDeploymentReplicaMembers(kept, ordinal); len(members) > 0 &&
				!retirement.names(role.Name, ordinal) &&
				modelDeploymentPodRenderVersion(members[0]) < modelDeploymentRenderVersion {
				rollout.olderRenderVersion++
			}
		}

		if connection == nil && md.Status.KVCache != nil && len(outdated) > 0 {
			// 丢失连接器不是重建副本的理由，但没有此保护时，哈希值会将其视为需要重建。无法解析连接的
			// 轮次会渲染不带连接器的副本，导致每个运行中副本的哈希都与期望值不同，从而使下方的重建逻辑
			// 同时触发全部副本的重建。
			//
			// 此保护必须同时包含两个条件。从未解析过域的部署本来就没有连接器可丢，因此其中的哈希差异
			// 源于 spec 变更且仍必须滚动更新——仅凭 `connection == nil` 抑制滚动更新会破坏所有根本不
			// 使用缓存的部署的正常滚动，而这正是未写后半部分条件时 spec 变更测试所反映的问题。
			//
			// 触发该状态的场景很常见而非罕见：存储 leader 重启会使池上的每个 Binding 短暂变为非 Ready
			// 状态——在本项目中实测为 3.5 到 32 秒——而 Binding 不可用的部署无法解析连接。因此存储
			// 几秒钟的不可用就会删除该池上所有部署的全部副本，并导致每个副本重新加载权重，使短暂波动
			// 演变成服务中断。
			//
			// 这样做的代价是，在连接恢复之前，也会暂缓会改变运行中副本渲染 Pod 的 spec 变更。
			//
			// 本设计拒绝采用可规避此问题的拆分方案，且原因并非最初搁置它时的代价。该方案在不含连接器
			// 的 spec 基础哈希之外单独维护连接器指纹，从而在跳过指纹比较时仍可按基础哈希滚动更新
			// spec 修改。真正产生代价的是按这种精确度行动本身：基础哈希差异会删除副本，替换副本由
			// 仍无连接器可提供的轮次渲染，而当存储恢复时期望渲染重新获得连接器并再次滚动替换该副本。
			// 当有修改在等待时，拆分方案会让一个副本付出两次重新加载的代价，而非一次。
			//
			// 随着其依托的重建机制退出，operator 过去持有的权衡已不复存在：副本数变更不再替换任何副本，
			// 因此没有任何操作能绕过此保护将暂缓的修改带过去。中断期间的扩缩容仍会继续——新增的序号
			// 尚不存在，下方的创建逻辑会执行——但新创建的副本在存储恢复时必须自行承受第二次轮替，这
			// 正是旧机制本就会收取且无人能再免除的代价。如今限制等待时长的是存储自身的恢复速度，而
			// condition 中的消息仅说明正在等待什么，而不承诺其他出路。
			//
			// 保留现有副本不动也是本设计对邻近场景已做出的决定：管理员删除 Binding 时会让运行中的 Pod
			// 继续运行，因为仅仅由于管理对象消失就拆掉正在提供服务的部署，比无缓存提供服务更糟糕。
			// 同样的逻辑也适用于仅处于不健康状态的 Binding。
			//
			// 连接恢复后不会造成任何损失。连接器会解析为相同的值，期望哈希会回到这些副本已携带的值，
			// 此分支也随之停止触发。如果恢复时返回了不同的端点，期望哈希就会与副本携带的哈希不同，
			// 它们会在该轮次被重建——这正是应当发生的滚动更新。
			//
			// 下方仍会创建不带连接器的新副本：尚不存在的副本同样无法被赋予尚不存在的地址。
			logger.V(3).Info("no connection this pass; leaving the replicas as built", "role", role.Name)
			// 按副本数统计，与上方的 rollout.outdated 保持一致：`outdated` 是待删除 Pod 的列表，多成员
			// 副本会向其中贡献多个 Pod，因此对于报告等待中实例数量的指标而言，其长度并不是正确的单位。
			rollout.held += len(outdatedByOrdinal) + len(orphaned)
			outdated = nil
		}

		if weights != nil && weights.Blocked && len(outdated) > 0 {
			// 在替换副本无法创建期间不会轮替现有副本。权重是替换副本所必需的，在权重受阻期间——如访问
			// 权限被撤销或申领无法调度放置——下方的创建逻辑会被暂缓，此时执行重建只会删除正在提供服务
			// 的副本。修改会等待权重就绪，正如上方等待丢失的连接器恢复一样。
			rollout.heldByWeights += len(outdatedByOrdinal) + len(orphaned)
			outdated = nil
		}

		// 滚动更新采用重建而非激增策略：spec 变更前构建的副本会在此处被删除，并在后续轮次中
		// 被替换。其代价是该副本已缓存的数据块，当它离开时其同伴也会失去这些缓存。
		//
		// 滚动更新的计量单位是已准入副本，而非存活副本。替换副本需要重新准入——删除离开副本的
		// Workload 才会释放名额，而随着该删除操作，预留也会一并消失——因此已创建但仍处于排队中
		// 的替换副本在等待期间不持有任何容量。若保护条件统计存活副本，就会把排队中的 Pod 视为
		// 进度并继续删除其后的过期副本；在资源竞争的池中，滚动更新会将部署一路削减至零个已准入
		// 副本并停在那里，形成既不被声明数量覆盖、也无法被屏障捕获的缺口。因此，保护条件仅在
		// 该角色声明的每个副本都持有已准入的 Workload 时才轮替一个副本：同一时间只有一个准入处于
		// 进行中，待替换副本实际获准后再进入下一轮以淘汰下一个。仍在等待首次准入的完整组可直接
		// 替换，无需消耗已准入容量。此前曾获准入的组在准入被撤销后仍受此保护条件约束。
		//
		// 离开副本的 Workload 会随之一同删除，这并非形式化的记账。该组被标记为 serving，因此
		// 无论 Pod 经历何种排空阶段，Kueue 都不会释放其持有的 Pod finalizer；留存的 Workload
		// 会使该序号持续被占用，导致替换副本永远无法写入。正是该删除操作清空了下方创建门控所
		// 观察的槽位，并消耗了预留——这也是上方保护条件拒绝同时运行两个轮替的原因。
		//
		// 每个角色每轮最多轮替一个，绝不与本轮已裁减的盈余副本并行，且优先轮替最高序号——即缩容
		// 裁减的那一端——从而使滚动更新保持最新的序号从零开始密集排列，且对同一状态的两次执行
		// 会选中同一个目标。
		// 无法作为已声明副本获准入的 Pod 会绕过保护条件直接轮替，这一例外使保护条件成为刹车机制
		// 而非死锁。满足条件的有两种形态，原因只有一个。
		//
		// 保护条件的作用是逐个副本消耗已准入容量，它通过将已准入 Workload 数量与已声明副本数量
		// 进行比较来读取容量。不完整的副本没有任何容量可消耗：对于成员不足声明总数的组，Kueue
		// 根本不会生成 Workload，因此它不持有预留、不提供服务，对计数贡献为零。而未声明序号的 Pod
		// 则会破坏该比较所依赖的对应关系——它不占据任何副本席位，因此无论哪个 Workload 拥有它，
		// 都无法对应任何已声明槽位：自持 Workload 会使计数偏多，与同伴共享则会使计数偏少。两者是
		// 同一缺陷从不同角度的体现，无论哪种情况，比较基准都是该角色永远无法再达到的数字。
		//
		// 若交由保护条件处理，这两种情况都会陷入双向死锁——它永远无法获准入，导致该角色永远无法
		// 再次满足 `admitted == declared`，从而既不会被替换，后续的 spec 修改也永远无法滚动生效。
		// 该角色会一直处于损坏状态，并静默忽略后续所有变更。
		//
		// 下方的创建门控也无法覆盖这两种情况：哪怕只持有一个成员的序号也会被视为已占用，而没有
		// 序号的 Pod 会在那里被计入声明数量，因此不会为它们补建。与其他所有成员丢失情况一样，
		// 整体替换是唯一的修复方式——不完整副本的存活成员持有一个 Kueue 既不会准入也不会释放的组，
		// 在它们旁边放入新成员只会加入一个已被拒绝的组。
		//
		// 这也保证了下方已准入计数逻辑的准确性。该计数遍历角色下的所有存活 Pod（包括无序号 Pod），
		// 本应是错误的分母——但根据上述规则，所有此类 Pod 都属于过期副本，因此能走到比较这一步
		// 就意味着不存在此类 Pod，读取到的计数精确对应已占席位的副本。
		// 有两类副本无需健康副本所需的准入即可轮替，由于它们的等待机制不同，此处将它们分开处理。
		//
		// 未声明序号的 Pod 不占据席位也无准入，没有任何槽位能用序号描述它，因此它无需在任何槽位
		// 后等待：它在此处走自己的路径完成轮替。
		//
		// 缺少成员的副本属于损坏副本，位于槽位之后的损坏副本必须等待该槽位。槽位的替换副本必须先
		// 获准入，因为那是该角色唯一承诺推进的组；其后的损坏组既未获准入也不会获准入，现在处理
		// 它反而会删掉它仍持有的成员去重建一个其余成员已过期的组。一旦槽位清空，它会优先于任何
		// 健康的过期副本被整体替换——参见下方的挑选逻辑。
		ungated := make([]*core.Pod, 0, len(outdated))
		broken := make([]*core.Pod, 0, len(outdated))
		for _, pod := range outdated {
			ordinal, ok := modelDeploymentPodOrdinal(pod)
			switch {
			case !ok:
				ungated = append(ungated, pod)
			case incompleteByOrdinal[ordinal]:
				broken = append(broken, pod)
			}
		}

		if len(outdated) > 0 && !shed {
			workloads, wlErr := listWorkloads()
			if wlErr != nil {
				logger.Error(wlErr, "read the role's workloads", "role", role.Name)
				return ctrl.Result{}, wlErr
			}

			// 只要存在此类副本，目标就从它们之中挑选，使保护条件无法清理的副本得以离开。挑选规则与
			// 下方相同——取最高序号——从而对同一状态的两次执行仍会选中同一个 Pod，且已占席位的副本
			// 会排在无序号副本之前，因为挑选逻辑将缺失序号视为低于所有席位。
			_, holding := holdingRoles[role.Name]
			admitted := modelDeploymentAdmittedReplicas(workloads, live)
			// 损坏的副本会被整体替换，且优先于任何健康的旧副本。它是两者中唯一已经停机的副本，若让它
			// 等在健康副本之后，角色就会在替换正在提供服务的容量的同时让已丢失的容量继续等待。它所在
			// 的序号并不影响优先级：序号低于健康副本的损坏副本也是两者中更紧急的那个。
			brokenTurnsOver := len(broken) > 0 && !holding
			unguarded := len(ungated) > 0
			var initiallyQueued []*core.Pod
			if !holding && !brokenTurnsOver && !unguarded && admitted != declared {
				for _, view := range modelDeploymentGroupPodsByReplica(modelDeploymentPodValues(outdated)) {
					queued, queueErr := r.initialQueuedReplica(ctx, view, workloads)
					if queueErr != nil {
						return ctrl.Result{}, queueErr
					}
					if queued {
						initiallyQueued = append(initiallyQueued, view.Members...)
					}
				}
			}
			pick := outdated
			switch {
			case brokenTurnsOver:
				pick = broken
			case unguarded:
				pick = ungated
			case len(initiallyQueued) > 0:
				pick = initiallyQueued
			}

			// 替换副本尚未获准入的角色不会再消耗更多健康容量。排队中的替换副本不持有任何容量，因此若在
			// 其后执行第二次删除，每一轮都会移除另一个已准入副本，直到角色不再持有任何副本为止。
			//
			// 该暂缓同样涵盖损坏的副本：槽位自身的替换副本才是该角色当前承诺推进的组，其后的损坏组会
			// 在该替换副本获准入后再整体修复，而不会为了省一轮处理而提前拆解。
			turnsOver := brokenTurnsOver || unguarded || len(initiallyQueued) > 0 || (!holding && admitted == declared)
			if turnsOver {
				// 轮替的是整个副本，而不仅是被选中的成员。挑选结果指定一个 Pod，实际离开的是该 Pod 所属
				// 的副本：其成员共同持有一个 Kueue 组，因此只删除部分成员会留下一个 Kueue 既不准入也不
				// 释放的组，况且下方的 Workload 删除操作无论如何都会终止其余成员。
				pod := modelDeploymentHighestOrdinalReplica(pick)
				ordinal, seated := modelDeploymentPodOrdinal(pod)
				departing := []*core.Pod{pod}
				if seated {
					departing = modelDeploymentReplicaMembers(pick, ordinal)
				}

				// 未声明序号的 Pod 不属于任何槽位，以序号为键的槽位也无法描述它：槽位的空位检查针对的
				// 是其成员并未占据的序号，因此它会一直等待一个该 Pod 从未属于过的组。此类 Pod 仅走无门控
				// 修复路径完成滚动，其所携带的组名就是依据。
				held, hasSlot := slots.slotFor(role.Name)
				switch {
				case hasSlot && seated && held.Ordinal == ordinal:
					if err = r.cleanReplacementSlot(ctx, md, held, departing); err != nil {
						logger.Error(err, "clean the replacing replica",
							"role", role.Name, "ordinal", ordinal)
						return ctrl.Result{}, err
					}
				case seated:
					logger.Info("recreating replica",
						"pod", pod.Name, "members", len(departing),
						"why", map[bool]string{true: "short of its members", false: "built from an earlier spec"}[brokenTurnsOver])

					// 槽位在执行任何删除之前写入，用于在删除与创建之间发生重启时进行恢复。
					slot := replacementCapturedIdentities(modelDeploymentReplicaView{
						Role:    role.Name,
						Ordinal: ordinal,
						Members: departing,
					}, workloads)
					if err = r.beginReplacementSlot(ctx, md, slot); err != nil {
						return objectWriteResult(
							logger, err, "record the replacement slot", _requeueAfterConflict,
						)
					}
					slots = slots.setSlot(slot)

					if err = r.cleanReplacementSlot(ctx, md, slot, departing); err != nil {
						logger.Error(err, "clean the replaced replica",
							"role", role.Name, "ordinal", ordinal)
						return ctrl.Result{}, err
					}

				default:
					logger.Info("recreating replica",
						"pod", pod.Name,
						"why", map[bool]string{
							true:  "holds no ordinal, so no slot can name it",
							false: "short of its members",
						}[unguarded])
					departed := make([]core.Pod, 0, 1)
					uid := pod.UID
					if err = r.Client.Delete(ctx, pod, ctrlcli.Preconditions{UID: &uid}); err != nil &&
						!kerrors.IsNotFound(err) {
						logger.Error(err, "delete the ordinal-less replica member", "pod", pod.Name)
						return ctrl.Result{}, err
					}
					departed = append(departed, *pod)
					if err = r.deleteModelDeploymentGroupWorkload(ctx, md, departed); err != nil {
						logger.Error(err, "delete the departing replica's workload", "pod", pod.Name)
						return ctrl.Result{}, err
					}
				}

				if seated {
					if replacedByRole[role.Name] == nil {
						replacedByRole[role.Name] = make(map[int]bool)
					}
					replacedByRole[role.Name][ordinal] = true
				}

				requeue = true
			}
			// 否则滚动更新会暂缓而非在容量不足的情况下继续推进，且不会请求重新入队。解除该暂缓的准入
			// 操作会推动等待中副本的 Workload 状态变更，Workload watch 会在该变更时唤醒此部署；随后
			// 保护条件会从 API server 读取 Workload，其数据至少与事件来源的缓存一样新。
		}

		// 复用序号要求在 API server 上同时存在 Pod 和 Workload 空位。缓存可能会漏掉刚成功的创建，
		// 或尚未完全退出的带 finalizer 占用者。
		// 缺失的序号按从低到高的顺序填充，且仅填充差额数量：若一个角色缺少一个副本但有两个空闲序号
		// （其中混有按副本分组改版前遗留的仍在提供服务的 Pod），则会创建较低序号的槽位，并让多余
		// Pod 自行退出以解决另一个。
		// 按副本数而非 Pod 数计算。角色低于其声明数量意味着缺少完整实例，而 `kept` 是 Pod 列表——
		// 当组大小为 1 时两者数值相同，这也是过去每个副本对应一个 Pod 时这里写成 len(kept) 的原因。
		// 只要某个序号有任何占用，此处就计为存在，即便它缺少成员也是如此；该问题由上方的滚动更新
		// 修复，因为原地填补缺口会把新成员放进一个已被 Kueue 拒绝的组中。
		if missing := declared - len(occupied) - orphans; missing > 0 {
			// 通过两次无缓存列表查询确定该角色所有序号的空位情况。
			taken, takenErr := r.modelDeploymentTakenGroups(ctx, md, role.Name, true)
			if takenErr != nil {
				return ctrl.Result{}, takenErr
			}
			workloads := new(kueue.WorkloadList)
			if err := r.APIReader.List(ctx, workloads, ctrlcli.InNamespace(md.Namespace)); err != nil {
				return ctrl.Result{}, err
			}
			for ordinal := range declared {
				group := modelDeploymentReplicaGroupName(md, role.Name, ordinal)
				for i := range workloads.Items {
					if workloadOwnsGroupName(&workloads.Items[i], group) {
						taken.Insert(group)
						break
					}
				}
			}

			// 槽位对应的序号会最先填充：它是该角色已承诺推进的序号，否则因其他原因缺少副本的角色会把
			// 创建配额花在别处而让此序号继续等待。
			//
			// 它会执行与其他所有序号相同的无缓存检查。该槽位删除的成员可能因 finalizer 仍驻留在
			// 服务端上，而替换副本会以该成员仍占用的名称来渲染——因此若绕过门控提前填充此序号，就会
			// 在同一组中放置两个成员并发生名称冲突。
			if slot, held := holdingRoles[role.Name]; held && !occupied[slot.Ordinal] {
				if !taken.Has(modelDeploymentReplicaGroupName(md, role.Name, slot.Ordinal)) {
					createOrdinals[role.Name] = append(createOrdinals[role.Name], slot.Ordinal)
				} else {
					requeue = true
				}
			}

			for ordinal := 0; ordinal < declared && len(createOrdinals[role.Name]) < missing; ordinal++ {
				if occupied[ordinal] || slices.Contains(createOrdinals[role.Name], ordinal) {
					continue
				}

				// 退役操作正在清空的槽位在操作结束前必须保持空置。保留的副本在上方仍会把自己计为已占用，
				// 因此此保护仅在协议自身的删除已落地且 Pod 已从 API server 消失后才会生效——否则此时创建
				// 门控会在预留清除前一轮将该序号判定为空闲并为其构建替换副本。填充操作即将释放的槽位，
				// 正是退役操作演变为滚动更新的原因。
				if retirement.Reservation != nil &&
					retirement.Reservation.RoleName == role.Name &&
					int(retirement.Reservation.ReplicaOrdinal) == ordinal {
					requeue = true

					continue
				}

				if taken.Has(modelDeploymentReplicaGroupName(md, role.Name, ordinal)) {
					// 服务端上存在缓存列表尚未计入的该序号 Pod：可能是响应丢失的创建操作，或是仍在排空的
					// 退出过程。本轮不会在旁另行创建副本，而是先返回等待——informer 最终会投递该 Pod 的事件，
					// 而重新入队则无论哪种情况都能兜底，因为排空完成时没有其他机制会唤醒此部署。
					requeue = true

					continue
				}

				createOrdinals[role.Name] = append(createOrdinals[role.Name], ordinal)
			}
		}
	}

	// 进行中的操作需要重新入队，且该状态在此处而非上方删除操作旁读取，是因为准入发生在刚刚
	// 结束的循环内部。对于仅仅在等待队列排空或 Service 收敛的副本，没有其他机制能唤醒该部署：
	// 等待期间没有任何对象发生变更，而最终真正触发进展的正是该操作所等待的事件。
	if retirement.InFlight {
		requeue = true
	}

	// 在权重受阻期间不会创建任何副本，也不会触动正在运行的副本：被撤销的凭证只阻止新的消费，
	// 绝不影响运行中的消费，而无法调度放置的申领只会产生处于 Pending 状态的 Pod，同时其
	// Workload 还会持续占用配额。
	if weights != nil && weights.Blocked {
		clear(createOrdinals)
		clear(partialCreates)
	}

	// 放置偏好在本轮创建任意成员时读取一次，若本轮不创建任何成员则完全不读取。
	var preference *core.PreferredSchedulingTerm
	for _, ordinals := range createOrdinals {
		if len(ordinals) > 0 {
			preference = weights.placementPreference(ctx, r.Client)
			break
		}
	}
	if preference == nil {
		for _, created := range partialCreates {
			if len(created) > 0 {
				preference = weights.placementPreference(ctx, r.Client)
				break
			}
		}
	}

	// 创建失败同样不会中止本轮处理，这正是让不完整分组进入“已报告”状态而非静默状态的
	// 原因。若在此处直接返回，就会跳过下方的状态写入，导致唯一知道分组人数不足的那一轮
	// 处理反而什么都不说——而不完整分组本身的症状就已经是静默：Pod 虽已存在，但处于
	// gated 状态，Kueue 根本不会组装任何 Workload。
	//
	// 其余创建请求仍会继续发出，因为该分组在任何成员获准调度前都必须凑齐全部成员；在首次
	// 失败时就停止，只会让分组比实际需要更加残缺。error 会在状态写入之后返回，因此本轮
	// 依然不会被视为成功，缺失的副本也会得到重试。
	var createErr error
	for _, role := range modelDeploymentCreateRoleOrder(md) {
		for _, ordinal := range createOrdinals[role.Name] {
			if slot, held := slots.slotFor(role.Name); held && slot.Ordinal == ordinal &&
				slot.Phase == modelDeploymentReplacementCleanup {
				slot = modelDeploymentReplacementSlot{
					Role: role.Name, Ordinal: ordinal, Phase: modelDeploymentReplacementWaitingForAdmission,
					MemberHashes: replacementRenderHashes(desired[role.Name][ordinal]),
				}
				if err := r.advanceReplacementSlot(ctx, md, slot); err != nil {
					return objectWriteResult(logger, err, "record replacement creation", _requeueAfterConflict)
				}
				slots = slots.setSlot(slot)
			}

			// 副本的每个成员都会被发出创建请求，单个成员失败不会放弃其余成员。
			// 这些成员共享同一个 Kueue 分组，其声明的总数为该副本的规模；在存在足够数量的成员
			// 之前，Kueue 根本不会组装 Workload——因此一个残缺的副本会让其成员一直处于 gated
			// 状态，且没有任何指明原因的信息。当失败只是瞬时问题时，继续发出其余成员毫无代价；
			// 当失败不是瞬时问题时，这也是唯一的出路。
			for _, want := range desired[role.Name][ordinal] {
				// 每个成员对应一个新对象：模板针对该角色的每个 Pod，分别打上各自对应的序号和成员索引。
				pod := want.DeepCopy()
				// 在创建时注入而非在渲染阶段写入，这样在 claim 绑定之前创建的副本（此时尚无亲和性）
				// 不会在 claim 绑定后被滚动重建。放置偏好也出于更强的原因遵循同一规则：持有权重的
				// 节点会随着每次下载和回收而变化，若将其纳入指纹，每次都会重建所有副本。它在本轮
				// 处理中只读取一次，因此同一副本的每个成员都携带相同的规则项——Kueue 正需要这一点，
				// 因为它根据单个成员的 spec 来构建整个副本的 PodSet。
				if weights != nil {
					injectModelArtifactAffinity(pod, weights.Affinity)
				}
				injectModelPlacementPreference(pod, preference)
				if err = r.Client.Create(ctx, pod); err != nil {
					logger.Error(err, "create replica member",
						"role", role.Name, "ordinal", ordinal,
						"member", pod.Labels[modelDeploymentMemberIndexLabel])
					if createErr == nil {
						createErr = err
					}

					continue
				}
				logger.Info("created replica member", "pod", pod.Name)
			}
			// 本轮刚刚渲染并创建的副本在构造上就是最新的，因此本轮可以将其计入。
			// 否则 Deployment 的第一轮处理将完全无法统计，而第二轮又会为无人修改过的 spec 写入一次状态。
			//
			// 这里统计的是副本数而非其成员数，因为这组 rollout 数据面向的是实例：一个包含两个
			// 副本、每个副本四个 Pod 的角色，可能过时的对象是 2 个而非 8 个。
			rollout.accounted++
		}

		// 部分缺失副本中的缺失成员会按照完整副本成员的方式发出创建请求，进入其自身序号命名的
		// 分组并携带相同的放置规则，因为它本就是该分组的成员，Kueue 会一视同仁地由全部成员
		// 组装该分组。
		for _, rendered := range partialCreates[role.Name] {
			pod := rendered.DeepCopy()
			if weights != nil {
				injectModelArtifactAffinity(pod, weights.Affinity)
			}
			injectModelPlacementPreference(pod, preference)
			if err = r.Client.Create(ctx, pod); err != nil {
				logger.Error(err, "create the missing member of a partial replica",
					"role", role.Name,
					"ordinal", pod.Labels[modelDeploymentReplicaOrdinalLabel],
					"member", pod.Labels[modelDeploymentMemberIndexLabel])
				if createErr == nil {
					createErr = err
				}

				continue
			}
			logger.Info("created the missing member of a partial replica", "pod", pod.Name)
		}
	}

	for name, slot := range slots {
		if slot.Phase != modelDeploymentReplacementWaitingForAdmission ||
			(len(createOrdinals[name]) == 0 && len(partialCreates[name]) == 0) {
			continue
		}
		members, err := r.replacementOccupancyOnServer(ctx, md, name, slot.Ordinal)
		if err != nil {
			return ctrl.Result{}, err
		}
		uids := sets.New[types.UID]()
		for _, member := range members {
			uids.Insert(member.UID)
		}
		slot.CurrentMemberUIDs = sets.List(uids)
		if err := r.advanceReplacementSlot(ctx, md, slot); err != nil {
			return objectWriteResult(logger, err, "record replacement members", _requeueAfterConflict)
		}
	}

	// 整组健康谓词在此处仅求值一次，位于任何携带资格等式条件的 Service 或 selector 之前，
	// 同一结果同时用于下方的标签写入和更后面的状态写入。按副本重新分组两次，正是导致
	// 两个统计口径对同一副本产生分歧的原因，而且这里的分歧会悄无声息：一个结果驱动
	// 流量选择，另一个结果则被对外发布。
	groupForwardFetch := r.groupForwardFetch
	if groupForwardFetch == nil {
		groupForwardFetch = defaultGroupForwardFetch
	}
	groupForwardPairFetch := r.groupForwardPairFetch
	if groupForwardPairFetch == nil {
		groupForwardPairFetch = defaultGroupForwardPairFetch
	}
	qualifications := qualifyModelDeploymentInstances(
		ctx, md, actual, modelDeploymentPendingReplacement{ordinals: replacedByRole},
		groupForwardFetch, groupForwardPairFetch,
	)
	qualificationByMember := modelDeploymentQualificationsByMember(qualifications)

	eligibilityDecided := modelDeploymentQualificationsDecided(qualifications)

	// 本轮观测在资格判定之后、标签写入之前仅采集一次，标签写入和状态写入均读取这份结果。
	// 资格判定可能耗费整次探测的时间，在此之前采集的结果等到恢复决策读取时就已经过时；
	// 而采集两次则会得到针对两个不同时刻的两份答案。
	observation := modelDeploymentPassObservation{
		Serving:        r.observeModelDeploymentServing(ctx, md),
		Qualifications: qualifications,
	}

	if err = r.convergeModelDeploymentEndpointEligibility(
		ctx, md, actual, qualificationByMember, replacedByRole, observation,
	); err != nil {
		logger.Error(err, "converge endpoint eligibility")
		return ctrl.Result{}, err
	}
	servicePods, err := readModelDeploymentPods(ctx, md, r.APIReader)
	if err != nil {
		return ctrl.Result{}, err
	}
	if err = r.syncModelDeploymentService(ctx, md, eligibilityDecided, servicePods); err != nil {
		logger.Error(err, "sync service")
		return ctrl.Result{}, err
	}
	// router 同步失败不会中止本轮处理，原因与创建失败相同：失败原因会被投射到下方写入的
	// 状态中，在此处直接返回会跳过唯一能说明问题所在的写入。error 仍会在该次写入后返回，
	// 因此本轮仍会按此处失败一样触发重试。
	routerErr := r.syncModelDeploymentRouter(ctx, md, eligibilityDecided)
	if routerErr != nil {
		logger.Error(routerErr, "sync router")
	}

	// 重新读取副本而非复用本轮开始时的列表：状态必须描述当前实际存在的对象，而非收敛逻辑
	// 当时作出决策所依据的快照。
	actual, err = r.listModelDeploymentPods(ctx, md)
	if err != nil {
		logger.Error(err, "list replicas")
		return ctrl.Result{}, err
	}

	r.recordModelDeploymentDepartures(md, actual)

	if err = r.syncModelDeploymentStatus(
		ctx, md, actual, domain, &rollout, weights, observation,
	); err != nil {
		// predicate 会丢弃仅更新 status 的事件，因此引发冲突的变更可能不会投递事件：此处主动
		// requeue 而不是干等事件。重新入队的这一轮也会重试此前失败的创建或 router 同步，
		// 否则它们的 error 会在下方返回。
		return objectWriteResult(logger, err, "sync status", _requeueAfterConflict)
	}

	// 到这里才返回 error，使本轮不被视为成功并触发缺失副本的重试——同时确保状态已经向
	// 读取方说明了分组人数不足的原因。
	if createErr != nil {
		return ctrl.Result{}, createErr
	}

	if routerErr != nil {
		return ctrl.Result{}, routerErr
	}

	if requeue {
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}

	// 因交付阻塞而等待的 Deployment 会被 CSIDriver 和 Settings 的 watch 唤醒；此外它也会
	// 自行定期重试，这样即使漏掉一次事件或当时读取的 Settings 仍是缓存值，代价也只是
	// 等待一分钟，而不是卡死 Deployment 的整个生命周期。
	if weights != nil && weights.Blocked && (weights.Reason == modelWeightsReasonNodeUnavailable ||
		weights.Reason == modelWeightsReasonFilterNeedsNode) {
		return ctrl.Result{RequeueAfter: modelDeploymentDeliveryRecheck}, nil
	}

	return ctrl.Result{}, nil
}

// recordModelDeploymentDepartures 为每个离开的副本记录一条事件，使运维人员在将突发请求
// 失败与抢占关联排查时有据可查，而不必自行推断。
func (r *ModelDeploymentReconciler) recordModelDeploymentDepartures(
	md *workercore.ModelDeployment, pods []core.Pod,
) {
	if r.Recorder == nil {
		return
	}

	for _, d := range modelDeploymentReplicaDepartures(pods) {
		r.Recorder.Event(md, core.EventTypeWarning, d.reason, d.message)
	}
}

// convergeModelDeploymentEndpointEligibility 是资格标签的唯一写入路径：
// 每一轮都会根据当前实际存在的 Pod 重新推导哪些成员可以提供服务，仅在这些成员上
// patch 该键，并从不再符合资格的成员上移除它。
//
// 它在同一轮中先于 Service 收敛运行，因为普通 Service 的 selector 带有资格等式条件：
// 在任何 selector 添加该条件之前，早于该特性创建的 Deployment 的健康成员就必须先带上
// 这些标签，否则首次启用时会把所有普通 Service 的 endpoints 清零。对于已经处于新形态
// 的对象树而言，这个顺序毫无额外开销——本轮会重新推导现状，仅写入存在差异的部分。
//
// 该键不参与任何渲染哈希。它在运行时被 patch 到运行中的 Pod 上，绝不渲染进模板，因此
// 加上或移除它不会触发 generation、UID、已存指纹或副本的变动——资格状态是唯一必须
// 在不引发 rollout 的前提下动态调整的开关。
//
// 冲突意味着状态已被外部观测改变。两个写入者会对同一问题给出两个答案，因此没有其他
// 路径写入该键；若 patch 在与其他变更的竞争中失败，本轮即失败，下一轮将根据届时的
// 现状重新推导。绝不盲目写入：diff 始终基于刚读取到的对象计算。
//
// 观测是写入的一部分，而非写入之后的第二步：标签与注解来自同一次决策，且该决策正是
// 同一轮对外发布的决策。
func (r *ModelDeploymentReconciler) convergeModelDeploymentEndpointEligibility(
	ctx context.Context, md *workercore.ModelDeployment, pods []core.Pod,
	qualificationByMember map[types.UID]modelDeploymentInstanceQualification,
	replacedByRole map[string]map[int]bool,
	observation modelDeploymentPassObservation,
) error {
	logger := ctrllog.FromContext(ctx)

	roles := make(map[string]*workercore.ModelDeploymentRole, len(md.Spec.Roles))
	for i := range md.Spec.Roles {
		roles[md.Spec.Roles[i].Name] = &md.Spec.Roles[i]
	}

	// 每个 Pod 所属的副本，仅分组一次。一个副本中哪些成员应答请求，是针对该副本自身的问题，
	// 因此应当根据该副本自己的成员来回答，而不是孤立地看单个 Pod 和当前 spec 中声明的角色。
	replicaOf := make(map[types.UID]modelDeploymentReplicaView, len(pods))
	for _, view := range modelDeploymentGroupPods(pods, true) {
		for _, member := range view.Members {
			replicaOf[member.UID] = view
		}
	}

	// 已经发布的 selector 决定了尚未作出决定的 Deployment 是否保留其现有路由。读取它而不
	// 是传入空值，才能让已启用的 selector 在未对其进行判定的各轮处理中保持启用状态。
	liveSelectors, err := r.modelDeploymentLiveRoleSelectors(ctx, md)
	if err != nil {
		return err
	}

	for i := range pods {
		pod := &pods[i]
		role := roles[modelDeploymentPodRole(pod)]
		if role == nil {
			continue
		}
		view, seated := replicaOf[pod.UID]
		if !seated {
			// 未持有自身席位的 Pod 仅按就绪状态（readiness）应答，这正是副本机制引入前的旧规则。
			view = modelDeploymentReplicaView{Members: []*core.Pod{pod}, Seated: false}
		}

		qualification, evaluated := qualificationByMember[pod.UID]
		decision := r.modelDeploymentGroupDecisionOf(observation, md, role, view, pod)
		eligible := modelDeploymentPodEligible(md, role, pod, view, qualification, evaluated, decision)
		// 门控检查针对的是该副本当前正在运行的模式，而非 spec 中此刻声明的模式。若按期望
		// 命令行判断，就会让一个已启用吊销机制的旧托管副本绕过该门控——而这个门控原本就是
		// 为了撤回这类副本而存在的。
		effective := *role
		if modelDeploymentReplicaRunsCommand(view.Members) {
			effective.Command = nil
		}
		gated := modelDeploymentEligibilitySelectorActive(md, &effective,
			liveSelectors[role.Name], modelDeploymentEligibilityDecided(md))
		answering := modelDeploymentPodAnswersAPI(pod, view, md.Spec.Engine.Name,
			eligible, gated, replacedByRole[role.Name])
		// candidate 跟随 operator 可控的角色状态。对于 operator 能够评估的角色，其成员会经过
		// 资格判定，标记也在同一次写入中随该判定结果变化。对于 operator 永远无法评估的角色，
		// 则没有判定结果可遵循，其成员保留原有的服务发现成员资格。
		candidate := modelDeploymentRouterCandidateOf(md,
			modelDeploymentRoleJudged(md, &effective), eligible,
			modelDeploymentShapeOfMember(pod, view, md.Spec.Engine.Name),
			pod.Labels[modelDeploymentRouterCandidateLabel] == modelDeploymentEndpointEligibleValue)
		patched := pod.DeepCopy()
		changed := modelDeploymentApplyLabel(
			patched, modelDeploymentLabelKeyEndpointEligible, eligible,
		)
		changed = modelDeploymentApplyLabel(
			patched, modelDeploymentLabelKeyAPIAnswering, answering,
		) || changed
		changed = modelDeploymentApplyGenerationAnnotation(
			patched, eligible, decision.Generation,
		) || changed
		changed = modelDeploymentApplyLabel(
			patched, modelDeploymentRouterCandidateLabel, candidate,
		) || changed
		if !changed {
			continue
		}

		// 此次写入绑定到本轮实际判定过的 Pod。名称并不代表对象身份，因此接管副本的旧有成员
		// 资格绝不能落到尚未通过任何资格判定的托管副本上。写入前会重新读取对象，若同一名称
		// 下已是不同的 Pod 实例则跳过，且 patch 会携带该次读取返回的版本。
		fresh := new(core.Pod)
		if err := r.Client.Get(ctx, ctrlcli.ObjectKeyFromObject(pod), fresh); err != nil {
			if kerrors.IsNotFound(err) {
				// 本轮已删除的 Pod 已无需写入标签，为一个正在退出的副本而把整个 Deployment 报告为
				// 不健康，比缺少标签更加糟糕。
				continue
			}

			return err
		}
		if fresh.UID != pod.UID {
			// 当前已有另一个对象占用了该名称。上面计算出的结果描述的是已经消失的旧对象，下一轮
			// 处理会对当前驻留在此的新对象重新判定。
			logger.V(3).Info("skipping a routing write for a member that was replaced",
				"role", role.Name, "pod", pod.Name,
				"observed", pod.UID, "standing", fresh.UID)

			continue
		}
		rewritten := fresh.DeepCopy()
		modelDeploymentApplyLabel(
			rewritten, modelDeploymentLabelKeyEndpointEligible, eligible,
		)
		modelDeploymentApplyLabel(
			rewritten, modelDeploymentLabelKeyAPIAnswering, answering,
		)
		modelDeploymentApplyGenerationAnnotation(
			rewritten, eligible, decision.Generation,
		)
		modelDeploymentApplyLabel(
			rewritten, modelDeploymentRouterCandidateLabel, candidate,
		)
		if err := r.Client.Patch(ctx, rewritten, ctrlcli.MergeFromWithOptions(fresh,
			ctrlcli.MergeFromWithOptimisticLock{})); err != nil && !kerrors.IsNotFound(err) {
			return err
		}
	}

	return nil
}

// modelDeploymentApplyLabel 写入或移除单个成员资格标签，并报告对象是否发生变更。
// 缺少该标签即表示不具备资格，因此 false 绝不会写入 "false"。
func modelDeploymentApplyLabel(pod *core.Pod, key string, present bool) bool {
	carried := pod.Labels[key] == modelDeploymentEndpointEligibleValue
	if present == carried {
		return false
	}
	if present {
		pod.Labels[key] = modelDeploymentEndpointEligibleValue
	} else {
		delete(pod.Labels, key)
	}

	return true
}

// modelDeploymentRouterCandidateOf 报告某个成员是否为其所在分组的路由候选者。
//
// operator 可控角色的成员仅在其分组具备资格时才是候选者，因此它会在与资格标签相同的
// 一次写入中、依据同一项决策获得或失去该标记。
//
// operator 无法评估的角色成员只要部署形态允许其应答，就是候选者，这与它一直以来的
// 成员资格一致：本 operator 无法评估的路由按其自身规则被发现，该标记也表达相同的
// 含义。此类角色的 follower 不应答请求，因此不是候选者，也不会进入整个 Deployment
// 范围的服务发现。
//
// 若部署形态无法读取，则保留成员当前已携带的标记，因为无人能读取的成员资格不应该被
// 随意改动。
//
// 该标记绝不作为保留依据。这里没有任何逻辑通过读取它来决定分组是否保留现有状态：
// 真正的保留依据是资格标签和 generation 注解，该标记仅表示某个成员可以被发现。
//
// 未声明 router 的 Deployment 没有服务发现暴露面，因此完全不携带任何标记。
func modelDeploymentRouterCandidateOf(
	md *workercore.ModelDeployment, controllable, eligible bool,
	shape modelDeploymentMemberShape, carried bool,
) bool {
	if md.Spec.Router == nil {
		return false
	}
	if controllable {
		return eligible
	}
	switch shape {
	case modelDeploymentMemberShapeAllows:
		return true
	case modelDeploymentMemberShapeForbids:
		return false
	default:
		// 无法读取部署形态时，保留成员当前已携带的成员资格。
		return carried
	}
}

// modelDeploymentMemberShape 表示已部署形态对单个成员资格的判定结果。
type modelDeploymentMemberShape int

const (
	// modelDeploymentMemberShapeUnknown 表示部署形态无法读取。
	modelDeploymentMemberShapeUnknown modelDeploymentMemberShape = iota
	// modelDeploymentMemberShapeAllows 表示部署形态允许该成员应答请求。
	modelDeploymentMemberShapeAllows
	// modelDeploymentMemberShapeForbids 表示部署形态仅允许其他成员应答请求。
	modelDeploymentMemberShapeForbids
)

// modelDeploymentShapeOfMember 返回已部署形态对单个成员资格的判定结果。
//
// 它仅负责应答规则中的形态判断部分：leader-served 副本中由 leader 应答，External-DP
// 副本中所有成员均应答，而无法读取的形态则记为 unknown，而非直接下结论。
func modelDeploymentShapeOfMember(
	pod *core.Pod, view modelDeploymentReplicaView, engine string,
) modelDeploymentMemberShape {
	switch modelDeploymentDeployedAnsweringShape(view, engine) {
	case modelDeploymentAnsweringAll, modelDeploymentAnsweringSole:
		return modelDeploymentMemberShapeAllows
	case modelDeploymentAnsweringLeader:
		if modelDeploymentAnsweringMemberLeader(pod) {
			return modelDeploymentMemberShapeAllows
		}

		return modelDeploymentMemberShapeForbids
	default:
		return modelDeploymentMemberShapeUnknown
	}
}

// modelDeploymentPodAnswersAPI 报告某个成员是否依据其构建时的模式与形态对应的路由规则，
// 位于 Deployment 的 HTTP Service 之后。
//
// 模式和形态均从 Pod 及其副本读取，绝不从 spec 读取。处于编辑过程中的角色会同时存在
// 两种模式的副本，各自保留其原有路由：接管副本按旧有成员规则应答（本 operator 无法
// 验证，也不会假装可以验证），而托管副本仅在通过资格判定后才应答（即其旁边的资格
// 标签）。若某个副本的 follower 仍在以先前的四成员形态提供服务，则该副本继续按该
// 形态应答，直到轮到它自身处理为止。
//
// 本轮被选中待替换的成员不再应答任何请求，无论其模式如何。这是唯一会缩减容量的规则，
// 且它只针对单个副本而非整个角色：其他过时的同级副本会继续提供服务，直到各自轮到被
// 替换。
func modelDeploymentPodAnswersAPI(
	pod *core.Pod, view modelDeploymentReplicaView, engine string,
	eligible, gated bool, replacingByOrdinal map[int]bool,
) bool {
	if !podIsReady(pod) {
		return false
	}
	ordinal, seated := modelDeploymentPodOrdinal(pod)
	if !seated || replacingByOrdinal[ordinal] {
		return false
	}

	switch modelDeploymentDeployedAnsweringShape(view, engine) {
	case modelDeploymentAnsweringAll, modelDeploymentAnsweringSole:
	case modelDeploymentAnsweringLeader:
		if !modelDeploymentAnsweringMemberLeader(pod) {
			return false
		}
	default:
		// 未知形态保留已携带的路由状态，同时仍受就绪状态和资格判定的约束。
		if pod.Labels[modelDeploymentLabelKeyAPIAnswering] != modelDeploymentAPIAnsweringValue {
			return false
		}
	}

	managed, readable := modelDeploymentMemberExecution(pod)
	if !readable {
		// 无法读取模式的成员保留仅由就绪状态允许的路由，即沿用旧有行为，而不对其作出新的
		// 判定。
		return true
	}
	if !managed {
		return true
	}
	if !gated {
		// 从未启用过资格 selector 的角色仍按其原有方式路由，在那里就绪状态一直是唯一规则。
		// 对这类角色按“托管即需通过资格判定”来解释，会在首次触及该 Deployment 的那一轮就
		// 撤回其中所有托管端点——而在模式编辑场景下，那一轮恰恰就是重写 selector 的那一轮。
		return true
	}

	return eligible
}

// modelDeploymentPodEligible 是针对成员的判定规则：当且仅当 Pod 所属的整个分组符合资格
// 且该成员自身属于部署形态允许应答的成员时，其端点才可被选中。
//
// 判定单元是整个分组，而非单个成员。若一个副本的成员并未全部符合资格，该副本就完全
// 不提供服务，因此即使某个成员自身正常，只要身旁的同组节点异常，它就不是任何可用
// 实例的有效成员，绝不能单独被选中。
//
// 撤回不设门控，恢复才设门控。任何一条检测链路只要报告了明确故障，无论分组谓词能否
// 观测到全部信息，都会立即移除该键；因为已经损坏的分组无需更多证据，而若某个故障能
// 被缺失的观测掩盖，那恰恰是最值得暴露的故障。恢复则相反：把端点重新放回流量选择是
// 绝不能在信号弱于 spec 要求时发生的操作，因此当分组谓词不可用时，reconciler 会原样
// 保留该键，并通过状态说明原因。
//
// 按成员区分的形态条件保持不变。由哪些成员应答仍由部署形态决定：External-DP 角色
// 为所有携带 rank 的成员，leader-served 角色仅为 leader，单成员规模则为整个副本。
// 这里的逻辑既不放宽也不收紧该范围。
//
// 观测合取条件仅施加在恢复决策处，别无他处：分组仅在获得针对其自身的新鲜、完整且一致
// 的观测时才会被恢复；若观测丢失，generation 未变的已具备资格分组保留现状，其余情况
// 一律撤回。
func modelDeploymentPodEligible(
	md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole, pod *core.Pod,
	view modelDeploymentReplicaView,
	qualification modelDeploymentInstanceQualification, evaluated bool,
	decision modelDeploymentGroupDecision,
) bool {
	if role.ElasticEP != nil {
		if pod.Annotations[modelDeploymentElasticWithdrawnAnnotation] == modelDeploymentTrueValue {
			return false
		}
		ordinal, valid := modelDeploymentPodOrdinal(pod)
		if !valid || ordinal != 0 {
			return false
		}
	}
	// 无条件的一半规则：不再就绪的成员立即离开服务池，无论本 operator 能否对该分组作出
	// 任何判断。
	if !podIsReady(pod) {
		return false
	}
	// carried 值是成员自身在写入前的事实，既不是本轮写入的标签，也不是回收逻辑读到的事实：
	// 它是该成员在本轮作出任何决定之前就已经持有的状态。
	//
	// 受保护的分组仅在建立该状态的同一 generation 下才承认它。若已携带标签对应的注解
	// 标明的不是该分组当前的成员代次，则说明它描述的是更早一届成员关系，不能沿用：正是
	// 这项检查防止了被替换的新分组借助其所替代的旧分组的凭据而恢复服务。
	carried := pod.Labels[modelDeploymentLabelKeyEndpointEligible] ==
		modelDeploymentEndpointEligibleValue
	carriedUnderThisGeneration := carried
	if decision.Protected && !decision.GenerationNamed {
		carriedUnderThisGeneration = false
	}
	if !evaluated {
		// 未对谓词求值的处理轮次会保持该键原样不变（无论增删）。在谓词构建前就返回的路径
		// 不会传入任何资格结果，而未出现在 map 中的成员代表其分组从未被检查过。若将零值
		// 视为空集合从而当作全部通过，就等于在毫无证据的情况下授予了资格。
		return carriedUnderThisGeneration
	}
	if !qualification.Activated() {
		// 明确故障会直接穿透门控触发撤回。门控只针对恢复：本 operator 无法验证的分组无法说明
		// 成员是否健康，因此成员身上已有的标签会被保留；但只要谓词明确检出某成员存在故障，
		// 无论该分组自身的链路如何判断，都会将该成员移出服务池。若无条件直接返回已携带的值，
		// 就会让处于不可验证副本中的 NotReady 成员继续处于可选中状态。
		if qualification.HasFailure() {
			return false
		}
		// 不安全的分组同样会被撤回，无论本 operator 能否对其前向路径作出判断。Ready=True 是
		// 成员给出的唯一正向回答，因此只要分组中有成员尚未报告就绪，或报告了就绪以外的任何
		// 状态，该分组就是不安全的，不安全的分组没有任何可选中的端点。故障成员自身已在上方
		// 的就绪检查中被剔除；此处逻辑负责将其健康的同组伙伴一并移出。
		//
		// 保持不变的是所有成员均已就绪、但本 operator 无法验证其前向路径的分组：此时该键会
		// 原样保留，因为依据一个谓词从未做出的信号来吊销资格，等于强加了一个它并未作出的裁决。
		if !qualification.MembersReady() {
			return false
		}
		// 既无吊销依据，也不允许恢复。该键被原样保留，这使得在首次针对引擎无法验证的
		// Deployment 启用该特性时，原有路由保持不变而不被收窄。
		return carriedUnderThisGeneration
	}
	if qualification.HasFailure() {
		return false
	}
	if !qualification.Eligible() {
		return false
	}

	// 由哪些成员应答取决于副本自身的形态，该形态从其正在运行的成员中读取，而非从 spec
	// 当前声明的角色读取。一个已激活的 External-DP 副本即便其 argv 后来被改成了内部模式，
	// 仍保留其各个 rank；若去读取角色配置，就会把一个仍在提供服务的分组中所有 follower
	// 全部撤回。
	shape := modelDeploymentDeployedAnsweringShape(view, md.Spec.Engine.Name)
	if shape == modelDeploymentAnsweringAll || shape == modelDeploymentAnsweringSole {
		return modelDeploymentPodRestores(decision, carried)
	}

	if !modelDeploymentAnsweringMemberLeader(pod) {
		return false
	}

	return modelDeploymentPodRestores(decision, carried)
}

// modelDeploymentPodRestores 是观测合取条件，应用于已取得入池资格的成员。
//
// 受保护的托管分组仅在获得针对其自身的新鲜、完整且一致的观测时才会恢复。
// 若观测丢失，generation 未变的已具备资格分组保留现状，而被屏障阻断的分组则会被
// 撤回，而非依赖过期结果继续提供服务。
//
// 兼容性类别不受此限，这正是设置它们的目的：本 operator 无法评估的路由，以及普通的
// 单成员 Server，均保留它们原有的行为。
func modelDeploymentPodRestores(
	decision modelDeploymentGroupDecision, carried bool,
) bool {
	if !decision.Protected {
		return true
	}
	if decision.Observed && decision.Fresh && decision.Fact.SnapshotValid {
		return true
	}
	if decision.Observed && decision.Fact.Retained {
		return carried
	}

	return false
}

// modelDeploymentGroupDecision 是针对某个成员所属组读取到的汇总结果。
//
// 它是恢复决策唯一读取的依据，仅此而已：资格判定与探测保持独立于它，因为无法读取 Router 并不代表成员是否健康。
type modelDeploymentGroupDecision struct {
	// Fact 表示汇总器对该组的已知信息，Observed 表示是否确实掌握了任何信息：汇总结果未携带其 generation 的组没有 fact，缺失的 fact 不是有效的 fact。
	Fact     modelDeploymentGroupServing
	Observed bool
	// Fresh 表示汇总结果是否仍处于有效期窗口内。
	Fresh bool
	// Generation 是该组当前的成员关系 generation。
	Generation modelDeploymentMembershipGeneration
	// GenerationNamed 表示该成员自身的资格注解是否标明了该组当前的成员关系 generation，这证明该标签属于当前成员关系。
	GenerationNamed bool
	// Protected 表示受保护组规则是否适用于该组：它是一个具有跨成员 fact 的托管组，且 deployment 声明了 Router。如果没有 Router，就没有需要等待的观测结果，该组沿用现有的资格判定约定。
	Protected bool
}

// modelDeploymentGroupDecisionOf 读取某个成员所属组的汇总结果。
//
// 汇总结果未包含的组即没有观测记录的组，这属于暂缓确认而非直接确认：汇总结果以该组当前的成员关系 generation 为键，因此自汇总以来成员关系发生变动的组，已不再是当时汇总所见的组。
func (r *ModelDeploymentReconciler) modelDeploymentGroupDecisionOf(
	observation modelDeploymentPassObservation, md *workercore.ModelDeployment,
	role *workercore.ModelDeploymentRole, view modelDeploymentReplicaView, pod *core.Pod,
) modelDeploymentGroupDecision {
	generation := modelDeploymentGenerationOf(view)
	key := modelDeploymentGroupKey{Role: role.Name, Ordinal: view.Ordinal, Generation: generation}
	fact, observed := observation.Serving.Groups[key]
	protected := md.Spec.Router != nil &&
		modelDeploymentGroupProtectionOf(md, role, view) == modelDeploymentGroupProtected

	return modelDeploymentGroupDecision{
		Fact:       fact,
		Observed:   observed,
		Fresh:      r.modelDeploymentServingFreshNow(observation.Serving),
		Generation: generation,
		GenerationNamed: pod.Annotations[modelDeploymentAnnotationKeyEndpointEligibleGeneration] ==
			string(generation),
		Protected: protected,
	}
}

// modelDeploymentApplyGenerationAnnotation 写入或移除资格 generation 注解，并报告对象是否发生了变更。
//
// 该注解与资格标签一同写入而非单独操作，因此两者不会在成员属于哪个 generation 下具备资格这一点上产生分歧。不具备资格的成员不携带此注解，因为没有标签的注解会宣称一项无人持有的资格。
func modelDeploymentApplyGenerationAnnotation(
	pod *core.Pod, eligible bool, generation modelDeploymentMembershipGeneration,
) bool {
	value := ""
	if eligible {
		value = string(generation)
	}
	if pod.Annotations[modelDeploymentAnnotationKeyEndpointEligibleGeneration] == value {
		return false
	}
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	if value == "" {
		delete(pod.Annotations, modelDeploymentAnnotationKeyEndpointEligibleGeneration)

		return true
	}
	pod.Annotations[modelDeploymentAnnotationKeyEndpointEligibleGeneration] = value

	return true
}

// modelDeploymentRoleExternalDP 读取该 role 自身的参数流以判断其负载均衡形态：每个成员都持有 rank 并响应请求的形态即为 External-DP 形态。该读取与状态检查及规模检查采用同一套逻辑，确保三方对 role 形态的判断一致。
func modelDeploymentRoleExternalDP(
	md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole,
) bool {
	reading, err := scanModelDeploymentParallelism(
		md.Spec.Engine.Name, role.ExtraArgs, role.Env,
	)
	if err != nil {
		// 扫描出错时按仅 leader 处理。准入检查解析相同的参数流，因此两处读取预期一致；若两者出现分歧，此默认值会在没有任何提示的情况下将 External-DP role 缩窄为仅 leader。改为向上暴露该错误属于可接受的错误路径语义变更，此处特意留待后续评审修改。
		return false
	}
	shape, _ := modelDeploymentLoadBalance(md.Spec.Engine.Name,
		ModelDeploymentEffectiveRoleKind(role), role.Env, reading.declared)

	return shape != workercore.ModelDeploymentLoadBalanceInternal &&
		shape != workercore.ModelDeploymentLoadBalanceUnknown
}

// syncModelDeploymentService 使 deployment 拥有的所有 Service 收敛：一个用于对外访问，以及每个 role 各一个。
//
// 它对现有 Service 进行对齐而非重建，因为已分配的 ClusterIP 是 operator 未曾写入的状态，且所有已解析该名称的客户端仍在使用它。这也是重建组时不会重建 Service 的原因：组的形态决定了哪些 Pod 存在，而它们响应请求的地址必须在变动中保持不变。
func (r *ModelDeploymentReconciler) syncModelDeploymentService(
	ctx context.Context, md *workercore.ModelDeployment, eligibilityDecided bool, pods []core.Pod,
) error {
	// 常驻成员列表直接来自 API server：尚未追平状态的缓存可能会错误淘汰仍在运行的集合通信成员的 peer 记录。
	rendered := renderModelDeploymentServices(md, r.modelDeploymentRoleManufacturers(ctx, md), pods)
	if ModelDeploymentElasticRole(md) != nil {
		for _, service := range rendered {
			service.Spec.Selector[modelDeploymentReplicaOrdinalLabel] = "0"
		}
		rendered = append(rendered, renderModelDeploymentElasticHeadService(md))
	}
	// 保留策略（RETENTION）：上一轮协调收窄到活跃 Service 上的资格条件属于集群状态，本轮不能仅因最新观测结果静默而将其抹去。从活跃对象中补全渲染期望；静态可激活性（activatability）条件仍占主导，因此不再具备可激活性的形态会在本轮失去该条件。
	liveServices := new(core.ServiceList)
	if err := r.Client.List(ctx, liveServices,
		ctrlcli.InNamespace(md.Namespace),
		ctrlcli.MatchingLabels{
			modelDeploymentLabelKeyName:     modelDeploymentLabelValueName,
			modelDeploymentLabelKeyInstance: md.Name,
		}); err != nil {
		return err
	}
	liveByName := make(map[string]*core.Service, len(liveServices.Items))
	for i := range liveServices.Items {
		liveByName[liveServices.Items[i].Name] = &liveServices.Items[i]
	}
	for i := range rendered {
		live := liveByName[rendered[i].GetName()]
		if live == nil {
			continue
		}
		role := modelDeploymentServiceRoleOf(md, rendered[i].GetName())
		if role == nil {
			continue
		}
		members := make([]*core.Pod, 0, len(pods))
		for i := range pods {
			if modelDeploymentPodRole(&pods[i]) == role.Name && modelDeploymentOwns(&pods[i], md) &&
				pods[i].DeletionTimestamp == nil {
				members = append(members, &pods[i])
			}
		}
		role = modelDeploymentEffectiveRoleForEligibility(role, members)

		selector := maps.Clone(live.Spec.Selector)
		if live.Annotations[modelDeploymentEligibilityActiveAnnotation] == modelDeploymentEndpointEligibleValue {
			selector[modelDeploymentLabelKeyEndpointEligible] = modelDeploymentEndpointEligibleValue
		}
		if modelDeploymentEligibilitySelectorActive(md, role, selector, eligibilityDecided) {
			modelDeploymentSelectEligibleEndpoints(rendered[i], md, role)
		}
	}
	expected := make([]ctrlcli.Object, len(rendered))
	for i := range rendered {
		expected[i] = rendered[i]
	}

	return r.syncModelDeploymentOwnedChildren(
		ctx,
		md,
		expected,
		func() ctrlcli.Object { return new(core.Service) },
		new(core.ServiceList),
		func(actual, expected ctrlcli.Object) bool {
			// 元数据对齐使 role 所属的 Service 保持在清理门控（prune gate）内：资源注解仅在 Create 时写入，而门控会豁免丢失了该注解的子资源。
			changed := alignModelDeploymentService(actual.(*core.Service), expected.(*core.Service))
			return alignModelDeploymentChildMetadata(actual, expected) || changed
		},
		ModelDeploymentResourceNoteRole,
		"service",
	)
}

// modelDeploymentServiceRoleOf 解析已渲染的 Service 对应哪个 role：入口 Service 与 deployment 同名并代理第一个 role；role Service 命名为 deployment-role。
func modelDeploymentServiceRoleOf(
	md *workercore.ModelDeployment, serviceName string,
) *workercore.ModelDeploymentRole {
	if serviceName == md.Name {
		return modelDeploymentFrontRole(md)
	}
	for i := range md.Spec.Roles {
		if serviceName == md.Name+"-"+md.Spec.Roles[i].Name {
			return &md.Spec.Roles[i]
		}
	}

	return nil
}

// syncModelDeploymentOwnedChildren 创建、对齐并清理一类由其拥有的已渲染子资源。
func (r *ModelDeploymentReconciler) syncModelDeploymentOwnedChildren(
	ctx context.Context,
	md *workercore.ModelDeployment,
	expected []ctrlcli.Object,
	newObject func() ctrlcli.Object,
	actualList ctrlcli.ObjectList,
	align func(actual, expected ctrlcli.Object) bool,
	resourceNote,
	kind string,
) error {
	wanted := sets.New[string]()
	for _, want := range expected {
		wanted.Insert(want.GetName())

		actual := newObject()
		err := r.Client.Get(ctx, ctrlcli.ObjectKeyFromObject(want), actual, ctrlclix.WithoutQuorum)
		if err != nil {
			if !kerrors.IsNotFound(err) {
				return err
			}

			if err = r.Client.Create(ctx, want); err != nil && !kerrors.IsAlreadyExists(err) {
				return err
			}
			continue
		}

		if !modelDeploymentOwns(actual, md) {
			return fmt.Errorf("%s %s/%s is not owned by this deployment",
				kind, actual.GetNamespace(), actual.GetName())
		}

		if align(actual, want) {
			if err = r.Client.Update(ctx, actual); err != nil {
				return err
			}
		}
	}

	logger := ctrllog.FromContext(ctx)
	err := r.Client.List(ctx, actualList,
		ctrlcli.InNamespace(md.Namespace),
		ctrlcli.MatchingLabels{
			modelDeploymentLabelKeyName:     modelDeploymentLabelValueName,
			modelDeploymentLabelKeyInstance: md.Name,
		},
		ctrlclix.WithoutQuorum)
	if err != nil {
		return err
	}

	return kmeta.EachListItem(actualList, func(obj runtime.Object) error {
		actual := obj.(ctrlcli.Object)
		if wanted.Has(actual.GetName()) ||
			!modelDeploymentOwns(actual, md) ||
			systemmeta.DescribeResourceNote(actual, resourceNote) == "" {
			return nil
		}

		logger.Info("removing a child no longer in the spec", kind, actual.GetName())
		if err = r.Client.Delete(ctx, actual); err != nil && !kerrors.IsNotFound(err) {
			return err
		}

		return nil
	})
}

// renderModelDeploymentPods 渲染 deployment 的期望副本：按 role 名称再按 ordinal 索引单个副本的成员列表。
//
// 按每个 ordinal 而非每个 role 渲染一次：spec 仅声明副本数量而未逐一命名，但其隐含的 ordinal（0 到 count-1）各自携带独立的组元数据，且 spec-hash 指纹涵盖该元数据。因此同一 role 的两个 ordinal 哈希值不同，收敛循环会将活跃 Pod 与其所属 ordinal 的渲染结果对比，从而保证扩容时不会滚动重建存量 Pod。
//
// 单个副本用切片表示是因为一个副本可能包含多个 Pod，且切片按成员索引排序，第 0 项为 leader。所有成员共享该副本的 Kueue 组——同一个 Workload、同一次准入、全员就绪或全不准入——因此协调器将切片作为整体进行创建、比对与删除。绝不部分应用：Kueue 会将已准入组中被删除的成员保留在 API server 上直至该组的 Workload 消失，而删除该 Workload 无论如何都会停止其余成员，因此半残副本既不是本 operator 有意达到的状态，也无法以半数形式恢复。
//
// 渲染仍保持每个 role 一套模板并按成员冲压生成：模板代表该 role 的所有 Pod，内部不得生成任何指代单个副本或成员的内容。区分两个成员的仅是冲压写入的信息：名称、主机名（hostname）和索引标签。
func (r *ModelDeploymentReconciler) renderModelDeploymentPods(
	ctx context.Context, md *workercore.ModelDeployment,
	connection *ModelDeploymentConnectorInput, interfaceProtocols []string, weights *modelArtifactWeights,
) (map[string]map[int][]*core.Pod, error) {
	if ModelDeploymentElasticRole(md) != nil {
		return r.renderModelDeploymentElasticPods(ctx, md, connection, interfaceProtocols, weights)
	}
	return r.renderModelDeploymentRolePods(ctx, md, connection, interfaceProtocols, weights)
}

func (r *ModelDeploymentReconciler) renderModelDeploymentRolePods(
	ctx context.Context, md *workercore.ModelDeployment,
	connection *ModelDeploymentConnectorInput, interfaceProtocols []string, weights *modelArtifactWeights,
) (map[string]map[int][]*core.Pod, error) {
	// 此处特意沿用 Instance 路径的 overcommit 设置：它决定了声明的资源如何转换为 request，而本渲染器推导出的值与 Instance Webhook 一致。若为同一种转换增设第二个开关，会导致同一集群中两处逻辑产生分歧。
	overcommit := settings.InstanceGeneralResourcesOvercommit.ShouldValueBool(ctx)
	// 每轮协调读取一次，使所有 role 基于同一个值渲染；由渲染器决定哪些 Pod 将其转换为 sysctl。
	tcpTWReuse := settings.ModelDeploymentTCPTWReuse.ShouldValueBool(ctx)

	// 每次协调解析一次而非按 role 解析：该结果属于整个集群而非单个 role，且读取的是启动时配置的版本快照，不产生 API 调用开销。
	clusterVersion := system.LoopbackKubeVersion.Get()
	nativeSidecar := kubediscovery.SupportsFeature(&clusterVersion, kubediscovery.FeatureNativeSidecar)

	// 配对声明的并行形态也在每次协调时解析一次，且仅当 deployment 确为一对（pair）时才解析：双端携带的传输配置文档均依赖该形态断言，因此两个 role 的配置合成必须读取同一份解析结果——任何无法解析的声明都会在此处令渲染失败，而不是静默退化为 1/1 并写入引擎随后会信任的文档中。仅持有单端的 deployment 不会渲染该文档，因此解析结果没有消费方，无法解析的声明保留为由引擎自身启动时拒绝，而不至于阻塞所有无关字段的协调。
	var pair inject.ParallelismPair
	if ModelDeploymentDeclaresBothHalves(md) {
		var err error
		pair, err = modelDeploymentDeclaredParallelismPair(md)
		if err != nil {
			return nil, err
		}
	}

	desired := make(map[string]map[int][]*core.Pod, len(md.Spec.Roles))
	for i := range md.Spec.Roles {
		role := &md.Spec.Roles[i]

		instType, err := r.getModelDeploymentInstanceType(ctx, role.InstanceType)
		if err != nil {
			return nil, err
		}

		in := ModelDeploymentRenderInput{
			Deployment:                 md,
			Role:                       role,
			InstanceType:               instType,
			RuntimeClassName:           r.getModelDeploymentRuntimeClassName(ctx, instType),
			GeneralResourcesOvercommit: overcommit,
			TCPTWReuse:                 tcpTWReuse,
			NativeSidecar:              nativeSidecar,
		}
		if weights != nil {
			in.Artifact = weights.Render
		}

		// 连接器按 role 合成，尽管其连接关系作用于整个 deployment，因为加速器属于各个 role：由它选择引擎注册的存储连接器，且只有该 role 的 InstanceType 知晓该信息。
		//
		// Encode 会进入渲染器，但不使用 KV 连接器。合成器会在其上渲染编码器缓存链路（encoder-cache leg），这正是它进入此流程的意义所在；而编码器不会访问共享 KV 存储。因此原本将 Encode 排除在此代码块之外的提前跳过逻辑，已调整为下方仅排除 KV 相关项的条件，从而配置好归 operator 管理的链路，同时不干预不归 operator 管理的存储。
		kvTransfer := modelDeploymentUsesKVTransfer(md, role, instType.Status.Detail.Manufacturer)
		publishKVEvents := modelDeploymentPublishesKVEvents(md, role, instType.Status.Detail.Manufacturer)
		routingSidecar := modelDeploymentFrontsDecodeWithSidecar(md, role)
		roleKind := ModelDeploymentEffectiveRoleKind(role)
		// 编码器阶段是该组合在映射表中的对应配置，按 role 获取一次即可，因为渲染是按 role 进行的：数据链路的端配置因半区而异，而 sidecar 标志在数据平面的两端相同。对于没有编码器阶段的 deployment 两者均为空，因此下方字段无条件赋值，无需额外前置判断。
		encoderLeg := modelDeploymentEPDEncoderLeg(md, role, instType.Status.Detail.Manufacturer)
		encoderURLs := modelDeploymentEPDEncoderURLs(md)
		if roleKind != workercore.ModelDeploymentRoleKindEncode &&
			ModelDeploymentMaySynthesizeConnector(md) &&
			(connection != nil || kvTransfer || publishKVEvents || routingSidecar) {
			roleConnection := ModelDeploymentConnectorInput{}
			if connection != nil {
				roleConnection = *connection
			}
			roleConnection.Engine = md.Spec.Engine.Name
			roleConnection.Manufacturer = instType.Status.Detail.Manufacturer
			// kind 属于各个 role，也是合成配置中唯一按 role 区分的项：正是它使 prefiller 与 decoder 成为两套独立配置而非同一配置的两份拷贝，这也是对该配对执行原子准入的意义所在。
			roleConnection.Kind = role.Kind
			// 作用于整个 deployment 而非单个 role，因为其控制的拆分模式回答的是“该 deployment 是否为一个配对”——与 router 自身模式回答的问题相同——未声明配对的单端如今在两层上均以不拆分方式运行。
			roleConnection.Disaggregated = ModelDeploymentDeclaresBothHalves(md)
			// 所有 role 使用相同的 pair，这使两个 Pod 的并行配置块保持一致：配置文档基于 pair 进行断言，因此每个 role 都携带这一份统一的解析结果，而非各自单独读取自身配置。
			roleConnection.Parallelism = pair
			roleConnection.KVTransfer = kvTransfer
			roleConnection.RoutingSidecar = routingSidecar
			roleConnection.EncoderCache = encoderLeg
			roleConnection.EncoderURLs = encoderURLs
			if md.Spec.KVTransfer != nil {
				roleConnection.KVTransferProtocol = md.Spec.KVTransfer.Protocol
			}
			if weights != nil && connection != nil {
				roleConnection.CachePrefix = weights.KVIdentity
			}
			roleConnection.PublishKVEvents = publishKVEvents
			if roleConnection.PublishKVEvents {
				roleConnection.KVEventsHost = md.Name + "-" + role.Name + "." + md.Namespace + ".svc"
			}

			connector, err := SynthesizeModelDeploymentConnector(roleConnection)
			if err != nil {
				return nil, fmt.Errorf("role %q cannot be given a cache client: %w", role.Name, err)
			}
			in.Connector = connector
		} else if roleKind == workercore.ModelDeploymentRoleKindEncode &&
			modelDeploymentEPDReachesEncoderStage(md, instType.Status.Detail.Manufacturer) {
			// 编码器仅使用数据链路而不涉及其他配置，因此此处走独立分支而非并入上述条件中：编码器没有存储连接、传输链路或 sidecar，若将其合入该分支，则需要经过四个为假的判断才能到达唯一为真的条件。
			//
			// 条件判断的是可达性，而非仅看 role。引擎之所以能感知编码器阶段，要么是因为匹配行指明了所运行的连接器，要么是因为该引擎按地址拆分编码器；两者皆无的组合会将此 role 渲染为不拆分模式，这与上述机制引入前的渲染行为一致，也是准入拒绝的对象在协调时应当保持的行为，而不应因一个无人消费的字段而失败。
			connector, err := SynthesizeModelDeploymentConnector(ModelDeploymentConnectorInput{
				Engine:       md.Spec.Engine.Name,
				Manufacturer: instType.Status.Detail.Manufacturer,
				Kind:         role.Kind,
				EncoderCache: encoderLeg,
				EncoderURLs:  encoderURLs,
			})
			if err != nil {
				return nil, fmt.Errorf("role %q cannot be given an encoder leg: %w", role.Name, err)
			}
			in.Connector = connector
		}
		// 与上方引擎端配置出自同一匹配行的 sidecar 端配置，采用 sidecar 自身的拼写格式。它直接从 deployment 读取而非取自上方的链路，因为 sidecar 运行在自身不携带链路的 decode Pod 上：读取该标志的一方永远不是配置了该链路的那两个 role 之一。
		//
		// 在此处而非 sidecar 调用处设置，是因为这里是唯一同时持有 deployment 与其 InstanceType 的位置，因而也是唯一能查询该 deployment 运行于哪一行配置的位置。空值表示不渲染该标志，即未声明编码器 role 的 deployment 的默认行为。
		in.ECConnector = ModelDeploymentECSidecarConnector(
			modelDeploymentEPDConnector(md, instType.Status.Detail.Manufacturer),
		)
		if role.Resources != nil && role.Resources.Interface != nil && role.Resources.Interface.Sign() > 0 {
			count, whole := role.Resources.Interface.AsInt64()
			if !whole {
				return nil, fmt.Errorf("role %q requests a fractional interface count", role.Name)
			}
			var protocols []string
			if len(role.Command) == 0 {
				protocols = interfaceProtocols
			}
			in.InterfaceResource, err = ModelDeploymentInterfaceResource(protocols,
				ModelDeploymentDirectInterfaceProtocol(md, role, instType.Status.Detail.Manufacturer), count)
			if err != nil {
				return nil, fmt.Errorf("role %q interface request: %w", role.Name, err)
			}
		}

		template, err := renderModelDeploymentPodTemplate(ctx, in)
		if err != nil {
			return nil, err
		}

		size := modelDeploymentRoleSize(role)
		rolePods := make(map[int][]*core.Pod, role.Replicas)
		for ordinal := range int(role.Replicas) {
			members := make([]*core.Pod, 0, size)
			for member := range size {
				pod := template.DeepCopy()
				stampModelDeploymentPod(pod, md, role, ordinal, member)
				members = append(members, pod)
			}
			rolePods[ordinal] = members
		}
		desired[role.Name] = rolePods
	}

	return desired, nil
}

// modelDeploymentDeclaredParallelismPair 解析 deployment 的 prefill/decode 配对两端声明的并行形态，读取每个 role 自身的配置——即其参数流，以及针对 vLLM 数据并行宽度的字面量环境变量。
//
// 按声明顺序，每种类型中第一个出现的 role 生效。准入控制已拒绝任一类型出现第二个 role，但本解析器仍固定自身规则，而不依赖其并不负责的校验逻辑：绕过准入执行的渲染仍必须确定性地选取一端的配置。缺少某个类型的 role 时，该端保持零值；未做任何声明的 role 解析为全 1；渲染器会将这两种情况均映射为引擎自身的默认值 1。
//
// 接管（take-over）类型 role 的配置即为其 command：被替换的 argv 是实际运行的唯一命令行，因此解析该端时读取的是它，而绝非其旁边的无效额外参数。隐藏在 shell 字符串、脚本或配置文件中的并行度仍不可见，这属于约定允许的范畴；直接写在命令行上的并行度则会按与托管 role 完全相同的方式读取。
//
// 配置无法读取的 role 会直接返回错误，绝不静默降级为 1/1：同样的声明原本就会阻止引擎自身启动，若在旁渲染默认值，无异于用错误的块布局掩盖引擎本应明确给出的拒绝报错。
func modelDeploymentDeclaredParallelismPair(md *workercore.ModelDeployment) (inject.ParallelismPair, error) {
	var pair inject.ParallelismPair
	var prefillSeen, decodeSeen bool

	for i := range md.Spec.Roles {
		role := &md.Spec.Roles[i]

		var half *inject.Parallelism
		switch ModelDeploymentEffectiveRoleKind(role) {
		case workercore.ModelDeploymentRoleKindPrefill:
			if prefillSeen {
				continue
			}
			prefillSeen = true
			half = &pair.Prefill
		case workercore.ModelDeploymentRoleKindDecode:
			if decodeSeen {
				continue
			}
			decodeSeen = true
			half = &pair.Decode
		default:
			continue
		}

		declared, err := ParseModelDeploymentDeclaredParallelism(
			md.Spec.Engine.Name, ModelDeploymentRoleArgs(role), role.Env,
		)
		if err != nil {
			return pair, fmt.Errorf("role %q declares parallelism the operator cannot read: %w", role.Name, err)
		}
		half.TensorParallel = declared.TensorParallel
		half.DataParallel = declared.DataParallel
	}

	return pair, nil
}

// modelDeploymentTakenGroups 返回某一个 role 中 API server 仍持有成员的副本组名称，采用无缓存读取。
//
// 直接读取 API server 而非本地缓存，因为两者恰恰在本读取旨在消除的时间窗口内存在分歧：服务端已持久化创建但响应尚未返回的 Pod 存在于服务端而尚未进入 informer 缓存，读缓存会误判该 ordinal 空闲并为其创建第二个 Pod——单成员组出现两个成员时，Kueue 会通过删除较新的 Pod 来处理。
//
// 一次读取覆盖整个 role，从而避免创建门控（create gate）落入本应规避的热路径中。门控每发现一个空闲 ordinal 都会询问此问题，若将每次读取收窄到单个组，缺少 n 个副本的 role 就要付出 n 次无缓存 list 开销——且只要有成员正在退出排空，每次重新入队都会重复支付。一次性列出整个 role 并按每个 Pod 携带的组标签分桶，即可用同一份数据回答所有 ordinal 的查询；该筛选范围严格更宽，因此不会漏掉按 ordinal 读取能找到的任何 Pod。
//
// 分桶键采用组名而非 ordinal 标签，两者仅在一类存量对象上存在差异：在引入按副本分组之前渲染的副本可能带有组的成员标签但没有自身的 ordinal 标签，而它仍处于该 ordinal 的组中并计为一名成员——在其旁创建同样会造成超员，因此门控读取的占用状态归属于组，这也是 Kueue 计数的单位。无论组规模多大，只要存在任意成员即触发占用阈值：未达到声明总数的组无法让 Kueue 合成 Workload，因此在掉队成员旁放置新成员只会产出一个永远无法准入的副本，而非仅仅拥挤而已。客户端侧仍会校验 owner reference，原因与 listModelDeploymentPods 一致：携带此 deployment 标签但受同名旧实例控制的 Pod 不占用本 deployment 应有的槽位，deployment 会绕过它直接创建，而非在其后等待。
func (r *ModelDeploymentReconciler) modelDeploymentTakenGroups(
	ctx context.Context, md *workercore.ModelDeployment, role string, includeTerminal bool,
) (sets.Set[string], error) {
	podList := new(core.PodList)
	err := r.APIReader.List(ctx, podList,
		ctrlcli.InNamespace(md.Namespace),
		ctrlcli.MatchingLabels{
			modelDeploymentLabelKeyName:      modelDeploymentLabelValueName,
			modelDeploymentLabelKeyInstance:  md.Name,
			modelDeploymentLabelKeyComponent: role,
		})
	if err != nil {
		return nil, fmt.Errorf("list the members of role %q's groups: %w", role, err)
	}

	taken := sets.New[string]()
	for i := range podList.Items {
		pod := &podList.Items[i]
		if !modelDeploymentOwns(pod, md) {
			continue
		}
		if !includeTerminal && (pod.Status.Phase == core.PodSucceeded || pod.Status.Phase == core.PodFailed) {
			continue
		}
		group := pod.Labels[kueuepodconst.GroupNameLabel]
		if group == "" && ModelDeploymentElasticRole(md) != nil && role == modelDeploymentElasticHeadName(md) {
			// 辅助节点没有 Kueue 组，但其未缓存的存在状态仍会占用该槽位。
			group = modelDeploymentReplicaGroupName(md, role, 0)
		}
		if group != "" {
			taken.Insert(group)
		}
	}

	return taken, nil
}

// modelDeploymentAdmittedReplicas 统计这些副本中有多少持有各自已准入的 Workload。
//
// 仅当副本的 Workload 已准入时才计入，任何更弱的状态均不作数。准入意味着配额已获批准；已创建但仍在排队的替代副本，其 Workload 要么已合成但尚未准入，要么尚未生成，无论哪种都未持有任何资源——若将其计入进度，在资源竞争的池上进行滚动更新时就会把 deployment 剥离至零个已准入副本。匹配基于所有权而非派生名称，原因与 findModelDeploymentGroupWorkloads 所述一致：组的 Workload 携带指向其成员的普通 owner reference，由成员自身决定哪个 Workload 属于它们。
func modelDeploymentAdmittedReplicas(workloads []kueue.Workload, pods []*core.Pod) int {
	ours := sets.New[types.UID]()
	for _, pod := range pods {
		ours.Insert(pod.UID)
	}

	admitted := 0
	for i := range workloads {
		wl := &workloads[i]
		if modelDeploymentWorkloadOwnsAny(wl, ours) &&
			kubeapistatus.ConditionType(kueue.WorkloadAdmitted).IsTrue(wl) {
			admitted++
		}
	}

	return admitted
}

// modelDeploymentPodValues 将活跃 Pod 指针列表复制为分组辅助函数所需的值切片形式，使持有指针列表的调用方无需再额外维护一份副本。
func modelDeploymentPodValues(pods []*core.Pod) []core.Pod {
	values := make([]core.Pod, 0, len(pods))
	for _, pod := range pods {
		values = append(values, *pod)
	}

	return values
}

// modelDeploymentReplicaMembers 返回落座于同一个 ordinal 上的所有活跃 Pod，当单个副本可包含多个成员时，这即构成一个完整副本。顺序沿用调用方列表顺序，本身无特定含义：副本总是整体退出，因此下游不会在其成员之间做单独挑选。
func modelDeploymentReplicaMembers(pods []*core.Pod, ordinal int) []*core.Pod {
	members := make([]*core.Pod, 0, 1)
	for _, pod := range pods {
		if at, ok := modelDeploymentPodOrdinal(pod); ok && at == ordinal {
			members = append(members, pod)
		}
	}

	return members
}

// seat 表示单个副本内部的一个席位：由 ordinal 及其内部的成员索引组成。
type seat struct{ ordinal, member int }

// modelDeploymentSurplusCount 计算本规则旨在消除的多余数量：超出可用席位的成员数。
//
// 它统计的是席位而非期望成员数：从 4 个成员编辑为 2 个成员的副本，仍占满其构建时的 4 个席位；若直接与期望数量比较，会把健康的已准入组裁剪至仅匹配 spec 为其替代品设定的规模。
//
// 未声明 ordinal 的 Pod 不计为多余。它不可能成为某个席位上的冗余成员，而是交由无门控滚动更新流程裁决：它持有自身的 Workload，因此是滚动更新每轮逐个替换的副本，而非此处要裁剪的重复成员。
func modelDeploymentSurplusCount(kept []*core.Pod) int {
	seats := make(map[seat]bool, len(kept))
	excess := 0
	for _, pod := range kept {
		ordinal, ok := modelDeploymentPodOrdinal(pod)
		if !ok {
			continue
		}
		key := seat{ordinal: ordinal, member: modelDeploymentPodMemberIndex(pod)}
		if seats[key] {
			excess++

			continue
		}
		seats[key] = true
	}

	return excess
}

// modelDeploymentSurplusReplicas 从超额计数的 role 中挑选要退出的 Pod：优先选择未声明 ordinal 的 Pod，其次是同一席位上超出单个占用者之外的 Pod，总数最多为 `surplus` 个。
//
// 席位是一个 (ordinal, member) 二元组而非仅 ordinal，因为大小为 n 的副本拥有 n 个席位，且每个席位恰好容纳一个 Pod。当大小为 1 时两种理解重合，这也正是该泛化设计不影响单成员决策的原因：每个 ordinal 只有一个席位，且属于 leader。
//
// 每个席位保留当前渲染所描述的 Pod——即指纹与该 role 为该席位渲染结果一致的那个 Pod——因为这本来就是重建时要构建的 Pod，将其驱逐只会让下一轮协调白白安排同样的重建抖动。当多个占用者无从区分时按名称最大者决定，这体现的是确定性而非业务含义：名称在多轮协调间保持稳定，而同一席位的两个占用者之间没有其他可资区分的属性。没有 ordinal 的 Pod 优先于任何已落座的 Pod 退出，因为所有已落座的 Pod 均在声明数量的统计范围内。
func modelDeploymentSurplusReplicas(
	kept []*core.Pod, want map[int][]*core.Pod, surplus, minRenderVersion int,
) []*core.Pod {
	type seat struct{ ordinal, member int }

	var shed []*core.Pod
	bySeat := make(map[seat][]*core.Pod, len(kept))
	for _, pod := range kept {
		ordinal, ok := modelDeploymentPodOrdinal(pod)
		if !ok {
			shed = append(shed, pod)

			continue
		}
		bySeat[seat{ordinal, modelDeploymentPodMemberIndex(pod)}] = append(
			bySeat[seat{ordinal, modelDeploymentPodMemberIndex(pod)}], pod,
		)
	}

	for at, occupants := range bySeat {
		if len(occupants) < 2 {
			continue
		}
		// 该席位的渲染结果；若该席位已不再属于角色描述的范围（例如早期形态的 Pod 遗留下来、成员索引
		// 超出声明大小的位置），则为空。缺失状态通过独立标志位而非空字符串表示，因为完全没有指纹的 Pod
		// （如手动创建的 Pod）否则会与其判定相等并被保留为该席位的当前占用者。没有任何对象会匹配
		// 缺失的渲染结果，因此这些占用者仅按名称排序，且该席位仍会缩减至只保留一个。
		var wantMember *core.Pod
		if members := want[at.ordinal]; at.member < len(members) {
			wantMember = members[at.member]
		}
		peerVersion := 0
		for peerSeat, peerOccupants := range bySeat {
			if peerSeat.ordinal != at.ordinal || peerSeat.member == at.member || len(peerOccupants) != 1 {
				continue
			}
			v := modelDeploymentPodRenderVersion(peerOccupants[0])
			if v <= 0 {
				peerVersion = -1
				break
			}
			if peerVersion == 0 {
				peerVersion = v
			} else if peerVersion != v {
				peerVersion = -1
				break
			}
		}
		slices.SortFunc(occupants, func(a, b *core.Pod) int {
			am := modelDeploymentPodMatchesCurrency(a, wantMember, minRenderVersion)
			bm := modelDeploymentPodMatchesCurrency(b, wantMember, minRenderVersion)
			if am != bm {
				if am {
					return -1
				}

				return 1
			}
			if am && bm {
				av := modelDeploymentPodRenderVersion(a)
				bv := modelDeploymentPodRenderVersion(b)
				if peerVersion > 0 {
					aPeer := av == peerVersion
					bPeer := bv == peerVersion
					if aPeer != bPeer {
						if aPeer {
							return -1
						}

						return 1
					}
				}
				if av != bv {
					return bv - av
				}
			}

			return strings.Compare(b.Name, a.Name)
		})
		shed = append(shed, occupants[1:]...)
	}

	// 无序数 Pod 优先，其次按最大序数排列，从而保证对同一状态的两轮处理按相同顺序驱逐，且占座
	// 副本与其他移除操作一样从高位到低位依次下线。
	//
	// 一旦下方的上限约束生效，前半条规则就至关重要。仅按序数排序会把无序数 Pod 排在最后，因为
	// 它的序数低于所有席位——这样当同时存在无序数 Pod 和重复占座副本时，系统会驱逐重复副本而保留
	// 不属于任何副本的 Pod。该 Pod 随后会在下一轮计入已声明的总数中，从而导致角色在无多余 Pod
	// 可驱逐的情况下仍缺少一个有效副本。
	slices.SortFunc(shed, func(a, b *core.Pod) int {
		aSeated, bSeated := modelDeploymentOrdinalOrFloor(a) >= 0, modelDeploymentOrdinalOrFloor(b) >= 0
		if aSeated != bSeated {
			if !aSeated {
				return -1
			}

			return 1
		}
		if c := modelDeploymentOrdinalOrFloor(b) - modelDeploymentOrdinalOrFloor(a); c != 0 {
			return c
		}

		return strings.Compare(b.Name, a.Name)
	})
	if len(shed) > surplus {
		shed = shed[:surplus]
	}

	return shed
}

// modelDeploymentHighestOrdinalReplica 选择滚动更新时优先替换的副本：取序数最大者；当同一
// 槽位的成员在其他方面无法区分时，取名称最大者。
//
// 选择依据是序数而非创建时间——时间戳只反映对象何时创建，而不代表它占据哪个槽位，而替换操作
// 正是基于槽位进行渲染的。名称最大仅作为确定性决胜规则而无语义含义：该名称由 API server 分配
// 且保持稳定，这正是水平触发选择所需的唯一特性。没有序数的 Pod 不参与竞争：它未占据任何席位，
// 因此所有占座副本均排在它之前。
func modelDeploymentHighestOrdinalReplica(pods []*core.Pod) *core.Pod {
	choice := pods[0]
	for _, pod := range pods[1:] {
		if c := modelDeploymentOrdinalOrFloor(pod) - modelDeploymentOrdinalOrFloor(choice); c > 0 ||
			(c == 0 && strings.Compare(pod.Name, choice.Name) > 0) {
			choice = pod
		}
	}

	return choice
}

// modelDeploymentOrdinalOrFloor 读取副本持有的序数；未携带序数的 Pod 返回低于所有席位的值：
// 它未声明任何槽位，因此在排序时位于所有占座副本之后。
func modelDeploymentOrdinalOrFloor(pod *core.Pod) int {
	ordinal, ok := modelDeploymentPodOrdinal(pod)
	if !ok {
		return -1
	}

	return ordinal
}

// getModelDeploymentInstanceType 读取角色准入所依据的 InstanceType。
//
// 类型缺失会被视为 error，而非在无类型的情况下继续渲染：该类型同时提供了加速器资源键名
// 的拼写方式以及用于推导主机资源请求的单卡资源规格，缺少它渲染出的 Pod 将请求与角色声明
// 不符的资源。
func (r *ModelDeploymentReconciler) getModelDeploymentInstanceType(
	ctx context.Context, name string,
) (*worker.InstanceType, error) {
	instType := new(worker.InstanceType)
	err := r.Client.Get(ctx, ctrlcli.ObjectKey{Name: name}, instType,
		ctrlclix.WithoutQuorum)
	if err == nil {
		return instType, nil
	}
	if !kerrors.IsNotFound(err) {
		return nil, fmt.Errorf("get instance type %s: %w", name, err)
	}

	// InstanceType 可能尚未进入缓存；直接向 API server 穿透读取，而不是将冷缓存误判为类型缺失。
	err = r.APIReader.Get(ctx, ctrlcli.ObjectKey{Name: name}, instType,
		ctrlclix.WithoutQuorum)
	if err != nil {
		return nil, fmt.Errorf("get instance type %s: %w", name, err)
	}

	return instType, nil
}

// modelDeploymentRoleManufacturers 解析每个角色的加速器厂商（以角色名为键），供依赖该信息的
// 路由路径决策使用。未启用路由的部署将返回 nil，因为其渲染过程不读取厂商信息。
//
// 此读取遵循尽力而为原则，且作用域有意比 getModelDeploymentInstanceType 更窄：若某角色的
// 类型无法读取，则仅不为其添加条目，所有使用方都将缺失条目视为“未确认为 Ascend”——这与
// 这些路径在查询前给出的默认结论一致。Pod 渲染阶段在类型不可读时本就会使本轮调谐失败，
// 因此此处的缺失意味着类型在调谐中途被删除，沿用一轮旧结论优于为此直接中断同步。
func (r *ModelDeploymentReconciler) modelDeploymentRoleManufacturers(
	ctx context.Context, md *workercore.ModelDeployment,
) map[string]string {
	if md.Spec.Router == nil {
		return nil
	}

	manufacturers := make(map[string]string, len(md.Spec.Roles))
	for i := range md.Spec.Roles {
		instType := new(worker.InstanceType)
		if err := r.Client.Get(ctx, ctrlcli.ObjectKey{Name: md.Spec.Roles[i].InstanceType}, instType,
			ctrlclix.WithoutQuorum); err != nil {
			continue
		}
		manufacturers[md.Spec.Roles[i].Name] = instType.Status.Detail.Manufacturer
	}

	return manufacturers
}

// getModelDeploymentRuntimeClassName 返回加速副本所需的 RuntimeClass 名称；若不需要或集群中
// 不存在该类，则返回 ""。
//
// 缺少该类并不构成 error：这表明集群可能以其他名称运行厂商运行时，或根本未配置该运行时。
// 这里采用查询而非假定存在的方式，是因为指定了不存在的 RuntimeClass 的 Pod 会被直接拒绝。
func (r *ModelDeploymentReconciler) getModelDeploymentRuntimeClassName(
	ctx context.Context, instType *worker.InstanceType,
) string {
	if !instType.Spec.Acceleratable {
		return ""
	}

	name := nodefeature.GetAcceleratableRuntimeName(instType.Status.Detail.Manufacturer)
	if name == "" {
		return ""
	}

	rc := new(node.RuntimeClass)
	if err := r.Client.Get(ctx, ctrlcli.ObjectKey{Name: name}, rc, ctrlclix.WithoutQuorum); err != nil {
		return ""
	}

	return name
}

// listModelDeploymentPods 返回当前部署所拥有的副本。
//
// 它先按身份标签筛选，再校验控制器引用，从而避免新创建的同名部署错误接管带有相同标签但
// 属于已重建旧部署的 Pod。
func (r *ModelDeploymentReconciler) listModelDeploymentPods(
	ctx context.Context, md *workercore.ModelDeployment,
) ([]core.Pod, error) {
	return readModelDeploymentPods(ctx, md, r.Client)
}

// readModelDeploymentPods 按身份标签过滤 Pod，并要求其所有者 UID 与当前部署匹配。
func readModelDeploymentPods(
	ctx context.Context, md *workercore.ModelDeployment, reader ctrlcli.Reader,
) ([]core.Pod, error) {
	podList := new(core.PodList)
	err := reader.List(ctx, podList,
		ctrlcli.InNamespace(md.Namespace),
		ctrlcli.MatchingLabels{
			modelDeploymentLabelKeyName:     modelDeploymentLabelValueName,
			modelDeploymentLabelKeyInstance: md.Name,
		})
	if err != nil {
		return nil, err
	}

	owned := make([]core.Pod, 0, len(podList.Items))
	for i := range podList.Items {
		if !modelDeploymentOwns(&podList.Items[i], md) {
			continue
		}
		owned = append(owned, podList.Items[i])
	}

	return owned, nil
}

// modelDeploymentOwns 返回该对象是否受当前部署控制。
func modelDeploymentOwns(obj ctrlcli.Object, md *workercore.ModelDeployment) bool {
	for _, ref := range obj.GetOwnerReferences() {
		if ref.Controller != nil && *ref.Controller && ref.UID == md.UID {
			return true
		}
	}

	return false
}

func modelDeploymentOwnedResource(obj ctrlcli.Object) bool {
	if obj.GetLabels()[elasticOperationLabel] == modelDeploymentTrueValue ||
		obj.GetLabels()[elasticObservationLabel] == modelDeploymentTrueValue {
		return true
	}
	if !systemmeta.MatchResource(obj, ModelDeploymentResourceType) {
		return false
	}

	return systemmeta.DescribeResourceNote(obj, ModelDeploymentResourceNoteRole) != "" ||
		systemmeta.DescribeResourceNote(obj, modelDeploymentResourceNoteRouter) != ""
}

func (r *ModelDeploymentReconciler) SetupController(ctx context.Context, opts controller.SetupOptions) error {
	r.Client = opts.Manager.GetClient()
	r.APIReader = opts.Manager.GetAPIReader()
	r.Recorder = opts.Manager.GetEventRecorderFor("modeldeployment")

	// 排空读取器按 UID 解析单个成员，而未经索引的缓存不支持直接按 UID 查询。在 manager 启动前
	// 于此处注册该索引，可使每次查询保持为单次缓存读取，而无需在每次读取待退役副本的各个成员时
	// 扫描集群中的所有 Pod。
	err := opts.Manager.GetFieldIndexer().IndexField(ctx, &core.Pod{}, modelDeploymentDrainIndexPodUID,
		func(obj ctrlcli.Object) []string {
			if obj == nil {
				return nil
			}
			pod := obj.(*core.Pod)
			if pod.UID == "" {
				return nil
			}
			return []string{string(pod.UID)}
		})
	if err != nil {
		return fmt.Errorf("index pod by uid for the drain reader: %w", err)
	}

	// Exec 传输通道仅在此处构建，因此生产环境调谐轮次实际解析到的读取器就是下方装配的实例，
	// 而非测试接缝回退的拒绝实现。nil 读取器代表拒绝执行，此处即用于将其替换。
	coreClient, err := corev1client.NewForConfig(opts.Manager.GetConfig())
	if err != nil {
		return fmt.Errorf("build core client for the drain reader: %w", err)
	}
	// Manager 的 APIReader 作为权威数据源，而带缓存的 client 仅用于定位。排空操作所依据的每一项
	// 状态都会在 exec 前后实时读取，因为仅依赖自身缓存的控制器无法区分重启后的成员与外观相同
	// 的旧成员。
	r.drainReader = newModelDeploymentDrainCollector(
		r.Client, opts.Manager.GetAPIReader(), opts.Manager.GetConfig(), coreClient,
	)
	r.elasticObserver = newModelDeploymentElasticObserver(
		r.Client, r.APIReader, opts.Manager.GetConfig(), coreClient,
	)

	return ctrl.NewControllerManagedBy(opts.Manager).
		Named("modeldeployment").
		For(
			&workercore.ModelDeployment{},
			ctrlbuilder.WithPredicates(
				ctrlpredicate.Or(
					ctrlpredicate.GenerationChangedPredicate{},
					modelDeploymentMinRenderVersionChangedPredicate(),
				),
			),
		).
		Owns(
			// 监听此部署渲染出的副本。此处依据所有权而非标签匹配来触发入队，从而防止失去所有者的 Pod
			// 持续唤醒已不再认领它的部署。
			&core.Pod{},
			ctrlbuilder.WithPredicates(
				ctrlpredicate.NewPredicateFuncs(modelDeploymentOwnedResource),
			),
		).
		Owns(
			// Service 由该控制器负责调谐，因此必须受其监听。若无此监听，被外部删除或修改的 Service
			// 只有在其他事件恰好唤醒部署，或数小时后的下一次 resync 时才会被修正：在此期间端点将
			// 一直处于异常状态，而本可重新对齐状态的调谐代码却不会执行。
			//
			// 监听机制使状态收敛成为水平触发而非创建时的一次性操作。手动测试中往往不会暴露此问题，
			// 因为通常不会手动删除 Service——但集群中的其他组件可能会这么做。
			&core.Service{},
			ctrlbuilder.WithPredicates(
				ctrlpredicate.NewPredicateFuncs(modelDeploymentOwnedResource),
			),
		).
		Owns(
			&app.Deployment{},
			ctrlbuilder.WithPredicates(ctrlpredicate.NewPredicateFuncs(modelDeploymentOwnedResource)),
		).
		Owns(
			&core.ConfigMap{},
			ctrlbuilder.WithPredicates(ctrlpredicate.NewPredicateFuncs(modelDeploymentOwnedResource)),
		).
		Owns(
			&core.ServiceAccount{},
			ctrlbuilder.WithPredicates(ctrlpredicate.NewPredicateFuncs(modelDeploymentOwnedResource)),
		).
		Owns(
			&rbac.Role{},
			ctrlbuilder.WithPredicates(ctrlpredicate.NewPredicateFuncs(modelDeploymentOwnedResource)),
		).
		Owns(
			&rbac.RoleBinding{},
			ctrlbuilder.WithPredicates(ctrlpredicate.NewPredicateFuncs(modelDeploymentOwnedResource)),
		).
		Watches(
			// Binding 的就绪状态是观测所得而非声明配置，因此其状态变化时必须唤醒部署。若缺少此监听，
			// 状态转换直到数小时后的下一次 resync 才会被察觉：一旦存储主节点重启，其上的所有部署即便
			// 已恢复连接，仍会继续报告旧的域状态。
			&workercore.KVCachePoolBinding{},
			ctrlhandler.EnqueueRequestsFromMapFunc(r.mapModelDeploymentBinding),
		).
		Watches(
			// InstanceType 的观测详情现已成为渲染 Pod 的输入：未指定镜像的角色会根据节点池的加速器
			// 运行时版本自动合成镜像，因此驱动滚动升级会改变每个副本应运行的镜像。若无此监听，该变更
			// 只能在其他事件唤醒部署时才被感知，而 spec 哈希比对也会继续匹配基于节点池已不再报告的
			// 旧版本所构建的 Pod。
			//
			// 此处特意不使用 GenerationChangedPredicate：发生变化的正是不会递增 generation 的
			// status。若沿用主对象上的该谓词，恰好会过滤掉此监听旨在捕获的更新。
			&worker.InstanceType{},
			ctrlhandler.EnqueueRequestsFromMapFunc(r.mapModelDeploymentInstanceType),
		).
		Watches(
			// 节点池发布的客户端端点是每个副本拨号连接的地址，并会被渲染进 Pod。该地址会动态变化：
			// 存储主节点重启或 Service 重建都会为池分配新地址，而部署自身的 Binding 在此期间可能保持
			// Ready 状态，因此上方的 Binding 监听无法覆盖此类变更。若无此监听，spec 哈希将继续匹配
			// 使用无人响应的旧地址所构建的副本——这正是本设计刻意杜绝的故障，因为从 Pod 外部来看，它
			// 与缓存未命中毫无区别。
			&workercore.KVCachePool{},
			ctrlhandler.EnqueueRequestsFromMapFunc(r.mapModelDeploymentPool),
		).
		Watches(
			// 后端的传输协议是连接器渲染的另一项输入，且 `spec.transport` 未包含在后端 Webhook 的
			// 不可变性规则中，因此管理员可以在运行中的池上修改它。缺少监听同样会导致静默过期。
			&workercore.KVCacheBackend{},
			ctrlhandler.EnqueueRequestsFromMapFunc(r.mapModelDeploymentBackend),
		).
		Watches(
			// 制品的解析与访问状态决定了是否允许创建副本以及副本基于什么进行渲染，且这些状态仅在
			// 制品自身的 status 中发生变化。
			&workercore.ModelArtifact{},
			ctrlhandler.EnqueueRequestsFromMapFunc(r.mapModelDeploymentArtifact),
		).
		Watches(
			// 声明类制品的绑定状态决定了是否允许创建副本及其可调度的位置，且该状态变化发生在
			// PersistentVolumeClaim 上而非制品本身。
			&core.PersistentVolumeClaim{},
			ctrlhandler.EnqueueRequestsFromMapFunc(r.mapModelDeploymentClaim),
		).
		Watches(
			// 插件的 CSIDriver 决定了节点分发制品是否具备分发条件。
			&storage.CSIDriver{},
			enqueueDeliveryChange(r.mapModelDeploymentDelivery, 0),
			ctrlbuilder.WithPredicates(namePredicate(modelstore.DriverName)),
		).
		Watches(
			// 分发 Setting 决定了 Hub 制品的分发方式；其值通过 Settings 缓存读取，因此需待该缓存过期后
			// 再检查相关部署。
			&core.Secret{},
			enqueueDeliveryChange(r.mapModelDeploymentDelivery, settingsReadDelay),
			ctrlbuilder.WithPredicates(ctrlpredicate.NewPredicateFuncs(func(o ctrlcli.Object) bool {
				return o.GetNamespace() == setting.DelegatedSecretNamespace && o.GetName() == setting.DelegatedSecretName
			})),
		).
		Watches(
			// 节点分发制品的落盘状态由节点上报，而等待该制品的副本 Pod 在节点下载或失败期间不会发生
			// 任何变化，因此 WeightsReady 需跟踪节点的 NodeModelStore。
			&workercore.NodeModelStore{},
			ctrlhandler.EnqueueRequestsFromMapFunc(r.mapModelDeploymentNodeModelStore),
			ctrlbuilder.WithPredicates(nodeModelStoreModelsChanged()),
		).
		Watches(
			// Kueue 对副本的裁决仅写入该副本的 Workload 中：Workload 的创建、配额预留及准入均不会修改
			// 此部署或其 Pod，Kueue 在解除调度门控前不会改动这些 Pod。部署状态依赖这些裁决结果，
			// 若无此监听，QuotaReserved 条件会一直保留上一轮调谐的旧结果（例如将已拥有 Workload
			// 的副本仍报告为“尚无 Workload”），直到其他无关事件唤醒该部署。
			//
			// Workload 通过普通所有者引用归属于其 Pod，因此映射路径从 Workload 追溯到 Pod，再定位到
			// Pod 的控制部署。谓词仅放行状态逻辑实际读取的字段变更；Kueue 内部调度账目更新的频率
			// 远高于其最终裁决的变化。
			&kueue.Workload{},
			ctrlhandler.EnqueueRequestsFromMapFunc(r.mapModelDeploymentWorkload),
			ctrlbuilder.WithPredicates(modelDeploymentWorkloadPredicate()),
		).
		Complete(r)
}

// mapModelDeploymentPool 通过授予池访问权限的 Binding，将关联到该池的所有部署加入队列。
//
// 映射路径采用 池 -> Binding -> 部署，而非遍历所有部署并逐个解析其 Binding：池名称直接
// 记录在 Binding 中，且集群中 Binding 的数量远少于全量遍历所需的 N+1 次读取开销。每个部署
// 仅匹配唯一一个 Binding，因此不会重复生成入队请求。
func (r *ModelDeploymentReconciler) mapModelDeploymentPool(
	ctx context.Context, obj ctrlcli.Object,
) []ctrlreconcile.Request {
	kvcpbList := new(workercore.KVCachePoolBindingList)
	if err := r.Client.List(ctx, kvcpbList, ctrlclix.WithoutQuorum); err != nil {
		ctrllog.FromContext(ctx).Error(err, "list kv cache pool bindings for kv cache pool",
			"kv cache pool", ctrlcli.ObjectKeyFromObject(obj))

		return nil
	}

	var reqs []ctrlreconcile.Request
	for i := range kvcpbList.Items {
		kvcpb := &kvcpbList.Items[i]
		if kvcpb.Spec.PoolRef.Name != obj.GetName() {
			continue
		}
		reqs = append(reqs, r.mapModelDeploymentBinding(ctx, kvcpb)...)
	}

	return reqs
}

// mapModelDeploymentBackend 将运行在指定了该后端的池上的所有部署加入队列。
//
// 出于同样原因，比池映射多了一跳：部署引用 Binding，Binding 引用池，池再引用后端。每个池
// 仅准入一个后端，但该字段为列表类型，因此需通过成员包含关系而非等值比较来判断。
func (r *ModelDeploymentReconciler) mapModelDeploymentBackend(
	ctx context.Context, obj ctrlcli.Object,
) []ctrlreconcile.Request {
	kvcpList := new(workercore.KVCachePoolList)
	if err := r.Client.List(ctx, kvcpList, ctrlclix.WithoutQuorum); err != nil {
		ctrllog.FromContext(ctx).Error(err, "list kv cache pools for kv cache backend",
			"kv cache backend", ctrlcli.ObjectKeyFromObject(obj))

		return nil
	}

	var reqs []ctrlreconcile.Request
	for i := range kvcpList.Items {
		kvcp := &kvcpList.Items[i]
		if !slices.Contains(kvcp.Spec.Backends, obj.GetName()) {
			continue
		}
		reqs = append(reqs, r.mapModelDeploymentPool(ctx, kvcp)...)
	}

	return reqs
}

// mapModelDeploymentInstanceType 将包含按该类型准入的角色的所有部署加入队列。
//
// 与 Binding 映射不同，此扫描必须跨 Namespace 进行：InstanceType 是集群级资源，引用它的
// 部署并不局限于特定 Namespace。遍历集合为集群中的全部 ModelDeployment，其规模受限于集群
// 中部署的数量，而非节点或 Pod 的数量。
func (r *ModelDeploymentReconciler) mapModelDeploymentInstanceType(
	ctx context.Context, obj ctrlcli.Object,
) []ctrlreconcile.Request {
	mdList := new(workercore.ModelDeploymentList)
	if err := r.Client.List(ctx, mdList, ctrlclix.WithoutQuorum); err != nil {
		ctrllog.FromContext(ctx).Error(err, "list model deployments for instance type",
			"instance type", ctrlcli.ObjectKeyFromObject(obj))

		return nil
	}

	var reqs []ctrlreconcile.Request
	for i := range mdList.Items {
		md := &mdList.Items[i]
		for j := range md.Spec.Roles {
			if md.Spec.Roles[j].InstanceType != obj.GetName() {
				continue
			}
			reqs = append(reqs, ctrlreconcile.Request{
				NamespacedName: ctrlcli.ObjectKeyFromObject(md),
			})

			break // 无论多少角色匹配，每个部署仅生成一个请求
		}
	}

	return reqs
}

// mapModelDeploymentBinding 将与 Binding 处于同一 Namespace 且引用该 Binding 的部署加入队列。
//
// 此处采用按 Namespace 的 List 而非字段索引，因为该引用是 Namespace 级对象上的普通字段：
// 查询永不跨 Namespace，因此遍历范围仅限于单个团队的部署。
func (r *ModelDeploymentReconciler) mapModelDeploymentBinding(
	ctx context.Context, obj ctrlcli.Object,
) []ctrlreconcile.Request {
	mdList := new(workercore.ModelDeploymentList)
	if err := r.Client.List(ctx, mdList,
		ctrlcli.InNamespace(obj.GetNamespace()), ctrlclix.WithoutQuorum); err != nil {
		ctrllog.FromContext(ctx).Error(err, "list model deployments for kv cache pool binding",
			"kv cache pool binding", ctrlcli.ObjectKeyFromObject(obj))

		return nil
	}

	var reqs []ctrlreconcile.Request
	for i := range mdList.Items {
		md := &mdList.Items[i]
		if md.Spec.KVCache == nil || md.Spec.KVCache.PoolRef.Name != obj.GetName() {
			continue
		}
		reqs = append(reqs, ctrlreconcile.Request{
			NamespacedName: ctrlcli.ObjectKeyFromObject(md),
		})
	}

	return reqs
}

// mapModelDeploymentWorkload 将副本被 Workload 列为所有者的部署加入队列。
//
// Kueue 将副本的 Workload 归属于其各个 Pod，每个成员对应一条普通所有者引用且无控制器引用，
// 因此映射路径为 Workload -> Pod -> 控制该 Pod 的部署。Pod 的匹配方式与 Pod 监听一致：
// 必须携带当前部署的资源标识，且受控于某个 ModelDeployment。
//
// 缓存中已不存在的 Pod 会直接忽略而不重试。Pod 本身的删除已通过 Pod 监听唤醒了其所属部署，
// 且同一 Workload 的其他成员仍可正常解析。
func (r *ModelDeploymentReconciler) mapModelDeploymentWorkload(
	ctx context.Context, obj ctrlcli.Object,
) []ctrlreconcile.Request {
	mdGVK := workercore.SchemeGroupVersionKind("ModelDeployment")

	var reqs []ctrlreconcile.Request
	for _, ref := range obj.GetOwnerReferences() {
		if !modelDeploymentOwnerRefNamesAPod(ref) {
			continue
		}

		pod := new(core.Pod)
		err := r.Client.Get(ctx,
			ctrlcli.ObjectKey{Namespace: obj.GetNamespace(), Name: ref.Name}, pod, ctrlclix.WithoutQuorum)
		if err != nil {
			if !kerrors.IsNotFound(err) {
				ctrllog.FromContext(ctx).Error(err, "get pod for workload",
					"workload", ctrlcli.ObjectKeyFromObject(obj), "pod", ref.Name)
			}

			continue
		}
		if !modelDeploymentOwnedResource(pod) || !kubemeta.IsControlledByGVK(pod, mdGVK) {
			continue
		}

		req := ctrlreconcile.Request{NamespacedName: types.NamespacedName{
			Namespace: pod.Namespace,
			Name:      kubemeta.GetOwnerRefOfNoCopy(pod, mdGVK).Name,
		}}
		if !slices.Contains(reqs, req) { // 同组成员共享同一个部署
			reqs = append(reqs, req)
		}
	}

	return reqs
}

// modelDeploymentWorkloadPredicate 放行 Workload 的创建与删除事件，且仅在更新涉及部署
// 状态实际读取的字段时放行更新事件。
//
// 这些字段包括所有者引用（决定 Workload 准入哪些副本）、spec.active（协同阻塞屏障），以及
// status 中的 conditions、admission 与 admissionChecks。比较 conditions 和 checks 时会忽略
// 时间戳：状态逻辑仅读取其 type、status、reason 和 message，单纯的时间戳变化不会改变结果。
// Kueue 写入的其他所有信息（重入队状态、调度统计、资源请求等）均属于每次尝试都会变动的调度
// 账目，若放行它们将导致每个调度周期都触发所有等待资源的部署重新调谐。
func modelDeploymentWorkloadPredicate() ctrlpredicate.Predicate {
	return ctrlpredicate.Funcs{
		UpdateFunc: func(e ctrlevent.UpdateEvent) bool {
			oldWl, ok := e.ObjectOld.(*kueue.Workload)
			if !ok {
				return true
			}
			newWl, ok := e.ObjectNew.(*kueue.Workload)
			if !ok {
				return true
			}

			return modelDeploymentWorkloadVerdictMoved(oldWl, newWl)
		},
	}
}

// modelDeploymentWorkloadVerdictMoved 返回更新操作是否改变了部署状态从 Workload 读取的信息。
func modelDeploymentWorkloadVerdictMoved(oldWl, newWl *kueue.Workload) bool {
	oldConditions, oldChecks := modelDeploymentWorkloadVerdict(oldWl)
	newConditions, newChecks := modelDeploymentWorkloadVerdict(newWl)

	return kueueworkload.IsActive(oldWl) != kueueworkload.IsActive(newWl) ||
		!kubemeta.DeepEqual(oldWl.OwnerReferences, newWl.OwnerReferences) ||
		!kubemeta.DeepEqual(oldWl.Status.Admission, newWl.Status.Admission) ||
		!kubemeta.DeepEqual(oldConditions, newConditions) ||
		!kubemeta.DeepEqual(oldChecks, newChecks)
}

// modelDeploymentWorkloadVerdict 复制 Workload 的 conditions 和 admission checks，并清除其中的时间戳。
func modelDeploymentWorkloadVerdict(wl *kueue.Workload) ([]meta.Condition, []kueue.AdmissionCheckState) {
	conditions := make([]meta.Condition, 0, len(wl.Status.Conditions))
	for _, c := range wl.Status.Conditions {
		conditions = append(conditions, meta.Condition{
			Type: c.Type, Status: c.Status, Reason: c.Reason, Message: c.Message,
		})
	}
	checks := make([]kueue.AdmissionCheckState, 0, len(wl.Status.AdmissionChecks))
	for _, c := range wl.Status.AdmissionChecks {
		checks = append(checks, kueue.AdmissionCheckState{Name: c.Name, State: c.State, Message: c.Message})
	}

	return conditions, checks
}

// modelDeploymentMinRenderVersionChangedPredicate 在部署的
// modeldeployment.gpustack.ai/min-render-version 注解发生变化时放行更新事件。
func modelDeploymentMinRenderVersionChangedPredicate() ctrlpredicate.Predicate {
	return ctrlpredicate.Funcs{
		CreateFunc: func(ctrlevent.CreateEvent) bool {
			return false
		},
		DeleteFunc: func(ctrlevent.DeleteEvent) bool {
			return false
		},
		GenericFunc: func(ctrlevent.GenericEvent) bool {
			return false
		},
		UpdateFunc: func(e ctrlevent.UpdateEvent) bool {
			if e.ObjectOld == nil || e.ObjectNew == nil {
				return false
			}
			return e.ObjectOld.GetAnnotations()[modelDeploymentMinRenderVersionAnnotation] !=
				e.ObjectNew.GetAnnotations()[modelDeploymentMinRenderVersionAnnotation]
		},
	}
}
