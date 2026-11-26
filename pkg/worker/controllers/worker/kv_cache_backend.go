package worker

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"slices"
	"strings"
	"time"

	apps "k8s.io/api/apps/v1"
	coordination "k8s.io/api/coordination/v1"
	core "k8s.io/api/core/v1"
	rbac "k8s.io/api/rbac/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlrecord "k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlbuilder "sigs.k8s.io/controller-runtime/pkg/builder"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlcontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	ctrlevent "sigs.k8s.io/controller-runtime/pkg/event"
	ctrlhandler "sigs.k8s.io/controller-runtime/pkg/handler"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	ctrlpredicate "sigs.k8s.io/controller-runtime/pkg/predicate"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/controller"
	"gpustack.ai/gpustack/pkg/kubeapistatus"
	"gpustack.ai/gpustack/pkg/kubeclientset"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/systemmeta"
	"gpustack.ai/gpustack/pkg/utils/ctrlclix"
	"gpustack.ai/gpustack/pkg/utils/ctrlhandlerx"
	"gpustack.ai/gpustack/pkg/utils/json"
	"gpustack.ai/gpustack/pkg/utils/stringx"
	"gpustack.ai/gpustack/pkg/worker/kuberess"
	"gpustack.ai/gpustack/pkg/worker/kvcache"
	"gpustack.ai/gpustack/pkg/worker/kvcache/mooncake"
	"gpustack.ai/gpustack/pkg/worker/settings"
)

// IMPORTANT: 本文件是 KVCacheBackend 的主控制器

// KVCacheBackend 的阶段来自实际观测。即使 leader Pod 为 Running，只要进程尚未准备好提供服务，后端仍处于 Provisioning。
const (
	// KVCacheBackendPhaseProvisioning 表示后端正在启动，尚未观测到可用服务。
	KVCacheBackendPhaseProvisioning = "Provisioning"
	// KVCacheBackendPhaseReady 表示 leader 可以服务，且未发现成员短缺。通常至少要挂载一个成员。如果该版
	// 本不提供内存段列表，则结合成员 Pod 无故障和容量非零来判断。
	KVCacheBackendPhaseReady = "Ready"
	// KVCacheBackendPhaseDegraded 表示 leader 仍可服务，但成员组在部分匹配节点上缺少成员。
	KVCacheBackendPhaseDegraded = "Degraded"
	// KVCacheBackendPhaseError 表示 leader 无法访问或无法调度。
	KVCacheBackendPhaseError = "Error"
	// KVCacheBackendPhaseDeleting 表示对象正在删除，等待持有方释放也属于该阶段。
	KVCacheBackendPhaseDeleting = "Deleting"
)

// 条件类型由 KVCacheBackend 报告，每个轴一个。它们是 kubeapistatus.ConditionType，因此该包的访问器可以读写
// 它们；这些访问器是反射式的，既适用于该组自己的条件类型，也适用于上游的条件类型。
//
// 如果观察到其中不止一个情况，从它们推导出的阶段属于 kubeapistatus.StatusSummarizer，而非某个开关——这就是该
// 包中的 walker 的作用，而 ClusterQueue 视图也正是采用这种方式实现。
const (
	KVCacheBackendConditionLeaderAvailable  kubeapistatus.ConditionType = "LeaderAvailable"
	KVCacheBackendConditionMembersMounted   kubeapistatus.ConditionType = "MembersMounted"
	KVCacheBackendConditionCapacityObserved kubeapistatus.ConditionType = "CapacityObserved"
	// KVCacheBackendConditionPoolWrites 报告了自当前主节点进程启动以来的全存储写入活动情况。它不会将写
	// 入操作归因于某个部署。
	KVCacheBackendConditionPoolWrites kubeapistatus.ConditionType = "PoolWrites"
	// 只要 status.usedBy 仍列有使用方，KVCacheBackendConditionDeletable 就为 False，说明终结器为何仍须保留。
	KVCacheBackendConditionDeletable kubeapistatus.ConditionType = "Deletable"
	// KVCacheBackendConditionTierWasEmpty 用于回答“当此后端启动时，磁盘层是空的吗？”该条件仅撰写一次，
	// 之后不再修改，与此处其他条件不同，因为第一次写入之后，集群上没有任何内容能够回答该条件——此后端自
	// 己的数据与其所找到的数据无法区分。reportKVCacheBackendTierReuse 则说明了为何其他选项更差。
	KVCacheBackendConditionTierWasEmpty kubeapistatus.ConditionType = "TierWasEmpty"
	// KVCacheBackendConditionElectionObserved 用于判断在要求进行选举的后端是否真的发生了选举。在两个副
	// 本以下的情况下，该值为空，因为没有需要选举的对象，因此也不存在作为任何事物的载体的租约。
	KVCacheBackendConditionElectionObserved kubeapistatus.ConditionType = "ElectionObserved"
	// KVCacheBackendConditionRolloutComplete 用于判断主节点的最后一次更新是否已完成。它的存在是因为当前
	// 工作负载取消了原本会用于判断的截止时间，而该截止时间被取消是因为在此情况下其两种结果无法区分。
	KVCacheBackendConditionRolloutComplete kubeapistatus.ConditionType = "RolloutComplete"
)

// KVCacheBackendReconciler 调谐 KVCacheBackend。
//
// 它管理该对象的生命周期，并在后续任务中一并管理为其提供服务的工作负载：
//
//   - 存活对象会附加本仓库专属的 finalizer 加以锁定，防止与调谐并发的删除操作遗留孤儿
//     资源。
//   - 删除时若 status.usedBy 仍列有使用方，则保留 finalizer。此处的阻塞无需重新入队：
//     usedBy 由使用方自身的控制器写入本对象的 status，该写入事件自然会重新触发清理流程。
//     使用定时器只会反复查询一个答案尚未改变的问题。
//   - status 在 DeepEqual 保护下整块写入，因此处于稳态的后端不会产生写入操作，也就不会
//     自身触发额外事件。
type KVCacheBackendReconciler struct {
	Client ctrlcli.Client

	// AdminHTTP 用于读取 leader 的管理接口。它自带独立超时控制，因为若 leader 接受连接后
	// 发生阻塞，没有超时将会使本次调谐一直挂起直至传输层超时。
	//
	// 该超时仅限制单次阻塞的时长，并不能隔离其他后端受其影响；这正是 kvCacheBackendConcurrency
	// 的作用，两者配合使用才能发挥效果。
	AdminHTTP *http.Client

	// Recorder 用于发布 status 字段无法承载的事件信息。磁盘层的清理事件记录在 Node 而非本
	// 后端对象上，因为需要记录该信息时后端正在被删除，记录在后端上会随对象一并销毁。
	Recorder ctrlrecord.EventRecorder

	// leaseHolders 是该调谐器在多次调谐之间保留的唯一状态，保留它是为了识别状态跃迁：
	// 租约易主与单纯续约在连续两次读取中表现为同一个对象。其零值即可直接使用，因此
	// 无需在构造路径中额外初始化。
	leaseHolders leaseHolders
}

// adminReadTimeout 限制单次读取 leader 管理接口的耗时。该超时较短：此处读取的所有数据
// 均由同一集群内的进程从内存提供，耗时更长说明 leader 已出现异常，尽快上报比苦等更有价值。
const adminReadTimeout = 5 * time.Second

// newAdminHTTPClient 构建所有管理接口读取共用的客户端。该客户端在控制器整个生命周期内
// 仅创建一次，以便在多次调谐之间复用连接池：每次调谐新建客户端会导致每次都重新连接
// 同一个 leader。
//
// 客户端会拒绝重定向而非自动跟随。对于外部后端，地址由 spec 编写者指定，默认客户端会
// 跟随其返回的 302 重定向访问本算子可达的任意主机——即以算子的网络身份发起读取，并将
// 响应摘录写入所有拥有该对象读权限的用户皆可查看的 status 中。这三次读取均针对准入校验
// 已验证地址上的固定路径，因此绝不应采信指向别处的重定向，而是直接上报携带该重定向的响应。
func newAdminHTTPClient() *http.Client {
	return &http.Client{
		Timeout: adminReadTimeout,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

var _ ctrlreconcile.Reconciler = (*KVCacheBackendReconciler)(nil)

// adminRead 允许对主节点的管理界面进行一次读取操作，且有自己的截止时间。
//
// 其存在是因为三次读取操作是依次进行的，而围绕它们的一个截止时间并非固定值。adminReadTimeout 描述的情况如下：
// 慢速的 /health 检测会导致容量下降，同时内存段列表会显示几乎过期的上下文，这两种情况都会被报告为失败——而一
// 个仅表现缓慢的主节点则会被视为其指标和列表出现问题的主节点。
func adminRead[T any](ctx context.Context, read func(context.Context) (T, error)) (T, error) {
	readCtx, cancel := context.WithTimeout(ctx, adminReadTimeout)
	defer cancel()

	return read(readCtx)
}

// kvCacheBackendObserveInterval 决定后端稳定后多久重新采集一次状态。
//
// 关于正在运行的存储的所有状态报告——容量指标、内存段列表、某个地址是否仍可访问——都是通过 HTTP 读取的，不会
// 产生任何 Kubernetes 事件。当工作负载发生变化时，这些监视器会唤醒该控制器，例如主节点重启或节点加入群组，
// 但对于那些 Pods 稳定而内容在变动的存储来说，这些监视器则毫无作用。由于外部后端本身没有我们的工作负载需要
// 监视，因此没有这些监视器后，其状态将保持第一次检查时的状态。
//
// 和 InstanceType 使用相同的汇总周期。容量变化在几秒后即可反映，每分钟四次读取也不会给 leader 造成明显负载。
const kvCacheBackendObserveInterval = 15 * time.Second

// kvCacheBackendTeardownInterval 决定后端等待工作负载退出时多久再检查一次。
//
// Workload watch 功能无需赘述：它根据请求进行数据重复利用，且所有三种渲染形式都对应同一个后端系统——因此最后
// 一种的删除操作可以被前一种覆盖，从而不会再次触发对对象的唤醒。
const kvCacheBackendTeardownInterval = 2 * time.Second

// kvCacheBackendWorkloadTerminationMargin 是控制器在工作负载声明的终止时限之外额外等待的时间。
//
// 预算本身是依据工作负载来确定的，而非固定值。因为具有磁盘层级的成员组，其 Pod 的
// terminationGracePeriodSeconds 值来源于规范要求的缩容缓存时间，该时间长达一小时。如果该常数为足够短的边界
// 值，那么会限制那些按配置方式正在消耗资源的成员；如果该常数为足够长的边界值，那么其他成员则不受限制。
//
// 所宣称的“优雅时间”是一个严格的截止时间，正因如此，它才可能成为停止的节点：当工作负载完成时，无论其正在执
// 行什么操作，kubelet 都会终止该容器。因此，在自身“优雅时间”过后仍会终止的工作负载并非缓慢的，而是那种
// kubelet 没有进行操作的类型——这种状态根本没有限制，这就是存在此边界以停止等待的状态。
//
// 这种空间用于容器消失后发生的情况，这些并非由容器自身导致的：卷卸载、Pod 被移除，以及前景删除操作对工作负
// 载本身的处理。这个空间是充足的，而非有限的，因为出错的成本是不对称的——如果空间太短，就会过早放弃健康的清
// 理过程；如果空间太长，则只会延迟那些原本不会消失的对象。
const kvCacheBackendWorkloadTerminationMargin = 2 * time.Minute

// kvCacheBackendEventWorkloadAbandoned 记录的是那些在等待结束后仍然存在的渲染后工作负载，而非后端状态：后端
// 即将被释放，其事件也会随之消失，而工作负载的持续时间则完全取决于维持其存在的因素。
const kvCacheBackendEventWorkloadAbandoned = "KVCacheWorkloadNotTerminated"

// kvCacheBackendMaxMembers 限定了 status.members 将承载的段数。
//
// 每个条目都会在每次更新时重新发布，而 API 服务器会拒绝超过约 1.5 MiB 大小的对象——因此超过此限制的列表会导
// 致所有状态写入操作失败并无限重试，而产生该状态的观察结果却显示为成功。在最大的 Kubernetes 集群节点成员数
// 量超过一个的情况下，达到这一数量意味着该列表不符合此 API 的模型要求。
const kvCacheBackendMaxMembers = 5000

// kvCacheBackendMaxMembersBytes 限制成员列表中所有标识符编码后的总字节数。
//
// 仅“COUNT”这一项并不具有限制性。内存段中的每一串数据都由管理端点选择，而非此操作员选择；对于外部后端来说，
// 该端点是其他人的。少数带有长识别信息的条目比数千个普通条目更重要，因此仅以计数作为保护的机制会导致无法写
// 入的列表出现，并且在每次重试时都会失败，包括那些本应能报告该列表的重试操作。
//
// 预留空间使成员标识的大小远低于单个 Kubernetes 对象约 1.5 MiB 的上限。对象还要存储 spec、conditions、端点、
// 容量以及 managedFields。
const kvCacheBackendMaxMembersBytes = 512 << 10

// segmentListingSize 用于衡量一个列表对状态的影响，而不涉及其构建过程。仅包含标识符：每条条目的 JSON 开销是
// 固定的，计数保护机制也已限制了其数量。
func segmentListingSize(segments []mooncake.SegmentDetail) int {
	var n int
	for i := range segments {
		n += encodedStringSize(segments[i].ID) + encodedStringSize(segments[i].ClientID) +
			encodedStringSize(segments[i].Name) + encodedStringSize(segments[i].State) +
			encodedStringSize(segments[i].Protocol) + encodedStringSize(segments[i].TEEndpoint)
	}
	return n
}

// encodedStringSize 返回字符串经过 JSON 编码后传输占用的字节数。
//
// 两者之间的差异可达六倍，而这种差异则由接收端决定：JSON 会转义控制字符，而 `<`、`>` 和 `&` 则作为 `\uXXXX`
// 被处理——每个字符占六个字节。在这种情况发生之前进行测量会低估那些从外部端点接收到的标识符的实际大小，因为
// 攻击者可能会选择这种低估方式，因此被计入“已准入”列表的列表大小可能仅为半兆字节，但实际大小却可能是三兆字节。
//
// 根据此处制定的规则，不应将其计入计数中，因为该规则属于 Encoder 的设定：重新表述该规则是一种可能偏离实际运
// 行方式的第二种实现方式。
func encodedStringSize(s string) int {
	encoded, err := json.Marshal(s)
	if err != nil {
		// 对于字符串来说是不可达的，而这里的零值会取消对那个达到该值的输入的约束。相反，Encoder 所
		// 能产生的最坏情况将占据主导地位。
		return len(s) * 6
	}
	return len(encoded)
}

// 生成后的架构将状态消息字符数限制为 32768 个，当超过此限制时，API 服务器会拒绝整个状态的写入——这将导致状态
// 在存在需要报告的故障的节点上被冻结。这里有两种情况可能超过此限制，而这两种情况都不属于本操作者的控制范围，不能限制。
//
// 第一个是名称列表，随着集群的扩展而增长：一个主节点会列出所有 Pod 应属于的内存段，每个 Pod 只需简短的账户
// 即可，因此几百个节点就足够了，而后端服务可以被尽可能多的消费者使用，取决于有人创建了多少内容。此类列表中
// 的数字本身就体现了规模，因此样本数据不会让读者感到困扰——而且提及数百个对象的消息本来就不易阅读。
//
// 第二个是其他人写的字符串：可能是 kubelet 或调度器的消息，也可能是外部后端提供的 HTTP 请求体中的字段。这些
// 属于故障的可处理部分，因此被传递过来；但它们的长度则取决于集群或最终使用方。其中任何一个就能满足限制——管
// 理员响应的最大大小为 8 MiB，而并未说明这些内容是如何在多个字段中分配的。
const (
	kvCacheBackendMaxNames       = 20
	kvCacheBackendMaxDetailRunes = 2 << 10

	// kvCacheBackendMaxAmbiguityBytes 限定了歧义条款列表，该值由模式限制决定，而非从上述名称上限中复用。
	//
	// Condition.message 的存储上限为 32 KiB。通过 COUNT 对列表进行限制无法满足这一限制，因为一个子句没
	// 有固定的宽度：它包含一个键和一个嵌套名称列表，而这两个名称都是长度可达 253 个字符的对象名称，因此
	// 二十个子句的大小可能在几百字节到超过限制值之间。仅通过计数字节数才能实现限制效果。
	//
	// 32 中的 2 KiB 被预留用于构成此列表的句子以及“and N more”这一后缀，这两部分内容均写在预算之外。
	//
	// 有一个限制条款始终适用，因此列表永远无法被截断为空：listBoundedNames 将其限制在
	// kvCacheBackendMaxNames 个名称的限度，这样就能将最严格的限制放在 5.4 KiB 附近。这种关系是通过测试
	// 来确定的，而非靠运气，因为提高名称限制上限就会破坏它。
	kvCacheBackendMaxAmbiguityBytes = 30 << 10
)

// listBoundedNames 返回限长的名称示例，并指出省略了多少个名称。
func listBoundedNames(names []string) string {
	if len(names) <= kvCacheBackendMaxNames {
		return strings.Join(names, ", ")
	}
	return fmt.Sprintf("%s and %d more",
		strings.Join(names[:kvCacheBackendMaxNames], ", "),
		len(names)-kvCacheBackendMaxNames)
}

// clipFaultDetail 限定了该运算符未写入的字符串，该字符串将用于条件消息中。
//
// 使用 Unicode 码点而不是显示单元格，这就是 stringx.Truncate 不是合适工具的原因：零宽度或组合字符不占用任何
// 单元格空间，因此无论长度多长，由这些字符组成的字符串都会被完整返回——而这里的限制指的是字符数量。
func clipFaultDetail(detail string) string {
	return stringx.TruncateRunes(detail, kvCacheBackendMaxDetailRunes, "… (truncated)")
}

// IMPORTANT: KVCacheBackend 建起并看管好共享存储的总控流程，总控入口
//   - 拉起并观测底层 Mooncake 集群
func (r *KVCacheBackendReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := ctrllog.FromContext(ctx)

	// 读取本次调谐的对象。
	kvcb := new(workercore.KVCacheBackend)
	err := r.Client.Get(ctx, req.NamespacedName, kvcb, ctrlclix.WithoutQuorum)
	if err != nil {
		if !kerrors.IsNotFound(err) {
			logger.Error(err, "fetch kv cache backend")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// 对象已标记删除，由终结器负责清理。
	if kvcb.DeletionTimestamp != nil {
		return r.teardownKVCacheBackend(ctx, kvcb)
	}

	// 没加 Finalizer 锁? 添加终结器，确保对象删除前还有机会清理它创建的资源。
	if !systemmeta.Lock(kvcb) {
		if err = r.Client.Update(ctx, kvcb); err != nil {
			logger.Error(err, "lock kv cache backend")
			return ctrl.Result{}, err
		}
	}

	// 条件写入器需要含有 Status 的对象。复制当前 ObjectMeta 和 Status，在副本上更新条件，使 DeepEqual
	// 最后仍能比较新旧状态。
	holder := &workercore.KVCacheBackend{
		ObjectMeta: *kvcb.ObjectMeta.DeepCopy(),
		Status:     r.computeStatus(kvcb, r.liveKVCacheBackendConsumers(ctx, kvcb)),
	}
	// Status 副本带着上一轮公布的地址。先清空，再由本轮实际观测到的分支填回，以免继续公布已删除的
	// Service 或误判后端已完成渲染。
	holder.Status.Endpoints = nil

	// 本轮可能无法渲染新配置，但上一轮的工作负载还在服务。用 phase 报告渲染问题，运行条件继续反映实际存储的状态。
	var renderBlocked error

	// 管理型后端创建工作负载，外部后端直接提供现有地址。两条路径都先写入端点，后续观测统一从这些端点读取。
	switch {
	case kvcb.Spec.Connection.Managed != nil:
		// IMPORTANT: Managed (托管自建) 的连接模式
		//
		// 每轮只解析一次后端镜像，再交给 leader 和成员组。未单独指定镜像的组会使用同一个值，避免两
		// 次读取之间设置发生变更。
		image, imageErr := resolveKVCacheBackendImage(ctx, kvcb)
		if imageErr != nil {
			// 镜像设置可以在运行中被清空。跳过本轮渲染，继续观测仍在运行的存储，并通过 phase 报
			// 告新配置暂时无法应用。此时直接返回会使旧状态一直保留，控制器还会按错误退避。
			logger.Error(imageErr, "resolve kv cache backend image")

			// 地址可以由对象名推导，但渲染跳过时相应的 Service 可能从未建立。先查询 Service，否
			// 则 DNS 或连接错误会被误读为 leader 仍在启动。
			//
			// Service 名称可能已被无关工作负载占用。沿用清理流程的归属标记核对所有者，再向消费
			// 者公布并读取该后端的地址。
			svc := new(core.Service)
			switch getErr := r.Client.Get(ctx, ctrlcli.ObjectKey{
				Name:      mooncake.LeaderObjectName(kvcb),
				Namespace: kuberess.SystemNamespaceName,
			}, svc); {
			case getErr == nil:
				if renderedForKVCacheBackend(svc, kvcb) {
					holder.Status.Endpoints = mooncake.LeaderEndpoints(kvcb)
				}
			case !kerrors.IsNotFound(getErr):
				return ctrl.Result{}, getErr
			}
		} else {
			if err = r.syncLeaderWorkload(ctx, kvcb, image); err != nil {
				return ctrl.Result{}, err
			}
			if err = r.syncMemberWorkloads(ctx, kvcb, image); err != nil {
				return ctrl.Result{}, err
			}
			// 仅当 HA 已关闭时才执行，并放在最后。此时工作负载已经更新为不再引用先前的
			// ServiceAccount。
			if err = r.pruneHARBAC(ctx, kvcb); err != nil {
				return ctrl.Result{}, err
			}
			// 工作负载和 Service 收敛以后才发布地址。地址所指对象已经存在，是否已经能够服务由后续条件反映。
			holder.Status.Endpoints = mooncake.LeaderEndpoints(kvcb)
		}
		renderBlocked = imageErr

	case kvcb.Spec.Connection.External != nil:
		// IMPORTANT: External (外部接入) 的连接模式
		//
		// 外部后端已运转，直接复制声明中的地址，无需解析镜像，也无需创建 Deployment、Service 或 DaemonSet。
		holder.Status.Endpoints = slices.Clone(kvcb.Spec.Connection.External.Endpoints)

	default:
		// 准入会拒绝两种连接方式均未填写的对象。如果历史对象仍有这种情况，保留可观测状态并跳过渲染
		// 和地址访问，以免解引用空指针。
		return ctrl.Result{}, r.syncStatus(ctx, kvcb, holder.Status)
	}

	r.observeLeader(ctx, kvcb, holder)
	// 将首次勘察的结论记在对象上。以后成员重启时看到的是本后端写过的数据，已无法据此判断目录最初是否被复用。
	r.reportKVCacheBackendTierReuse(ctx, kvcb, holder)
	// 下面两个观测共用一次 Lease 读取。把错误一起传递，因为两个观测对 Lease 尚不存在有各自的解释。
	lease, leaseErr := r.leaderLease(ctx, kvcb)
	// 这里要针对实际后端发布 Kubernetes Event，因此传入观测到的对象。正在组装的副本用于写回 status。
	r.reportLeaderHandover(kvcb, lease, leaseErr)
	// 用同一份 Lease 再判断是否曾有实例获得过领导权。
	r.reportElectionObserved(ctx, kvcb, holder, lease, leaseErr)
	// 单独检查工作负载的最近一次更新是否完成，健康状况由上面的条件报告。
	r.reportRolloutComplete(ctx, kvcb, holder)
	// 最后用本轮刚写入的条件推导 phase，避免汇总上一轮的状态。
	deriveKVCacheBackendPhase(holder, renderBlocked)

	// 即使本轮成功也定时重新入队。Mooncake 内部状态变化不会触发 Kubernetes 事件。
	return ctrl.Result{RequeueAfter: kvCacheBackendObserveInterval},
		r.syncStatus(ctx, kvcb, holder.Status)
}

// observeLeader 读取后端的服务状态、可用容量和内存段列表，将实际观测结果汇入本轮 status。
//
// 一次遍历，每条路径各读取一次。这三个轴是共同推导出来的，因为它们共享一个门——service_ready——如果读取两次
// `/health`，可能会得到两个不同的结果，而这在“协调”过程中会导致条件相互矛盾。
//
// 以健康文档的 service_ready 为准。已启动但尚未提供服务的 leader 会让 /metrics 返回格式正确的零值，而内存段
// 列表会返回 503。仅看数值无法区分尚未就绪与真正的空缓存，因此两种读数都须通过这个检查。
//
// 需要两个对象，因为它们回答不同的问题：kvcb 是被观测对象，也是读取规范的来源；holder 是正在构建的状态，也
// 是条件访问器写入的内容。
func (r *KVCacheBackendReconciler) observeLeader(
	ctx context.Context, kvcb, holder *workercore.KVCacheBackend,
) {
	logger := ctrllog.FromContext(ctx)

	// 前面已明确为“空”，因此下面的每条路径要么发布一个观察值，要么完全不留下任何字段。将上一次遍历的结
	// 果继续传递，就会将其作为当前值重新发布；而此处一个空的结构体则会被序列化为 "capacity"：{}——这种结
	// 构不符合 API 契约的要求。
	holder.Status.Capacity = nil

	client := r.adminClientFor(holder.Status.Endpoints)
	if client == nil {
		reason, message := "NoAdminEndpoint", "the backend publishes no admin endpoint to read"
		KVCacheBackendConditionLeaderAvailable.False(holder, reason, message)
		KVCacheBackendConditionCapacityObserved.False(holder, reason, message)
		KVCacheBackendConditionPoolWrites.Unknown(holder, reason, message)
		setMembersObservationFailed(holder, reason, message)
		return
	}

	health, err := adminRead(ctx, client.Health)
	if err != nil {
		logger.V(2).Info("kv cache backend leader health unreadable", "error", err.Error())

		// 无法读取主节点可能包含两种不同的情况，区别在于其 Pod 是否处于就绪状态。如果主节点仍在拉取
		// 其镜像，或者虽然已启动但尚未提供推理服务，那么这种情况是不可接受的——其就绪探针机制会限制
		// 访问，因此它也不满足就绪条件，报告错误则会让每次安装在最初的一分钟内看起来就有问题。而如
		// 果主节点的 Pod 已经就绪 yet 无法被读取，那则是另一种情况。
		//
		// Deployment 已通过探针执行连续失败阈值判断，直接读取 readyReplicas 即可。在本控制器中重新
		// 累计失败次数会依赖事件顺序，还要保存多余状态。
		reason := "LeaderStarting"
		message := fmt.Sprintf("the leader is not serving yet and cannot be read: %v", err)
		switch {
		case kvcb.Spec.Connection.Managed == nil:
			// 外部后端没有这种特权，原因就是上述借口对其并不适用。其地址所指向的内容，是操作员
			// 已经声明的运行状态；这里没有任何处于创建过程中的部分，也没有我们的任何 Pod 处于“
			// 尚未就绪”的状态。将输入错误或防火墙拦截的端点报告为 Provisioning，只会使其永远等
			// 待永远不会到来的启动。
			reason = "LeaderUnreachable"
			message = fmt.Sprintf("the external backend at %s could not be read: %v",
				client.Address, err)
		case r.leaderPodIsReady(ctx, kvcb):
			reason = "LeaderUnreachable"
			message = fmt.Sprintf("the leader's pod is ready but its health could not be read: %v", err)
		case r.leaderPodIsUnschedulable(ctx, kvcb):
			reason = "LeaderUnschedulable"
			message = "the leader's pod cannot be scheduled onto any node, so it will not start " +
				"until the cluster or this object changes"
		default:
			// 其他待处理的故障仍未解决：Pod 已部署，但其容器无法运行——可能是无法拉取镜像，也可
			// 能是进程启动后迅速退出。它既未准备好，也不可调度，因此没有这些处理措施的话，就会
			// 一直被标记为处于启动状态的主节点。
			if podReason, podMessage, failed := r.leaderPodContainerFailure(ctx, kvcb); failed {
				reason, message = podReason, podMessage
			}
		}
		KVCacheBackendConditionLeaderAvailable.False(holder, reason, message)
		KVCacheBackendConditionCapacityObserved.False(holder, reason, message)
		KVCacheBackendConditionPoolWrites.Unknown(holder, reason, message)
		// 保留上一次符合当前 schema 的成员列表。一次观测失败无法证明所有成员已经消失。旧 schema 写
		// 入且不再合法的条目需要略去，否则 API Server 会拒绝更新。
		setMembersObservationFailed(holder, reason, message)
		return
	}

	if !health.ServiceReady {
		// 不是错误。主节点正在回应，并说明目前尚未提供服务，这是正常阶段。它现在报告的任何内容都意
		// 味着什么：其指标显示为零，列表显示为 503s。
		reason := "ServicePlaneNotActive"
		message := fmt.Sprintf(
			"the leader is up in role %q and reports ha_state %q, but its service plane is not active",
			clipFaultDetail(health.Role), clipFaultDetail(health.HAState),
		)
		KVCacheBackendConditionLeaderAvailable.False(holder, reason, message)
		KVCacheBackendConditionCapacityObserved.False(holder, reason,
			"the leader is up but its service plane is not active, so it holds no capacity to report")
		KVCacheBackendConditionPoolWrites.Unknown(holder, reason,
			"the leader is up but its service plane is not active")
		setMembersObservationFailed(holder, reason,
			"the leader is up but its service plane is not active, so it lists no segment")
		return
	}

	KVCacheBackendConditionLeaderAvailable.True(holder, "Serving",
		fmt.Sprintf("the leader serves in role %q", clipFaultDetail(health.Role)))

	metrics := r.observeCapacity(ctx, kvcb, holder, client)
	segments, listingServed := r.observeMembers(ctx, kvcb, holder, client)
	reportKVCacheBackendPoolWrites(holder, metrics, segments, listingServed)
}

// leaderPodIsReady 判断 leader 的 Deployment 是否至少有一个可用副本。
//
// 对于管理型后端，只有当满足特定条件时，读取失败才会被报告为故障——外部环境没有需要处理的 Deployment，因此遵
// 循不同的规则进行判断。这本质上是`DEPLOYMENT`自身的视角，而非我们自己的探针视角：就绪探针已经向主节点询问
// 了受保护的路由，因此 readyReplicas 就是 Kubernetes 确定的主节点的就绪状态，在此重新决定该状态可能会产生不同的意见。
//
// 只要求至少一个就绪副本。原 leader 丢失 Lease 时会在进程内立即停止服务，但 kubelet 要连续三次探针失败才会
// 撤销其就绪状态。正常交接期间新旧副本可能同时显示 Ready 最多约 15 秒。
func (r *KVCacheBackendReconciler) leaderPodIsReady(
	ctx context.Context, kvcb *workercore.KVCacheBackend,
) bool {
	deploy := new(apps.Deployment)
	err := r.Client.Get(ctx, ctrlcli.ObjectKey{
		Name:      mooncake.LeaderObjectName(kvcb),
		Namespace: kuberess.SystemNamespaceName,
	}, deploy)
	if err != nil {
		// 尚无 Deployment 可供检查，后端仍处于启动阶段，暂时没有就绪副本。
		return false
	}
	return deploy.Status.ReadyReplicas > 0
}

// leaderPodIsUnschedulable 检查调度器是否已明确拒绝放置 leader Pod。
//
// 这是主节点出现的问题，始终无法解决：集群中没有节点满足 Pod 的要求，因此除了协调过程之外——无论是节点、污点
// 还是对象自身的资源——都必须发生变化，否则无法运行。仅通过 readyReplicas 是无法区分的，因为此时 Pod 仍在拉
// 取其镜像，并被报告为“准备中”状态，这会让运维人员等待无法启动的进程，因此会单独处理并将其列为故障。
//
// 问题涉及的是 Pods，而非 Deployment，因为 Deployment 仅会在十分钟后通过 ProgressDeadlineExceeded 机制被触
// 发；PodScheduled 则是由调度器在首次尝试时设定的。
func (r *KVCacheBackendReconciler) leaderPodIsUnschedulable(
	ctx context.Context, kvcb *workercore.KVCacheBackend,
) bool {
	for _, pod := range r.leaderPods(ctx, kvcb) {
		for _, cond := range pod.Status.Conditions {
			if cond.Type == core.PodScheduled &&
				cond.Status == core.ConditionFalse &&
				cond.Reason == core.PodReasonUnschedulable {
				return true
			}
		}
	}

	return false
}

// leaderPodContainerFailure 报告了主节点容器为何无法运行，以及该问题为何无法通过等待解决。
//
// 这是第二个问题：readyReplicas 无法看到，其出现路径与第一个问题相同：计划运行但尚未准备好的 Pod，通常意味
// 着另一个 Pod 正在启动中。因此调用者的默认行为就是如此——但无法拉取镜像，或者进程启动后很快就退出，也说明没
// 有开始运行。如果没有这一点，两者看起来会非常相似，而一个永远无法启动的后端系统就会一直显示“正在准备中”的状态。
//
// kubelet 自身的原因和消息会被传递，而非重新表述。它们会指出无法拉取的镜像或缺失的加载器符号，这是可操作的
// 方面；同时，这也是该操作员无法在其他地方看到的故障的唯一描述。
func (r *KVCacheBackendReconciler) leaderPodContainerFailure(
	ctx context.Context, kvcb *workercore.KVCacheBackend,
) (reason, message string, failed bool) {
	for _, pod := range r.leaderPods(ctx, kvcb) {
		for _, status := range pod.Status.ContainerStatuses {
			// 最后的终止操作会首先执行，这种顺序正是其设计初衷。还有一个处于死循环状态的容器在
			// 等待，它的等待信息是 kubelet 自带的通用模板——“back-off 5m0s restarting failed
			// container=...”——其中并未说明该容器为何崩溃。退出代码和进程的最后输出内容在此。
			if t := status.LastTerminationState.Terminated; t != nil && status.RestartCount > 0 {
				return "LeaderCrashLooping", fmt.Sprintf(
					"the leader's container has restarted %d time(s); it last exited with code %d: %s",
					status.RestartCount, t.ExitCode, clipFaultDetail(t.Message),
				), true
			}
			// 有理由等待意味着 kubelet 表示暂时停止了尝试；一个从未运行的容器也没有需要报告的内
			// 容。ContainerCreating 和 PodInitializing 是表示进度的状态，它们被特意排除在外，因
			// 此任何这里未曾见过的理由都被视为问题，而非沉默。
			if w := status.State.Waiting; w != nil && isTerminalWaitReason(w.Reason) {
				return w.Reason, fmt.Sprintf("the leader's container is not running: %s: %s",
					w.Reason, clipFaultDetail(w.Message)), true
			}
		}
	}

	return "", "", false
}

// isTerminalWaitReason 判断处于等待状态的容器是否已陷入卡死而非正在启动。
//
// 被排除的两个原因属于 kubelet 自身的推进状态：容器正在创建，以及 Pod 的 init 容器
// 尚未完成。等待期间上报的其他所有原因——包括镜像拉取失败、各种退避重试——都会持续
// 存在，直到本次调谐之外的某些条件发生改变。
func isTerminalWaitReason(reason string) bool {
	switch reason {
	case "", "ContainerCreating", "PodInitializing":
		return false
	}
	return true
}

// leaderPods 列出了主节点的 Deployment 所拥有的 Pods，这些 Pods 通过 Deployment 自身的选择器进行识别。
//
// 任何失败情况下都保持空白，而且是故意如此：每个调用者都会询问是否存在特定错误，而“Pods 无法被读取”并非错误
// 迹象。不加以说明，各调用者采用的温和默认设置便得以保留。
func (r *KVCacheBackendReconciler) leaderPods(
	ctx context.Context, kvcb *workercore.KVCacheBackend,
) []core.Pod {
	deploy := new(apps.Deployment)
	err := r.Client.Get(ctx, ctrlcli.ObjectKey{
		Name:      mooncake.LeaderObjectName(kvcb),
		Namespace: kuberess.SystemNamespaceName,
	}, deploy)
	if err != nil || deploy.Spec.Selector == nil {
		return nil
	}

	pods := new(core.PodList)
	err = r.Client.List(ctx, pods,
		ctrlcli.InNamespace(kuberess.SystemNamespaceName),
		ctrlcli.MatchingLabels(deploy.Spec.Selector.MatchLabels))
	if err != nil {
		return nil
	}

	// 标签选择；控制器引用就是证明它的依据。任何调用此操作的人都会将其发现的内容转化为后端的错误状态，
	// 因此，持有这些派生标签的第三方会使得该操作员对处于健康状态的主节点发布故障。
	//
	// 这里的所有权涉及两次跳转，与成员路径的一次跳转不同：一个 Deployment 并不拥有其 Pods，而其
	// ReplicaSets 拥有。因此，首先解析的是 ReplicaSets，使用的是名称而非 UID——该名称来自后端，而
	// Deployment 被删除并重新创建后，该名称仍会被保留。
	owned := r.leaderReplicaSetNames(ctx, deploy)
	if len(owned) == 0 {
		return nil
	}

	live := make([]core.Pod, 0, len(pods.Items))
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp != nil {
			continue
		}
		ref := kubemeta.GetControllerOfNoCopy(pod)
		if ref == nil || ref.Kind != "ReplicaSet" || !owned[ref.Name] {
			continue
		}
		live = append(live, *pod)
	}
	return live
}

// leaderReplicaSetNames 是主节点的 Deployment 所控制的 ReplicaSets 集合，它代表了 Deployment 与承载其故障的
// Pods 之间的连接点。
func (r *KVCacheBackendReconciler) leaderReplicaSetNames(
	ctx context.Context, deploy *apps.Deployment,
) map[string]bool {
	sets := new(apps.ReplicaSetList)
	err := r.Client.List(ctx, sets,
		ctrlcli.InNamespace(deploy.Namespace),
		ctrlcli.MatchingLabels(deploy.Spec.Selector.MatchLabels))
	if err != nil {
		return nil
	}

	names := make(map[string]bool, len(sets.Items))
	for i := range sets.Items {
		ref := kubemeta.GetControllerOfNoCopy(&sets.Items[i])
		if ref == nil || ref.Kind != "Deployment" || ref.Name != deploy.Name {
			continue
		}
		names[sets.Items[i].Name] = true
	}
	return names
}

// observeCapacity 会发布主节点的计数器报告内容，也可能不发布任何内容。
func (r *KVCacheBackendReconciler) observeCapacity(
	ctx context.Context,
	kvcb *workercore.KVCacheBackend,
	holder *workercore.KVCacheBackend,
	client *mooncake.AdminClient,
) *mooncake.LeaderCapacity {
	capacity, err := adminRead(ctx, client.Capacity)
	if err != nil {
		KVCacheBackendConditionCapacityObserved.False(holder, "ScrapeFailed",
			fmt.Sprintf("the leader's metrics could not be read: %v", err))
		return nil
	}

	total, used := selectKVCacheBackendCapacity(kvcb, capacity)
	if total == nil || used == nil {
		KVCacheBackendConditionCapacityObserved.False(holder, "FamilyMissing",
			"the leader's exposition does not carry the capacity families for this backend's medium")
		return &capacity
	}

	holder.Status.Capacity = &workercore.KVCacheBackendCapacity{
		Total: resource.NewQuantity(*total, resource.BinarySI),
		Used:  resource.NewQuantity(*used, resource.BinarySI),
	}
	KVCacheBackendConditionCapacityObserved.True(holder, "Observed",
		"capacity is read from the leader's own counters")
	return &capacity
}

// observeMembers 将 leader 报告的内存段列表投影到 status.members。
//
// 该列表具有权威性：主节点是分配任务的最终决定者，因此即使某个成员 Pod 看起来非常健康，其列表中也不会包含任
// 何内容。该列表无法提供的两个字段——成员运行在哪个节点上，以及贡献了哪种介质——则由地址匹配的成员 Pod 来填充；
// 当没有匹配的 Pod 时，这些字段会保持为空，而非通过猜测来填充。
//
// 它还报告主节点是否负责处理任何列表，因为 PoolWrites 必须向读取操作说明：来自该主节点的读取失败，因为该主
// 节点的版本无法响应。
func (r *KVCacheBackendReconciler) observeMembers(
	ctx context.Context,
	kvcb *workercore.KVCacheBackend,
	holder *workercore.KVCacheBackend,
	client *mooncake.AdminClient,
) ([]mooncake.SegmentDetail, bool) {
	logger := ctrllog.FromContext(ctx)

	segments, err := adminRead(ctx, client.Segments)
	if errors.Is(err, mooncake.ErrRouteNotServed) {
		// 在 0.3.12 之前的 Master 没有提供列表，因此无法询问其挂载了哪些成员。这是未知的，并非读取
		// 失败：等待中不会改变任何情况，而“False”会使得推理服务后端在运行该版本期间一直处于降级状态。
		//
		// 这些行会被删除，与失败读取的情况不同。一个没有任何内容的主节点并不会产生这些行——这些行来
		// 自某个版本中更早的主节点进程——而那些在“成员资格不可观测”这一条件下被标记为可观测的行，则与其矛盾。
		//
		// 仍有一个卡住的成员 Pod 被报告到。无论主节点的版本如何，在 Pod 上都会显示该信息；而对于一
		// 个没有任何元素的群集，则显示“Unknown”以及“Ready”。
		//
		// 因此，容量为零，即没有任何成员被挂载：仪表盘显示已挂载的段数，而列表会将这种状态表示为
		// NoSegments。从状态中读取数据可得到本次遍历的结果：observeLeader 在读取之前会清空字段，而
		// observeCapacity 则首先运行，只有在观察到容量时才会发布数值。如果未观察到容量，则不会显示任何内容。
		holder.Status.Members = nil
		message := "the leader's master version does not serve the segment listing, which Mooncake " +
			"serves from 0.3.12 on, so which members are mounted cannot be observed"
		if faults := r.memberPodFaults(ctx, kvcb); len(faults) > 0 {
			KVCacheBackendConditionMembersMounted.False(holder, faults[0].reason,
				fmt.Sprintf("%s; %d selected member pod(s) will not start; %s",
					message, len(faults), faults[0].detail))
			return nil, false
		}
		if capacity := holder.Status.Capacity; capacity != nil && capacity.Total != nil &&
			capacity.Total.IsZero() {
			KVCacheBackendConditionMembersMounted.False(holder, "NoSegments",
				"the leader reports zero capacity, so no member has mounted a segment; its master "+
					"version does not serve the segment listing that would name them")
			return nil, false
		}
		KVCacheBackendConditionMembersMounted.Unknown(holder, "SegmentListingNotServed", message)
		return nil, false
	}
	if err != nil {
		// 之前的符合架构规范的列表是 KEPT，这与失败的容量抓取行为相反，两者的区别在于类型。容量相关
		// 的是两个指针，因此存在 "absent" 这一状态，意为“未观察到”，而该状态被使用了。列表则没有这
		// 种状态：空列表是一个可识别的值，意为“主节点未列出任何内存段”，因此在这里清除它会发布错误
		// 信息。过时的列表加上 False 条件则能反映实际发生的情况。在当前的必要身份之前生成的列表会被
		// 辅助工具忽略，因为保留它会使此状态更新无效。
		logger.V(2).Info("kv cache backend segment listing unreadable", "error", err.Error())
		setMembersObservationFailed(holder, "ListingFailed",
			fmt.Sprintf("the leader's segment listing could not be read, so membership is as of "+
				"the last successful read: %v", err))
		return nil, true
	}

	if size := segmentListingSize(segments); len(segments) > kvCacheBackendMaxMembers ||
		size > kvCacheBackendMaxMembersBytes {
		// 保留原样而非截断，之前的符合规范列表也因同样的原因被保留：被静默缩短的列表会被理解为丢失
		// 了成员的后端系统。以前符合规范的列表无法在当前规范状态更新时被保留。
		setMembersObservationFailed(holder, "ListingTooLarge",
			fmt.Sprintf("the leader lists %d segment(s) carrying %d byte(s) of identifiers, past "+
				"the %d entries or %d bytes this status can hold; publishing them would push the "+
				"object past the size the api server accepts, and every status write would fail "+
				"from then on — including this one",
				len(segments), size, kvCacheBackendMaxMembers, kvCacheBackendMaxMembersBytes))
		return nil, true
	}

	pods, ready, joinErr := r.listMemberPods(ctx, kvcb)
	if joinErr != nil {
		// 列表已读取；只有连接失败。发布没有节点和媒介的片段比不发布要好。
		logger.Error(joinErr, "list kv cache backend member pods")
	}

	// 多个待部署的 Pods 同时满足的键，以及某个内存段实际包含的键。两者都很重要。满足此类键的内存段无法
	// 追溯到某个 Pod，因此不会将其归给某个 Pod，下面的描述则反映了这种模糊性，而非由此衍生出的数字。其
	// 节点和介质仍被公开：这既不是关于单个 Pod 的事实，所以两者都来自候选者们所认同的内容。
	//
	// 在循环内部收集数据，而非整个索引范围内收集，因为某个内存段未使用的共享键并不构成问题：一个节点上
	// 的两个 TCP 组共享该节点的名称，而主节点通过各自独特的 Pod IP 报告其各个内存段，因此每个内存段都能
	// 被解析。如果以索引而非列表来评判，就会将那个健康的后端系统标记为“降级状态”，并提示操作员拆分那些
	// 从未发生冲突的两个组。
	ambiguous := map[string][]string{}
	ambiguousSegments := 0

	members := make([]workercore.KVCacheBackendMemberStatus, 0, len(segments))
	accounted := make(map[string]struct{}, len(segments))
	for _, segment := range segments {
		member := workercore.KVCacheBackendMemberStatus{
			SegmentID:   segment.ID,
			ClientID:    segment.ClientID,
			SegmentName: segment.Name,
			Protocol:    segment.Protocol,
			State:       mooncake.SegmentState(segment.State),
		}
		host := hostOf(segment.TEEndpoint)
		switch candidates := readyMemberPods(pods[host]); {
		case len(candidates) > 1:
			ambiguous[host] = memberPodNames(candidates)
			ambiguousSegments++
			// 是哪个成员产生了这个内存段，目前尚不可知。它运行的位置及其贡献内容又是什么问题，
			// 两者并不相同：这些候选者共享一个节点，该介质是它们群体的声明，而非关于单个 Pod 的
			// 任何信息。两者都源自候选者所认同的内容，因此没有哪个被另一个名称称为 Pod，这种模
			// 糊性本身也将在下文中说明。
			member.NodeName, member.Medium = agreedMemberFacts(candidates)
		case len(candidates) == 1:
			member.NodeName = candidates[0].nodeName
			member.Medium = candidates[0].medium
			accounted[candidates[0].podName] = struct{}{}
		case len(pods[host]) == 1:
			// 没有内容可以对应这个键，只有一个 Pod 可以。其上下行的成员提供了列表无法提供的两个
			// 字段，这比一个空白行更有价值。它未被计入：缺口导致有可用的 Pods 待在列表中，而这
			// 个不是其中之一。
			member.NodeName = pods[host][0].nodeName
			member.Medium = pods[host][0].medium
		}
		members = append(members, member)
	}

	// 按唯一标识一个内存段的字段进行排序，因此对一份列表进行两次遍历会产生完全相同的字节状态，且
	// DeepEqual 保护机制仍然有效。主节点的排序顺序则无法保证。
	slices.SortFunc(members, func(a, b workercore.KVCacheBackendMemberStatus) int {
		return strings.Compare(a.SegmentID, b.SegmentID)
	})
	holder.Status.Members = members

	// 差异值是一个集合差异，而非计数差异。比较数量即可让过时的内存段替代缺失的成员：一个已准备好的 Pod，
	// 主节点未列出，再加上一个已列出的内存段，其 Pod 已消失，这就是一个对一个的情况——而一个失去了成员的
	// 后端系统会显示“Mounted”。这是以名称而非计数来表示的，因为这条消息正是该条件成为消息而非数字的原因。
	var short []string
	for _, name := range ready {
		if _, ok := accounted[name]; !ok {
			short = append(short, name)
		}
	}

	// 阅读一次，被以下两个分支使用。一个从未完成准备的成员既不在列表中，也不在准备计数中，因此没有这个
	// 的话，它在两个系统中都不可见。
	faults := r.memberPodFaults(ctx, kvcb)

	switch {
	case joinErr != nil:
		// 在出现缺口之前，因为缺口本身就是失败的原因。没有列出 Pods，也就没有什么可以约束主节点的；
		// 差异为空，而若列出内容非空，就会进入 Mounted 状态——根据从未发生的比较结果，将每个成员都视
		// 为已计入。上述部分仍被发布；只是结果尚未公布。
		KVCacheBackendConditionMembersMounted.False(holder, "PodsUnreadable",
			fmt.Sprintf("the leader lists %d segment(s), but this operator's own member pods could "+
				"not be read, so none of them has been accounted for: %v", len(members), joinErr))
	case len(members) == 0:
		// Pod 数量仅在 Pods 属于该操作器自身时才属于有效信息，因为这就是答案中可操作的部分：没有内
		// 存段且没有 Pod 属于布局问题，有内存段且有 Pods 则属于挂载问题。外部后端的成员属于其他人，
		// 因此零数量的描述是针对该操作器而非后端。
		reason, message := "NoSegments", "the leader lists no segment"
		if kvcb.Spec.Connection.Managed != nil {
			message = fmt.Sprintf("%s, with %d member pod(s) running", message, len(ready))
			// 从未准备完成的成员不计入其中，其自身容器才是唯一能说明原因的地方。没有这个说明的
			// 话，消息就会显示“没有内存段，有 0 个成员 Pod 在运行”——确实如此，也毫无用处。
			if len(faults) > 0 {
				reason, message = faults[0].reason, message+"; "+faults[0].detail
			}
		}
		KVCacheBackendConditionMembersMounted.False(holder, reason, message)
	case len(ambiguous) > 0:
		// 在出现缺口之前，因为无法计算该缺口：具有模糊键的 Pods 因构造原因未被计入，因此都会被报告
		// 为缺失，该数字描述的是这个指标，而非集群。
		//
		// 该消息提到了密钥和 Pods，因为操作者的下一步是让它们停止共享它——一个能够将这两组分离在单独
		// 节点上的 nodeSelector——而这一步骤只有在消息中说明了哪些组以及哪个节点时才会清晰可见。
		KVCacheBackendConditionMembersMounted.False(holder, "AmbiguousMemberIdentity",
			fmt.Sprintf("%d of %d segment(s) cannot be traced to a member because %s: "+
				"co-located host-network members advertise the same address, and no pod "+
				"carries the client id that distinguishes their segments. Give the groups node "+
				"selectors that keep them on different nodes to make each member addressable",
				ambiguousSegments, len(members), describeAmbiguousKeys(ambiguous)))
	case len(short) > 0:
		KVCacheBackendConditionMembersMounted.False(holder, "SegmentsShort",
			fmt.Sprintf("the leader lists %d segment(s), and %d of %d ready member pod(s) match "+
				"none of them (%s); a pod the leader does not list holds nothing",
				len(members), len(short), len(ready), listBoundedNames(short)))
	case len(faults) > 0:
		// 各个部分都存在，每个可用的 Pod 也都被计入了统计，因此上述缺口并不存在——然而某个选定的节点
		// 却没有缓存，也永远不会有。在这里报告“可用状态”意味着那些失去了一半成员的群体与那些没有失
		// 去的群体看起来并无差异，而这一阶段的存在就是为了区分两者。
		KVCacheBackendConditionMembersMounted.False(holder, faults[0].reason,
			fmt.Sprintf("the leader lists %d segment(s), and %d selected member pod(s) will not "+
				"start; %s", len(members), len(faults), faults[0].detail))
	default:
		KVCacheBackendConditionMembersMounted.True(holder, "Mounted",
			fmt.Sprintf("the leader lists %d segment(s)", len(members)))
	}
	return segments, true
}

// reportKVCacheBackendPoolWrites 使用当前主节点进程作为观察窗口。Master 计数器会随该进程重置，而协调间隔可
// 能会错过短暂的写入操作。未完成的 PutStart 操作既没有 PutEnd 也没有 PutRevoke，因此仍被标记为未知状态。错
// 误结果意味着在该进程中发生了撤销操作，但并未出现写入结束请求或被占用的内存段；它并不表示所有当前写入操作都失败。
//
// 如果主节点的 Master 版本没有内存段列表，则 listingServed 为 false。其计数器仍然可以反映是否完成过 put 操
// 作，因为后者不需要列表记录；其他判断则用于读取成员的内存分配情况，只有列表记录会包含这些信息。
func reportKVCacheBackendPoolWrites(
	holder *workercore.KVCacheBackend,
	metrics *mooncake.LeaderCapacity,
	segments []mooncake.SegmentDetail,
	listingServed bool,
) {
	if metrics == nil || (listingServed && segments == nil) {
		KVCacheBackendConditionPoolWrites.Unknown(holder, "ReadFailed",
			"the leader's metrics or segment listing could not be read")
		return
	}
	if metrics.PutEndRequests != nil && *metrics.PutEndRequests > 0 {
		KVCacheBackendConditionPoolWrites.True(holder, "WriteObserved",
			"the leader saw a put end request since this process started")
		return
	}
	if !listingServed {
		KVCacheBackendConditionPoolWrites.Unknown(holder, "SegmentListingNotServed",
			"the leader reports no put end since this process started, and its master version "+
				"does not serve the segment listing that members' allocations are read from")
		return
	}
	allReported := true
	for _, segment := range segments {
		if segment.AllocatorUsedBytes == nil {
			allReported = false
			continue
		}
		if *segment.AllocatorUsedBytes > 0 {
			KVCacheBackendConditionPoolWrites.True(holder, "AllocationObserved",
				"a member reports allocated bytes")
			return
		}
	}
	if !allReported {
		KVCacheBackendConditionPoolWrites.Unknown(holder, "AllocationMissing",
			"a member did not report allocator_used_bytes")
		return
	}
	if metrics.PutStartRequests == nil || metrics.PutRevokeRequests == nil {
		KVCacheBackendConditionPoolWrites.Unknown(holder, "CountersMissing",
			"the leader did not report put start and revoke counters")
		return
	}
	if *metrics.PutStartRequests == 0 {
		KVCacheBackendConditionPoolWrites.Unknown(holder, "NoWritesObserved",
			"the leader has seen no put start since this process started")
		return
	}
	if *metrics.PutRevokeRequests > 0 {
		KVCacheBackendConditionPoolWrites.False(holder, "WritesRevoked",
			"the leader saw a put revoke and members report no allocated bytes")
		return
	}
	KVCacheBackendConditionPoolWrites.Unknown(holder, "WriteIncomplete",
		"the leader has seen a put start but no completed or revoked put")
}

// podIsReady 报告 Pod 自身的就绪状态。对于 member 而言，就绪意味着存储客户端已挂载其
// 分段，而不仅仅是容器已启动：就绪探针连接的是入口进程在挂载完成后才会开启服务的端口。
// 若没有该探针，容器一运行该状态就会为 true，使每个 member 在启动期间都会被计为缺口。
func podIsReady(pod *core.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == core.PodReady {
			return cond.Status == core.ConditionTrue
		}
	}
	return false
}

// memberPodFault 记录停止推进的成员 Pod 及其原因。
type memberPodFault struct {
	reason string
	detail string
}

// memberPodFaults 返回所有已停止进展的成员 Pod——即不会启动的容器，或是没有节点可分配的 Pod。它是
// leaderPodContainerFailure 和 leaderPodIsUnschedulable 共同针对领导节点的作用。
//
// 它所划出的那条线，是处于启动状态的 Pod 与卡住的 Pod 之间的界限，而这条线正是它存在的全部原因。正在拉取或
// 等待调度任务的 Pod 并非一种不足：将其计入统计，就能在每次部署过程中为降级状态的后端服务保留足够的资源。但
// 那些容器无法启动的 Pod 永远无法到来；而一个报告为“准备就绪”的后端服务，当选定的节点没有内容时，其实只是在
// 报告一种无法通过等待解决的虚幻状态。
//
// 读取失败时的空状态：无法读取的 Pod 列表并非故障的迹象。
func (r *KVCacheBackendReconciler) memberPodFaults(
	ctx context.Context, kvcb *workercore.KVCacheBackend,
) []memberPodFault {
	managed := kvcb.Spec.Connection.Managed
	if managed == nil {
		return nil
	}

	var faults []memberPodFault
	for group := range managed.Members {
		pods := new(core.PodList)
		err := r.Client.List(ctx, pods,
			ctrlcli.InNamespace(kuberess.SystemNamespaceName),
			ctrlcli.MatchingLabels(mooncake.MemberSelectorLabels(kvcb, group)))
		if err != nil {
			return nil
		}

		for i := range pods.Items {
			pod := &pods.Items[i]
			if pod.DeletionTimestamp != nil || podIsReady(pod) {
				continue
			}
			if !memberPodIsOurs(pod, mooncake.MemberObjectName(kvcb, group)) {
				continue
			}
			if fault, stuck := memberPodStuck(pod); stuck {
				faults = append(faults, fault)
			}
		}
	}

	return faults
}

// memberPodStuck 判断尚未就绪的成员 Pod 是否已经卡住，正常启动中的 Pod 不返回故障。
func memberPodStuck(pod *core.Pod) (memberPodFault, bool) {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == core.PodScheduled && cond.Status == core.ConditionFalse &&
			cond.Reason == core.PodReasonUnschedulable {
			return memberPodFault{
				reason: core.PodReasonUnschedulable,
				detail: fmt.Sprintf("member pod %s has no node to run on: %s",
					pod.Name, clipFaultDetail(cond.Message)),
			}, true
		}
	}

	// 首先处理初始化容器，因为陷入初始化的 Pod 无法与其他容器连接，其状态也保持为空。当没有成员时，这个
	// 循环就不需要了；磁盘层检查是第一个步骤，没有这一步的话，一个无法运行的镜像组就会报告一个未准备就
	// 绪的成员，而没有任何原因——问题出在 Pod 上，在列表中根本没有任何相关记录。
	for _, status := range pod.Status.InitContainerStatuses {
		if t := status.LastTerminationState.Terminated; t != nil && status.RestartCount > 0 {
			return memberPodFault{
				reason: "MemberInitCrashLooping",
				detail: fmt.Sprintf(
					"member pod %s init container %s has restarted %d time(s); it last exited with "+
						"code %d: %s",
					pod.Name, status.Name, status.RestartCount, t.ExitCode,
					clipFaultDetail(t.Message),
				),
			}, true
		}
		if w := status.State.Waiting; w != nil && isTerminalWaitReason(w.Reason) {
			return memberPodFault{
				reason: w.Reason,
				detail: fmt.Sprintf("member pod %s init container %s is not running: %s: %s",
					pod.Name, status.Name, w.Reason, clipFaultDetail(w.Message)),
			}, true
		}
	}

	for _, status := range pod.Status.ContainerStatuses {
		// 最后的终止操作优先执行：一个陷入崩溃循环的容器也在等待，其等待消息是 kubelet 自带的退避模
		// 板，而非某种原因。
		if t := status.LastTerminationState.Terminated; t != nil && status.RestartCount > 0 {
			return memberPodFault{
				reason: "MemberCrashLooping",
				detail: fmt.Sprintf(
					"member pod %s has restarted %d time(s); it last exited with code %d: %s",
					pod.Name, status.RestartCount, t.ExitCode, clipFaultDetail(t.Message),
				),
			}, true
		}
		if w := status.State.Waiting; w != nil && isTerminalWaitReason(w.Reason) {
			return memberPodFault{
				reason: w.Reason,
				detail: fmt.Sprintf("member pod %s is not running: %s: %s",
					pod.Name, w.Reason, clipFaultDetail(w.Message)),
			}, true
		}
	}

	return memberPodFault{}, false
}

// memberPodFacts 指的是成员 Pod 对列表条目所贡献的内容：主节点无法报告的两个字段，因为它们属于 Kubernetes
// 相关信息，并不存储具体事实，此外还有 Pod 自身的名称。
//
// 用 Pod 名称为成员缺口去重，同一个 Pod 可能同时被两个索引键找到。反过来，一个键也可能对应多个 Pod，例如两
// 个成员组选中同一节点，或 RDMA 成员共享该节点的网络地址。因此索引值是列表，Pod 名称本身无需公布。
//
// 当多个处于“READY”状态的 Pod 对某个键作出响应时，该内存段就无法被识别出来。Pod 的归属关系也无法恢复。主节
// 点会为每个成员保留唯一的段 ID 和客户端 ID，但这些 Pods 却不会展示这些信息。同属一个集群的主机-网络成员会
// 使用相同的地址，因此任何列表字段都无法将其中一个独立段映射到某个 Pod 上。
//
// 因此，那里没有提及 Pod。此功能有三个早期版本尝试过——要么将剩余的 Pod 作为来源，要么将整个集合作为来源，又
// 要么通过多重性来分配——每个版本都存在下一个评估中发现的缺陷，因为每个版本都是针对一个输入无法决定输出的问
// 题而做出的近似实现。相反，所报告的只是该身份具有模糊性。
//
// 这两个字段仍然被公开，因为它们都不是关于单个 Pod 的实际情况：候选人位于一个节点上，而介质则是他们群体的声
// 明，因此两者都来自候选人的共识，而非其中任何一个。参见 agreedMemberFacts。
type memberPodFacts struct {
	podName  string
	nodeName string
	medium   string
	// 这里的“ready”指的是就绪状态，而非在“join”旁边进行查找，因为就绪状态决定了 Pod 是否具备成为内存段
	// 的候选资格。一个就绪 Pod 和一个未就绪 Pod 共享一个关键属性，该属性会优先选择最后被记录的那个：内
	// 存段会采用未就绪 Pod 的节点和介质，而就绪成员——也就是唯一可能产生该状态的成员——则会被报告为缺失。
	ready bool
}

// listMemberPods 根据主节点能够报告的名称对此后端成员 Pods 进行索引，从而将列表条目与产生该条目的 Pod 关联起来。
//
// 一个 Pod 会被同时标注其地址和节点名称作为索引，其中地址是会被匹配的那个。主节点会根据成员集合指定的本地主
// 机名来报告内存段的 te_endpoint，而该渲染器则通过向下接口中的 status.podIP 来设置该值——因此，在渲染的后端
// 界面上，列表会显示为“<pod IP>:<port>”。
//
// `node-name`键正是它原本所携带的内容。在变更之前由模板渲染仍在运行的成员会显示“<node>:<port>”，并且会显示
// 出来，而不是默默留空——这正是整个评论存在的意义所在：仅基于一个键进行索引处理会导致每个成员出现空节点和
// medium 情况，而这与真正的“没有 Pod 匹配”情况完全相符。第二个键则需要每个 Pod 对应一次映射写入操作；它所防
// 范的失败情况则会导致无法进行诊断。
//
// 外部后端中的成员属于其他人的 Pods——该操作员既不显示也不选择它们——因此索引为空，每个列表条目都保留空白节点
// 和中等状态。这是当没有 Pod 与之匹配时，通过其他路径访问的受管理条目遵循的同一规则。它返回索引以及可用的
// Pods 的 NAMES，而不是它们的数量。一个 Pod 占用两个键，因此无论是索引的大小还是累计总数都无法说明主节点统
// 计了哪些 Pods——而这正是缺失数据需要解答的问题。
func (r *KVCacheBackendReconciler) listMemberPods(
	ctx context.Context, kvcb *workercore.KVCacheBackend,
) (map[string][]memberPodFacts, []string, error) {
	facts := map[string][]memberPodFacts{}
	var ready []string

	managed := kvcb.Spec.Connection.Managed
	if managed == nil {
		return facts, ready, nil
	}

	for group := range managed.Members {
		pods := new(core.PodList)
		err := r.Client.List(ctx, pods,
			ctrlcli.InNamespace(kuberess.SystemNamespaceName),
			ctrlcli.MatchingLabels(mooncake.MemberSelectorLabels(kvcb, group)))
		if err != nil {
			return facts, ready, err
		}

		for i := range pods.Items {
			pod := &pods.Items[i]
			if pod.DeletionTimestamp != nil {
				continue
			}
			// 携带这些标签的陌生人，可能会被 ITS 节点和地址索引——因此一个内存段可能会被连接到错
			// 误的节点和介质上——并被视为已准备就绪，从而对一份从未打算命名的清单产生误差。
			if !memberPodIsOurs(pod, mooncake.MemberObjectName(kvcb, group)) {
				continue
			}

			// 无论处于何种状态，都会进行索引处理，但仅在准备就绪时才会进行核算。索引能够回答“是
			// 哪个 Pod 产生了这个内存段”，而当一个 Pod 拥有节点后，它也能立即回答这个问题；核算
			// 则能回答“主节点应该列出哪些 Pods”，只有准备就绪的成员才能承担这一责任。如果持有正
			// 在拉取数据、待处理或处于崩溃循环的 Pod，就会产生缺口，并在部署过程中将健康的后端
			// 状态标记为“降级”。
			fact := memberPodFacts{
				podName:  pod.Name,
				nodeName: pod.Spec.NodeName,
				medium:   managed.Members[group].Medium,
				ready:    podIsReady(pod),
			}
			if fact.ready {
				ready = append(ready, pod.Name)
			}

			// 它被标记为“两者都包含”，而一个不包含任何内容的 Pod 也没有挂载任何内容，因此无法匹
			// 配。该 Pod 的 IP 地址是此操作器所渲染的广告中描述的，节点名称则是之前所渲染的节点
			// 名称——无论是外部后端，还是仍运行在旧模板中的成员，都仍然被连接，而非默默留下空白。
			//
			// 通过 indexMemberPod 写入，而非赋值，因为这两个键可能已经被另一个组的 Pod 持有，且
			// 对于当前情况，这两个键也可以是相同的字符串。
			if pod.Spec.NodeName != "" {
				indexMemberPod(facts, pod.Spec.NodeName, fact)
			}
			if pod.Status.PodIP != "" {
				indexMemberPod(facts, pod.Status.PodIP, fact)
			}
		}
	}

	// 排序后，基于密钥的 Pods 构建的消息在各个协调中保持字节级一致：API 服务器的列表顺序并非固定不变，
	// 而条件消息在每次迭代中都会重新排序并重写对象。
	for key := range facts {
		slices.SortFunc(facts[key], func(a, b memberPodFacts) int {
			return strings.Compare(a.podName, b.podName)
		})
	}

	return facts, ready, nil
}

// readyMemberPods 从同一索引键下筛出确实可能持有内存段的 Pod。
//
// 就绪状态决定了是否存在歧义，因为只有处于就绪状态的成员才会被纳入主节点的列表：两个共享关键字的 Pods，其中
// 一个仍在拉取其镜像，这就是滚动更新，而非操作员错误；此时，处于就绪状态的那个成员将成为唯一的候选人，而不
// 是两个中的任何一个。
//
// 这些名称按 listMemberPods 排序，因此由此构建的消息在各个对账过程中保持字节一致，不会改变对象状态。
func readyMemberPods(facts []memberPodFacts) []memberPodFacts {
	ready := make([]memberPodFacts, 0, len(facts))
	for _, fact := range facts {
		if fact.ready {
			ready = append(ready, fact)
		}
	}

	return ready
}

// memberPodNames 是某个键的 Pods 名称，这是条件消息所能包含的内容——事实本身并不说明操作员可以操作什么。
func memberPodNames(facts []memberPodFacts) []string {
	names := make([]string, 0, len(facts))
	for _, fact := range facts {
		names = append(names, fact.podName)
	}

	return names
}

// agreedMemberFacts 是每个关键候选项所携带的节点和媒介，而若两者无法达成一致，则为空字符串。
//
// 协议，而非随机选择。从某个候选人身上读取任一字段，就相当于这个控制器拒绝编造的 Pod 归属关系；当候选人恰好
// 匹配时，读取结果也相同——因此只有在其存在差异时才会区分两者，这也是为何进行比较而非假设的原因。
//
// 该节点之所以能通过比较而保留下来，是出于结构上的原因，而非偶然：当索引键为节点名称时，位于该键后的 Pods
// 就属于同一个节点；因为只有当一个节点上的主机网络 Pods 能够共享该节点的 Pod IP 时，该 IP 才有效。今天该介
// 质保持一致，因为其枚举值只有一个；在枚举值扩展的那一天，比较过程确保了这种一致性——届时可能出现分歧，答案
// 变为 "unknown" 而非“第一个”。
//
// 这两个决定是独立作出的：候选人意见不一的中等程度并不会使他们的节点变得未知。
func agreedMemberFacts(facts []memberPodFacts) (nodeName, medium string) {
	if len(facts) == 0 {
		return "", ""
	}

	nodeName, medium = facts[0].nodeName, facts[0].medium
	for _, fact := range facts[1:] {
		if fact.nodeName != nodeName {
			nodeName = ""
		}
		if fact.medium != medium {
			medium = ""
		}
	}

	return nodeName, medium
}

// describeAmbiguousKeys 将共享操作合并为一条语句，排序后条件消息在多个协调过程中保持完全一致，不会改变对象
// 状态。在两个维度上均受到限制，原因是该消息可能是该控制器生成的最大内容。每个节点有一个模糊键，这正是两组
// 式 RDMA 后端产生的结果，因此语句数量反映了集群的大小，而非此处的其他因素——一千个节点会突破
// `Condition.message` 的 32 KiB 架构限制，所有状态写入操作都会被拒绝，而协调过程会永久重试，从未发布其试图
// 报告的模糊情况。随着故障的扩大而增长的故障报告，正是那种在需要时就会失败的形态。
//
// 因此，外部边界以 BYTES 为单位，而非 clause。Clause 的宽度差异超过一个数量级，因此适用于短节点名称的计数方
// 式会超出长节点名称的限制，这种超出会导致已经出现问题的路径受到影响。
func describeAmbiguousKeys(ambiguous map[string][]string) string {
	clauses := make([]string, 0, len(ambiguous))
	for key, sharing := range ambiguous {
		clauses = append(clauses, fmt.Sprintf("%d ready member pod(s) answer to %q (%s)",
			len(sharing), key, listBoundedNames(sharing)))
	}
	slices.Sort(clauses)

	const separator = "; "

	kept, used := 0, 0
	for _, clause := range clauses {
		next := used + len(clause)
		if kept > 0 {
			next += len(separator)
		}
		if next > kvCacheBackendMaxAmbiguityBytes {
			break
		}
		used, kept = next, kept+1
	}

	if kept == len(clauses) {
		return strings.Join(clauses, separator)
	}

	return fmt.Sprintf("%s; and %d more shared key(s)",
		strings.Join(clauses[:kept], separator), len(clauses)-kept)
}

// indexMemberPod 文件将每个 Pod 放在一个键下，同时保留原有的 Pods。
//
// 这就是原本的简单映射赋值方式，每次两个组在节点上相遇时，就会丢弃一个 Pod：被丢弃的 Pod 会处于未列出状态，
// 并被视为资源不足，需要长期处理。按键存储每个 Pod，使得 join 功能能够询问“有多少 Pod 能够产生这个内存段”，
// 而不是仅仅读取映射中偶然保留的某个 Pod。
//
// 同一 key 下重复创建一个 Pod 实际上并非两个 Pods。当节点以地址作为名称时，其所有 key 都是相同的字符串，这
// 是合法的，在 --hostname-override 设置以及某些托管平台上也会出现这种情况。再次追加该 Pod 会导致其中一个成
// 员被识别为两个，从而造成歧义；选择提前返回而非覆盖操作则能保留之前调用产生的真实冲突记录，而覆盖操作则会抹去这些记录。
func indexMemberPod(facts map[string][]memberPodFacts, key string, fact memberPodFacts) {
	for _, filed := range facts[key] {
		if filed.podName == fact.podName {
			return
		}
	}
	facts[key] = append(facts[key], fact)
}

// hostOf 会从主节点报告的地址中去除端口信息。列表中会显示 `host:port`，而 Pod 则只显示裸地址，因此必须提供其中一项信息。
//
// 按照标准解析器进行分割，而不是以第一个冒号为界，而 IPv6 字面量包含多个冒号：“[2001:db8::1]:15002”会切分为
// “[2001”，且无法匹配任何内容。这是一个实时路径，而非防御性路径：一个成员会公布其 Pod IP，因此在 IPv6 集群
// 中，列表中的每个条目都是带括号的字面量。该索引的填充会无声地失败——每个成员都只有空节点和中等级别，无法与
// 任何 Pod 匹配情况区分开来——因此出错会毫无痕迹。
func hostOf(address string) string {
	if host, _, err := net.SplitHostPort(address); err == nil {
		return host
	}
	// 没有可去除的端口，或者解析器无法接受的形状。无论哪种情况，地址都是其自身的主机：列出该解析器无法
	// 解析的条目仍属于列表项，而连接行为只是遗漏了其他内容，而非匹配其他内容。
	return address
}

// selectKVCacheBackendCapacity 会选取该后端所报告的容量指标。
//
// 主节点保留两对独立的资源——一组用于其内存中存储的片段，另一组用于成员卸载到的磁盘——并且它无条件地对这两部
// 分进行序列化处理。因此，在描述中说明不存在用于后端资源池的零值情况。
//
// 因此问题在于后端是由什么构成的，有两种答案：
//
//   - 没有磁盘层：仅由内存对构成。文件对则为零，虽然将其加入后今天的结果仍相同，但在该运算符生成第二个基于
//     文件的内容的那一天，它才会开始具有意义——这是该对象本身所不具备的。
//   - 磁盘层或外部后端：这两种组合都已被添加。具有层的后端实际上由两者共同构成，而外部后端则没有这种操作方
//     式——它只是说明如何访问后端，而非由何种元素构成——因此，求和也是唯一的准确解读方式。
//
// 过去是按群体的 MEDIUM 来选择的，但当磁盘层成为群体中的一个层级而非独立的介质时，这种方式就不再适用了：现
// 在一个群体同时贡献于两个对，没有任何单一的介质名称能够做到这一点。
func selectKVCacheBackendCapacity(
	kvcb *workercore.KVCacheBackend, capacity mooncake.LeaderCapacity,
) (total, used *int64) {
	managed := kvcb.Spec.Connection.Managed
	if managed == nil {
		return sumCapacityGauges(capacity.TotalBytes, capacity.TotalFileBytes),
			sumCapacityGauges(capacity.AllocatedBytes, capacity.AllocatedFileBytes)
	}

	if len(managed.Members) == 0 {
		return nil, nil
	}

	if kvCacheBackendHasDiskTier(managed) {
		return sumCapacityGauges(capacity.TotalBytes, capacity.TotalFileBytes),
			sumCapacityGauges(capacity.AllocatedBytes, capacity.AllocatedFileBytes)
	}
	return capacity.TotalBytes, capacity.AllocatedBytes
}

// kvCacheBackendHasDiskTier 判断是否有成员组将数据卸载到本地磁盘。
//
// 准入机制最多允许一个组拥有一个层级，这种限制是为了实现该功能而设定的：主节点通过一对计量器来报告每个层级，
// 因此第二个计量器会与第一个混在同一个图表中，无法区分两者。
func kvCacheBackendHasDiskTier(managed *workercore.KVCacheBackendManaged) bool {
	for i := range managed.Members {
		if len(managed.Members[i].LocalDisks) > 0 {
			return true
		}
	}
	return false
}

// sumCapacityGauges 添加两个 gauges，每个都可能未出现在说明中；只有当两者都存在时，该操作才会生效。如果说明
// 中只包含其中一个，则仍会显示其对应的资源池，若称其为“未观察到”，则会导致已读取的数据被忽略。
func sumCapacityGauges(memory, file *int64) *int64 {
	switch {
	case memory == nil:
		return file
	case file == nil:
		return memory
	}

	// 饱和状态而非包裹状态。每个计量器由 Decode 实例单独限制在一个非负的 int64 范围内，但它们的总和不一
	// 定符合该范围——而包裹后的总和则为负数，这被解读为容量，并被作为容量来发布。
	//
	// 外部后端暴露的 /metrics 和管理型磁盘层成员声明的容量上限都取决于后端。两种模式在合计容量时都要做
	// 饱和处理，避免溢出。
	if *file > math.MaxInt64-*memory {
		sum := int64(math.MaxInt64)
		return &sum
	}

	sum := *memory + *file
	return &sum
}

// adminClientFor 构建用于读取本次发布的管理员端接口的读取器，若不存在可读取的数据时则返回 `nil`。它使用控制
// 器自带的 HTTP 客户端，该客户端由 SetupController 进行安装；由构建此控制器的调用者自己提供该客户端，这也是
// 测试能够将测试套件与网络隔离的方式。地址来自 `status` 字段，而非另一种推导方式，因此所读取的内容正是该对
// 象消费者会读取的内容。
func (r *KVCacheBackendReconciler) adminClientFor(
	endpoints []workercore.KVCacheBackendEndpoint,
) *mooncake.AdminClient {
	for _, endpoint := range endpoints {
		if endpoint.Name != workercore.KVCacheBackendEndpointNameAdmin || endpoint.Address == "" {
			continue
		}
		return &mooncake.AdminClient{Address: endpoint.Address, HTTP: r.AdminHTTP}
	}
	return nil
}

// syncLeaderWorkload 使 leader 的 Deployment 和对外 Service 与后端声明一致。
func (r *KVCacheBackendReconciler) syncLeaderWorkload(
	ctx context.Context, kvcb *workercore.KVCacheBackend, image string,
) error {
	logger := ctrllog.FromContext(ctx)

	// GRANTING 在 Deployment 之前执行，因为 Pod 用于标识账户：一个尚未存在的 Pod 被已准入，随后便无法获
	// 得令牌，因此又处于静默待机状态。
	//
	// REVOKING 并未在此，这种不对称性正是关键——可参考 pruneHARBAC，调用者在每次工作负载停止请求访问时都会运行它。
	if err := r.ensureHARBAC(ctx, kvcb); err != nil {
		return err
	}

	eDeploy := mooncake.RenderLeaderDeployment(kvcb, image)
	_, err := kubeclientset.CreateWithCtrlClient(ctx, r.Client, eDeploy,
		kubeclientset.WithUpdateIfExisted(alignLeaderDeploymentFn(kvcb, eDeploy)))
	if err != nil {
		logger.Error(err, "sync kv cache backend leader deployment")
		return err
	}

	eSvc := mooncake.RenderLeaderService(kvcb)
	_, err = kubeclientset.CreateWithCtrlClient(ctx, r.Client, eSvc,
		kubeclientset.WithUpdateIfExisted(alignLeaderServiceFn(kvcb, eSvc)))
	if err != nil {
		logger.Error(err, "sync kv cache backend leader service")
		return err
	}

	logger.V(2).Info("synced kv cache backend leader workload")
	return nil
}

// ensureHARBAC 会创建或实现两个角色所需的访问机制，当 HA 关闭时则不会执行任何操作。
//
// 主节点使用其账户进行选举。成员账户允许使用明确的 Lease 地址；默认的 Service 地址则不会使用其令牌。此处展
// 示而非在 DaemonSets 旁边展示，因为整个后端只使用一个账户。
//
// REQUIRED：“不做任何操作”属于 RETURN 操作，并非传递给 syncHARBAC 的空渲染结果。此操作在工作负载执行之前进
// 行；syncHARBAC 会将不需要的集项视为“要删除”，因此传递此类集项会取消 Lease 的生效，而此时 Pods 仍被配置为
// 使用 Lease，这是 pruneHARBAC 存在的目的，旨在避免这种情况，导致 pruneHARBAC 无任务可执行。
func (r *KVCacheBackendReconciler) ensureHARBAC(
	ctx context.Context, kvcb *workercore.KVCacheBackend,
) error {
	if !mooncake.RenderLeaderRBAC(kvcb).Wanted() {
		return nil
	}

	if err := r.syncHARBAC(ctx, kvcb,
		mooncake.LeaderObjectName(kvcb), mooncake.RenderLeaderRBAC(kvcb)); err != nil {
		return err
	}

	return r.syncHARBAC(ctx, kvcb,
		mooncake.MemberRBACObjectName(kvcb), mooncake.RenderMemberRBAC(kvcb))
}

// pruneHARBAC 在 Kubernetes 选举被关闭时会再次移除访问权限，并且在每次没有该访问权限的情况下重新渲染完所有
// 工作负载之后执行。
//
// REQUIRED：排序顺序与 ensureHARBAC 相反，这种非对称性并非出于自身目的。先撤销权限会使主节点失去 Lease 的授
// 权，而该主节点仍被配置为可以续租，且成员也仍被配置为可以读取该数据，因此后续工作负载更新失败时，后端将没
// 有 Master 节点，直到后续操作成功。最后撤销权限则意味着最坏的情况是权限的有效期超过了一个 reconcile 周期。
//
// 删除后端时，所有者引用会使这些对象随之回收。把 electionBackend 改成 None 时后端仍然存在，须显式撤销仍能操
// 作 Lease 的服务账号及其授权。
func (r *KVCacheBackendReconciler) pruneHARBAC(
	ctx context.Context, kvcb *workercore.KVCacheBackend,
) error {
	if mooncake.RenderLeaderRBAC(kvcb).Wanted() {
		return nil
	}

	if err := r.syncHARBAC(ctx, kvcb, mooncake.LeaderObjectName(kvcb), mooncake.HARBAC{}); err != nil {
		return err
	}

	return r.syncHARBAC(ctx, kvcb, mooncake.MemberRBACObjectName(kvcb), mooncake.HARBAC{})
}

// alignOwnedIdentity 会将活对象身份与渲染后的身份对齐，并报告是否有任何内容发生移动：被其识别的标签、资源清
// 理路径中记录的说明，以及回收其的拥有者引用。
//
// 要求：它拒绝接受该操作器未创建的、由渲染名称所标识的对象，而不是将其接管。这三个对象是根据名称在操作器与
// 其渲染的其他内容共享的命名空间中的，因此可能会发生冲突——而占取其中一个对象并非可逆操作：该注释使得销毁路
// 径愿意移除它，而所有者引用则会在 BACKEND 被删除时将其交给垃圾回收。此时，第三方的账户将被无关对象的生命周
// 期所销毁。拒绝操作会破坏 BACKEND 的选举权，直到有人重新命名该对象，这一过程需要明确说明、有边界且可撤销。
//
// 与本文件的其他清理逻辑一样，仅用资源归属注解确认所有者。renderedForKVCacheBackend 说明了不能单凭标签或控
// 制器引用判断的原因。
//
// 标签会被放回原处，而不是被替换，因此手动添加到该控制器上的键在更新后仍然保留。如果整体替换地图，就会丢失
// 其他内容，包括另一个控制器能找到的键。
func alignOwnedIdentity(
	actual, expected kubemeta.MetaObject, kvcb *workercore.KVCacheBackend,
) (bool, error) {
	if !renderedForKVCacheBackend(actual, kvcb) {
		return false, fmt.Errorf(
			"%T %q already exists and carries no note of this backend: rename or remove it, "+
				"this operator will not take it over", actual, actual.GetName(),
		)
	}

	changed := restoreRenderedLabels(actual, expected)
	if !systemmeta.EqualResourceTypeAndNotes(expected, actual) {
		systemmeta.SyncResourceTypeAndNotes(expected, actual)
		changed = true
	}
	if !kubemeta.IsControlledBy(actual, kvcb) {
		kubemeta.ControlOnWithoutBlock(actual, kvcb,
			workercore.SchemeGroupVersion.WithKind("KVCacheBackend"))
		changed = true
	}

	return changed, nil
}

// syncHARBAC 会合并或删除某个角色的三个对象。
func (r *KVCacheBackendReconciler) syncHARBAC(
	ctx context.Context, kvcb *workercore.KVCacheBackend, name string, wanted mooncake.HARBAC,
) error {
	logger := ctrllog.FromContext(ctx)
	key := ctrlcli.ObjectKey{Namespace: kuberess.SystemNamespaceName, Name: name}

	if !wanted.Wanted() {
		// 先绑定，再绑定角色，最后绑定账户：按照授予它们的顺序相反进行，因此没有任何窗口会留下指向
		// 已消失角色的绑定。
		for _, obj := range []ctrlcli.Object{
			&rbac.RoleBinding{}, &rbac.Role{}, &core.ServiceAccount{},
		} {
			if _, err := r.deleteOwnedWorkload(ctx, kvcb, obj, key); err != nil {
				logger.Error(err, "delete kv cache backend leader rbac", "object", fmt.Sprintf("%T", obj))
				return err
			}
		}
		return nil
	}

	eSA := wanted.ServiceAccount
	_, err := kubeclientset.CreateWithCtrlClient(ctx, r.Client, eSA,
		kubeclientset.WithUpdateIfExisted(
			func(aSA *core.ServiceAccount) (*core.ServiceAccount, bool, error) {
				// 该账户没有可收敛的规格值；它拥有的只有身份标识。名为此名的账户，若早于后
				// 端系统，则会被拒绝而非被认领——参见 alignOwnedIdentity。
				changed, err := alignOwnedIdentity(aSA, eSA, kvcb)
				return aSA, !changed, err
			},
		))
	if err != nil {
		logger.Error(err, "sync kv cache backend ha service account")
		return err
	}

	eRole := wanted.Role
	_, err = kubeclientset.CreateWithCtrlClient(ctx, r.Client, eRole,
		kubeclientset.WithUpdateIfExisted(func(aRole *rbac.Role) (*rbac.Role, bool, error) {
			changed, err := alignOwnedIdentity(aRole, eRole, kvcb)
			if err != nil {
				return aRole, false, err
			}
			// 整个规则集被替换而非合并。在授予关系中，有一种情况在任一方向上都不允许进行人工编
			// 辑：如果扩大范围，那是一种无人审查的特权；如果缩小范围，则只是默默等待。
			if !kubemeta.DeepEqual(aRole.Rules, eRole.Rules) {
				aRole.Rules = eRole.Rules
				changed = true
			}
			return aRole, !changed, nil
		}))
	if err != nil {
		logger.Error(err, "sync kv cache backend ha role")
		return err
	}

	// 要求：在创建或更新之前，会先检查绑定项是否存在 roleRef 不匹配的情况，然后会重新创建，而非进行补丁处理。
	//
	// roleRef 是不可变的，因此包含不同值的更新会被 API 服务器拒绝——但从未对其进行更新的更新则被接受，而
	// 这正是危险的情况：例如，原本指向 ClusterRole/cluster-admin 的绑定关系，在操作员将其主题改为存储中
	// 的账户时，仍会保持指向该地址。最终结果就是：一个由成功报告过的协调操作产生的、绑定在无人授予的角
	// 色上的第三方二进制文件。
	//
	// 要求：仅会重新创建携带此后端标记的绑定。同样规则 alignOwnedIdentity 适用于所有三个对象，此处再次
	// 验证是因为此删除操作在创建或更新操作之前进行——否则，一个外部绑定会在被关联对象被处理之前被销毁。
	eBinding := wanted.RoleBinding
	aBinding := new(rbac.RoleBinding)
	switch err = r.Client.Get(ctx, key, aBinding); {
	case err == nil:
		if !kubemeta.DeepEqual(aBinding.RoleRef, eBinding.RoleRef) {
			if !renderedForKVCacheBackend(aBinding, kvcb) {
				return fmt.Errorf(
					"role binding %q already exists, bound to %s/%s and carrying no note of this "+
						"backend: rename or remove it, this operator will not delete it",
					name, aBinding.RoleRef.Kind, aBinding.RoleRef.Name,
				)
			}
			logger.Info("recreating a role binding whose roleRef does not match",
				"name", name, "actual", aBinding.RoleRef.Name, "expected", eBinding.RoleRef.Name)
			if err = r.Client.Delete(ctx, aBinding); err != nil && !kerrors.IsNotFound(err) {
				logger.Error(err, "delete kv cache backend ha role binding")
				return err
			}
		}
	case !kerrors.IsNotFound(err):
		logger.Error(err, "get kv cache backend ha role binding")
		return err
	}

	_, err = kubeclientset.CreateWithCtrlClient(ctx, r.Client, eBinding,
		kubeclientset.WithUpdateIfExisted(
			func(aBinding *rbac.RoleBinding) (*rbac.RoleBinding, bool, error) {
				changed, err := alignOwnedIdentity(aBinding, eBinding, kvcb)
				if err != nil {
					return aBinding, false, err
				}
				if !kubemeta.DeepEqual(aBinding.Subjects, eBinding.Subjects) {
					aBinding.Subjects = eBinding.Subjects
					changed = true
				}
				return aBinding, !changed, nil
			},
		))
	if err != nil {
		logger.Error(err, "sync kv cache backend ha role binding")
		return err
	}

	return nil
}

// resolveKVCacheBackendImage 会选择后端运行的图像：即对象名称所代表的图像，否则则使用集群范围内的设置。
//
// 准入机制会拒绝那些两个字段都为空的对象，但某个设置可以在对象被准入后被清空，且这一操作会在每次同步时执行。
// 因此，此处将此情况视为错误而非被忽略：渲染空图像会导致 API 服务器拒绝 Deployment 操作，同时会显示消息指向
// 容器，而非被清空设置的那个对象。
func resolveKVCacheBackendImage(ctx context.Context, kvcb *workercore.KVCacheBackend) (string, error) {
	if image := strings.TrimSpace(kvcb.Spec.Image); image != "" {
		return image, nil
	}
	if image := strings.TrimSpace(settings.KVCacheBackendImage.ShouldValue(ctx)); image != "" {
		return image, nil
	}
	return "", fmt.Errorf("no image: neither spec.image nor the %q setting names one",
		settings.KVCacheBackendImage.Name())
}

// leaderCountRiseMustWait 报告了将活跃主节点 Deployment 提升超过一个副本后，是否必须等待选中的模板先进行部署。
//
// 它会等待实时 Deployment 输出三个信息：其模板被选中，Deployment 控制器已观察到生成该模板的代次，且每个副本
// 在没有任何先前模板的情况下运行该模板。这三项信息均来自实时对象，因为渲染器是无状态的，该对象是运行中的
// Pods 来自哪个模板的唯一记录。
//
// REQUIRED：需要的是实际观察到的代次，而非单纯的计数。在模板编写完成后，这些计数仍反映了滚动更新前的状态——
// 一个未经过选的模板上的副本，已更新且总计——将其视为此次滚动更新的状态，正是存在此写入门控机制的目的。
//
// 不会等待一直降到 1。那里的缩放事件会移除多余的副本；如果也禁用选举，Recreate 会在未当选的进程开始之前停止
// 最后当选的进程。
func leaderCountRiseMustWait(aDeploy, rendered *apps.Deployment) bool {
	replicas := ptr.Deref(aDeploy.Spec.Replicas, 1)
	if ptr.Deref(rendered.Spec.Replicas, 1) <= 1 || replicas > 1 {
		return false
	}

	rolledOut := aDeploy.Status.ObservedGeneration >= aDeploy.Generation &&
		aDeploy.Status.UpdatedReplicas == replicas &&
		aDeploy.Status.Replicas == replicas
	return !mooncake.LeaderTemplateElects(aDeploy.Spec.Template) || !rolledOut
}

// alignLeaderDeploymentFn 会将正在运行的主节点 Deployment 合并到已渲染的节点上。
//
// 只比较本控制器负责生成的字段。API Server 会补上 terminationMessagePath、imagePullPolicy、dnsPolicy、
// schedulerName 等默认值，若对整个 Pod 模板做 DeepEqual，每轮都会误判为变化、重写 Deployment 并滚动重启 leader。
//
// spec.selector 永远不会被修改：它是不可变的，任何试图进行更新的操作都会被直接拒绝，导致对象会一直处于锁定
// 状态，直到有人手动删除它。
func alignLeaderDeploymentFn(
	kvcb *workercore.KVCacheBackend, rendered *apps.Deployment,
) func(*apps.Deployment) (*apps.Deployment, bool, error) {
	return func(aDeploy *apps.Deployment) (*apps.Deployment, bool, error) {
		skip := true

		// 现有 Deployment 从未启用选举的单副本升至多副本时分两次更新。第一次只让模板启用选举，副本
		// 数、更新策略和期限仍按单副本处理，避免新旧 master 同时对外服务。先检查线上对象，再修改 aDeploy。
		eDeploy := rendered
		if leaderCountRiseMustWait(aDeploy, rendered) {
			eDeploy = mooncake.LeaderDeploymentAtOneReplica(rendered)
		}

		if !kubemeta.DeepEqual(aDeploy.Spec.Replicas, eDeploy.Spec.Replicas) {
			aDeploy.Spec.Replicas = eDeploy.Spec.Replicas
			skip = false
		}

		// 该策略的收敛原因与副本数量的原因相同：两者都规定了同时可能存在多少主节点，对其中任何一方
		// 的修改都会导致分裂或停滞。
		//
		// 比较完整的更新策略。单副本的 Recreate 没有额外字段，但从三副本升到五副本时还要更新
		// maxUnavailable。沿用较小的旧值会让只有一个就绪副本的工作负载卡住。
		if !kubemeta.DeepEqual(aDeploy.Spec.Strategy, eDeploy.Spec.Strategy) {
			aDeploy.Spec.Strategy = eDeploy.Spec.Strategy
			skip = false
		}

		// 截止日期随策略一同变化，原因也相同。渲染器在待处理状态时会禁用它，因为
		// `DeploymentComplete`要求所有可用的副本都可用，而在这里这一要求始终不成立——因此，服务器默
		// 认设定的截止日期最终会针对正在运行的工作负载报告`ProgressDeadlineExceeded`。
		//
		// REQUIRED：将 nil 视为“API 服务器默认设置”，而非“清除它”。渲染器在单个副本情况下会保持此状
		// 态不变，而 API 服务器会写入 600——直接比较两者会导致每次迭代都有差异，需要重写该字段，并永
		// 久保持主节点状态。因此，只有当实时值被该渲染器自身写入的禁用哨兵时，nil 才会被恢复，这是
		// 服务器默认设置无法自行恢复的唯一情况。
		switch {
		case eDeploy.Spec.ProgressDeadlineSeconds != nil:
			if !kubemeta.DeepEqual(aDeploy.Spec.ProgressDeadlineSeconds,
				eDeploy.Spec.ProgressDeadlineSeconds) {
				aDeploy.Spec.ProgressDeadlineSeconds = eDeploy.Spec.ProgressDeadlineSeconds
				skip = false
			}
		case aDeploy.Spec.ProgressDeadlineSeconds != nil &&
			*aDeploy.Spec.ProgressDeadlineSeconds == math.MaxInt32:
			aDeploy.Spec.ProgressDeadlineSeconds = nil
			skip = false
		}

		// 即使渲染器始终保持为 false 状态，也依然会收敛，因为这正是使其值得收敛的原因：hostNetwork
		// 是主节点被设计为不应拥有的权限，而 API 服务器也不会将其设为可混淆的默认状态。如果开启这一
		// 权限的编辑操作会永久保留，因为这里没有其他内容会查看它——这是手动授予并通过忽略来保持的权限。
		if aDeploy.Spec.Template.Spec.HostNetwork != eDeploy.Spec.Template.Spec.HostNetwork {
			aDeploy.Spec.Template.Spec.HostNetwork = eDeploy.Spec.Template.Spec.HostNetwork
			skip = false
		}
		// 相同的形状，它负责将现有的工作负载转移到更安全的状态：在设置此字段之前渲染的主节点会持有
		// 挂载的服务账户令牌，直到有人查看它为止。
		if !kubemeta.DeepEqual(aDeploy.Spec.Template.Spec.AutomountServiceAccountToken,
			eDeploy.Spec.Template.Spec.AutomountServiceAccountToken) {
			aDeploy.Spec.Template.Spec.AutomountServiceAccountToken = eDeploy.Spec.Template.Spec.AutomountServiceAccountToken
			skip = false
		}
		// REQUIRED：与上面的挂载组件在双向方向上协同工作。它们是一个设置，由两个字段表示，每个单独
		// 的部分都是静默的——没有令牌的账户会认证为“无用户”，而用于无名称账户的令牌则属于默认账户。
		// 两者都不会记录任何内容；主节点只会不断重试其选举过程。这也是那个承载了后端服务的字段，该
		// 后端在 Deployment 操作存在后已启用高可用性，而仅有一个渲染器时则无法实现此状态。
		//
		// 已废弃的别名仍被保留。API 服务器将别名存储为名称，并从该名称中填充空名称，因此仅清空名称
		// 即可恢复旧账户——而 HA OFF 刚刚删除了该账户，导致所有新的 Pod 都无法使用。
		if aDeploy.Spec.Template.Spec.ServiceAccountName !=
			eDeploy.Spec.Template.Spec.ServiceAccountName {
			aDeploy.Spec.Template.Spec.ServiceAccountName = eDeploy.Spec.Template.Spec.ServiceAccountName
			aDeploy.Spec.Template.Spec.DeprecatedServiceAccount = eDeploy.Spec.Template.Spec.ServiceAccountName
			skip = false
		}

		// 凭证问题，因此删除这些凭证的修改必须被撤销，而不是被容忍。与拉取策略不同，API 服务器在此
		// 处不会默认任何内容，因此明确的“无期望”意味着“无机密信息”，比较起来是安全的。
		if !kubemeta.DeepEqual(
			aDeploy.Spec.Template.Spec.ImagePullSecrets, eDeploy.Spec.Template.Spec.ImagePullSecrets,
		) {
			aDeploy.Spec.Template.Spec.ImagePullSecrets = eDeploy.Spec.Template.Spec.ImagePullSecrets
			skip = false
		}

		// 因为下方的容器通过步骤会移动 VolumeMounts，所以出现了冲突。一个在通过该步骤传递挂载配置但
		// 没有指定对应卷的通过步骤，会向 API 服务器提供一个拒绝的 Pod 模板——
		// `volumeMounts[0].name: Not found`——并且之后每次通过该步骤都会继续拒绝。比失败本身更糟糕的
		// 是其呈现方式：Deployment 仍保持其原有的代次，因此该对象会持续报告为“准备就绪”，其状态也保
		// 持为“真”，而 Master 在未被编辑时启用的标志下正常运行。多租户功能正是这种编辑行为，也是资
		// 源池 webhook 通知给操作员需要执行的任务。
		//
		// 会对整个切片进行比较，当开关重新关闭时，这些量也会再次被移除。这仅是安全的，因为渲染器会
		// 明确列出 API 服务器原本会默认使用的字段——参见 quotaPolicyVolumes。
		if !kubemeta.DeepEqual(aDeploy.Spec.Template.Spec.Volumes, eDeploy.Spec.Template.Spec.Volumes) {
			aDeploy.Spec.Template.Spec.Volumes = eDeploy.Spec.Template.Spec.Volumes
			skip = false
		}

		// 初始化容器会逐字段进行，使用与容器相同的功能，也是出于同样的原因：服务器在容器内默认包含
		// 字段，因此比较整个对象会导致每次操作都是写入操作。
		if len(aDeploy.Spec.Template.Spec.InitContainers) != len(eDeploy.Spec.Template.Spec.InitContainers) {
			aDeploy.Spec.Template.Spec.InitContainers = eDeploy.Spec.Template.Spec.InitContainers
			skip = false
		} else {
			for i := range eDeploy.Spec.Template.Spec.InitContainers {
				if alignRenderedContainer(
					&aDeploy.Spec.Template.Spec.InitContainers[i],
					eDeploy.Spec.Template.Spec.InitContainers[i],
				) {
					skip = false
				}
			}
		}

		if len(aDeploy.Spec.Template.Spec.Containers) != len(eDeploy.Spec.Template.Spec.Containers) {
			aDeploy.Spec.Template.Spec.Containers = eDeploy.Spec.Template.Spec.Containers
			skip = false
		} else {
			for i := range eDeploy.Spec.Template.Spec.Containers {
				if alignRenderedContainer(
					&aDeploy.Spec.Template.Spec.Containers[i],
					eDeploy.Spec.Template.Spec.Containers[i],
				) {
					skip = false
				}
			}
		}

		if !kubemeta.DeepEqual(aDeploy.Spec.Template.Labels, eDeploy.Spec.Template.Labels) {
			aDeploy.Spec.Template.Labels = eDeploy.Spec.Template.Labels
			skip = false
		}

		if restoreRenderedLabels(aDeploy, eDeploy) {
			skip = false
		}

		if !systemmeta.EqualResourceTypeAndNotes(eDeploy, aDeploy) {
			systemmeta.SyncResourceTypeAndNotes(eDeploy, aDeploy)
			skip = false
		}

		if !kubemeta.IsControlledBy(aDeploy, kvcb) {
			kubemeta.ControlOnWithoutBlock(aDeploy, kvcb,
				workercore.SchemeGroupVersion.WithKind("KVCacheBackend"))
			skip = false
		}

		return aDeploy, skip, nil
	}
}

// alignRenderedContainer 将渲染出的字段复制到运行中的容器对象，并返回是否发生变更。
// 此处逐一显式列出字段，因为未列出的字段由 API Server 填充默认值，若覆盖它们会导致
// 每次调谐都产生写入。
//
// 该函数同时服务于 leader 和 member。仅其中一方使用的字段在另一方上比较时自然相等，
// 将字段列表集中在一处维护，可以防止渲染器新增字段后只渲染却永不收敛的问题。
func alignRenderedContainer(actual *core.Container, expected core.Container) (changed bool) {
	if actual.Image != expected.Image {
		actual.Image = expected.Image
		changed = true
	}
	// 无条件比对，因为渲染器会直接解析出策略而非将其留空。此前的问题恰恰出在期望值为空：
	// API Server 会在写入时填充该字段，导致拿空值与默认值比对时工作负载不断滚动更新；
	// 而为规避该问题跳过比对又会遗留过期策略——例如已从 spec 中移除的值，或基于 spec 已
	// 不再使用的镜像标签推导出的值。如今该规则统一由 kvcache.EffectivePullPolicy 负责。
	if actual.ImagePullPolicy != expected.ImagePullPolicy {
		actual.ImagePullPolicy = expected.ImagePullPolicy
		changed = true
	}
	// 与拉取策略不同，此处可安全比对：渲染器会指定非空值，而 API Server 仅在字段为空时
	// 填充默认值。
	if actual.TerminationMessagePolicy != expected.TerminationMessagePolicy {
		actual.TerminationMessagePolicy = expected.TerminationMessagePolicy
		changed = true
	}
	if !kubemeta.DeepEqual(actual.Command, expected.Command) {
		actual.Command = expected.Command
		changed = true
	}
	if !kubemeta.DeepEqual(actual.Args, expected.Args) {
		actual.Args = expected.Args
		changed = true
	}
	if !kubemeta.DeepEqual(actual.Env, expected.Env) {
		actual.Env = expected.Env
		changed = true
	}
	if !kubemeta.DeepEqual(actual.Ports, expected.Ports) {
		actual.Ports = expected.Ports
		changed = true
	}
	if !kubemeta.DeepEqual(actual.ReadinessProbe, expected.ReadinessProbe) {
		actual.ReadinessProbe = expected.ReadinessProbe
		changed = true
	}
	if !kubemeta.DeepEqual(actual.LivenessProbe, expected.LivenessProbe) {
		actual.LivenessProbe = expected.LivenessProbe
		changed = true
	}
	// 比对整个字段值，而非仅比对 Requests。渲染器完全不设置 limits，即将其保持为零值——而
	// 仅比对渲染器所填字段恰恰会让外部注入的 limit 残留下来：过低的内存 limit 会导致容器
	// 陷入 OOM 崩溃循环，且本对象中没有任何信息能解释原因。后端自身的声明在设计上就是
	// REQUEST，因为 member 会依据同一数值计算其分段大小，低于该值的 limit 会杀死本可正常
	// 容下的 member。
	if !kubemeta.DeepEqual(actual.Resources, expected.Resources) {
		actual.Resources = expected.Resources
		changed = true
	}
	if !kubemeta.DeepEqual(actual.VolumeMounts, expected.VolumeMounts) {
		actual.VolumeMounts = expected.VolumeMounts
		changed = true
	}
	// 安全上下文按整体比对，而非逐个 capability 比对：它是作为整体渲染的，运行中的容器若
	// 通过带外方式获得了额外配置，必须予以清除。
	if !kubemeta.DeepEqual(actual.SecurityContext, expected.SecurityContext) {
		actual.SecurityContext = expected.SecurityContext
		changed = true
	}
	// 停机钩子的 argv 内部携带了缩容宽限时间，且该宽限时间可编辑。若将其排除在比对之外，
	// 该钩子只会被写入一次而不再更新：Pod 指纹会随变更移动，从而触发 member 被删除重建——
	// 但所使用的模板中钩子仍保留旧的宽限时间。其结果比不收敛更糟，因为其余一切都表现为
	// 变更已经生效。
	if !kubemeta.DeepEqual(actual.Lifecycle, expected.Lifecycle) {
		actual.Lifecycle = expected.Lifecycle
		changed = true
	}
	return changed
}

// syncMemberWorkloads 为每个成员组维护一个对应的 DaemonSet。
//
// 准入机制在此范围内仅允许一个组，但循环仍然写在列表上：索引用于标识对象，因此稍后添加的第二个组将成为一个
// 新的 DaemonSet，而非对第一个的重新编写。
func (r *KVCacheBackendReconciler) syncMemberWorkloads(
	ctx context.Context, kvcb *workercore.KVCacheBackend, image string,
) error {
	logger := ctrllog.FromContext(ctx)

	for group := range kvcb.Spec.Connection.Managed.Members {
		eDs := mooncake.RenderMemberDaemonSet(kvcb, group, image)
		// 是 LIVE 对象，而非渲染后的对象：它是承载 UID 的载体，下面的重启操作证明，一个 Pod 属于该
		// DaemonSet，其 UID 是由该 UID 决定的，而非由选择器决定的。
		aDs, err := kubeclientset.CreateWithCtrlClient(ctx, r.Client, eDs,
			kubeclientset.WithUpdateIfExisted(alignMemberDaemonSetFn(kvcb, eDs)))
		if err != nil {
			logger.Error(err, "sync kv cache backend member daemonset", "group", group)
			return err
		}

		if err = r.restartOutdatedMemberPods(ctx, aDs); err != nil {
			logger.Error(err, "restart outdated kv cache backend member pods", "group", group)
			return err
		}
	}

	if err := r.pruneRemovedMemberWorkloads(ctx, kvcb); err != nil {
		logger.Error(err, "prune removed kv cache backend member workloads")
		return err
	}

	logger.V(2).Info("synced kv cache backend member workloads",
		"groups", len(kvcb.Spec.Connection.Managed.Members))
	return nil
}

// pruneRemovedMemberWorkloads 会删除规范中不再存在的成员组的 DaemonSets。
//
// 上面的循环会遍历当前组，因此单独使用时，它只能创建和更新：从列表中移除的组会留下一个 DaemonSet 空地址，其
// 成员仍会处理后端不再声明的片段。在仅允许一个组时，该地址无法访问——移除唯一的组意味着删除该对象，属于清理
// 过程；而一旦有多个组存在，该地址便可以访问了。
//
// 它会在同步之后运行，而不是在之前运行，因此那种既能添加又能移除组的规范，永远不会出现成员数量瞬间减少的情况。
//
// 所有权通过拆解过程得以验证：将其标注在资源类型标签上，然后仅保留包含该后端标识的部分。各组的名称源自后端
// 的名称，因此与无关对象也可以拥有该名称，仅删除名称则会导致其他对象也受到影响。
func (r *KVCacheBackendReconciler) pruneRemovedMemberWorkloads(
	ctx context.Context, kvcb *workercore.KVCacheBackend,
) error {
	logger := ctrllog.FromContext(ctx)

	expected := make(map[string]struct{}, len(kvcb.Spec.Connection.Managed.Members))
	for group := range kvcb.Spec.Connection.Managed.Members {
		expected[mooncake.MemberObjectName(kvcb, group)] = struct{}{}
	}

	dss := new(apps.DaemonSetList)
	err := r.Client.List(ctx, dss,
		ctrlcli.InNamespace(kuberess.SystemNamespaceName),
		ctrlcli.MatchingLabels(
			systemmeta.GetResourcesLabelSetOfType[map[string]string](kvcache.ResourceType),
		))
	if err != nil {
		return err
	}

	for i := range dss.Items {
		ds := &dss.Items[i]
		if !renderedForKVCacheBackend(ds, kvcb) {
			continue
		}
		if _, ok := expected[ds.Name]; ok {
			continue
		}
		if ds.DeletionTimestamp != nil {
			continue
		}
		// 前景部分，因此 Pods 在 DaemonSet 之前就消失了：它们拥有离开的组所占有的节点内存，如果先释
		// 放该对象，就会让它们无计可施地继续运行。
		if err = r.Client.Delete(ctx, ds,
			ctrlcli.PropagationPolicy(meta.DeletePropagationForeground)); err != nil &&
			!kerrors.IsNotFound(err) {
			return err
		}
		logger.V(1).Info("deleted the daemonset of a member group the spec no longer declares",
			"daemonset", ds.Name)
	}

	return nil
}

// restartOutdatedMemberPods 会删除由模板构建的 Pods 成员，而该模板已发生重要的变更。
//
// 这是 OnDelete 策略中的操作器部分，两部分必须协同工作才能生效。DaemonSet 被设置为 OnDelete，因为其默认行为
// 是每当节点选择器发生变化时，就会重新加载所有成员——而扩大组范围正是添加成员的方式，也正好符合这一行为。但
// 仅使用 OnDelete 会导致已修改的图像被保留而不被应用，因此在此处根据指纹判断做出重启决策，该指纹涵盖了整个
// 模板，除了节点选择器部分。
//
// Pods 被删除而非补丁更新：DaemonSet 会根据当前模板重新创建每个 Pod，这是 Pod 规范发生变化的唯一方式。
// memberPodIsOurs 用于指示与成员选择器匹配的 Pod 是否实际属于该 DaemonSet。仅使用标签进行选择——选择器是一种
// 查询方式，任何携带这三个身份标签的 Pod 都会与之匹配，无论是由谁构建的。每次使用成员 Pod（删除它、将其计入
// 主节点的计数、读取其中的故障信息）都会发布关于此后端的决策，因此每个 Pod 都需要控制器引用作为证明，而不是依赖标签。
//
// 通过类型和名称而非 UID 进行比较：该名称源自后端，因此当 DaemonSet 被删除并重新创建时，其 Pods 会处于孤立
// 状态，在模板变更生效的期间会被重新采用。通过 UID 比较则无法反映这种重新采用的过程——这正是模板变更生效的窗
// 口期。没有 Pod 能够携带指向该名称 DaemonSet 的控制器引用而不属于它。
func memberPodIsOurs(pod *core.Pod, daemonSetName string) bool {
	ref := kubemeta.GetControllerOfNoCopy(pod)
	return ref != nil && ref.Kind == "DaemonSet" && ref.Name == daemonSetName
}

func (r *KVCacheBackendReconciler) restartOutdatedMemberPods(
	ctx context.Context, ds *apps.DaemonSet,
) error {
	logger := ctrllog.FromContext(ctx)

	want := ds.Spec.Template.Annotations[mooncake.MemberPodSpecHashAnnotation]
	if want == "" {
		// 没有可比较的对象。删除所有具有未知指纹的 Pod，会导致渲染错误，进而引发整个集群的重启，因
		// 此这样做毫无效果。
		return nil
	}

	pods := new(core.PodList)
	err := r.Client.List(ctx, pods,
		ctrlcli.InNamespace(ds.Namespace),
		ctrlcli.MatchingLabels(ds.Spec.Selector.MatchLabels))
	if err != nil {
		return err
	}

	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp != nil {
			continue
		}
		if pod.Annotations[mooncake.MemberPodSpecHashAnnotation] == want {
			continue
		}
		if !memberPodIsOurs(pod, ds.Name) {
			logger.V(2).Info("skipping a pod that matches the selector and is not this daemonset's",
				"pod", pod.Name)
			continue
		}

		logger.V(2).Info("restarting a member pod whose spec changed",
			"pod", pod.Name, "node", pod.Spec.NodeName)
		if err = r.Client.Delete(ctx, pod); err != nil && !kerrors.IsNotFound(err) {
			return err
		}
	}

	return nil
}

// alignMemberDaemonSetFn 将正在运行的成员 DaemonSet 对齐到渲染后的状态。
//
// 其对齐方式与主节点一致，原因也相同——API 服务器在写入时默认使用 Pod 模板——仅有一个补充：互联网络相关字段。
// hostNetwork、DNS 策略以及设备卷的设定均由 spec.transport.protocol 决定，而准入机制允许对此进行更新；因此，
// 从 TCP 切换至 RDMA 的后端必须获取这些设置，而重新切换回 TCP 的后端则必须放弃这些设置。
func alignMemberDaemonSetFn(
	kvcb *workercore.KVCacheBackend, eDs *apps.DaemonSet,
) func(*apps.DaemonSet) (*apps.DaemonSet, bool, error) {
	return func(aDs *apps.DaemonSet) (*apps.DaemonSet, bool, error) {
		skip := true
		aPod, ePod := &aDs.Spec.Template.Spec, eDs.Spec.Template.Spec

		// 扩大组的选择器是添加成员的支持方式，因此这一操作会进行。
		if !kubemeta.DeepEqual(aPod.NodeSelector, ePod.NodeSelector) {
			aPod.NodeSelector = ePod.NodeSelector
			skip = false
		}

		if aPod.HostNetwork != ePod.HostNetwork {
			aPod.HostNetwork = ePod.HostNetwork
			skip = false
		}
		if aPod.DNSPolicy != ePod.DNSPolicy {
			aPod.DNSPolicy = ePod.DNSPolicy
			skip = false
		}
		if !kubemeta.DeepEqual(aPod.AutomountServiceAccountToken, ePod.AutomountServiceAccountToken) {
			aPod.AutomountServiceAccountToken = ePod.AutomountServiceAccountToken
			skip = false
		}
		// REQUIRED：与上面的挂载组件合并在一起，原因同上，它位于主节点的侧边。它们在两个字段中属于
		// 同一个设置，而每个部分单独运行时都是静默的。这也是那个字段，其中包含一个成员组，在
		// DaemonSet 已存在后，其后端开启了高可用性，而单独的渲染器永远无法达到这种状态。已废弃的别
		// 名也与之相关，原因同上，它位于主节点的侧边。
		if aPod.ServiceAccountName != ePod.ServiceAccountName {
			aPod.ServiceAccountName = ePod.ServiceAccountName
			aPod.DeprecatedServiceAccount = ePod.ServiceAccountName
			skip = false
		}
		if !kubemeta.DeepEqual(aPod.Volumes, ePod.Volumes) {
			aPod.Volumes = ePod.Volumes
			skip = false
		}
		if !kubemeta.DeepEqual(aPod.ImagePullSecrets, ePod.ImagePullSecrets) {
			aPod.ImagePullSecrets = ePod.ImagePullSecrets
			skip = false
		}

		if len(aPod.Containers) != len(ePod.Containers) {
			aPod.Containers = ePod.Containers
			skip = false
		} else {
			for i := range ePod.Containers {
				if alignRenderedContainer(&aPod.Containers[i], ePod.Containers[i]) {
					skip = false
				}
			}
		}

		if !kubemeta.DeepEqual(aDs.Spec.Template.Labels, eDs.Spec.Template.Labels) {
			aDs.Spec.Template.Labels = eDs.Spec.Template.Labels
			skip = false
		}

		// 指纹信息存储在这里，因此只有当此内容发生变动时，模板的修改才会被合并——之后创建的 Pod 会继
		// 承新值，这就使得在修改前构建的 Pod 与修改后构建的 Pod 能够区分开来。
		if !kubemeta.DeepEqual(aDs.Spec.Template.Annotations, eDs.Spec.Template.Annotations) {
			aDs.Spec.Template.Annotations = eDs.Spec.Template.Annotations
			skip = false
		}

		if aDs.Spec.UpdateStrategy.Type != eDs.Spec.UpdateStrategy.Type {
			aDs.Spec.UpdateStrategy = eDs.Spec.UpdateStrategy
			skip = false
		}

		if !kubemeta.DeepEqual(aPod.TerminationGracePeriodSeconds, ePod.TerminationGracePeriodSeconds) {
			aPod.TerminationGracePeriodSeconds = ePod.TerminationGracePeriodSeconds
			skip = false
		}

		if restoreRenderedLabels(aDs, eDs) {
			skip = false
		}

		if !systemmeta.EqualResourceTypeAndNotes(eDs, aDs) {
			systemmeta.SyncResourceTypeAndNotes(eDs, aDs)
			skip = false
		}

		if !kubemeta.IsControlledBy(aDs, kvcb) {
			kubemeta.ControlOnWithoutBlock(aDs, kvcb,
				workercore.SchemeGroupVersion.WithKind("KVCacheBackend"))
			skip = false
		}

		return aDs, skip, nil
	}
}

// alignLeaderServiceFn 会将正在运行的 Service 合并到已渲染的版本上。
//
// spec.clusterIP 永远不会被修改——它由 API 服务器分配且不可更改——这就是为什么采用命名字段的方式，而非整体分
// 配渲染后的 spec 的原因。
func alignLeaderServiceFn(
	kvcb *workercore.KVCacheBackend, eSvc *core.Service,
) func(*core.Service) (*core.Service, bool, error) {
	return func(aSvc *core.Service) (*core.Service, bool, error) {
		skip := true

		if !kubemeta.DeepEqual(aSvc.Spec.Selector, eSvc.Spec.Selector) {
			aSvc.Spec.Selector = eSvc.Spec.Selector
			skip = false
		}

		// 类型已收敛，其他类型可能包含的字段也随之一清。这一点至关重要：主节点在 9003 端口上提供管
		// 理 API，无需自身认证，仅依靠作为 ClusterIP 来运作。如果切换为 NodePort 或 LoadBalancer，
		// 它就会对外公开——此时其他任何东西都不会引起注意，因为端口比较故意忽略了分配的 nodePort，所
		// 以端口始终保持相等，而暴露情况也依然存在。
		//
		// externalIPs 的聚合与类型无关，因为它不需要类型就能进行聚合。携带 spec.externalIPs 的
		// ClusterIP Service 会将这些地址路由到其声明的每一个端口——其中 9003 个端口——而
		// `kubectl get svc`仍显示 ClusterIP，上述类型比较也看不到需要聚合的内容。这与 NodePort 的暴
		// 露方式相同，无需更改控制该暴露方式的字段即可实现。
		if len(aSvc.Spec.ExternalIPs) > 0 {
			aSvc.Spec.ExternalIPs = nil
			skip = false
		}

		// 达到同一目标的第三种方式，也是另一种无法被其他方式实现的方式：一个由就绪门限故意隐藏的端
		// 点。主节点的探针依赖于其服务平面的活跃状态，因此直到存储系统能够提供服务之前，解析此名称
		// 的客户端将无法获得地址——除非该字段明确要求发布地址。那么，推理引擎首先连接到的对象就是一
		// 个能够响应但无法提供服务的主节点。
		//
		// 与拉取策略不同，这里比较零值是安全的：API 服务器默认将其设为 false，而渲染器也保持这一状态。
		if aSvc.Spec.PublishNotReadyAddresses != eSvc.Spec.PublishNotReadyAddresses {
			aSvc.Spec.PublishNotReadyAddresses = eSvc.Spec.PublishNotReadyAddresses
			skip = false
		}

		if aSvc.Spec.Type != eSvc.Spec.Type {
			aSvc.Spec.Type = eSvc.Spec.Type
			aSvc.Spec.ExternalTrafficPolicy = ""
			aSvc.Spec.HealthCheckNodePort = 0
			aSvc.Spec.AllocateLoadBalancerNodePorts = nil
			aSvc.Spec.LoadBalancerClass = nil
			for i := range aSvc.Spec.Ports {
				aSvc.Spec.Ports[i].NodePort = 0
			}
			skip = false
		}

		if !equalLeaderServicePorts(aSvc.Spec.Ports, eSvc.Spec.Ports) {
			aSvc.Spec.Ports = eSvc.Spec.Ports
			skip = false
		}

		if restoreRenderedLabels(aSvc, eSvc) {
			skip = false
		}

		if !systemmeta.EqualResourceTypeAndNotes(eSvc, aSvc) {
			systemmeta.SyncResourceTypeAndNotes(eSvc, aSvc)
			skip = false
		}

		if !kubemeta.IsControlledBy(aSvc, kvcb) {
			kubemeta.ControlOnWithoutBlock(aSvc, kvcb,
				workercore.SchemeGroupVersion.WithKind("KVCacheBackend"))
			skip = false
		}

		return aSvc, skip, nil
	}
}

// equalLeaderServicePorts 比较控制器生成的端口字段。nodePort 由 API Server 分配，包含它会让每轮调谐都产生虚假的差异。
func equalLeaderServicePorts(actual, expected []core.ServicePort) bool {
	if len(actual) != len(expected) {
		return false
	}
	for i := range expected {
		if actual[i].Name != expected[i].Name ||
			actual[i].Port != expected[i].Port ||
			actual[i].Protocol != expected[i].Protocol ||
			actual[i].TargetPort != expected[i].TargetPort {
			return false
		}
	}
	return true
}

// teardownKVCacheBackend 会在没有任何对象占用该后端时释放锁，如果有其他对象占用，则不会释放。
//
// 这种拒绝行为仅属于状态写入操作，没有其他含义：没有重新请求，也没有计时器。status.usedBy 由使用此后端的对
// 象的管理器维护，他们对该状态的写入操作会再次触发协调过程。这就是基于级别的读取方式——当系统因等待变更而暂
// 停时，就会重新开始卸载过程，而不是按照某种预测可能的时间表来执行。
//
// 等待渲染后的工作负载开始执行是唯一的例外情况，且有时间限制。区别在于两个等待过程所关注的内容：usedBy 是此
// 对象中的一个字段，不唤醒此协调过程则无法更改；而工作负载则属于另外三种类型，其删除事件最终都汇总到这一请求中。
//
// 它拒绝的是那些仍带有某种名称的声明——而非原始列表。一个在未清除其条目的情况下消失的消费者无法再回来清除它，
// 因此拒绝处理原始列表将永远维持这种状态，没有任何事件能够结束它。
func (r *KVCacheBackendReconciler) teardownKVCacheBackend(
	ctx context.Context, kvcb *workercore.KVCacheBackend,
) (ctrl.Result, error) {
	logger := ctrllog.FromContext(ctx)

	live := r.liveKVCacheBackendConsumers(ctx, kvcb)
	if len(live) > 0 {
		logger.V(2).Info("kv cache backend is in use; holding the lock",
			"usedBy", len(live))
		if err := r.syncHeldKVCacheBackendWorkloads(ctx, kvcb); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.syncStatus(ctx, kvcb, r.computeStatus(kvcb, live))
	}

	// 渲染后的对象会先出现，而锁定机制则决定了这种排序的 의미。
	//
	// 它们是集群级所有者的命名依赖项，收集器会尊重这一所有权——因此它会自行访问这些项。但在终结器被移除
	// 且相关操作发生之间，主节点仍会在某个地址上继续提供推理服务，而成员则仍保留他们所声称的节点内存。
	// 所有权是保障机制，而非运作方式。
	remaining, err := r.deleteRenderedWorkloads(ctx, kvcb)
	if err != nil {
		logger.Error(err, "delete kv cache backend workloads")
		return ctrl.Result{}, err
	}
	if remaining > 0 {
		// 锁会一直保持开启状态，直到它们被移除，而不是在删除操作被接受之后才关闭：主节点会继续提供
		// 推理服务，成员则会一直保留他们声称占有的节点内存，直到自身被终止为止。
		//
		// 代价是，无法终止的依赖项会保留此后端，因此会留下日志名称，而不是静默等待。
		logger.V(2).Info("waiting for the backend's workloads to go", "remaining", remaining)
		// 发布过程并非仅发生在上述正在使用的分支上。未使用的后端拆卸过程，其耗时与工作负载的终止时
		// 间相同；若没有这一步骤，该对象会保持其当前的阶段——通常是“待处理”——直至整个窗口结束，而
		// API 协议要求必须等到拆卸完成后才能报告“删除”状态。
		if err = r.syncStatus(ctx, kvcb, r.computeStatus(kvcb, live)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: kvCacheBackendTeardownInterval}, nil
	}

	// 磁盘层位于工作负载之后，在锁定解除之前，这种排序顺序的两部分都具有承载能力：如果某个成员仍在运行，
	// 存储操作就会写入正在被清空的目录中；一旦锁定解除，就再也没有对象可以运行此操作了。
	//
	// 它会在放弃以及完成时报告结果，这样就能避免那些永远不会回应的节点成为无法被删除的对象的一部分。它
	// 放弃的是节点自身的事件，而非此处的事件。
	cleaned, err := r.cleanKVCacheBackendTier(ctx, kvcb)
	if err != nil {
		logger.Error(err, "clean the kv cache backend disk tier")
		return ctrl.Result{}, err
	}
	if !cleaned {
		return ctrl.Result{RequeueAfter: kvCacheBackendTeardownInterval}, nil
	}

	systemmeta.Unlock(kvcb)
	if err := r.Client.Update(ctx, kvcb); err != nil {
		return ctrl.Result{}, ctrlcli.IgnoreNotFound(err)
	}
	// 这个控制器在多次迭代过程中保留的单一状态数据被存储在对象所在位置。它会被保留下来，在进程生命周期
	// 内持续增长；如果后端以相同名称重新创建，其首次选举结果会被视为来自旧主节点的交接。
	r.leaseHolders.forget(kvcb.Name)
	logger.V(2).Info("released kv cache backend")
	return ctrl.Result{}, nil
}

// syncHeldKVCacheBackendWorkloads 汇总了由活跃消费者负责拆除的后端渲染工作负载。
//
// 删除被占用的后端时，工作负载仍在服务，所以还要把 spec 更新应用到运行中的进程。某些消费者只有在管理员调整
// master 配置后才能释放，如果忽略删除期间的修改，双方会一直等待。
//
// 它会整合该后端已经渲染的内容，不会开始任何新的内容，这也是已发布的地址所决定的：推理服务仅在主节点对象合
// 并后才会发布这些地址，因此没有承载任何内容的后端则不会进行渲染。进行渲染将会在对象被移除时创建 Deployment、
// Service 和成员组，而此后的推理服务将再次删除它们。
//
// 缺失的图像会被跳过，不会返回，其处理规则与推理服务路径相同：该图像部分来自可编辑设置，因此管理员对其进行
// 清除操作不应导致正在进行的拆除流程失败。
func (r *KVCacheBackendReconciler) syncHeldKVCacheBackendWorkloads(
	ctx context.Context, kvcb *workercore.KVCacheBackend,
) error {
	if kvcb.Spec.Connection.Managed == nil || len(kvcb.Status.Endpoints) == 0 {
		return nil
	}

	image, err := resolveKVCacheBackendImage(ctx, kvcb)
	if err != nil {
		ctrllog.FromContext(ctx).Error(err,
			"resolve kv cache backend image while its teardown is held")
		return nil
	}

	if err = r.syncLeaderWorkload(ctx, kvcb, image); err != nil {
		return err
	}
	return r.syncMemberWorkloads(ctx, kvcb, image)
}

// deleteRenderedWorkloads 清理管理型后端生成的对象，并报告还有几个对象存在。控制器据此等到资源真正消失再解
// 除终结器。外部后端没有这类对象。
//
// 它是基于级别的：NotFound 是它试图达到的状态，因此部分被删除的后端会收敛到重复状态，而不是因为其他用户已移
// 除而失败。通过每个组所携带的标签来查找 DaemonSets 成员，而不是通过可能不再描述它们的规范来查找，因此从规
// 范中删除的组在对象被删除之前不会留下痕迹。
//
// 每次删除操作都会受到资源注释的约束，而这种约束并非形式上的要求：这里的名称是派生出来的，因此具有该名称的
// 对象可能早于这个后端存在，而对齐路径也无法作为依赖依据。该路径从不写入 spec.selector，因为该字段是不可变
// 的——因此，即使经过多次迭代，名称相同但选择器不同的工作负载也会被拒绝更新，且不会附带任何注释。仅通过派生
// 名称进行删除即可移除它，而没有任何方式可以重新创建它。
//
// 采用前台删除，使对象在子资源仍然运行期间保持可见，调用者可据此继续等待。
//
// 报告为剩余的，正是仍值得等待的内容，这与实际存在的数量并不相同。那些超过自身预算的 workload 会被从统计中
// 剔除，转而进行报告，因为调用者持有后端终结器对这个数字的掌控权：无论何时统计，只要一个 kubelet 未能完成终
// 结过程，该对象就会永久无法删除；而一个停止响应的节点就足以产生一个此类问题。以只有一个节点停止的集群为例：
// 经过十二分钟仍无结束迹象，但在节点恢复后的十秒内，该对象便被清除。
func (r *KVCacheBackendReconciler) deleteRenderedWorkloads(
	ctx context.Context, kvcb *workercore.KVCacheBackend,
) (remaining int, err error) {
	if kvcb.Spec.Connection.Managed == nil {
		return 0, nil
	}

	logger := ctrllog.FromContext(ctx)

	leaderKey := ctrlcli.ObjectKey{
		Name:      mooncake.LeaderObjectName(kvcb),
		Namespace: kuberess.SystemNamespaceName,
	}
	for _, obj := range []ctrlcli.Object{&apps.Deployment{}, &core.Service{}} {
		present, err := r.deleteOwnedWorkload(ctx, kvcb, obj, leaderKey)
		if err != nil {
			return remaining, err
		}
		if present && !r.abandonKVCacheBackendWorkload(ctx, kvcb, obj) {
			remaining++
		}
	}

	// 它被标注在 RESOURCE-TYPE 标签上，而非本后端自身的两个标签上，因此识别对象以及证明它们属于我们的机
	// 制是相同的。身份标签是渲染细节，编辑者可以将其删除；资源类型属于 systemmeta 的，而下方筛选的备注
	// 则随之携带。通过一个键进行发现，通过另一个键进行判断，这就是一个对象如何变得无法被其自身的销毁操作发现。
	dss := new(apps.DaemonSetList)
	err = r.Client.List(ctx, dss,
		ctrlcli.InNamespace(kuberess.SystemNamespaceName),
		ctrlcli.MatchingLabels(
			systemmeta.GetResourcesLabelSetOfType[map[string]string](kvcache.ResourceType),
		))
	if err != nil {
		return remaining, err
	}
	for i := range dss.Items {
		ds := &dss.Items[i]
		if !renderedForKVCacheBackend(ds, kvcb) {
			logger.V(2).Info("skipping a daemonset that matches the labels and carries no note of this backend",
				"daemonset", ds.Name)
			continue
		}
		remaining++
		if ds.DeletionTimestamp != nil {
			if r.abandonKVCacheBackendWorkload(ctx, kvcb, ds) {
				remaining--
			}
			continue
		}
		if err = r.Client.Delete(ctx, ds, ctrlcli.PropagationPolicy(meta.DeletePropagationForeground)); err != nil {
			if kerrors.IsNotFound(err) {
				remaining--
				continue
			}
			return remaining, err
		}
	}

	return remaining, nil
}

// deleteOwnedWorkload 会删除该后端以派生名称渲染的一个对象，并在调用返回时报告该对象是否仍然存在。
//
// 在删除之前，该对象会被读取，因为仅名称本身无法证明它属于谁。不存在“非本后端”的情况：既不是本后端的，也不
// 需要等待任何一方的处理。
func (r *KVCacheBackendReconciler) deleteOwnedWorkload(
	ctx context.Context, kvcb *workercore.KVCacheBackend, obj ctrlcli.Object, key ctrlcli.ObjectKey,
) (present bool, err error) {
	if err = r.Client.Get(ctx, key, obj); err != nil {
		return false, ctrlcli.IgnoreNotFound(err)
	}

	if !renderedForKVCacheBackend(obj, kvcb) {
		ctrllog.FromContext(ctx).V(2).Info(
			"skipping an object that carries this backend's derived name and no note of it",
			"object", fmt.Sprintf("%T", obj), "name", key.Name,
		)
		return false, nil
	}

	if obj.GetDeletionTimestamp() != nil {
		return true, nil
	}

	err = r.Client.Delete(ctx, obj, ctrlcli.PropagationPolicy(meta.DeletePropagationForeground))
	if err != nil {
		return !kerrors.IsNotFound(err), ctrlcli.IgnoreNotFound(err)
	}
	return true, nil
}

// abandonKVCacheBackendWorkload 报告某个渲染完成的工作负载是否停止时间超过了其销毁等待时间，并在出现这种情况时说明原因。
//
// THE CLOCK 是工作负载自身的删除时间戳，这是候选项中唯一能衡量这种等待状态的指标。后端系统无法做到这一点：
// 此步骤之前的任何操作都是无限制的——消费者可以随心所欲地保留对后端的访问权，因此对象被删除一小时后，该对象
// 就会失效并被放弃，无需等待即可被丢弃。内存中保存的计数器同样不行；它在每个控制器重启时都会重置，而被阻塞
// 的节点则会在所有控制器中保持对该对象的访问权。
//
// 当其他人开始删除操作时，它还能准确衡量真正重要的指标。操作员在删除后端之前手动移除的工作负载，已经花费了
// 时间进行终止操作，因此这会被计入其中，因为所谓的等待时间，指的是该对象已经处于死亡状态的时间，而非是是谁要求其死亡的。
//
// 放弃从来不是无声的：工作负载会收到一个警告事件，指出其卡住了的位置，日志中也会记录下该事件故意遗漏的精确时间。
func (r *KVCacheBackendReconciler) abandonKVCacheBackendWorkload(
	ctx context.Context, kvcb *workercore.KVCacheBackend, obj ctrlcli.Object,
) bool {
	deletedAt := obj.GetDeletionTimestamp()
	if deletedAt.IsZero() {
		return false
	}
	elapsed := time.Since(deletedAt.Time)

	// 该模板只是个廉价入口，绝非最终定论。它描述了当前工作负载会产生的 Pod，而正在终止的 Pod 则会运行其
	// 创建时使用的模板——只要存在滚动重启，这两者就存在差异。降低 scaleIn.gracePeriodSeconds 参数值并在
	// 重启完成前删除后端服务，会导致某个成员在模板中不再提及的 1 小时宽限期内处于空闲状态，若放弃该模板，
	// 就会在存储仍在写入的情况下开始磁盘清理工作。
	budget := kvCacheBackendWorkloadTerminationBudget(obj)
	if elapsed <= budget {
		return false
	}

	// 因此，一旦模板中指定时间结束，这些 Pod 本身就会获得决定性的结果。其中任何仍处于有效范围内的 Pod
	// 都承载着工作负载，这与选择其中耗时最长的那个进行单次比较而非两次比较的规则相同。
	//
	// 每个 Pod 的删除时间以其自身的删除时间戳为准，而非工作负载的时间戳。这两个时间并不相同，且这种差异
	// 并非四舍五入误差：前景删除通过 Pod 之间的层级机制来影响 Pod——一个 Deployment 则通过其 ReplicaSet
	// 来执行操作——而驱逐操作则可能在工作负载消失很久之后删除一个 Pod。以工作负载的时间戳为参照，一个已
	// 停止运行的 Pod 会承受其上方对象整个生命周期的计费，该对象在 kubelet 执行任务时过期。这里每个 Pod
	// 的删除时间戳均不为零，因为这就是读取操作所依据的条件。
	//
	// 如果读取失败，则不会生成 Pod，模板的判定结果也会保持不变。这其实是一个更弱的答案，而非错误的答案：
	// 模板仍然处于受限状态，拒绝在此处做出决策会使对象回到没有任何内容的状态。
	pods := r.terminatingKVCacheBackendWorkloadPods(ctx, obj)
	for i := range pods {
		podElapsed := time.Since(pods[i].DeletionTimestamp.Time)
		if podElapsed <= kvCacheBackendTerminationBudget(pods[i].Spec.TerminationGracePeriodSeconds) {
			return false
		}
	}

	// 该事件不包含已过去的时间，仅包含耗尽的预算以及其耗尽的节点。每次重启时都会触发此操作，而此时其他
	// 操作仍在进行中，因此每次都会再次触发——而 client-go 的重复操作则会被合并为一次事件，其计数仅在消息
	// 内容完全相同时才会发生。如果记录精确的时间，那么每次重复都会成为一个独立的事件。
	where := "no pod of it names a node"
	if nodes := terminatingKVCacheBackendWorkloadNodes(pods); len(nodes) > 0 {
		where = "its pods are still terminating on " + strings.Join(nodes, ", ")
	}
	r.recordWarning(obj, kvCacheBackendEventWorkloadAbandoned,
		"%T %q rendered by deleted KVCacheBackend %q was still terminating after %s and the deletion "+
			"was not held open for it: %s; a pod that outlives its own grace usually means the node "+
			"it runs on stopped answering",
		obj, obj.GetName(), kvcb.Name, budget, where)
	ctrllog.FromContext(ctx).Error(nil, "gave up waiting for a rendered workload to terminate",
		"workload", fmt.Sprintf("%T", obj), "name", obj.GetName(),
		"age", time.Since(deletedAt.Time), "budget", budget)

	return true
}

// kvCacheBackendWorkloadTerminationBudget 计算等待一项受管工作负载退出的时间。
//
// 按工作负载来分配，而非按任务分解来分配，因为这三种情况处理时间并不相同：成员组的 Pod 会在指定的预算限制下
// 终止，主节点的 Pod 则按默认设置处理，而 Service 则根本不会运行任何 Pod，仅存在一定的余量——这已经远远高于
// 删除没有依赖对象的对象所需的时间。
//
// 没有额外说明的模板名称被理解为 kubelet 的默认设置，而非零。如果为零，则意味着整个预算都用于主节点，而该主
// 节点是工作负载，此操作器从未对其 Pod 进行明确的分配。
func kvCacheBackendWorkloadTerminationBudget(obj ctrlcli.Object) time.Duration {
	// POD 模板会决定一切，其缺失会与未设置字段的情况产生不同影响。一种不运行 pod 的模板仅依赖于自身的集
	// 合，因此将默认设置读入该模板，会赋予那些没有容器的 Service 一种不公平的待遇。
	var template *core.PodSpec
	switch o := obj.(type) {
	case *apps.Deployment:
		template = &o.Spec.Template.Spec
	case *apps.DaemonSet:
		template = &o.Spec.Template.Spec
	}
	if template == nil {
		return kvCacheBackendWorkloadTerminationMargin
	}

	return kvCacheBackendTerminationBudget(template.TerminationGracePeriodSeconds)
}

// kvCacheBackendTerminationBudget 将所声明的宽限期转换为等待的时间。
//
// 名为未知的 grace 是 kubelet 的默认设置，并非为零，因为对于从未被命名的 Pod 中的容器来说，kubelet 会提供这一设置。
func kvCacheBackendTerminationBudget(grace *int64) time.Duration {
	seconds := int64(core.DefaultTerminationGracePeriodSeconds)
	if grace != nil {
		seconds = *grace
	}

	return time.Duration(seconds)*time.Second + kvCacheBackendWorkloadTerminationMargin
}

// terminatingKVCacheBackendWorkloadPods 返回指定受管工作负载中尚在终止的 Pod。
//
// 它回答了一个问题中的两个疑问，这两个问题是同时被提出的，因为它们只有在一次读取 Pod 时才会得到相同的答案：
// 这个工作负载实际依赖的权限是什么？以及当被放弃时应该指定哪些节点。一个 Pod 会保留其创建时所拥有的权限，也
// 就是 kubelet 会尊重的权限；而工作负载的模板则规定了现在创建的 Pod 会获得哪些权限。
//
// 失败的读取操作不会产生错误，而是不会生成 Pod。两个调用方都处于已经确定模板预算为“开启”的状态路径上，若在
// 此处失败则会导致该对象回到无绑定状态——这相当于以更长的等待时间和更清晰的提示信息作为代价来修复整个问题。
func (r *KVCacheBackendReconciler) terminatingKVCacheBackendWorkloadPods(
	ctx context.Context, obj ctrlcli.Object,
) []core.Pod {
	var selector *meta.LabelSelector
	switch o := obj.(type) {
	case *apps.Deployment:
		selector = o.Spec.Selector
	case *apps.DaemonSet:
		selector = o.Spec.Selector
	}
	if selector == nil || len(selector.MatchLabels) == 0 {
		return nil
	}

	pods := new(core.PodList)
	err := r.Client.List(ctx, pods,
		ctrlcli.InNamespace(obj.GetNamespace()),
		ctrlcli.MatchingLabels(selector.MatchLabels))
	if err != nil {
		ctrllog.FromContext(ctx).Error(err, "list the pods holding a rendered workload open",
			"workload", fmt.Sprintf("%T", obj), "name", obj.GetName())
		return nil
	}

	terminating := make([]core.Pod, 0, len(pods.Items))
	for i := range pods.Items {
		if !pods.Items[i].DeletionTimestamp.IsZero() {
			terminating = append(terminating, pods.Items[i])
		}
	}

	return terminating
}

// terminatingKVCacheBackendWorkloadNodes 用于标识那些 Pod 所在的节点，排序方式确保每次处理时都会产生相同的提示信息。
//
// 这是放弃事件用于移交的指针。工作负载名称中未包含的内容是什么；只有其 Pod 能够指出位置，而一个读取到正在删
// 除操作的后端操作器则无法做到这一点。
func terminatingKVCacheBackendWorkloadNodes(pods []core.Pod) []string {
	var nodes []string
	for i := range pods {
		node := pods[i].Spec.NodeName
		if node == "" || slices.Contains(nodes, node) {
			continue
		}
		nodes = append(nodes, node)
	}
	slices.Sort(nodes)

	return nodes
}

// recordWarning 会针对对象发布一个警告事件，当该控制器没有记录器时则不会执行任何操作。
//
// 记录器在 SetupController 中被指定，其他位置则没有。因此，任何跳过它的构建路径——测试套件自身的协调辅助函数
// 也会跳过——在首次执行销毁操作时将会崩溃。测量结果显示：在层级清理的共享路径分支上出现了 nil 解引用。
//
// 降级为静默是安全的，因为每个调用者都会记录与事件相关的相同事实；该事件会超越后端系统，而没有记录器的控制
// 器就等同于没有任何人在监视。
func (r *KVCacheBackendReconciler) recordWarning(
	obj ctrlcli.Object, reason, format string, args ...any,
) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(obj, core.EventTypeWarning, reason, format, args...)
}

// restoreRenderedLabels 会将渲染好的标签重新应用到实时对象上，并报告是否有任何内容发生移动。
//
// 对象的自身标签在此并非装饰性元素，而是发现键——成员扫描通过列出这些键来查找后端的 DaemonSets。如果未进行对
// 齐处理，删除某项编辑操作会使该对象从拥有它的控制器中隐藏起来，且销毁操作会在无法实现对齐的情况下进行，因
// 此没有任何操作能及时将其恢复。
//
// 按键进行恢复，而非整体替换：真实集群中的工作负载带有非此操作器所添加的标签，移除这些标签并非此控制器的职责。
func restoreRenderedLabels(actual, expected ctrlcli.Object) (changed bool) {
	for k, v := range expected.GetLabels() {
		if !kubemeta.IsLabeled(actual, k, v) {
			kubemeta.SetLabel(actual, k, v)
			changed = true
		}
	}
	return changed
}

// renderedForKVCacheBackend 判断对象是否由当前后端的渲染流程创建。
//
// 资源注释是判断依据，而标签并非如此，因为标签只是一个查询：这个命名空间中的任何内容都可以包含它们，而删除
// 操作必须比列表更确定。控制器引用也不被查询——它会第二次回答同一个问题，因为对齐路径在每次迭代中都会将注释
// 和引用一起写入。
func renderedForKVCacheBackend(obj ctrlcli.Object, kvcb *workercore.KVCacheBackend) bool {
	return systemmeta.MatchResource(obj, kvcache.ResourceType, kvcache.ResourceNoteBackend, kvcb.Name)
}

// syncStatus 仅在观测值发生变化时更新状态，稳定对象不会因为自己的写入而触发新事件。
func (r *KVCacheBackendReconciler) syncStatus(
	ctx context.Context, kvcb *workercore.KVCacheBackend, desired workercore.KVCacheBackendStatus,
) error {
	omitLegacyMemberListing(&desired)

	if kubemeta.DeepEqual(desired, kvcb.Status) {
		return nil
	}

	logger := ctrllog.FromContext(ctx)

	kvcb.Status = desired
	if err := r.Client.Status().Update(ctx, kvcb); err != nil {
		// 后端没有预设条件，因此冲突背后的变化会使其再次被触发。
		_, err = objectWriteResult(logger, err, "update kv cache backend status", ctrl.Result{})
		return err
	}
	logger.V(2).Info("refreshed kv cache backend status", "phase", desired.Phase)
	return nil
}

// omitLegacyMemberListing 会保留在旧架构下写入的状态信息，在 segmentID 和 clientID 成为必需参数后，所有后续
// 状态更新将不再有效。由于两种身份信息都没有真实的值可供合成，因此整个过时的列表会被忽略，直到成功的主节点
// 读取替换该列表。如果仅保留具有身份信息的行，将会发布部分列表；而将 segmentName 复制到 segmentID 中，则会
// 将地址变成一个虚假的标识符。
func omitLegacyMemberListing(status *workercore.KVCacheBackendStatus) {
	legacy := slices.ContainsFunc(status.Members, func(member workercore.KVCacheBackendMemberStatus) bool {
		return member.SegmentID == "" || member.ClientID == ""
	})
	if !legacy {
		return
	}

	status.Members = nil
	holder := &workercore.KVCacheBackend{Status: *status}
	message := legacyMemberStatusExplanation
	if KVCacheBackendConditionMembersMounted.IsFalse(holder) {
		message = withLegacyMemberStatus(KVCacheBackendConditionMembersMounted.GetMessage(holder))
	}
	KVCacheBackendConditionMembersMounted.False(holder, legacyMemberStatusReason, message)
	if holder.Status.Phase == KVCacheBackendPhaseReady {
		holder.Status.Phase = KVCacheBackendPhaseDegraded
		holder.Status.PhaseMessage = message
	}
	*status = holder.Status
}

const legacyMemberStatusReason = "LegacyMemberStatus"

const legacyMemberStatusExplanation = "the prior member listing predates the required segment and " +
	"client identities and was omitted until a successful read replaces it"

// setMembersObservationFailed 会在多次读取失败时保留迁移说明。成功的列表操作会替换相关行，并使用下面的常规
// 条件路径，这样无需在 API 中设置持久化迁移字段即可清除该说明。
func setMembersObservationFailed(holder *workercore.KVCacheBackend, reason, message string) {
	legacy := slices.ContainsFunc(holder.Status.Members,
		func(member workercore.KVCacheBackendMemberStatus) bool {
			return member.SegmentID == "" || member.ClientID == ""
		}) || KVCacheBackendConditionMembersMounted.GetReason(holder) == legacyMemberStatusReason
	if legacy {
		holder.Status.Members = nil
		message = withLegacyMemberStatus(message)
		reason = legacyMemberStatusReason
	}
	KVCacheBackendConditionMembersMounted.False(holder, reason, message)
}

func withLegacyMemberStatus(message string) string {
	if strings.Contains(message, legacyMemberStatusExplanation) {
		return message
	}
	if message == "" {
		return legacyMemberStatusExplanation
	}
	return message + "; " + legacyMemberStatusExplanation
}

// computeStatus 根据观测结果构建整体状态。
//
// 目前观察到一个现象：任何事物是否占据后端资源，这正是 Deletable 能够响应的内容，也是 teardown 拒绝处理的内
// 容。三种工作负载状况伴随着能够观测到它们的任务出现——目前没有主节点可询问，也没有成员可统计——在此处创造这
// 些状况将导致一个未被测量的状态。此轮操作未涉及的任何状态都会被保留，因此后续任务的数值不会被当前操作清除。
//
// 这些声明作为参数被传递，而不是从对象中读取出来，因为判断其中哪些仍然指代某物属于一种查找操作，而这一行为
// 完全取决于所观察到的内容。
func (r *KVCacheBackendReconciler) computeStatus(
	kvcb *workercore.KVCacheBackend, live []workercore.KVCacheObjectReference,
) workercore.KVCacheBackendStatus {
	// 条件访问器会修改其所操作的对象，因此它们操作的是一个副本：调用者会将此结果与观察到的状态进行比较，
	// 仅对差异部分进行写入。
	desired := &workercore.KVCacheBackend{Status: *kvcb.Status.DeepCopy()}

	inUseBy := formatKVCacheBackendConsumers(live)
	switch {
	case inUseBy != "":
		KVCacheBackendConditionDeletable.False(desired, "InUse",
			fmt.Sprintf("in use by %s", inUseBy))
	case len(kvcb.Status.UsedBy) > 0:
		// 此后端上的每一项声明都指向某样已消失的事物。与其直接说“该后端没有对象声明”，不如这样表述
		// 更能让读者理解为什么 usedBy 和 Deletable 会存在分歧——否则就会出现一种看似自相矛盾且无法解释的状态。
		KVCacheBackendConditionDeletable.True(desired, "Unused",
			fmt.Sprintf("no object claims this backend; status.usedBy still names %s, which no longer "+
				"exists", formatKVCacheBackendConsumers(kvcb.Status.UsedBy)))
	default:
		KVCacheBackendConditionDeletable.True(desired, "Unused", "no object claims this backend")
	}

	switch {
	case kvcb.DeletionTimestamp != nil:
		desired.Status.Phase = KVCacheBackendPhaseDeleting
		desired.Status.PhaseMessage = KVCacheBackendConditionDeletable.GetMessage(desired)
		if inUseBy == "" {
			desired.Status.PhaseMessage = ""
		}
	default:
		desired.Status.Phase = KVCacheBackendPhaseProvisioning
		desired.Status.PhaseMessage = "waiting for the backend's workloads"
	}

	return desired.Status
}

// deriveKVCacheBackendPhase 将刚观测到的各项条件汇总为后端的 phase。
//
// 它仅根据本次实际写入的条件运行，因此该阶段所声称的内容不会超过实际观测到的内容。正在进行的删除操作在
// computeStatus 中已提前确定，此处不再赘述：一个即将被淘汰的后端会删除其主节点正在进行的任何操作。
//
// 共有五个阶段，没有单独的“待处理”状态。对于读者来说，未安排工作负载且主节点尚未启动的状态是相同的——后端尚
// 未可用——而 phaseMessage 则用于区分这两种状态。leaderIsStillStarting 用于说明“LeaderAvailable”=False 的原
// 因是指正在进行的启动过程，而非故障。
//
// LeaderStarting 是一个主节点，其 Pod 尚未准备就绪，这是每次全新安装后的最初几分钟状态。
// ServicePlaneNotActive 则是一个主节点，它正在响应并表明其服务平面尚未启动——这是它自己报告的，因此它正在运行。
//
// 其他所有情况都是错误，包括 kubelet 写入的原因。这就是该列表的排列方向：此操作符拥有两种进度状态，但不拥有
// 错误词汇表，因此那些没人预料到会被报告为“错误”的情况会被处理。
func leaderIsStillStarting(reason string) bool {
	return reason == "LeaderStarting" || reason == "ServicePlaneNotActive"
}

func deriveKVCacheBackendPhase(holder *workercore.KVCacheBackend, renderBlocked error) {
	if holder.Status.Phase == KVCacheBackendPhaseDeleting {
		return
	}

	switch {
	case renderBlocked != nil && len(holder.Status.Endpoints) == 0:
		// 没有任何内容被渲染出来，因此没有地址可以读取，且由于根本没有什么需要查询的内容，
		// LeaderAvailable 为 False。对主节点的排名检查正是为了确认这一点：否则，其原因会被理解为“仍
		// 在加载中”，而一个永远无法启动的后端服务将无限期地处于“Provisioning”状态，而不会指出缺失的内容是什么。
		holder.Status.Phase = KVCacheBackendPhaseDegraded
		holder.Status.PhaseMessage = fmt.Sprintf("nothing has been rendered for this backend and "+
			"nothing can be, so it cannot start: %v", renderBlocked)

	case KVCacheBackendConditionLeaderAvailable.IsFalse(holder):
		// 根据 condition 的 reason 区分 leader 正在启动和已经故障。可达但尚未成为活动 leader 时为
		// Provisioning，不可达时为 Error。只识别表示启动中的两种原因，其余按故障处理，使 kubelet 以
		// 后新增的故障原因也能如实上报。
		holder.Status.Phase = KVCacheBackendPhaseError
		if leaderIsStillStarting(KVCacheBackendConditionLeaderAvailable.GetReason(holder)) {
			holder.Status.Phase = KVCacheBackendPhaseProvisioning
		}
		holder.Status.PhaseMessage = KVCacheBackendConditionLeaderAvailable.GetMessage(holder)

	case renderBlocked != nil:
		// 位于一个不提供推理服务的主节点之下——商店故障是更紧急的问题——并且还存在缺口，因为缺口通常
		// 是这个问题的症状：添加到规格中的成员组根本不会被渲染。报告这一缺口就能指出缺失的内容，从而掩盖原因。
		//
		// 这不是一个问题，因为这里的所有问题都涉及 STORE，而该存储系统可能完全正常；真正有问题的，
		// 是这个操作器对其的掌控能力。
		holder.Status.Phase = KVCacheBackendPhaseDegraded
		holder.Status.PhaseMessage = fmt.Sprintf("the backend is running, but its workloads can no "+
			"longer be rendered, so a change to this object would not reach them: %v", renderBlocked)

	case KVCacheBackendConditionMembersMounted.IsFalse(holder):
		// 主节点负责服务，因此后端也存在——只是其存储量低于应有的水平。
		holder.Status.Phase = KVCacheBackendPhaseDegraded
		holder.Status.PhaseMessage = KVCacheBackendConditionMembersMounted.GetMessage(holder)

	case KVCacheBackendConditionLeaderAvailable.IsTrue(holder):
		// MembersMounted=Unknown 也出现在此处：它是一个主节点，其版本未出现在列表中，这是关于该版本
		// 的实际情况，并非缺陷。
		holder.Status.Phase = KVCacheBackendPhaseReady
		holder.Status.PhaseMessage = ""
	}
}

// formatKVCacheBackendConsumers 在拒绝删除的消息中列出占用后端的对象，便于管理员找到并释放它们。
//
// 被采样了，因为 usedBy 没有绑定项：由使用后端的人编写，每个消费者一个条目，这条消息就承载了
// `Deletable=False` 和“删除”阶段。无限制连接会随之导致拒绝——删除会被拒绝，而对象本身也不会表示同意。
func formatKVCacheBackendConsumers(refs []workercore.KVCacheObjectReference) string {
	names := make([]string, 0, len(refs))
	for _, ref := range refs {
		names = append(names, ref.Kind+"/"+ref.Name)
	}
	return listBoundedNames(names)
}

// liveKVCacheBackendConsumers 筛选 status.usedBy 中实际仍存在的使用方。
//
// 该列表由消费者自己编写，并在离开时由他们清除，因此通常每个条目都是有效的，此操作会返回所有条目。这种机制
// 适用于消费者未清除其条目的情况——比如某个操作器强制释放一个被阻塞的 KVCachePool 的终结器。之后无法进行任何
// 清理工作，因为正是消费者进行了清理，所以读者必须在没有清理发生的情况下继续使用。否则，指向已消失资源池的
// 条目会永久保留此后端的卸载操作，唯一的迹象就是某个对象被标记为 Deletable=False，以至于无人能找到它。
//
// 只有当明确显示“未找到”时才会删除条目。对于无法查询的此类内容，或读取失败的情况，则会被保留：将“无法验证”
// 转化为“不存在”与删除功能的本意相悖。
//
// 该规则的有效性取决于其背后的读取机制，因此会明确说明读取行为，而非假设。它经过管理器的信息缓存，而该缓存
// 的缺陷在于单向性：一个条目在 ADD 操作下进入索引器，在 DELETE 操作下离开，因此背后的缓存仍会保留它尚未被删
// 除的资源池。该缓存会拒绝清理操作，而不会允许清理发生。
//
// 从另一个方向获取资源池时，需要一个从未见过该资源池的缓存，共有两种获取方式。未启动的缓存会被
// controller-runtime 关闭，该组件会在启动控制器之前同步所有 informer。刚刚创建的资源池，其 ADD 操作仍在进行
// 中，会被关闭，而这是该循环能够读取条目所必须发生的步骤：该条目是由资源池自身的控制器写入的，该控制器运行
// 在相同的管理者上，因此也来自同一个缓存，也就是说，在条目出现之前，该缓存就已经看到了资源池。“此缓存不包含
// 资源池”和“usedBy 指定了资源池”不能同时成立。
//
// 故意不设置 ResourceVersion 选项。缓存的 Get 调用会忽略该选项——CacheReader.Get 仅会读取选项中的
// UnsafeDisableDeepCopy，因此传递该选项会暗示一种不真实的过时权衡，而此文件中的读取操作实际上并未产生这种权
// 衡；如果错误设置过时性，则会导致无法取回的锁被释放。
func (r *KVCacheBackendReconciler) liveKVCacheBackendConsumers(
	ctx context.Context, kvcb *workercore.KVCacheBackend,
) []workercore.KVCacheObjectReference {
	if len(kvcb.Status.UsedBy) == 0 {
		return nil
	}

	live := make([]workercore.KVCacheObjectReference, 0, len(kvcb.Status.UsedBy))
	for _, ref := range kvcb.Status.UsedBy {
		if ref.Kind != KVCachePoolKind {
			live = append(live, ref)
			continue
		}
		err := r.Client.Get(ctx, ctrlcli.ObjectKey{Name: ref.Name}, new(workercore.KVCachePool))
		if err == nil || !kerrors.IsNotFound(err) {
			live = append(live, ref)
		}
	}
	return live
}

// enqueueKVCacheBackendWhenPoolChanged 将 pool 映射回其引用的所有后端。
//
// 它读取 pool 的 spec 而非后端的 usedBy，因为最关键的事件是 pool 的删除操作——而删除
// 事件会携带对象的最后已知状态，其中仍列出了这些后端。
func (r *KVCacheBackendReconciler) enqueueKVCacheBackendWhenPoolChanged(
	_ context.Context, obj ctrlcli.Object,
) []ctrlreconcile.Request {
	kvcp, ok := obj.(*workercore.KVCachePool)
	if !ok {
		return nil
	}

	reqs := make([]ctrlreconcile.Request, 0, len(kvcp.Spec.Backends))
	for _, name := range kvcp.Spec.Backends {
		reqs = append(reqs, ctrlreconcile.Request{NamespacedName: ctrlcli.ObjectKey{Name: name}})
	}
	return reqs
}

// enqueueKVCacheBackendWhenWorkloadChanged 将渲染出的对象映射回拥有它的后端。
//
// 它直接从对象自身的资源注解中读取后端名称，而非遍历其 owner references：该注解正是
// predicate 已经筛选过的依据，因此两者在构造上天然一致，且删除事件与更新事件同样
// 携带该注解。
func (r *KVCacheBackendReconciler) enqueueKVCacheBackendWhenWorkloadChanged(
	_ context.Context, obj ctrlcli.Object,
) []ctrlreconcile.Request {
	name := systemmeta.DescribeResourceNote(obj, kvcache.ResourceNoteBackend)
	if name == "" {
		return nil
	}
	return []ctrlreconcile.Request{{NamespacedName: ctrlcli.ObjectKey{Name: name}}}
}

// kvCacheBackendWorkloadPredicate 将监听范围限定为本算子自身创建的对象。若无此过滤，
// 集群中的每个 Deployment 和 Service 都会唤醒该调谐器。
//
// Namespace 校验占检查条件的一半，且这一半并非为了降噪。资源注解本质上是 annotation，
// 任何能在任意 Namespace 创建对象的人都可以写入匹配的注解——每次匹配都会触发对应后端
// 的一次调谐，即对其管理接口发起三次 HTTP 读取。渲染只会在系统 Namespace 中进行，
// 因此无论其他 Namespace 中的对象如何标注自身，都不可能是本算子的对象。
func kvCacheBackendWorkloadPredicate() ctrlpredicate.Predicate {
	return ctrlpredicate.NewPredicateFuncs(func(obj ctrlcli.Object) bool {
		return obj.GetNamespace() == kuberess.SystemNamespaceName &&
			systemmeta.MatchResource(obj, kvcache.ResourceType)
	})
}

func (r *KVCacheBackendReconciler) SetupController(_ context.Context, opts controller.SetupOptions) error {
	r.Client = opts.Manager.GetClient()
	r.Recorder = opts.Manager.GetEventRecorderFor("kvcachebackend")
	if r.AdminHTTP == nil {
		r.AdminHTTP = newAdminHTTPClient()
	}

	dedupWindow := ctrlhandlerx.NewDedupWindow[ctrlreconcile.Request]()

	return ctrl.NewControllerManagedBy(opts.Manager).
		Named("kvcachebackend").
		For(&workercore.KVCacheBackend{}).
		Watches(
			// pool 会通过将自身写入 status.usedBy 来占用该后端，并在退出时由其自身的调谐器清除
			// 该条目——因此通常后端会由自身 status 变更唤醒，此监听并无额外作用。它所兜底的是
			// pool 在未清除该条目的情况下消失的场景，否则没有任何事件能唤醒后端：该占用记录会
			// 指向一个已不存在的对象并卡住销毁流程，且再无事件触发重新检查。
			//
			// 与工作负载监听不同，此处不做去重。前者按渲染对象触发并聚合到同一后端；此处按 pool
			// 触发，而每个 pool 对其引用的每个后端仅列出一次。
			&workercore.KVCachePool{},
			ctrlhandler.EnqueueRequestsFromMapFunc(r.enqueueKVCacheBackendWhenPoolChanged),
		).
		Watches(
			// 监听 leader 的 Deployment 并将拥有它的后端入队。若无此监听，调谐器只会在后端对象
			// 自身变化时才收敛渲染出的对象，人工对 Deployment 的修改将一直保留，直到后端对象
			// 恰好发生其他变更——这并非持续调谐，而是碰巧调谐。
			&apps.Deployment{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFuncWithWindow(
				3*time.Second,
				dedupWindow,
				r.enqueueKVCacheBackendWhenWorkloadChanged,
			),
			ctrlbuilder.WithPredicates(kvCacheBackendWorkloadPredicate()),
		).
		Watches(
			// 对其前端承载已发布端点的 Service 同样如此。
			&core.Service{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFuncWithWindow(
				3*time.Second,
				dedupWindow,
				r.enqueueKVCacheBackendWhenWorkloadChanged,
			),
			ctrlbuilder.WithPredicates(kvCacheBackendWorkloadPredicate()),
		).
		Watches(
			// 以及各 member 组的 DaemonSet，节点加入或离开分组最先在此体现。
			&apps.DaemonSet{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFuncWithWindow(
				3*time.Second,
				dedupWindow,
				r.enqueueKVCacheBackendWhenWorkloadChanged,
			),
			ctrlbuilder.WithPredicates(kvCacheBackendWorkloadPredicate()),
		).
		Watches(
			// 以及 leader 自身的 Pod，用于捕获仅会写入 Pod 的那类故障。调度器会将“无可用节点”
			// 记录在 Pod 上（表现为 PodScheduled=False），而该写入不会引起 Deployment 的任何变化——
			// Deployment 自身要到十分钟后才会上报 ProgressDeadlineExceeded。若无此监听，上报
			// leader 不可调度的那次调谐就只能被动等待其他事件恰好触发。
			&core.Pod{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFuncWithWindow(
				3*time.Second,
				dedupWindow,
				r.enqueueKVCacheBackendWhenLeaderPodChanged,
			),
			ctrlbuilder.WithPredicates(kvCacheBackendLeaderPodPredicate()),
		).
		Watches(
			// 以及选主所依赖的 Lease，它是唯一能反映主备切换发生的对象。该对象由存储引擎而非
			// 本算子创建，因此不携带资源注解，映射函数必须依据名称反推出归属关系。
			//
			// 此处设置 predicate 并非出于代码整洁。在每个 leader 的整个生命周期中，租约每隔几秒
			// 就会续约一次，若不过滤监听事件，每次续约都会触发一次调谐，进而对该后端的管理接口
			// 发起三次串行读取——这相当于一个仅在每次故障切换时记录一条事件的功能，却对集群中的
			// 每个存储引擎施加持续负载。
			&coordination.Lease{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFuncWithWindow(
				3*time.Second,
				dedupWindow,
				r.enqueueKVCacheBackendWhenLeaseChanged,
			),
			ctrlbuilder.WithPredicates(ctrlpredicate.Funcs{
				// 首次出现的 Lease 用于建立基线而非上报任何事件，它所唤醒的该次调谐负责保存该基线。
				CreateFunc: func(e ctrlevent.CreateEvent) bool {
					return e.Object.GetNamespace() == kuberess.SystemNamespaceName
				},
				UpdateFunc: func(e ctrlevent.UpdateEvent) bool {
					return kvCacheBackendLeaseHolderChanged(e.ObjectOld, e.ObjectNew)
				},
				// 被删除的 Lease 表示选主已停止，而非发生主备切换。负责清除持有者记录的调谐将由后端
				// 自身的变更唤醒。
				DeleteFunc:  func(ctrlevent.DeleteEvent) bool { return false },
				GenericFunc: func(ctrlevent.GenericEvent) bool { return false },
			}),
		).
		WithOptions(ctrlcontroller.Options{
			MaxConcurrentReconciles: kvCacheBackendConcurrency,
		}).
		Complete(r)
}

// kvCacheBackendConcurrency 指定该控制器同时调谐的后端数量。
//
// 设置该并发度的原因在于，这是此处唯一会阻塞在集群外部操作上的调谐器：每个后端都会
// 按定时器重新入队，且每次调谐都会向对象指定的地址发起三次串行读取——对于外部后端，
// 该地址完全不受本算子控制。在默认的单 worker 模式下，只要有一个端点在建立连接后
// 阻塞，就会在每次调谐中占用队列直至超时，导致其他所有后端（包括正在删除的后端）
// 都在其后排队等待。
//
// 单请求超时无法替代并发控制。它仅限制单次阻塞的持续时间，无法避免其他后端被其阻塞。
//
// 取值为 4 而非更大值：该工作每次仅针对不同主机发起一次 HTTP 读取，因此目的在于防止
// 少数异常地址独占队列，而非追求高吞吐量。
const kvCacheBackendConcurrency = 4

// kvCacheBackendLeaderPodPredicate 将 Pod 的 watch 范围限制为本 operator 的 leader Pod。
//
// 它不能像另外三个 watch 那样按资源标记过滤：Pod 由 Deployment 的控制器根据模板创建，
// 而该标记位于 Deployment 上，而非模板内部。leader Pod 上确实带有身份标签——而伪造标签
// 比伪造标记更容易，因此这里的 Namespace 同样起着关键的校验作用。
func kvCacheBackendLeaderPodPredicate() ctrlpredicate.Predicate {
	return ctrlpredicate.NewPredicateFuncs(func(obj ctrlcli.Object) bool {
		return obj.GetNamespace() == kuberess.SystemNamespaceName &&
			mooncake.LeaderPodBackendName(obj.GetLabels()) != ""
	})
}

// enqueueKVCacheBackendWhenLeaderPodChanged 将 leader Pod 映射回其所属的 backend。
func (r *KVCacheBackendReconciler) enqueueKVCacheBackendWhenLeaderPodChanged(
	_ context.Context, obj ctrlcli.Object,
) []ctrlreconcile.Request {
	name := mooncake.LeaderPodBackendName(obj.GetLabels())
	if name == "" {
		return nil
	}
	return []ctrlreconcile.Request{{NamespacedName: ctrlcli.ObjectKey{Name: name}}}
}
