package mooncake

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/systemmeta"
	"gpustack.ai/gpustack/pkg/systemname"
	"gpustack.ai/gpustack/pkg/utils/stringx"
	"gpustack.ai/gpustack/pkg/worker/kuberess"
	"gpustack.ai/gpustack/pkg/worker/kvcache"
)

const (
	// memberResourceNoteRole 用于将 member 组的对象与 leader 的对象区分开来。
	memberResourceNoteRole = "member"

	// memberEntrypoint 是镜像自身的控制台脚本名称，经实测确认而非臆测。它负责
	// 构建存储、挂载段并随后提供 HTTP API 服务；其 main() 会安装停机信号处理器，
	// stop() 会关闭存储，因此在收到 SIGTERM 时会自行卸载。
	memberEntrypoint = "mc_store_rest_server"

	// memberRESTPortBase 是首个组的入口进程提供 HTTP API 的端口，也是该入口
	// 自身的默认端口。后续组会在此基础上偏移，原因参见 memberRESTPort。
	memberRESTPortBase int32 = 8080

	// RDMADevicePath 是宿主机上的设备树路径，任何 member 都不会挂载它。每种宿主机
	// 网络结构的设备插件都会在容器内的同一路径下注入其分配的每个设备的 verbs 节点：
	// RDMA 由本 operator 自身的 Device Manager 注入，EFA 则由 AWS 的插件注入。
	//
	// 在该分配之外再挂载整个设备树不仅多余，还会破坏部分设备分配场景。设备树会把
	// 节点上的所有其他适配器都带入仅被授予其中部分设备的容器中，而 verbs 库通过查找
	// 设备节点来枚举设备，因此未授予的设备也会被列出。打开未授予设备会被设备 cgroup
	// 以 EPERM 拒绝，而 libfabric 的 EFA provider 在遇到第一个 EPERM 时就会放弃
	// 整个设备列表：被授予的 EFA 设备少于节点总数的 member 将完全无法初始化网络结构，
	// 而不挂载该路径的同一 member 则能正常启动。
	//
	// 因此，对于未注入任何设备的分配器，把设备树挂载回来也无济于事：挂载只是让设备
	// 可见，只有设备资源请求才能让设备可被打开。
	//
	// 将其导出是因为准入控制必须拒绝会落到已注入设备节点路径上的声明式挂载：两者最终
	// 位于同一容器内，此处的路径冲突将由容器运行时静默处理，而不会由本 operator 报错。
	RDMADevicePath = "/dev/infiniband"

	// MemberPodSpecHashAnnotation 保存 member 的 Pod 模板（不含节点选择器）的指纹。
	// 它附着在模板上，因此 DaemonSet 创建的每个 Pod 都会继承它，协调器借此可以区分
	// 按当前规约构建的 Pod 与变更前构建的 Pod。
	MemberPodSpecHashAnnotation = "kvcache." + systemname.LabelPrefix + "pod-spec-hash"

	// memberShutdownSeconds 是入口进程自行停机所需的时间：关闭其存储并让 leader
	// 感知到客户端离开，而不是在停机中途被强行截断，导致 leader 只能等其 client_ttl
	// 超时后才判定客户端离线。
	//
	// 它不是内存段的排空窗口，这里也不会渲染任何内存卸载钩子。
	//
	// member 自身的 API 无法卸载本渲染器为其分配的段，这一事实比本注释过去所写的
	// 理由更具决定性：MOONCAKE_GLOBAL_SEGMENT_SIZE 所请求的段由客户端自身的 setup()
	// 挂载，而该方法既未将其记录在 /api/unmount 查询的记录集中，也未记录在另一个
	// 卸载路由查询的记录集中。基于 mooncake 0.3.13 的实测结果：
	//
	//   - /api/unmount 返回 500，报错 "segment_id not found in allocated records"
	//     (real_client.cpp:1791)，/api/unmount_shm 返回 500，报错 "not found in mounted
	//     records" (real_client.cpp:1626)。
	//   - 无论尝试多长的宽限期，针对 leader 自身为该段发布的 id，两个接口均在
	//     3 毫秒内返回上述错误。
	//   - 同一个 /api/unmount 对通过 /api/mount 挂载的段会返回 200，说明被拒绝是
	//     因为记录集边界所致，而非传错了 id。
	//
	// 因此，身份识别从来不是阻塞点。先前的理由——即 host-network 模式的 member 无法
	// 获知自身客户端 id——描述的是一个根本不会触及的问题：即便 member 准确持有属于
	// 自己的段的正确 id，请求同样会被拒绝。任何将 member 与段进行匹配的逻辑都无法
	// 改变这一点，因此本仓库侧做再多工作也无法实现能够排空数据的钩子。另见
	// renderMemberEnv，了解此次实测同时促成的段名称修正。
	//
	// 带有本地磁盘层的组会在此基础上叠加其缩容宽限期，而非与之共享时间，从而避免
	// kubelet 在规约要求的等待过程中提前杀死容器。参见 memberTerminationGracePeriodSeconds。
	memberShutdownSeconds int64 = 60

	// memberProtocolRDMA 和 memberProtocolEFA 是访问宿主机网络结构的两个已解析协议，
	// 采用存储自身的拼写而非 API 层的拼写。它们定义为常量，是因为有两处逻辑必须对
	// 这对值保持一致——宿主机网络结构判断谓词与据此派生的资源——若出现 "efa" 的
	// 第三种拼写，二者就会产生偏离。
	memberProtocolRDMA = "rdma"
	memberProtocolEFA  = "efa"

	// efaDeviceResource 是 AWS EFA 设备插件通告的扩展资源名称。EFA 是 AWS 专有
	// 网络结构，其插件只通告这一个资源名，因此协议即可直接确定资源族。该插件按每个
	// EFA 接口通告 1 个单位资源，因此请求数量超过节点拥有数量的 member 将保持 Pending。
	//
	// 正是该资源请求使设备可被打开。仅仅挂载 /dev/infiniband 只会把设备节点带入
	// 容器的挂载命名空间而不会产生其他效果：设备 cgroup 仍然会拒绝 open()，即便节点上
	// 文件权限允许所有人访问且容器内为 uid 0，也会返回 EPERM。存储会将此视作无网络
	// 结构的宿主机——它发现 0 个 HCA 并转而安装 TCP 传输，且不报任何错误。插件分配
	// 才会添加对应的 cgroup 规则，而且无需使用原本唯一能绕过该限制的 privileged 标志。
	//
	// 请求该资源也决定了无 EFA 节点的调度表现。此类节点完全不通告该资源，因此 member
	// 根本不会被调度到这些节点上，而不是在规约指定 EFA 的情况下依然启动并退化运行
	// TCP。这种拒绝调度正是期望行为：若集群中的节点无法提供对应网络结构，则后端规约
	// 本就应当声明为 TCP。
	efaDeviceResource core.ResourceName = "vpc.amazonaws.com/efa"

	// memberLocalDiskVolumeName 为承载某个组磁盘层的宿主机目录卷命名。
	memberLocalDiskVolumeName = "local-disk"

	// memberHostPathVolumeNamePrefix 是为声明的宿主机路径提供支撑的卷名称前缀，
	// 后面会拼接该条目在列表中的位置索引。
	//
	// 使用位置索引而非路径本身，是因为由用户输入派生的名称可能与上方的卷名冲突，
	// 且 DNS 标签本身也无法容纳路径字符串。位置索引在其他所有地方都已经唯一标识了
	// 各条目，因此这种映射无需额外状态即可记住。
	memberHostPathVolumeNamePrefix = "host-path"

	// MemberLocalDiskSurveyContainerName 是用于上报磁盘层目录中已有数据的 init 容器名称。
	// 控制器按此名称查找其检测结果，因此二者是同一事实，而非同一概念的两种拼写。
	MemberLocalDiskSurveyContainerName = "local-disk-survey"

	// memberUnmountLocalDiskPath 是入口进程自身的路由，用于在进程退出前注销该存储的
	// SSD 层。master 会停止将该存储列为其已卸载键的持有者，从而让读取方得到干净的
	// 未命中结果，而不是去请求一个即将消失的对端节点。
	//
	// 该调用随后会保持等待直到宽限期结束，而这段等待仅仅维持进程存活——并不会让
	// 磁盘层继续保持可读。注销会立即生效，因此不会排空任何数据；上游文档注释的说法
	// 与此相反，并已被实测证伪。
	//
	// 该接口不需要段 id，这也是此处存在该路由的原因。内存卸载需要段 id，而本渲染器
	// 为 member 分配的段根本无法向内存卸载接口指明——实测详情参见 memberShutdownSeconds。
	memberUnmountLocalDiskPath = "/api/unmount_local_disk"

	// MemberMaxGracePeriodSeconds 是入口进程对该调用设置的时间上限。超过该值时
	// 处理程序会返回 400，因此更大的值会渲染出一个每次执行必失败的钩子。
	//
	// 将其导出是因为准入控制负责拒绝超限值，而这条规则应当放在它所保护的路由旁边，
	// 而不是在另一个包里重复写成一个数字。
	MemberMaxGracePeriodSeconds = 3600

	// memberHookTimeoutMarginSeconds 是停机钩子在放弃自身请求之前，比宽限期额外
	// 多等待的时长。
	//
	// 该值有意设置得远小于 memberShutdownSeconds。Pod 的终止时间预算按顺序涵盖
	// 钩子执行和入口进程停机两部分，因此钩子在宽限期之后多消耗的每一秒，都会从
	// 随后的关闭阶段中扣除。
	memberHookTimeoutMarginSeconds int64 = 10
)

// member 的环境变量配置。客户端读取的每个配置项都有真实对应的命名变量，因此整个
// CONFIGURATION 均渲染为环境变量：不使用 ConfigMap，也不挂载或准备任何用于配置
// member 的文件。
//
// 声明了磁盘层的组确实会携带一个卷和一个 init 容器，但两者都不负责配置任何参数——
// 一个是磁盘层本身，另一个仅上报其中已存在的内容。这里的表述针对的是配置来源，
// 之所以这样写，是因为不加限定的表述在 member 没有磁盘层时成立，而在引入磁盘层后
// 不再严格成立。
const (
	// memberEnvMetadataServer 在 META_DATA 中带有一个下划线。它不是
	// MOONCAKE_TE_METADATA_SERVER；将其规范化为看起来更顺眼的拼写并不会报错——
	// 而是会静默地导致元数据平面降级。单元测试会按字节断言该名称。
	memberEnvMetadataServer = "MOONCAKE_TE_META_DATA_SERVER"
	// memberMetadataServerValue 是元数据平面无条件采用的字面量值：该作用域
	// 提供的是 peer-to-peer 平面，因此没有需要指向的存储。
	memberMetadataServerValue = "P2PHANDSHAKE"

	memberEnvMaster            = "MOONCAKE_MASTER"
	memberEnvProtocol          = "MOONCAKE_PROTOCOL"
	memberEnvLocalHostname     = "MOONCAKE_LOCAL_HOSTNAME"
	memberEnvGlobalSegmentSize = "MOONCAKE_GLOBAL_SEGMENT_SIZE"
	memberEnvLocalBufferSize   = "MOONCAKE_LOCAL_BUFFER_SIZE"

	// 厂商设备可见性环境变量，在主机内存组上设为 "void"，从而不向其中注入任何加速器。
	//
	// "void" 不同于 "none"：它通知厂商的容器运行时 hook 完全不要介入该容器，而不是
	// 注入一个恰好为空的设备列表，从而避免驱动库也被引入容器。
	//
	// 这里会一次性设置全部厂商的变量，而不询问 Node 上实际搭载了什么设备。一个组的介质取决于
	// 其声明而非调度落点，且该 Operator 管理的 Node 可能搭载其中任意一种设备；若仅渲染匹配
	// 当前 Node 的集合，会导致同一份 spec 在不同主机上生成不同的容器。
	//
	// 这五个是目前运行时确实会读取可见性变量的厂商。其余厂商（Ascend、Hygon、Metax、T-Head）
	// 则是通过显式挂载的 DevicePlugin 分配来访问设备，未请求设备的容器绝不会收到这类分配。
	memberEnvVisibleDevicesAMD       = "AMD_VISIBLE_DEVICES"
	memberEnvVisibleDevicesCambricon = "CAMBRICON_VISIBLE_DEVICES"
	memberEnvVisibleDevicesIluvatar  = "IX_VISIBLE_DEVICES"
	memberEnvVisibleDevicesMThreads  = "MTHREADS_VISIBLE_DEVICES"
	memberEnvVisibleDevicesNVIDIA    = "NVIDIA_VISIBLE_DEVICES"

	// memberVisibleDevicesNone 是上述各个变量在主机内存组上取的值。
	memberVisibleDevicesNone = "void"

	// memberEnvTransferMetrics 用于开启传输引擎自身的指标上报，该功能在构建产物中默认关闭，
	// 且是获取成员吞吐量与任务延迟分布的唯一来源。leader 的 Prometheus 接口统计的是键数与字节数，
	// 并不度量数据平面，因此若不开启此项，每次传输的接收端都将无法度量。
	//
	// 引擎侧已经设置了该变量，此处同样设置是为了让单次传输的两端保持对称，而不会给池子
	// 带来原本不存在的开销。
	//
	// 该上报输出到容器日志而非端口：每个时间间隔输出一行，若该间隔内未传输任何数据则完全不输出。
	memberEnvTransferMetrics   = "MC_TE_METRIC"
	memberTransferMetricsValue = "1"

	// 本地磁盘层的配置键。前两个对应客户端自身的 enable_ssd_offload 与 ssd_offload_path；
	// 缺少任意一个，客户端都不会注册本地磁盘段，leader 的卸载队列也将无处下发数据。
	memberEnvOffloadEnabled = "MOONCAKE_OFFLOAD_ENABLED"
	memberEnvOffloadPath    = "MOONCAKE_OFFLOAD_FILE_STORAGE_PATH"
	// memberEnvOffloadSizeLimit 和 memberEnvOffloadKeysLimit 分别按字节数和键数量限制
	// 该层级可存储的上限。若不渲染，则使用客户端自身的上限值，这样当上游调整默认上限时，
	// 变更会作为需要排查的变化显现出来，而不是被本渲染器静默覆盖重写。
	memberEnvOffloadSizeLimit = "MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES"
	memberEnvOffloadKeysLimit = "MOONCAKE_OFFLOAD_TOTAL_KEYS_LIMIT"

	// memberEnvOffloadBucketSizeLimit 和 memberEnvOffloadBucketKeysLimit 是用于判定关闭
	// 一个 bucket 的阈值对。其渲染原因见 MemberBucketSizeLimit 的说明。
	memberEnvOffloadBucketSizeLimit = "MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES"
	memberEnvOffloadBucketKeysLimit = "MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT"

	// memberEnvOffloadBucketMaxTotalSize 是下方水位线所占比例对应的配额基数；尽管本渲染器
	// 会将同一个值同时写入这两项，但它与 memberEnvOffloadSizeLimit 是不同的配置项。
	// 其自身默认值为 0，而客户端的水位线驱逐逻辑会将 0 视作"无配额"并直接返回而不执行任何驱逐——
	// 因此若渲染了水位线却未渲染该项，就会落入被关闭的执行路径，使水位线看似已配置实则毫无作用。
	memberEnvOffloadBucketMaxTotalSize = "MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE"

	// memberEnvOffloadEvictionPolicy 指定 bucket 淘汰出磁盘层的顺序。
	//
	// 客户端会将无法识别的字符串静默映射为 NO EVICTION，既不返回 error 也不会启动失败——
	// 因此本渲染器仅发送来自 memberEvictionPolicies 的值，绝不传入从外部直接透传来的字符串。
	memberEnvOffloadEvictionPolicy = "MOONCAKE_OFFLOAD_BUCKET_EVICTION_POLICY"

	// 水位线驱逐开关及其两个水位阈值。该开关位于落盘格式之上，而上述策略位于其内部，
	// 这就是关闭驱逐时需要同时渲染这两者的原因。
	memberEnvOffloadWatermarkEnabled = "MOONCAKE_OFFLOAD_ENABLE_DISK_WATERMARK_EVICTION"
	memberEnvOffloadWatermarkHigh    = "MOONCAKE_OFFLOAD_DISK_EVICTION_HIGH_WATERMARK_RATIO"
	memberEnvOffloadWatermarkLow     = "MOONCAKE_OFFLOAD_DISK_EVICTION_LOW_WATERMARK_RATIO"
)

// MemberBucketSizeLimit 和 MemberBucketKeysLimit 是客户端关闭一个 bucket 并将其写入
// 磁盘层的大小与对象数量阈值。它们会被显式渲染，而非保留客户端默认值——这是本包
// "不渲染等于其自身默认值的配置"规则的例外，也是唯一不应遵循该规则的地方。
//
// 客户端会将卸载的对象拼装进一个 bucket，在达到上述两个阈值之一前完全不会写入；
// 未达到阈值的内容会无限期滞留到下一次尝试。客户端自身的默认阈值（256Mi 和 500 个对象）
// 是按高负载生产存储规模设定的，而一个从未触及任一阈值的后端会出现这样的状态：磁盘层已挂载、
// 已上报容量、驱逐也在运行，但其中却没有任何数据。没有任何信号会报告这一情况：运维人员
// 能看到的每项指标反映的都是声明值而非实际观测值。
//
// 因此，放任这些默认值不管并不会保留一个日后可排查的默认行为，而是交付了一个无法工作的特性。
// 下面这组值足够小，使中等规模的后端也能正常关闭 bucket，代价仅仅是产生更多更小的文件，
// 而不会影响任何正确性依赖。
//
// 将它们导出是因为准入校验会依据这两个阈值来限制三个字段——组的 capacityPerMember，以及
// 其磁盘层的 capacity 和 keyLimit——且这些边界与这组值表达的是同一个事实，而非在旁另行复述的数字。
const (
	MemberBucketSizeLimit int64 = 16 * 1024 * 1024
	MemberBucketKeysLimit int64 = 64
)

// memberEvictionPolicies 将此 API 对驱逐策略的命名与客户的命名进行对应。这是两个术语交汇的地方，就像另一侧
// 的 leaderAllocationStrategies 一样。
//
// memberEvictionPolicyNone 不在映射中，因为它不是 API 值：它是“驱逐功能关闭”所生成的名称，也是客户端用于表
// 示该状态的自定义名称。
var memberEvictionPolicies = map[string]string{
	"FIFO": "fifo",
	"LRU":  "lru",
}

const memberEvictionPolicyNone = "none"

// memberProtocols 将本 API 中的传输协议拼写映射为构建产物的拼写。
//
// Auto 会映射为构建产物的 tcp，而非根据 Node 向上解析，原因正如 API 类型自身的注释所述：
// 一个 DaemonSet 覆盖其组选中的所有 Node，因此无法携带按 Node 区分的传输协议，而且自动提升为
// RDMA 会授予无人请求的 hostNetwork 和两项 capabilities。构建产物自身也没有 "auto"——
// 它会在传输映射表中查找协议字符串，因此原样渲染该字面量将找不到任何条目。
//
// CANN 和 ROCM 是仅有的两个在构建产物中不只是大小写差异的条目：API 命名的是成员镜像
// 所基于的工具链（与已发布的镜像标签一致），而构建产物的传输映射表识别的是厂商自身的
// 协议字符串（ascend 和 hip）。其余条目仅存在大小写差异，但这也足以使按字符串比较
// API 值与渲染值时出现本不存在的差异。
// MemberProtocolAuto 是 schema 默认采用的传输协议，在此命名是因为渲染器会在处理
// 从未经过 API server 的对象时回退到该值。
const MemberProtocolAuto = "Auto"

var memberProtocols = map[string]string{
	MemberProtocolAuto: "tcp",
	"TCP":              "tcp",
	"RDMA":             "rdma",
	"EFA":              "efa",
	"CANN":             "ascend",
	"ROCM":             "hip",
	"MUSA":             "musa",
	"MACA":             "maca",
}

// TransportProtocol 将 KVCacheBackend 的传输协议值映射为 Mooncake 的拼写。空值会
// 解析为 Auto（渲染为 tcp）；未知值返回空字符串。
func TransportProtocol(protocol string) string {
	if protocol == "" {
		protocol = MemberProtocolAuto
	}
	return memberProtocols[protocol]
}

// memberProtocolIsHostFabric 报告一个已解析的协议是否属于访问主机 fabric 的那两种协议：
// 即需要启用 hostNetwork、获取两项 capabilities 并分配设备的协议。
// 它接收 MemberProtocol 的输出，而非 API 中的拼写。
//
// 只有 rdma 和 efa 属于此类。musa 和 maca 是节点内 IPC 传输，而非主机 fabric，
// 其余协议则从不离开成员自身的网络命名空间，因此它们都不会获得 hostNetwork、
// capabilities 或设备。
//
// 这两种协议都会触发设备请求。生效的协议决定其资源族，而成员的接口数量决定其请求数量
// 与 RDMA 分配模式，因此该谓词同时也回答了"是否授予设备"。两者均不挂载设备树；
// 原因见 RDMADevicePath。
//
// 将其设为非导出函数，是为了让这一协议对仅在两处声明，两处均位于本文件且均完全覆盖它——
// 即此处与 FabricDeviceResource。若在其他任何地方出现第二套拼写，当未来加入第三种
// fabric 时两者便可能产生漂移。准入逻辑通过 MemberGroupIsHostFabric 询问同一问题，
// 该函数会委托至此。
func memberProtocolIsHostFabric(protocol string) bool {
	return protocol == memberProtocolRDMA || protocol == memberProtocolEFA
}

// MemberGroupIsHostFabric 报告单个成员组的生效协议是否为 RDMA 或 EFA，
// 此时由该组的 fabric 接口数量决定设备请求。
func MemberGroupIsHostFabric(kvcb *workercore.KVCacheBackend, group workercore.KVCacheBackendMember) bool {
	return memberProtocolIsHostFabric(MemberProtocolForGroup(kvcb, group))
}

// MemberObjectName 是为单个成员组渲染的对象的名称。
//
// 名称中包含组的索引，是因为组本身没有名称，且 spec 中的列表本来就是通过位置来标识它的。
// 因此，第二个组会对应第二个 DaemonSet，而非重命名当前这一个。
func MemberObjectName(kvcb *workercore.KVCacheBackend, group int) string {
	return fmt.Sprintf("%s-member-%d", kvcb.Name, group)
}

// MemberSelectorLabels 既用于 DaemonSet 选择其 Pod，也用于控制器列出该后端的成员 Pod。
//
// DaemonSet 的 spec.selector 是不可变的，与 Deployment 的 spec.selector 一样。它仅携带身份信息——即是哪个后
// 端、哪个角色、哪个组——而从不包含组的 nodeSelector，因为扩大组的范围是预期中的，否则会使该对象无法使用。
func MemberSelectorLabels(kvcb *workercore.KVCacheBackend, group int) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":      "kv-cache-backend",
		"app.kubernetes.io/instance":  kvcb.Name,
		"app.kubernetes.io/component": fmt.Sprintf("member-%d", group),
	}
}

func memberPodLabels(kvcb *workercore.KVCacheBackend, group int) map[string]string {
	labels := MemberSelectorLabels(kvcb, group)
	labels["app.kubernetes.io/part-of"] = "gpustack-operator-worker"
	return labels
}

// MemberProtocol 返回某个组解析后的传输协议，采用构建产物自身的拼写。未识别的值
// 不会渲染任何内容而非猜测：schema 已枚举了该字段，因此返回空结果意味着该对象
// 从未经过准入校验。
func MemberProtocol(kvcb *workercore.KVCacheBackend) string {
	return TransportProtocol(kvcb.Spec.Transport.Protocol)
}

// MemberProtocolForGroup 返回单个成员组解析后的传输协议，采用构建产物自身的拼写：
// 当组自身声明了 transport.protocol 时使用该值，否则使用后端的取值——并沿用其对
// Auto 与空值的处理逻辑。若组携带了不含 protocol 的 transport 块，同样会继承后端的值，
// 这是从未经过 API server 默认值填充的对象所呈现的形态。
func MemberProtocolForGroup(kvcb *workercore.KVCacheBackend, group workercore.KVCacheBackendMember) string {
	if group.Transport != nil && group.Transport.Protocol != "" {
		return TransportProtocol(group.Transport.Protocol)
	}
	return MemberProtocol(kvcb)
}

// MemberProtocols 按组声明顺序返回池子后端向引擎提供的协议列表：即每个成员组的
// 生效协议；对于未声明任何组的后端（即其存储由外部描述的外部后端），则仅返回
// 后端级的值。返回的切片永不为空，因此基于它进行匹配的调用方无需判断是否提供了协议。
func MemberProtocols(kvcb *workercore.KVCacheBackend) []string {
	managed := kvcb.Spec.Connection.Managed
	if managed == nil || len(managed.Members) == 0 {
		return []string{MemberProtocol(kvcb)}
	}

	offers := make([]string, 0, len(managed.Members))
	for i := range managed.Members {
		offers = append(offers, MemberProtocolForGroup(kvcb, managed.Members[i]))
	}
	return offers
}

// RenderMemberDaemonSet 为指定成员组生成一个 DaemonSet。
//
// 成员贡献所在节点的内存、磁盘或设备，因此按节点部署。每个成员组独立使用 DaemonSet，同一节点可同时运行 DRAM
// 组和 VRAM 组的各一个成员。Deployment 加反亲和只能近似这种放置约束。
//
// 与 leader 一样，调用方传入后端镜像，成员组可显式覆盖它。不同组可能选择不同型号的加速卡，需要匹配硬件的客
// 户端构建。介质和传输协议也按组读取。
func RenderMemberDaemonSet(
	kvcb *workercore.KVCacheBackend, group int, image string,
) *apps.DaemonSet {
	member := kvcb.Spec.Connection.Managed.Members[group]
	if member.Image != "" {
		image = member.Image
	}

	ds := &apps.DaemonSet{
		ObjectMeta: meta.ObjectMeta{
			Name:      MemberObjectName(kvcb, group),
			Namespace: kuberess.SystemNamespaceName,
			Labels:    memberPodLabels(kvcb, group),
		},
		Spec: apps.DaemonSetSpec{
			Selector: &meta.LabelSelector{MatchLabels: MemberSelectorLabels(kvcb, group)},
			// 使用 OnDelete，让控制器选择哪些 Pod 需要重启。节点选择器属于 Pod 模板，如果使用
			// RollingUpdate，仅增加一个匹配节点就会改变模板哈希，使其余节点也重启并重新挂载内存
			// 段。Deployment 和 StatefulSet 的模板也有这个特性。
			//
			// OnDelete 不会自动应用镜像更新。因此控制器给影响成员进程的配置计算指纹，只删除指纹已变化的 Pod。
			UpdateStrategy: apps.DaemonSetUpdateStrategy{
				Type: apps.OnDeleteDaemonSetStrategyType,
			},
			Template: core.PodTemplateSpec{
				ObjectMeta: meta.ObjectMeta{Labels: memberPodLabels(kvcb, group)},
				Spec: core.PodSpec{
					// DaemonSet 在每个匹配成员组选择器的节点上运行一个成员。扩大选择范
					// 围时，可以增加新成员并保留原有节点上的 Pod。
					NodeSelector:                  member.NodeSelector,
					TerminationGracePeriodSeconds: ptr.To(memberTerminationGracePeriodSeconds(kvcb, member)),
					// 显式写出默认 DNS 策略以便双向收敛。从使用主机网络的互联切回 TCP
					// 时，需要把 ClusterFirstWithHostNet 还原。
					DNSPolicy: core.DNSClusterFirst,
					// 关闭 HA 时，成员只访问 leader。显式关闭默认的令牌挂载，避免给第三
					// 方镜像提供多余的 Kubernetes 凭据，尤其是使用主机互联的 Pod 已持有
					// 两项 Linux capabilities。
					//
					// 启用 HA 后，默认通过 Service 寻址时用不到令牌，但显式使用 Lease
					// 地址时要用，因此仍为成员挂载对应账号的凭据。
					AutomountServiceAccountToken: ptr.To(leaderNeedsAPIAccess(
						kvcb.Spec.Connection.Managed.Leader,
					)),
					ServiceAccountName: memberServiceAccountName(kvcb),
					ImagePullSecrets:   kvcb.Spec.ImagePullSecrets,
					Containers: []core.Container{
						memberContainerSpec(kvcb, member, group, image),
					},
				},
			},
		},
	}

	// 成员组未指定 FabricInterfaceCount 时按一个互联接口处理。schema 没有存储默认值，省略与显式写 1 应
	// 产生相同的 Pod 模板。
	applyMemberFabric(ds, MemberProtocolForGroup(kvcb, member), ptr.Deref(member.FabricInterfaceCount, 1))
	applyMemberLocalDisk(ds, kvcb, member, group)
	applyMemberHostPaths(ds, member)
	// 最后应用这项配置，以便在互联配置已经设置的安全上下文上合并所需权限。
	applyMemberSecurityContext(ds, member)

	if member.RuntimeClassName != "" {
		ds.Spec.Template.Spec.RuntimeClassName = ptr.To(member.RuntimeClassName)
	}

	// 在完整模板上计算指纹，确保覆盖刚设置的互联字段。计算时从副本移除节点选择器和指纹注解本身，避免指纹依赖自身。
	ds.Spec.Template.Annotations = map[string]string{
		MemberPodSpecHashAnnotation: MemberPodSpecHash(ds.Spec.Template),
	}

	systemmeta.NoteResource(ds, kvcache.ResourceType, map[string]string{
		kvcache.ResourceNoteBackend: kvcb.Name,
		"role":                      memberResourceNoteRole,
		"group":                     strconv.Itoa(group),
	})
	kubemeta.ControlOnWithoutBlock(ds, kvcb,
		workercore.SchemeGroupVersion.WithKind("KVCacheBackend"))

	return ds
}

// memberContainerSpec 构造单个成员的容器配置。
func memberContainerSpec(
	kvcb *workercore.KVCacheBackend,
	member workercore.KVCacheBackendMember,
	group int,
	image string,
) core.Container {
	return core.Container{
		Name:            "member",
		Image:           image,
		ImagePullPolicy: kvcache.EffectivePullPolicy(kvcb, image),
		// 缺少运输组件厂商运行时环境的图像——由于需要调用 CANN——会在动态链接器中的错误输出中崩溃，而
		// 默认策略无法处理此情况。
		TerminationMessagePolicy: core.TerminationMessageFallbackToLogsOnError,
		Command:                  []string{memberEntrypoint},
		Args:                     renderMemberArgs(member, group),
		Env:                      renderMemberEnv(kvcb, member),
		// 任何路径上都不存在数据平面 containerPort。传输引擎会随机绑定其端口——有一次测试中出现了
		// 15002 和 15995，另一次客户端出现了 16566 和 16655，而这些都是未配置的端口——因此此处使用固
		// 定列表会是对该过程的错误描述。可达性要求属于端口范围，文档也将其列为一种要求。
		//
		// 入口点会在服务此端口之前挂载其内存段，因此连接能够证明是挂载行为，而非仅仅是进程。若没有
		// 这一点，kubelet 会在容器启动后立即报告成员已准备就绪，而主节点尚未将其列入——这会被控制器
		// 视为一种不足。TCP 是因为 API 仅支持 /api/* 数据操作动词：没有密钥或副作用的话，就无法进行 GET 请求。
		ReadinessProbe: &core.Probe{
			ProbeHandler: core.ProbeHandler{
				TCPSocket: &core.TCPSocketAction{Port: intstr.FromInt32(memberRESTPort(group))},
			},
			PeriodSeconds:    5,
			FailureThreshold: 3,
		},
		Resources: memberResources(member),
	}
}

// MemberMasterEntry 默认会为成员分配主节点 Service 地址。若选择显式的 Lease 选项，则在 HA 运行时将为客户端
// 提供 Lease 坐标。该 Service 仅发布已准备好的端点，因此备用节点不会作为推理服务主节点进行响应。
func MemberMasterEntry(kvcb *workercore.KVCacheBackend) string {
	leader := kvcb.Spec.Connection.Managed.Leader
	if leaderNeedsAPIAccess(leader) && leader.MemberAddressing == MemberAddressingLease {
		return fmt.Sprintf("k8s://%s/%s", kuberess.SystemNamespaceName, LeaderObjectName(kvcb))
	}

	return fmt.Sprintf("%s:%d", LeaderServiceHost(kvcb), LeaderRPCPort)
}

// renderMemberEnv 构建成员的完整配置，最后是组的 extraEnv。
//
// 未设置的项是有意为之。MOONCAKE_DEVICE 保持未设置状态，因此客户端的设备筛选器为空，这被解读为“所有设备”——
// 请参考 keys.go 了解为何文档中所述的 "auto-discovery" 字符串是一个陷阱。MOONCAKE_TENANT_ID 则保持其默认值，
// 因为配额规范中规定了租户关系，此处的数值会优先于该规范。
func renderMemberEnv(
	kvcb *workercore.KVCacheBackend, member workercore.KVCacheBackendMember,
) []core.EnvVar {
	env := []core.EnvVar{
		{Name: memberEnvMetadataServer, Value: memberMetadataServerValue},
		{
			Name:  memberEnvMaster,
			Value: MemberMasterEntry(kvcb),
		},
		{Name: memberEnvProtocol, Value: MemberProtocolForGroup(kvcb, member)},
		{
			// 这是主节点传递给客户端的地址，不仅仅是一个标签：它成为内存段 te_endpoint 的宿主部
			// 分。传输引擎将其数据端口绑定在 Pod 的网络命名空间中，因此这里的节点名称所代表的端
			// 口，实际上没有任何节点监听——在双节点集群中，一个客户端 Pod 因节点名称和节点 IP 均
			// 被拒绝连接，最终通过 Pod IP 进行了连接。
			//
			// 它成为内存段名称的半部分，而非全部。此评论的早期版本声称名称就是该字符串的纯文本
			// 形式，且每次启动时新的端口仅属于 te_endpoint。与 mooncake 0.3.13 相比，这两点都是
			// 错误的：主节点将 segment_name 发布为“<此值>:<端口>”，而重启过程中不仅移动了该端口，
			// 还把 te_endpoint 的端口移动到同一次重启中的不同值。存在两个新的端口，而非一个，其
			// 中一个位于名称中。
			//
			// 这对任何在上面进行匹配的操作者来说都是一种负担：将 segment_name 与这个值进行比较
			// 时什么也没有找到，而找不到任何东西则意味着无响应。需要按名称选择的调用者必须比较
			// 主机部分，而主节点自身的列表已经将这部分区分出来了。
			//
			// 有限制：这个名称的唯一性取决于此值。在主机-织物路径上，Pod 会持有主机的网络命名空
			// 间，因此一个节点上的每个成员组都会报告相同的主机状态——这是基于实际情况报告的，而非猜测。
			Name: memberEnvLocalHostname,
			ValueFrom: &core.EnvVarSource{
				FieldRef: &core.ObjectFieldSelector{FieldPath: "status.podIP"},
			},
		},
		{Name: memberEnvTransferMetrics, Value: memberTransferMetricsValue},
	}

	if member.Medium != "VRAM" {
		// 主机-内存组被远离加速器，这就是使所声明的介质能够与任何事物结合的原因。
		//
		// 内存段实际位于主机内存还是设备显存，取决于镜像的编译方式。若容器运行时默认暴露加速器，支
		// 持显存段的构建会使用它，即便成员声明的是 DRAM。此时 Pod 没有申请加速器，调度和配额均未登
		// 记该用量，真正申请这张卡的工作负载可能因显存不足无法启动。
		//
		// 隐藏这些设备能确保两种结果都真实有效。能够使用主机内存的图像可以正常运作。无法启动的图像
		// 会明显地违背那些要求其执行但未实际实施的任务的群体——这种错误就出现在该位置，因为图像是否
		// 使用了某种媒介是关于该图像的一个事实。
		env = append(env,
			core.EnvVar{Name: memberEnvVisibleDevicesAMD, Value: memberVisibleDevicesNone},
			core.EnvVar{Name: memberEnvVisibleDevicesCambricon, Value: memberVisibleDevicesNone},
			core.EnvVar{Name: memberEnvVisibleDevicesIluvatar, Value: memberVisibleDevicesNone},
			core.EnvVar{Name: memberEnvVisibleDevicesMThreads, Value: memberVisibleDevicesNone},
			core.EnvVar{Name: memberEnvVisibleDevicesNVIDIA, Value: memberVisibleDevicesNone},
		)
	}
	if size := member.CapacityPerMember.Value(); size > 0 {
		// 两种介质的拼写均相同：在 VRAM 版本中，相同大小的内存段用于设备分配，因为编译时的切换改变
		// 了内存段的分配位置，而非其总大小如何被分配器处理。
		env = append(env, core.EnvVar{
			Name:  memberEnvGlobalSegmentSize,
			Value: strconv.FormatInt(size, 10),
		})
	}
	if size := member.LocalBufferSize.Value(); size > 0 {
		env = append(env, core.EnvVar{
			Name:  memberEnvLocalBufferSize,
			Value: strconv.FormatInt(size, 10),
		})
	}

	// 今天有一个条目构成完整列表，此渲染器会读取第一个条目：准入限制将一组元素归为同一层级，因此第二个
	// 条目则是一个无法被集群承载的形状，而非此处未解决的渲染问题。
	if len(member.LocalDisks) > 0 {
		disk := member.LocalDisks[0]
		// 要么两个键，要么没有。当卸载功能被启用且存在存放字节的位置时，客户端才会注册一个本地磁盘
		// 内存段；因此，没有开关的路径，或者没有路径的开关，都是不会包含任何层的成员，尽管对象声称包含一层。
		//
		// 那个桶对会无条件地随它们一起行动：正是它使得这一层级能够获得一个字节，因此它和路径一样，
		// 也是定义该层级的一部分。
		env = append(env,
			core.EnvVar{Name: memberEnvOffloadEnabled, Value: "true"},
			core.EnvVar{Name: memberEnvOffloadPath, Value: disk.Path},
			core.EnvVar{
				Name:  memberEnvOffloadBucketSizeLimit,
				Value: strconv.FormatInt(MemberBucketSizeLimit, 10),
			},
			core.EnvVar{
				Name:  memberEnvOffloadBucketKeysLimit,
				Value: strconv.FormatInt(MemberBucketKeysLimit, 10),
			})

		if size := disk.Capacity.Value(); size > 0 {
			// 一个数值，两个设置。第一个设置决定了层级的内容；第二个设置则是配额值，而水印值是
			// 该配额的一部分，默认值为驱逐路径所识别的“已关闭”数值。
			env = append(env,
				core.EnvVar{
					Name:  memberEnvOffloadSizeLimit,
					Value: strconv.FormatInt(size, 10),
				},
				core.EnvVar{
					Name:  memberEnvOffloadBucketMaxTotalSize,
					Value: strconv.FormatInt(size, 10),
				})
		}
		if disk.KeyLimit > 0 {
			env = append(env, core.EnvVar{
				Name:  memberEnvOffloadKeysLimit,
				Value: strconv.FormatInt(disk.KeyLimit, 10),
			})
		}

		env = append(env, renderMemberEviction(disk.Eviction)...)
	}

	// 这种退出机制最后被采用，按照书写顺序，同一个规范的两种实现版本在字节层面是完全相同的。它无法覆盖
	// 其上方的任何内容：准入机制拒绝了这个实现方式所生成的名称，这也是确保此处所有变量为单一来源的原因—
	// —Kubernetes 会接受携带一个名称的容器两次，由运行时来决定最终结果。
	for _, e := range member.ExtraEnv {
		env = append(env, core.EnvVar{Name: e.Name, Value: e.Value})
	}

	return env
}

// renderMemberEviction 将该层的驱逐块转换为客户端设置。
//
// 一个空的块会显示“无内容”，因此一个从未处理驱逐问题的后端会运行在字段存在之前的环境，同时也会应用客户端自己的行为。
//
// 关闭驱逐时需要同时设置磁盘格式中的策略和上层水位开关，保证两层都禁用驱逐。启用时客户端已有相应默认值，无需重复渲染。
func renderMemberEviction(eviction *workercore.KVCacheBackendMemberLocalDiskEviction) []core.EnvVar {
	if eviction == nil {
		return nil
	}

	// 模式默认设置为启用，因此这里的 `nil` 表示一个从未经过接纳的对象。将其视为 "on"，可以使得该对象的
	// 渲染结果与模式生成的结果几乎一致，而非完全相同。
	if eviction.Enabled != nil && !*eviction.Enabled {
		return []core.EnvVar{
			{Name: memberEnvOffloadEvictionPolicy, Value: memberEvictionPolicyNone},
			{Name: memberEnvOffloadWatermarkEnabled, Value: "false"},
		}
	}

	var env []core.EnvVar

	// 无法识别的策略会导致无果而终。该字段由架构定义，因此结果为空意味着既没有请求任何策略——在这种情况
	// 下适用客户端自身的默认设置——也意味着该对象从未经过审核。将字符串传递过去则属于更糟糕的失败情况：
	// 客户端会将其未知内容都视为不会发生驱逐，而并未明确说明。
	if mapped, ok := memberEvictionPolicies[eviction.Policy]; ok {
		env = append(env, core.EnvVar{Name: memberEnvOffloadEvictionPolicy, Value: mapped})
	}

	if watermark := eviction.Watermark; watermark != nil {
		env = append(env,
			core.EnvVar{
				Name:  memberEnvOffloadWatermarkHigh,
				Value: memberWatermarkRatio(watermark.High),
			},
			core.EnvVar{
				Name:  memberEnvOffloadWatermarkLow,
				Value: memberWatermarkRatio(watermark.Low),
			})
	}

	return env
}

// memberWatermarkRatio 将百分比转换为客户端可解析的分数形式。
//
// 始终使用两位小数，采用固定格式而非最短格式：客户端通过流提取机制读取值，该机制必须完整消耗整个字符串，对
// 于无法处理的情形则静默回退到自己的默认值——因此仅符合 Go 语法的表示方式是不够的。它还拒绝处理超出范围的值
// （即 (0, 1] 之外），而该字段自带的 1 到 100 的边界限制已经将其包含在内。
func memberWatermarkRatio(percent int32) string {
	return strconv.FormatFloat(float64(percent)/100, 'f', 2, 64)
}

// renderMemberArgs 构建成员的 argv：即该成员在需要移动时使用的 REST 端口；随后 extraArgs 作为入口点自身的按键覆盖参数。
//
// 这里的覆盖配置为“-D key=value”，而在主节点上则为“-key=value”，因为这两个二进制文件接受不同的配置。入口点
// 会在环境设置之后应用这些配置，从而决定最终的结果，这也使得该配置对渲染器来说非常有用，因为渲染器是根据节
// 点特定的事实来生成这些配置的。
//
// 客户端配置对象中未包含的关键项会被静默忽略——入口点使用 hasattr 来保护该赋值操作，只有当缺少“=”符号时才会
// 发出警告。因此，此处出现拼写错误会导致配置无效，且不会产生任何警告。
//
// --port 是此入口点解析器的真实标志，而非配置键，因此无法通过 extraArgs 访问它——使用`-D port=...`会设置名为
// 该名称的配置键，服务器仍会保持原样进行监听。
func renderMemberArgs(member workercore.KVCacheBackendMember, group int) []string {
	var args []string

	// 仅在需要移动端口的情况下才会渲染，而第一组并不在那个位置：基础配置是入口点的默认设置，因此将其传
	// 递至该位置会为当前运行的每个成员增加一个参数，移动指纹，并删除和重新创建所有内容，以让每个成员继
	// 续做它原本在做的事情。
	if port := memberRESTPort(group); port != memberRESTPortBase {
		args = append(args, "--port", strconv.Itoa(int(port)))
	}

	// 按照书写顺序，因此同一个规范的两种实现版本在字节层面是完全相同的，且 DaemonSet 不会发生变化。每个
	// 条目都带有其特定的分号标记；这些分号是此 API 用于表示“一个标志令牌”的装饰符，而入口点自身的覆盖项
	// 则被精简成了这些分号。
	for _, entry := range member.ExtraArgs {
		args = append(args, "-D", strings.TrimLeft(entry, "-"))
	}
	return args
}

// memberRESTPort 是某个组入口点用于提供其 HTTP API 的端口，也是唯一决定该端口的地方：argv、就绪状态探针以及
// preStop 钩子都会调用此端口，因此无法为同一个容器定义多个不同的端口。
//
// 该端口是根据该组的位置确定的，因为在主机-架构路径上，该成员拥有主机的网络命名空间，而 API 会绑定 0.0.0.0。
// 两个节点选择器重叠的组因此会在同一个节点上放置两个主机-网络 Pods，而一个固定的端口则只能让第一个组绑定：
// 第二个组会运行，但永远不会通过其就绪探针，也不会报告任何原因。准入机制无法拒绝这一配对，因为两个选择器是
// 否会相遇取决于该组没有的节点标签——这种冲突仅在部署时存在，因此解决方案就是不存在这种冲突。
//
// 与分配不同，基于位置进行分配才使其稳定：该位置在其他地方已经标识了一个组（其 DaemonSet 名称和选择器标签），
// 因此一个组的端口与其身份一样持久，无需保留任何状态来记录哪个端口被分配到了哪里。
//
// 为什么这次冲突如此安静，值得被保留下来，因为这正是让这项工作值得去做而非仅仅记录的原因：这里没有任何关于
// containerPort 或 hostPort 的声明。如果存在 hostPort，调度器会将其视为节点级资源，导致第二个 Pod 处于待处
// 理状态并带有冲突标记。hostNetwork 加上清单中从未提及的端口，在那种检查中是看不见的——Pod 被放置了，进程无
// 法绑定，唯一的证据就是容器日志。因此，根本不能允许该端口产生冲突。
func memberRESTPort(group int) int32 {
	return memberRESTPortBase + int32(group)
}

// memberResources 声明了成员的权益，以便容量规划能够识别它。不符合要求的成员会处于待处理状态，后端会显示“降
// 级”，这是真实的结果——另一种情况则是成员过度占用其所在的节点。
//
// 该声明所依据的内容遵循了相关介质。DRAM 成员的内存段属于主机内存，因此 capacityPerMember 和
// localBufferSize 都归属于内存。VRAM 成员的内存段属于设备内存，此处没有任何分配行为：主机内存仅承载
// localBufferSize，而设备内存则通过分配获得，没有其他方式。
//
// 没有任何操作会占用它，因为该说法并不成立。在 v0.3.13.post1 上游进行测量：该成员使用一次 cudaMalloc 分配其
// 整个内存段，每次拆分操作都会执行一次（mooncake-store/src/client_buffer_allocation.cpp）；这些拆分操作是为
// 了保持在传输层的注册限制范围内，而非为了跨越不同设备（GetTransportRegistrationLimit, real_client.cpp）；
// 该路径上没有任何操作调用 cudaSetDevice。因此，一个成员的内存段位于某一设备上——无论容器首先看到哪台设备——
// 而节点的其他加速器则不受其影响。如果向调度器请求一个扩展资源，则必须让整个加速器从推理任务中退出，以覆盖
// 该设备内存的极小部分，而该成员无法使用其剩余资源。
//
// 共享设备不单独记入调度配额时，引擎仍会看到设备上的全部显存。管理员需要结合引擎配置的显存使用比例设置
// capacityPerMember，两者的显存不会在此路径上自动隔离。
//
// 磁盘层无论以何种方式都会增加一个主机目录，这完全不在 kubelet 的临时存储计费范围之内——它涵盖容器文件系统、
// emptyDir 卷和日志，而绝不会包含 hostPath。针对该层进行请求会预留一个不受任何约束的额度，随后会使节点成员
// 无法使用该磁盘所在的节点。该文件系统的管理权属于操作员，文档也明确说明了这一点。
func memberResources(member workercore.KVCacheBackendMember) core.ResourceRequirements {
	buffer := member.LocalBufferSize.DeepCopy()

	if member.Medium != "VRAM" {
		buffer.Add(member.CapacityPerMember)
	}

	return core.ResourceRequirements{
		Requests: core.ResourceList{core.ResourceMemory: buffer},
	}
}

// applyMemberFabric 提供互联网络所需的资源，且仅适用于需要这些资源的路径。
//
// RDMA 和 EFA 共享主机-织物基础：hostNetwork，两个能力以及一项扩展资源请求——且永远不享有特权，否则就会为了
// 两项操作而让成员拥有整个节点。
//
//   - 该请求源自协议，并未被明确声明。这两个互联网络中，每个网络都恰好有一个名称，该名称出现在运行此操作系
//     统的每个集群中：EFA 的插件属于 AWS 自身，而 RDMA 的插件则由该操作系统的设备管理器发布。无法获取该名称
//     的节点会处于待处理状态，这就是所期望的失败情况——无法为互联网络提供服务的集群，其后端应显示 TCP，这种
//     描述是在对象中给出的，而非在存储日志中后续发现的。
//   - 没有挂载设备树：插件注入了它分配的节点，而 RDMADevicePath 说明了为何其旁边的树会导致部分分配失效。
//   - libfabric 的 EFA 组件在镜像中运行，因此没有需要挂载的主机安装前缀，也无需设置环境进行渲染。
//   - 其他所有路径，包括解析为 tcp 的 Auto 路径，都保持原样：没有安全上下文，而不是空上下文，因为空结构体实
//     际上是一个可以添加功能的机会。
func applyMemberFabric(ds *apps.DaemonSet, protocol string, interfaceCount int32) {
	if !memberProtocolIsHostFabric(protocol) {
		return
	}

	podSpec := &ds.Spec.Template.Spec

	podSpec.HostNetwork = true
	// 一个保留 ClusterFirst 设置的 hostNetwork Pod，会依赖主机的解析器，但无法找到主节点的 Service 名称。
	podSpec.DNSPolicy = core.DNSClusterFirstWithHostNet

	container := &podSpec.Containers[0]
	container.SecurityContext = &core.SecurityContext{
		Capabilities: &core.Capabilities{
			// IPC_LOCK 用于锁定互联网络注册的内存，SYS_RESOURCE 用于提高锁定内存的限制值，从而
			// 支持锁定操作。两种操作，两种功能。
			Add: []core.Capability{"IPC_LOCK", "SYS_RESOURCE"},
		},
	}

	// 仅涉及设备分配，其他无关内容不讨论。libfabric 的 EFA 组件在镜像中运行，因此没有挂载主机安装前缀，
	// 也不存在 LD_LIBRARY_PATH：主机树会从主机的`/usr`目录中解析自己的`libefa`和`libibverbs`库，而此处
	// 并未挂载该目录，因此指向它的加载器在符号版本上会失败，而不是进行加载——此外，不同发行版中该前缀可
	// 能是`lib`或`lib64`，因此没有一种路径能同时适用于两者。
	//
	// 仅通过 `Set on Limits` 来实现扩展资源的需求：一个实例的请求与限制可能不同，而默认配置会从限制中填
	// 充请求内容。该默认配置在 Pod 上运行，且特意不使用 Pod 模板，因此此 DaemonSet 会保留此处呈现的内容
	// ——这也能避免控制器在每次迭代时重写模板。该配置通过保护机制处理，而非直接索引：
	// FabricDeviceResource 定义了其协议，因此如果某个 `fabric` 被添加到 memberProtocolIsHostFabric 中，
	// 而未被添加到其中，则此处会返回空资源。若进行渲染，列表中会出现空键，这代表一个节点无法满足的请求，
	// 且没有相关说明；跳过该配置则意味着成员请求了不存在的设备，而这正是此渲染内容的其余部分所视为的可见故障。
	device := FabricDeviceResource(protocol, interfaceCount)
	if device == "" {
		return
	}

	if container.Resources.Limits == nil {
		container.Resources.Limits = core.ResourceList{}
	}

	container.Resources.Limits[device] = *resource.NewQuantity(int64(interfaceCount), resource.DecimalSI)
}

// FabricDeviceResource 是主机 fabric 协议所请求的扩展资源，它会显式列举协议名称，
// 而不会 fallthrough 到其中某一种协议。
//
// 此处避免 fallthrough 并非代码风格偏好。今天两种写法结果都正确，因为 memberProtocolIsHostFabric
// 只放行这一对协议，且 applyMemberFabric 会先查询它。但两者的区别会在引入第三种主机 fabric
// 之日显现——正如本文件注释所预期的那样：fallthrough 会静默地给该 fabric 的成员分配
// 一个 RDMA 适配器席位并挂载错误的设备，而显式列举协议则会返回空资源，使调用方完全不渲染
// 设备请求。未请求任何设备的成员会在 fabric 路径上表现为可见的失败；被授予错误适配器的成员则不会。
//
// 通过调用生成而非直接硬编码 RDMA 资源名，可以让仓库中只保留一处拼写：Device Manager
// 提供的是 GetRDMAResourceName 返回的任意名称，因此若成员请求字面量，一旦该函数的键值
// 发生变动就会悄无声息地不再匹配。单个接口请求的是 SHARED 模式，因为成员只需要适配器上的
// 一个席位而非整块适配器。多个接口则请求 EXCLUSIVE 模式，因为共享令牌可能会重复分配
// 同一个端点，从而在表面上满足数量要求却未能授予互不相同的接口。
func FabricDeviceResource(protocol string, interfaceCount int32) core.ResourceName {
	switch protocol {
	case memberProtocolRDMA:
		if interfaceCount > 1 {
			return nodefeature.GetRDMAResourceName(workercore.DeviceAllocationModeExclusive)
		}
		return nodefeature.GetRDMAResourceName(workercore.DeviceAllocationModeShared)
	case memberProtocolEFA:
		return efaDeviceResource
	default:
		return ""
	}
}

// applyMemberHostPaths 会从其节点上获取该组声明的所需内容。
//
// 按条目在列表中的位置生成卷名，避免与保留卷或同组其他条目冲突。挂载路径则由管理员指定，准入时拒绝重复路径。
//
// 声明为“无”的组会保持原样显示，因此此功能不会移动未使用此功能的后端中的 Pod 模板——进而也不会移动其指纹以及运行成员。
func applyMemberHostPaths(ds *apps.DaemonSet, member workercore.KVCacheBackendMember) {
	if len(member.HostPaths) == 0 {
		return
	}

	podSpec := &ds.Spec.Template.Spec
	container := &podSpec.Containers[0]

	for i, hp := range member.HostPaths {
		name := fmt.Sprintf("%s-%d", memberHostPathVolumeNamePrefix, i)

		podSpec.Volumes = append(podSpec.Volumes, core.Volume{
			Name: name,
			VolumeSource: core.VolumeSource{
				HostPath: &core.HostPathVolumeSource{
					Path: hp.Path,
					Type: hp.Type,
				},
			},
		})
		container.VolumeMounts = append(container.VolumeMounts, core.VolumeMount{
			Name:      name,
			MountPath: hp.MountPath,
			ReadOnly:  hp.ReadOnly,
		})
	}
}

// applyMemberSecurityContext 会将该组的声明安全上下文 ONTO 与 fabric 路径逐字段合并后的安全上下文，以及联合
// 的能力集合进行合并。
//
// 合并而非替换，因为双方了解不同的内容。互联网络路径知道：没有 IPC_LOCK 时，主机-互联网络成员无法注册内存，
// 也没有 SYS_RESOURCE 时无法提升绑定限制；声明则知道节点的厂商栈需要什么。如果进行整体结构替换，第一对内容
// 将被无声地丢弃——容器仅会在传输引擎注册其缓冲区时开始、运行并失败，因此这种合并使得两个声明相互独立。
//
// 仅“Add”被合并了。“Drop”被当作已声明存在：一种操作器永远不会添加的能力，此渲染器对此没有看法；而与“Add”重
// 叠的“Drop”则是 kubelet 需要解决的矛盾点，而非此函数的功能。
func applyMemberSecurityContext(ds *apps.DaemonSet, member workercore.KVCacheBackendMember) {
	if member.SecurityContext == nil {
		return
	}

	container := &ds.Spec.Template.Spec.Containers[0]
	derived := container.SecurityContext

	// 被深度复制了，因此由调用者提交并跨渲染环境共享的对象——比如列表器的缓存副本——永远不会被写入。
	merged := member.SecurityContext.DeepCopy()

	if derived != nil {
		if merged.Privileged == nil {
			merged.Privileged = derived.Privileged
		}
		if merged.RunAsUser == nil {
			merged.RunAsUser = derived.RunAsUser
		}
		if merged.RunAsGroup == nil {
			merged.RunAsGroup = derived.RunAsGroup
		}
		if merged.RunAsNonRoot == nil {
			merged.RunAsNonRoot = derived.RunAsNonRoot
		}
		if merged.ReadOnlyRootFilesystem == nil {
			merged.ReadOnlyRootFilesystem = derived.ReadOnlyRootFilesystem
		}
		if merged.AllowPrivilegeEscalation == nil {
			merged.AllowPrivilegeEscalation = derived.AllowPrivilegeEscalation
		}
		if merged.ProcMount == nil {
			merged.ProcMount = derived.ProcMount
		}
		if merged.SELinuxOptions == nil {
			merged.SELinuxOptions = derived.SELinuxOptions
		}
		if merged.WindowsOptions == nil {
			merged.WindowsOptions = derived.WindowsOptions
		}
		if merged.SeccompProfile == nil {
			merged.SeccompProfile = derived.SeccompProfile
		}
		if merged.AppArmorProfile == nil {
			merged.AppArmorProfile = derived.AppArmorProfile
		}

		if derived.Capabilities != nil && len(derived.Capabilities.Add) > 0 {
			if merged.Capabilities == nil {
				merged.Capabilities = &core.Capabilities{}
			}
			for _, capability := range derived.Capabilities.Add {
				if !slices.Contains(merged.Capabilities.Add, capability) {
					merged.Capabilities.Add = append(merged.Capabilities.Add, capability)
				}
			}
			// 按顺序排列后，同一规格的两种渲染结果在字节层面是完全相同的，无论双方贡献的顺序如
			// 何，DaemonSet 也不会发生变动。
			slices.Sort(merged.Capabilities.Add)
		}
	}

	container.SecurityContext = merged
}

// memberTerminationGracePeriodSeconds 表示 kubelet 在收到 SIGTERM 信号后，等待多久才会终止该节点。
//
// 它源自“等待期间”的设定，而非被置于其旁边；这就是“窗口始终保留等待状态”背后的全部机制：可以设置两个独立的
// 字段，使得 kubelet 在按照等待规范要求进行等待时终止容器进程，而无论进行多少验证都无法破坏这种关系——它只是
// 让该关系可被检查而已。作为衍生结果，它无法被违反。
//
// 没有磁盘层的分组没有等待对象，因此会保留入口点自身的关闭预算，其 Pod 模板也不会移动。
func memberTerminationGracePeriodSeconds(
	kvcb *workercore.KVCacheBackend, member workercore.KVCacheBackendMember,
) int64 {
	if len(member.LocalDisks) == 0 {
		return memberShutdownSeconds
	}
	return int64(memberScaleInGraceSeconds(kvcb)) + memberShutdownSeconds
}

// memberScaleInGraceSeconds 表示离开的成员进程在注销其磁盘层后还能存活的时间。此等待过程会持续，该层会立即停止响应。
func memberScaleInGraceSeconds(kvcb *workercore.KVCacheBackend) int32 {
	managed := kvcb.Spec.Connection.Managed
	if managed == nil || managed.ScaleIn == nil {
		return 0
	}
	return managed.ScaleIn.GracePeriodSeconds
}

// applyMemberLocalDisk 会授予该组的磁盘级别权限：其所在的主机目录，以及在成员离开前将其注销的钩子。
//
// 没有成员的组将保持原样显示，这确保了现有后端中的 Pod 模板——以及其识别特征和运行成员——不受此功能的影响。
func applyMemberLocalDisk(
	ds *apps.DaemonSet,
	kvcb *workercore.KVCacheBackend,
	member workercore.KVCacheBackendMember,
	group int,
) {
	if len(member.LocalDisks) == 0 {
		return
	}

	podSpec := &ds.Spec.Template.Spec

	// 目录模式为 `Directory`，而非 DirectoryOrCreate。后者更方便，但无法使用。测量结果显示：Kubelet 创
	// 建的目录由 root 用户拥有，权限为 0755；而存储镜像以 uid 65532 运行——因此成员节点会尝试访问该目录，
	// 发现无法写入，就会重新尝试存储设置，直到放弃为止。此渲染器无法添加任何从 Pod 内部进行的修复方案：
	// fsGroup 不适用于 hostPath（也进行了测量，该目录仍由 root 用户拥有）；其他替代方案则意味着需要在节
	// 点上授予容器根目录的权限，以便对主机目录进行 chown 操作。
	//
	// 因此，目录是节点需要提供的，也是其所属的位置——在主机上拥有路径并非操作员应代表管理员进行修改的内
	// 容。目录会将缺失的情况标记为 FailedMount，此时 Pod 会停止工作，而不是出现无法被写入的静默挂载。文
	// 档中提供了如何创建该目录的说明，以及所选镜像以哪个 UID 运行。
	//
	// 两个条件均满足：要么是 hostPath，而不是 emptyDir。emptyDir 会随 Pod 重启而消失，因此每次重启都会
	// 丢弃那些仅存于残骸中的缓存层；此外，kubelet 会处理 emptyDir，在缓存层填满时将其从节点中移除，这与
	// 缓存层在达到自身上限时应有的行为完全相反。
	podSpec.Volumes = append(podSpec.Volumes, core.Volume{
		Name: memberLocalDiskVolumeName,
		VolumeSource: core.VolumeSource{
			HostPath: &core.HostPathVolumeSource{
				Path: member.LocalDisks[0].Path,
				Type: ptr.To(core.HostPathDirectory),
			},
		},
	})

	container := &podSpec.Containers[0]
	// 安装在客户端被要求写入的路径上，因此其中一个字段同时说明了字节位于主机上的位置以及它们在容器中的地址。
	container.VolumeMounts = append(container.VolumeMounts, core.VolumeMount{
		Name:      memberLocalDiskVolumeName,
		MountPath: member.LocalDisks[0].Path,
	})
	container.Lifecycle = &core.Lifecycle{
		PreStop: memberUnmountLocalDiskHook(memberScaleInGraceSeconds(kvcb), memberRESTPort(group)),
	}

	podSpec.InitContainers = append(podSpec.InitContainers,
		memberLocalDiskSurveyContainer(container.Image, member.LocalDisks[0].Path))
}

// memberLocalDiskSurveyContainer 在成员启动时检查磁盘层目录中是否已有内容，以便管理员及时发现目录复用，避免
// 读到属于旧后端的缓存条目。
//
// 它只进行报告。它不会移除任何内容，也不会在管理员目录中写入任何标记，且其脚本在包括自身失败在内的所有路径上都以零退出。
//
// 若脚本能启动，它会正常退出。但镜像缺少 sh 或挂载不可读，初始化容器可能根本无法启动。此时 kubelet 会重试，
// 后续成员容器也会被阻塞。
//
// 因此，这增加了 members[].image 的一个要求：它必须提供一个外壳。该存储操作所依赖的组件确实如此，而成员自己
// 的入口点是一个 Python 控制台脚本，因此这个外壳是预先确定的。这一点在 API 字段和文档中有说明，而不是在升级
// 过程中由用户自行发现并解决潜在问题。
//
// 计数信息存储在终止消息中，而非日志中，因为当消息位于 Pod 上时，需要从已消失的容器中获取日志，而这一情况已
// 经被相同的协调过程处理过了。
//
// IT 计数第二次，它不可能为零。这项调查的每种失败模式都会导致空计数：没有壳文件、无法读取的挂载点，或命令不
// 在映像中。当询问“目录是否为空”时，用户所寻求的就是这种结果，因此单独的零值无法被处理。根目录永远不会为空，
// 因此控制参数为 0 的读数表明调查失败，会被拒绝而非相信。
//
// 该控制机制检查的是工具本身，而非测量结果，这两种情况都是不同的故障。它计数在根目录中，因此证明存在 shell
// 和 `ls` 命令——但与层级无关。如果容器的用户无法读取该层级，则 `ls` 会返回非零退出码和空标准输出，而
// `wc -l` 会显示出空目录产生的同样为 0 的结果，尽管控制机制完全正常。那么这一结果就会被相信，并且会长期被相信下去。
//
// 因此，该层自己的退出状态被单独处理并作为“-1”的条目进行报告，而读者对此表示拒绝。计数无法体现这种失败状态，
// 因为任何可能出现的计数，都是空目录也能产生的计数。
//
// 这种修改会更改 POD 模板，因此升级会重新启动声明了层的后端组件。该层本身是一个主机目录，在重启后依然保留；
// 没有任何内容被丢弃。没有层的组则不会受到任何影响，这也是其他后端组件组件能够保持原状的原因。
func memberLocalDiskSurveyContainer(image, path string) core.Container {
	// 该层级被特意列出两次。计数必须通过管道进行，而管道的状态由最后一个命令决定，因此 `ls | wc -l` 可
	// 以显示 `wc` 是否运行了，但无法显示 `ls` 是否读取了任何内容。第二次调用是唯一能获取状态的地方。
	//
	// 路径被用反引号包裹，而非单引号包裹。之前的版本使用 %q 进行插值处理，从而产生双引号——而 `sh` 会展
	// 开内部的 $、反引号和 $(...) 等符号，因此看起来就像真正的引用一样，但实际上并非如此。以 `sh` 的表
	// 示方式来看：路径 /tmp/qp/$(id -u) 以那种形式执行命令时，输出结果为 RAN；而单引号包裹的形式则保持
	// 原样。准入机制接受任何绝对路径，且不受 ".." 的限制，也不限制字符集，因此这种引用方式就是该对象中
	// 的字段与成员初始化容器中的命令执行之间唯一的连接点。
	script := fmt.Sprintf(
		`t=$(ls -A %[1]s 2>/dev/null | wc -l); c=$(ls -A / 2>/dev/null | wc -l); `+
			`ls -A %[1]s >/dev/null 2>&1 || t=-1; `+
			`printf 'entries=%%s control=%%s' "${t:-}" "${c:-}" > /dev/termination-log 2>/dev/null; `+
			`exit 0`,
		shellQuote(path),
	)
	return core.Container{
		Name:                     MemberLocalDiskSurveyContainerName,
		Image:                    image,
		Command:                  []string{"sh", "-c", script},
		TerminationMessagePolicy: core.TerminationMessageReadFile,
		VolumeMounts: []core.VolumeMount{
			{Name: memberLocalDiskVolumeName, MountPath: path},
		},
	}
}

// shellQuote 将字符串编码为 POSIX sh 可原样读取的单个参数。
//
// 单引号是唯一的引用符号，在内部不会进行展开操作，因此该值不能成为变量、命令替换或多个参数。值中的单引号根
// 本无法在单引号内被转义——这种序列会结束当前运行，产生一个被转义的引号，并开启下一个引号，这就是替换结果呈
// 现如此样子的原因。
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// memberUnmountLocalDiskHook 构建了关闭钩子，用于取消该成员磁盘层的注册。
//
// 这是一项执行操作，而非 httpGet 调用，这其实是一种限制而非偏好：生命周期中的 httpGet 不会发送请求体，也无
// 法选择方法，而此路由则是一个 POST 请求，其处理程序会先解码请求体，如果请求体为空则返回 400 错误。因此，
// httpGet 钩子在每次关闭时都会失败——而失败的 preStop 会被记录为事件，否则会被忽略，所以该钩子看起来是配置好
// 的，但实际上并未执行任何操作。
//
// 解释器是图像自带的：这个容器的命令是一个 Python 控制台脚本，因此无法运行 python3 的图像也无法运行该成员。
// 仅使用标准库，原因相同——如果图像未携带 curl 工具，也会以同样沉默的方式失败。
func memberUnmountLocalDiskHook(graceSeconds, restPort int32) *core.LifecycleHandler {
	// 超时时间等于宽容时间加一个很小的余量，这个“小”就是关键所在。
	//
	// 钩子超时必须长于处理程序等待的宽限时间，又要给入口进程留出关闭时间。kubelet 在执行 preStop 前就开
	// 始终止倒计时，直到钩子返回才发 SIGTERM。若钩子耗尽总预算，入口进程会在关闭途中收到 SIGKILL。
	script := fmt.Sprintf(
		`import json,urllib.request;`+
			`urllib.request.urlopen(urllib.request.Request(`+
			`"http://127.0.0.1:%d%s",`+
			`data=json.dumps({"grace_period_seconds":%d}).encode(),`+
			`headers={"Content-Type":"application/json"},method="POST"),timeout=%d)`,
		restPort, memberUnmountLocalDiskPath, graceSeconds,
		int64(graceSeconds)+memberHookTimeoutMarginSeconds,
	)

	return &core.LifecycleHandler{
		Exec: &core.ExecAction{Command: []string{"python3", "-c", script}},
	}
}

// MemberPodSpecHash 会识别成员 pod 模板中的所有内容，但节点选择器除外。
//
// 这种排除正是其目的所在。扩展操作只会改变节点选择器，不会有其他影响；同时，扩展操作不得重新启动已运行的成
// 员；对模板的其他任何修改都必须进行。因此，在哈希处理之前会移除节点选择器，而 DaemonSet 会保持 OnDelete 状
// 态，这样只有这个指纹才能决定是否重启。
//
// 存储的哈希值注释也被清除掉了，因此指纹永远不会自我覆盖——如果保留该值，每次生成和滚动组时都会发生变化。
func MemberPodSpecHash(template core.PodTemplateSpec) string {
	subject := *template.DeepCopy()
	subject.Spec.NodeSelector = nil
	delete(subject.Annotations, MemberPodSpecHashAnnotation)
	if len(subject.Annotations) == 0 {
		subject.Annotations = nil
	}

	// JSON 而非 fmt：它会对映射键进行排序，因此同一个规范哈希的两种呈现形式可能相同。对于
	// PodTemplateSpec 而言，无法发生序列化失败，将其视为空摘要会静默禁用所有重启操作，因此最终呈现为一
	// 个无法匹配的值。
	encoded, err := json.Marshal(subject)
	if err != nil {
		return "unhashable-" + err.Error()
	}

	return stringx.SumBytesBySHA256(encoded)
}
