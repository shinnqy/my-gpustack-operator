package mooncake

import (
	"fmt"

	core "k8s.io/api/core/v1"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/systemname"
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
