// Package mooncake 包含且仅包含特定于单一 KV 缓存后端实现（Mooncake 存储）的所有逻辑。
// 它负责渲染该存储的工作负载并回读其上报状态。
//
// 将其拆分为独立包，是因为其中每一项细节都源自 Mooncake 自身的源码——包括入口名称、gflag 拼写、
// 管理路由、Prometheus 指标序列以及租户配额策略结构——这些内容在引入第二种后端实现时均无法通用。
// 父包则存放适用于任意后端的通用逻辑。这里并不存在针对多后端的抽象：目前只有一种实现，`spec.type`
// 仅接受单一值，也没有基于它的分发逻辑。这种拆分定义了未来扩展第二种后端的边界，而非已经预设好的接口缝。
//
// 整个包遵循两项约定：
//
//   - 渲染器均为输出确定的纯函数，因此无需集群即可完整测试全部渲染接口，且重新渲染结果可与上次清晰地进行差异比对。
//   - 所有观测数值均使用指针。存储仅序列化实际观测到的值，因此缺失与零值代表不同事实，用零值代替缺失值发布会导致预热后的缓存看起来像空缓存。
//
// 本文件仅存放管理逃生口（escape hatch）的规则，因为两处调用方必须对此达成一致：渲染器输出派生参数与
// 变量，准入 Webhook 则拒绝与之冲突的配置项。将规则列表集中在此而非分散在任一调用方中，可以从根本上
// 杜绝“同一配置存在两处来源”的问题。
//
// 下方每一项均读取自本项目所锁定版本的组件源码。
package mooncake

// ExtraArgsRules 定义了准入阶段对单侧 extraArgs 实施的校验规则。三种类型彼此分开，
// 因为它们的拒绝原因各不相同，运维人员需要明确知晓具体原因：
//
//   - Derived：当前 API 已根据字段渲染了该参数，若再提供第二处来源会导致渲染结果
//     存在歧义。
//   - Exclusive：制品仅接受两个参数中的其中一个而非同时设置，同时设置会将最终结果
//     交由制品自身而非清单决定。
//   - Forbidden：该参数会原样传入制品，但其带来的代价得不偿失。
//     此类参数不存在名称冲突，这正是设立该分类的原因。每一项都附带了各自的理由，
//     因为它们并非同一类问题：有的会改变所有其他参数的读取方式，有的指向无人读取的
//     存储，有的会导致进程根本无法启动，有的会替换掉 spec 中声明的设置却仍让对象
//     显示原设置，有的会让存储运行在未按此处规则追踪过的路径上，还有几项由于本
//     operator 自身的渲染结果始终优先而不会改变任何值，导致它们实际未配置任何内容，
//     在表面上却像更改了某项设置。
type ExtraArgsRules struct {
	Derived   []string
	Exclusive [][]string
	Forbidden map[string]string
}

// LeaderExtraArgsRules 控制 leader 侧的参数透传规则。
//
// 下面两个列表针对本项目所固定的制品版本是完备的，而不仅仅是偶然注意到的名称记录。
// 本 API 从字段渲染的每一项设置都已追踪至制品源码中其有效值的写入位置，所有能触及
// 这些位置的其他参数均列于下方。若该值位于 const 成员中，构造函数的初始化列表即为
// 完整的写入点集合——这也使“完备”成为读者可核验的事实，而非仅由本注释断言的声明。
//
// REQUIRED: 升级固定的制品版本会使上述追踪失效，因此须在同一提交中重新追踪。上游
// 新增的参数可能会以此处规则未涵盖的名称触及已渲染的设置，而下方的拒绝规则均按名称
// 匹配——本仓库中没有其他机制能察觉此类变化。
//
// metrics_host 是追踪过程中发现并刻意保留可透传的参数，特此记录以防下次追踪将其
// 误加入列表。它会改变管理接口和 Prometheus 接口的绑定地址，两个探针均通过 Pod 自身
// 地址访问该接口——但制品文档表明 "::" 会监听 IPv6，在双栈主机上则同时监听两者。
// 在为 Pod 分配 IPv6 地址的集群上，制品默认的 0.0.0.0 完全无法响应探针请求，因此
// 该键是让 leader 达到就绪状态的唯一途径，拒绝它带来的代价远大于开放透传的风险。
// 若填入非 Pod 自身的具体地址仍会导致探针失败；该失败会阻断发布而非静默通过，这正
// 是本列表划定的界线。
//
// default_kv_lease_ttl 是第二个刻意保留可透传的参数，需要更明确地说明，因为它是
// leader 唯一会渲染但未予保留禁止覆盖的键。仅看渲染器代码会认为它属于 Derived，但若
// 将其加入 Derived 反而会破坏原有设计。渲染器将制品的 10 秒默认值调整为一个能支撑
// 其他引擎副本读取的起点值，但适合具体部署的值取决于其读取模式和存储规模，而本 API
// 并不描述这些信息——因此运维人员可自行提供更合适的默认值，此处传入的条目会覆盖
// 渲染结果（对于重复参数，制品采纳最后一个，且透传参数渲染在我们的参数之后）。若保留
// 禁止覆盖该键，就会拒绝此类条目，导致该数值无法通过任何途径配置。
var LeaderExtraArgsRules = ExtraArgsRules{
	// 此处的键为去掉前导短横线后的参数名称，这与 extraArgs 条目的传入格式不同：传入的
	// 条目带有短横线且可能带有参数值，因此准入校验会先去除前导短横线及首个 "=" 起的
	// 所有内容，再与这些表进行比对。
	Derived: []string{
		"allocation_strategy",
		// 选主逻辑渲染的四个参数，作为一组统一保留，因为它们在渲染时要么全有、要么全无。
		// 与下方的 offload 参数对一样，它们被无条件保留，即便 electionBackend 为 None 时
		// 不会渲染它们——若透传 enable_ha 而旁边没有连接字符串，leader 会在启动时直接退出；
		// 若通过这种方式传入 cluster_id，则会使存储的命名空间基于对象未声明的值来构建。
		"cluster_id",
		"enable_ha",
		"ha_backend_connstring",
		"ha_backend_type",
		// 选主逻辑对外广播的地址，以及制品据此推导该地址所用的网卡接口。基于相同原因一并
		// 保留，尽管渲染器仅渲染前者：制品会将 rpc_address 和 rpc_port 拼接成参与竞选的
		// local_hostname，因此该字符串既是选主身份标识，也是跟随 Lease 的 member 获取到的
		// 连接地址。此处的透传不会只是覆盖一项设置——它会直接决定所有 member 连接到哪台
		// 主机，且无法保证 Pod 在该地址上可达。
		"rpc_address",
		"rpc_interface",
		// 磁盘层在 leader 侧开关的两个部分。它们由 members[].localDisks 推导而来——
		// 声明了磁盘层的组即会开启该层级——若通过透传入口修改它们，会导致 leader 侧的决策
		// 与 member 侧不一致，而对象本身却没有任何体现。
		"enable_offload",
		"enable_multi_tenants",
		"metrics_port",
		"offload_on_evict",
		"pod_name",
		"pod_namespace",
		"rpc_port",
		// 与 enable_multi_tenants 一同由 MultiTenancy 渲染，基于相同原因列于此处。若不加以
		// 限制，透传键可能会将 master 指向一个本 operator 既未初始化也不做改写的文件——所有
		// 配额写入都会落入无人读取的位置，而整个池仍会继续上报 Ready 状态。
		"tenant_quota_connector_uri",
	},

	Forbidden: map[string]string{
		// 制品自身的检查为 LOG(FATAL)：当 ha_backend_type 不是 etcd 时，启用 enable_oplog
		// 会拒绝启动。本 operator 运行的镜像编译了 Kubernetes Lease 后端，上游构建无法将
		// 其与 etcd 组合使用，因此此处无法使用 etcd 后端，oplog 亦然。
		//
		// 之所以直接拒绝而非透传，是因为该失败是彻底且立即发生的。本规则的早期草案曾允许
		// 该键并上报其后果，理由是权衡取舍应由配置者自行承担——但该理由针对的是明文 etcd
		// 暴露问题，一旦该参数直接导致崩溃便不再成立，因为此时已无任何可权衡之处。
		"enable_oplog": "the leader refuses to start with it: the artifact requires the etcd " +
			"leadership backend for the operation log, and the image this operator runs compiles " +
			"the Kubernetes Lease backend, which upstream cannot build together with etcd",

		// 经查阅制品源码实测：main() 会先加载配置文件，再调用
		// LoadConfigFromCmdline(config, conf_set)，而后者的大多数赋值操作都受 "if (!conf_set)"
		// 保护。因此，配置文件在很大程度上会使命令行参数失效——这与常规优先级恰恰相反，
		// 且过程完全静默。
		//
		// 若通过逃生舱入口设置该参数，会导致其上方渲染的所有参数（端口、分配策略、Pod 身份
		// 标识）全部作废，且没有任何提示。
		"config_path": "it makes the artifact ignore the rest of the command line, " +
			"so every flag rendered from this spec would be silently discarded",

		// 与保留 URI 相对应，且放入此处而非 Derived，因为本 operator 从不渲染该参数：
		// 多租户机制建立在制品自身默认值为 "file" 的基础之上，而正因未渲染才使其能通过
		// extraArgs 触及。若将其改为其他来源类型，随之渲染的 URI 将不再指向文件，从而导致
		// 预置策略、leader 维护的可写副本以及 reconciler 执行的每次配额写入都指向一个
		// master 已不再读取的存储。
		"tenant_quota_connector_type": "it decides what kind of source the master reads the " +
			"tenant quota policy from, so anything other than the default file connector leaves " +
			"the policy this operator seeds and rewrites addressing a store nothing reads",

		// CXL 开关，属于第四种代价类型：它既不遮蔽某项设置，也不会导致启动失败——而是直接
		// 替换对象中声明的设置，且对象表面上仍维持原声明。
		//
		// 经查阅制品源码实测：master 在构造完成前会根据该参数解析策略类型，并在启动期间
		// 替换策略对象，因此渲染出的 -allocation_strategy 实际已失效（而非后期被覆盖），
		// 且解析后的类型会继续控制其他代码路径。
		//
		// 它会产生两方面的影响，且上报方式各不相同，因此拒绝原因中同时点明了这两点。策略
		// 替换本身在任何日志级别下都不会上报。启用 CXL 本身会有日志输出：在默认日志级别下，
		// 制品会打印其创建的分配器及大小，leader 的指标随后也会将该大小记为容量。这些日志
		// 和指标提及的是分配器而非策略，因此不能作为策略替换的依据，将其误读为策略提示正是
		// 本条目需要避免的混淆。
		//
		// 在节点不含 DAX 设备的集群上实测：仅开启该开关而不设置任何操作数（两者均默认使用
		// /dev/dax0.0 和 8 GiB），leader 仍能正常启动提供服务，并对外宣称拥有 8 GiB 毫无
		// 实际支撑的容量，既不报错也不降级。
		//
		// 拒绝该参数会关闭通过本 API 使用 CXL 的唯一路径（本 API 未提供对应字段），这属于
		// 刻意为之而非疏漏：透传入口只是意外触及了该能力，而正式交付该能力所需的工作已记录
		// 在介质设计规范中。上述划定的界线决定了处理方式——会导致发布中止的失败可以放行，
		// 静默通过的则不行——而该键的两种影响均属于静默生效一侧。
		//
		// 此处不存在名称冲突——本 API 并不渲染任何 CXL 参数——这也是这些键属于 Forbidden
		// 而非 Derived 的原因。使用 Derived 的报错信息也不符合事实：它会声称该键由 spec
		// 中的某个字段推导而来，但实际上并非如此。
		"enable_cxl": "it replaces the leader's allocation strategy outright, so the " +
			"-allocation_strategy rendered from leader.allocationStrategy is discarded and the " +
			"object goes on stating a strategy the process is not running with nothing reporting " +
			"that substitution, and it brings the leader up advertising the CXL allocator's size " +
			"as capacity, which on a node with no DAX device is capacity nothing backs",

		// 两者仅在设置了 enable_cxl 时才会被读取，而 enable_cxl 已在上方被拒绝。此处依然
		// 予以保留限制，因为一个被准入接受但实际不产生任何配置效果的键，会让运维人员误以为
		// 某个存储层已开启：清单中写明了 DAX 路径，进程却从未读取，且没有任何状态予以说明。
		// 两者共用同一个常量作为原因，因为两条表达相同含义的字面量容易逐渐产生偏差，而字段
		// 路径本身已指明具体输入了哪个键。
		"cxl_path": cxlCompanionKeyReason,
		"cxl_size": cxlCompanionKeyReason,

		// 制品中 rpc_port 的已弃用拼写；在此处属于无效而非有害：仅当 rpc_port 为 0 时才会
		// 读取它，而本 operator 会无条件渲染一个固定的正整数 -rpc_port 并优先生效。基于与
		// CXL 操作数相同的理由予以拒绝——准入接受却被进程忽略的键，在清单上看似更改了端口，
		// 而 Service 和对外发布的端点却仍在使用旧端口。
		"port": "it is the artifact's deprecated spelling of rpc_port, which this operator always " +
			"renders and which wins over it, so this key moves nothing while reading as a port " +
			"that moved",

		// etcd 选主后端的端点列表，由于双重原因在此不可用。它仅在 ha_backend_connstring
		// 为空时作为回退项被读取，而只要启用选主，本 operator 就会为其渲染非空值；同时，
		// 基于上方禁止 enable_oplog 的同一事实，镜像中并不包含该后端。
		"etcd_endpoints": "it supplies a connection string only when ha_backend_connstring is " +
			"empty, which this operator never leaves empty, and the etcd leadership backend it " +
			"names is not compiled into the image this operator runs",

		// 分配器配置。本列表中的规则均是在制品默认偏移量分配器启用时追踪得出的，且制品中
		// 至少有一条代码路径依赖该参数而不输出日志
		// line either way: the master builds its snapshot manager only under that allocator. Another
		// 该参数值会让存储运行在此处规则从未追踪覆盖的路径上。
		"memory_allocator": "it replaces the artifact's default offset allocator, the only one " +
			"the rules governing this list were traced against",

		// 存储快照及其唯二的开启键。恢复快照可能导致缓存返回错误数据而非未命中：快照记录了
		// 每个键在 member 内存中的位置，但在回读索引时并不校验该内存位置是否仍存有该键。
		// 引擎重置缓存时执行的强制删除会释放内存供后续写入使用，而备节点会在自身启动时加载
		// 一次快照，赶在另一个 leader 复用该内存之前。
		"enable_snapshot":         snapshotKeyReason,
		"enable_snapshot_restore": snapshotKeyReason,

		// 快照读取的所有其他配置：调度周期、保留数量、标准及两种已弃用拼写下的对象存储配置、
		// 上传失败时的本地备份目录，以及目录服务及其连接字符串。这些键仅在上方两个开关之一
		// 被设置时才会被读取，因此单独设置不会产生任何效果——基于与 CXL 操作数相同的理由
		// 予以拒绝，因为一个被准入接受却无人读取的键，会让运维人员误以为快照功能已开启。
		"snapshot_interval_seconds":           snapshotCompanionKeyReason,
		"snapshot_retention_count":            snapshotCompanionKeyReason,
		"snapshot_object_store_type":          snapshotCompanionKeyReason,
		"snapshot_payload_store_type":         snapshotCompanionKeyReason,
		"snapshot_payload_backend_type":       snapshotCompanionKeyReason,
		"snapshot_backup_dir":                 snapshotCompanionKeyReason,
		"snapshot_catalog_store_type":         snapshotCompanionKeyReason,
		"snapshot_catalog_backend_type":       snapshotCompanionKeyReason,
		"snapshot_catalog_store_connstring":   snapshotCompanionKeyReason,
		"snapshot_catalog_backend_connstring": snapshotCompanionKeyReason,
	},
}

// cxlCompanionKeyReason 说明为何单独拒绝这两个 CXL 操作数。它们并非本 API 渲染的
// 设置，因此不存在名称冲突；在没有那个真正带来代价的开关配合时它们毫无作用，而被
// 准入接受的无效键会让人误以为该存储层已开启。
const cxlCompanionKeyReason = "it is read only when enable_cxl is set, and that key is refused " +
	"here because it discards the -allocation_strategy rendered from leader.allocationStrategy, " +
	"so this key alone configures nothing at all"

// snapshotKeyReason 说明为何拒绝开启存储快照的两个键。两者共用一个常量，因为两条
// 表达相同含义的字面量容易逐渐产生偏差，而字段路径本身已指明具体输入了哪个键。
const snapshotKeyReason = "snapshots are not supported: a leader restoring one can serve another " +
	"key's bytes instead of a miss, because the member memory the snapshot points at may have been " +
	"reused since it was taken -- after a forced remove such as an engine's cache reset, or before " +
	"a standby that loaded it at its own start takes over"

// snapshotCompanionKeyReason 说明为何单独拒绝所有其他快照相关键：它们各自仅在某个
// 本身已被拒绝的开关下才会被读取。
const snapshotCompanionKeyReason = "it is read only when enable_snapshot or " +
	"enable_snapshot_restore is set, and both are refused here, so this key alone configures " +
	"nothing at all"

// MemberExtraArgsRules 控制 member 组的参数透传规则。
//
// 这些是配置键，而非环境变量名：member 自身的入口脚本将其文档化为按键覆盖项，渲染器
// 再将每项映射为对应的 MOONCAKE_* 变量。客户端仅从环境变量读取的名称根本无法通过
// 本列表触及，因此未列于此处；members[].extraEnv 才是它们的透传入口，并由
// MemberDerivedEnvs 进行限制。
//
// 磁盘层的两个键已不再允许透传：它们现由 members[].localDisks 渲染，因此属于 Derived。
//
// 此处没有 Forbidden 条目，原因在于渲染形式：member 的 extraArgs 会变为按键覆盖项，
// 因此即便键名与配置文件参数相同，也只会设置同名的配置键，而不会让入口脚本去加载
// 某个文件。leader 侧的漏洞在这一侧并不存在。
//
// device_name 被刻意不纳入 Derived，原因经过实测而非出于代码风格考虑。渲染器会将
// MOONCAKE_DEVICE 留空，客户端将其解读为空设备过滤器，即“使用找到的所有设备”。
// 这是此处唯一正确的默认值：单个 DaemonSet 覆盖其组所选中的所有节点，而 RDMA 设备
// 是按主机命名的——一台主机上是 mlx5_0，下一台可能是 erdma_0——因此无法为整个组
// 渲染单一名称。
//
// 文档中提到的值 "auto-discovery" 在代码中并非特殊值。客户端会按逗号将 device_name
// 拆分为过滤列表，并未对该字符串做任何特殊处理，因此设置该值反而会生成一个仅匹配
// 字面名为该字符串的设备的过滤器，而任何主机上都不存在该设备。文档注释写的是
// auto-discovery，但实际行为是留空表示自动发现，填入该字符串反而匹配不到任何设备。
//
// 将该键排除在 Derived 之外，才能为异构硬件上的运维人员留出配置入口：member 的
// extraArgs 会渲染为入口脚本自身的 -D 覆盖参数，它在环境变量之后生效并覆盖环境变量。
var MemberExtraArgsRules = ExtraArgsRules{
	Derived: []string{
		// 磁盘层的 member 侧配置，由 members[].localDisks 渲染。此处对其进行保留比在
		// leader 侧更为重要，原因在于 member 的 extraArgs 在优先级链中所处的位置：真实
		// 命令行参数优先于配置键，配置键优先于环境变量。这两项以环境变量形式渲染，而
		// extraArgs 渲染为 -D 配置键——因此此处的条目不只是增加第二处来源，而是会直接胜出。
		// 若保留可透传，ssd_offload_path 就能传入一个未经准入对原字段所施规则校验的主机
		// 路径（须为绝对路径、非根目录、不含 ".."、避开 RDMA 设备树），而 enable_ssd_offload
		// 则会在 leader 仍继续为其编排 offload 任务的同时关闭该磁盘层。
		//
		// 渲染器设置的第三个键（磁盘层大小限制）无需列于此处：客户端的配置对象中没有该名称的
		// 字段，因此传入 -D 只会设置一个无人读取的键。
		"enable_ssd_offload",
		"global_segment_size",
		"local_buffer_size",
		"local_hostname",
		"master_server_address",
		"metadata_server",
		"protocol",
		"ssd_offload_path",
	},
}

// MemberDerivedEnvs 是 member 渲染器输出的所有环境变量名，准入阶段会在组的
// extraEnv 中拒绝这些名称。
//
// 它是一个普通列表而非 ExtraArgsRules，因为该类型中的另外两种分类在此处均为空，而
// 空字段容易让读者困惑其中应当填入什么内容。这些名称彼此之间不存在互斥关系，因此
// 无需设为 Exclusive；此外该命名空间中也没有任何变量具备 leader 侧 config_path 的
// 特性（即一个透传项令所有其他设置失效），因此也无需彻底禁止。此处的每个变量仅配置
// 其自身名称对应的设置而无其他副作用，因此唯一需要报告的问题就是命名冲突。
//
// bucket 参数对列于此处的原因与其余变量不同。其他变量保护的是准入阶段在其他位置强制
// 约束的不变量——磁盘层的两端配置、通过路径规则校验的路径、计入 Pod 资源请求的
// 段大小。bucket 参数对并不保护任何不变量；它是本 operator 自行选定的值，将其保留
// 是刻意做出的设计决策：需要调整该值的调优人员应当通过正式字段而非一个会静默造成
// 同名重复的透传入口来修改。
//
// transfer metrics 开关基于相同理由予以保留，且它是唯一一个保留本身会带来代价、需要
// 向读者明示的条目：引擎侧将同一开关视为用户可关闭的默认值，而此处则无法关闭。这种
// 不对称源自渲染形式而非对该开关另有看法——member 的 extraEnv 采用追加方式，因此
// 本渲染器输出的同名变量会出现两次并由运行时决定胜出者，而引擎侧具备“若已声明则
// 让步”的逻辑，此处则没有。若要添加该逻辑，意味着某个条目需根据透传入口自身的内容
// 进行条件渲染，而上方列表的设计初衷正是为了杜绝这种模式。
var MemberDerivedEnvs = []string{
	memberEnvGlobalSegmentSize,
	memberEnvLocalBufferSize,
	memberEnvLocalHostname,
	memberEnvMaster,
	memberEnvMetadataServer,
	memberEnvOffloadBucketKeysLimit,
	memberEnvOffloadBucketMaxTotalSize,
	memberEnvOffloadBucketSizeLimit,
	memberEnvOffloadEnabled,
	memberEnvOffloadEvictionPolicy,
	memberEnvOffloadKeysLimit,
	memberEnvOffloadPath,
	memberEnvOffloadSizeLimit,
	memberEnvOffloadWatermarkEnabled,
	memberEnvOffloadWatermarkHigh,
	memberEnvOffloadWatermarkLow,
	memberEnvProtocol,
	memberEnvTransferMetrics,
	memberEnvVisibleDevicesAMD,
	memberEnvVisibleDevicesCambricon,
	memberEnvVisibleDevicesIluvatar,
	memberEnvVisibleDevicesMThreads,
	memberEnvVisibleDevicesNVIDIA,
}

// LeaderDerivedEnvs 是 leader 渲染器输出的所有环境变量名称，也是准入控制
// 在 leader 的 extraEnv 中拒绝使用的变量名。
//
// 它采用普通列表形式的原因与 MemberDerivedEnvs 相同：排他类（exclusive）和
// 禁用类（forbidden）在此都为空，且 leader 命名空间中没有任何设置会使另一项失效。
//
// 这三个变量都会在启用 Kubernetes 选举时渲染，包括单副本场景。它们会被无条件
// 保留，否则当 electionBackend 从 None 变为 Kubernetes 时，已有的透传变量
// 可能会覆盖 Pod 身份信息。
var LeaderDerivedEnvs = []string{
	LeaderPodIPEnv,
	LeaderPodNameEnv,
	LeaderPodNamespaceEnv,
}
