// 本文件保存渲染前必须检查的实测传输约束。如果向引擎传入其后端不支持的传输协议，
// 它在服务任何请求前就会报错退出，因此准入此类组合毫无意义，只会产生无法启动的容器。
package inject

// transportFacts 记录针对某一引擎版本实测得到的传输协议要求。
//
// 版本与源码位置紧邻该结论保存，以便后续复核下方的判别检查时无需重新查找上游文件。
type transportFacts struct {
	// Version 是读取该结论所对应的引擎发布版本。
	Version string

	// Source 是在该发布版本中读取该结论的文件路径与行号。
	//
	Source string

	// Required 是该引擎存储后端唯一接受的传输协议，采用渲染产物自身的拼写——
	// 即 mooncake.MemberProtocol 的渲染结果，而非 API 枚举所发布的值。
	//
	// 为空表示该引擎不拒绝任何传输协议，这是实测结论而非缺失值：每个引擎都有对应条目，
	// 其 Source 指明了读取该结论的位置。这一区分决定了相反的处理结果——
	// 未实测的引擎必须放行，而实测要求 "ascend" 的引擎则不能放行。
	Required string

	// RequiredAPIValue 是同一传输协议在 API 中的拼写：即运维人员写入
	// KVCacheBackend.spec.transport.protocol 的值。
	//
	// 此处同时保留两种拼写，因为拒绝信息中的不同语句需要各自使用它们。报错中指出的组合
	// 必须使用产物拼写，因为那是实际传给容器的值；而修复建议必须给出 schema 接受的值，
	// 且该枚举区分大小写。如果在修复建议中使用产物拼写，会引导运维人员填写一个被拒绝的值。
	//
	// 该值在此显式记录而非动态推导，这是必然选择而非偏好：mooncake 的映射并非单射，因此没有逆映射。
	// Auto 和 TCP 都会渲染为 "tcp"，所以“哪个 API 值生成了该产物值”在那里有两个答案，而在此处只有一个。
	// 通过辅助函数计算在 ascend 上虽然正确，但在对应两个 API 值的传输协议上会静默出错——
	// 这正是本表旨在消除的那类缺陷。
	//
	// 当且仅当 Required 为空时该字段为空。若某行只填了其中一个，会导致修复建议处打印空值，
	// 因此下方的校验约束要求两者必须同时出现或同时为空。
	RequiredAPIValue string
}

// engineTransportConstraint 记录每个引擎实测得到的传输约束：该引擎的存储后端接受哪种传输协议。
//
// 为什么采用查表而非 `if`。该约束是关于外部项目发布版本的客观事实，且仅当该项目发布新版本时才会变化，
// 唯一确认的方式是重新阅读该版本的源码。若写成条件语句，就会把这一事实埋入调用方的控制流中，
// 导致后续新增引擎时只顾得上两处中的一处。
//
// 本表防止的问题。有固定传输要求的引擎在收到其他传输协议时会在启动阶段直接报错，导致容器永远无法服务请求——
// 这是功能性故障而非性能降级。且触发条件并非用户选错了传输协议：KVCacheBackend.spec.transport.protocol
// 默认为 Auto，经 mooncake.MemberProtocol 渲染后为 "tcp"，因此未做显式配置的池子恰好会向
// vLLM-Ascend 传入它所拒绝的值。一切保持默认配置的场景正是本检查存在的意义。
//
// 如何复核条目（因为本表会静默过时）：
//
//  0. 首先，确定本项目为该引擎渲染了哪个存储后端——由 vllmConnectorFor 和
//     renderSGLang 决定——并复核对应后端。我们从未选用的后端上的约束不是本行的结论。
//  1. 找到该后端从配置中读取 `protocol` 的位置，并追踪该值的所有使用路径。
//  2. 存在比较不等于存在约束。查看不匹配时的分支行为：SGLang 会将 protocol 与 "rdma" 比较，
//     其 else 分支走常规路径，而 vLLM-Ascend 的 else 分支直接抛出异常。只有后者才是硬性要求。
//     若仅统计比较次数，会把 SGLang 误标为受约束，因此下文条目记录的是 else 分支的实际行为，而非仅仅存在分支。
//
// 如何触发某一行（这是另一个问题）。本表以 Engines() 为键，该列表比 SelectableEngines() 更宽：
// vLLM-Ascend 可渲染但不可直接命名。在 Pod annotation 中直接写该引擎名会被 ParseEngine
// 以引擎名不合法为由拒绝，而非因为传输协议不匹配——若读者将此误认为“表写错了”，其实是命中了另一条规则。
//
// 从带 annotation 的 Pod 路径抵达该行则是另一回事，该拒绝信息已说明做法：填写基础引擎名并同时声明运行时，
// 即可像 ModelDeployment 路径根据角色加速器选择那样选中本行。具体拼写写在拒绝信息中而非此处，
// 以保持单一来源。因此该行并非仅限 ModelDeployment 使用，若读者在看到拒绝后就止步，会误以为该路由不存在。
//
// 错误的条目无论偏松还是偏严都会出问题，且都不会静默无害：过严会拒绝本可正常运行的池子，过松会放行在服务任何请求前就崩溃的容器。
var engineTransportConstraint = map[Engine]transportFacts{
	EngineVLLM: {
		Version: "v0.29.0",
		// protocol 出现了三次但从未参与比较：在 :138 的配置类上声明，在 :163 从文件中读取，
		// 在 :1489 传给 store.setup()。没有任何分支读取它的值。
		Source:           "vllm/distributed/kv_transfer/kv_connector/v1/mooncake/store/worker.py:138,163,1489",
		Required:         "",
		RequiredAPIValue: "",
	},
	EngineVLLMAscend: {
		// 必须使用 ascend，这也是本表存在的直接原因。MooncakeBackend 构造函数在 :193-194
		// 对除 "ascend" 之外的所有协议都会抛出 NotImplementedError，因此 tcp、rdma 和 hip 均会导致容器在启动时中止。
		//
		// 如果只看配置类，文件读取器可能会掩盖这一点：from_file 在缺少 protocol 键时默认使用 "ascend"（:544）。
		// 但该默认值在此处永不生效，因为 vllmClientConfig.Protocol 未标记 omitempty，且 renderVLLMClientConfig
		// 会无条件写入该字段——因此最终到达 :193 的始终是本项目选定的值。
		Version: "v0.19.1rc1-2120-gcdad5a32e",
		Source:  "vllm_ascend/distributed/kv_transfer/kv_pool/ascend_store/backend/mooncake_backend.py:193-194,544",
		// 同一传输协议的两种拼写。API 枚举为大写
		// （KVCacheBackendTransport.Protocol），因此修复建议中应写 CANN。
		Required:         "ascend",
		RequiredAPIValue: "CANN",
	},
	EngineSGLang: {
		// 无约束，且其原因值得记录，因为该文件确实对 protocol 做了比较：
		// :487 将其与 "rdma" 比较，作为复用已初始化传输引擎的四个条件之一，而 :496-498 的 else 分支则自行构建传输引擎。
		// 无论走哪条分支，该值都会在 :515 未经检查地传给 store.setup()。该比较只是一种优化而非硬性要求，
		// 这也是上文第 2 步如此措辞的原因。
		Version:          "v0.5.18",
		Source:           "python/sglang/srt/mem_cache/storage/mooncake_store/mooncake_store.py:98,487,496-498,515",
		Required:         "",
		RequiredAPIValue: "",
	},
}

// checkTransport 拒绝引擎存储后端不支持的传输协议。
//
// 它是将表中的结论转化为拒绝错误的唯一位置，从而保证两个渲染客户端的入口——
// Pod 准入 Webhook 与 ModelDeployment 调谐器——对同一组合给出一致的结论。
// 若存在第二份实现，今天也许一致，但在下一个引擎版本发布时就会产生分歧。
//
// 没有实测条目的引擎会被放行，这是保守的一侧：依据无人读取过的事实去拒绝，
// 会把未实测的引擎变成不可用，而放行它则只是将失败保留在原本就会发生的位置。
//
// 错误信息会同时指明组合中的双方，因为单独来看每一方都无误：传输协议是合法值，引擎也是合法引擎，
// 只有两者的组合会导致启动报错。信息还会指明传输协议的来源——读代码的人去 Binding 上找是找不到的，
// 因为承载该字段的是池子的 KVCacheBackend。
//
// 两条语句刻意使用了两种拼写。池子提供的协议与引擎接受的协议按产物拼写报告，
// 因为那是实际传给容器的值；而需要设置的值按 API 拼写给出，因为该句指明的是 schema 会校验的字段，
// 其枚举区分大小写——在那里使用产物拼写会导致 schema 校验失败，使运维人员的下一步操作因另一个无关原因受阻。
//
// 它不会重复说明未设置协议时的解析规则。该映射属于 mooncake.memberProtocols，
// 在此处复制一份就会变成同一张表的第二份实现。
func checkTransport(engine Engine, protocol string) error {
	facts := engineTransportConstraint[engine]
	if facts.Required == "" || facts.Required == protocol {
		return nil
	}

	return newRefusal(ReasonTransportUnsupported,
		"engine %q accepts only the %q transport and this pool offers %q, so its store backend "+
			"would raise at startup instead of using the cache (measured at %s, %s). The transport "+
			"belongs to the KVCacheBackend the pool names, not to the Binding: set that backend's "+
			"spec.transport.protocol to %q, which is that same transport in the API's spelling, or "+
			"point this workload at a pool that already offers it. An unset protocol is not neutral "+
			"here: the field defaults to Auto, which the backend resolves to one concrete transport "+
			"rather than to whatever an engine wants",
		engine, facts.Required, protocol, facts.Version, facts.Source, facts.RequiredAPIValue)
}

// MatchTransport 是 checkTransport 面向池子的另一半：单一检查回答某个引擎-传输协议组合能否运行，
// 而本函数回答应向引擎传入池子众多有效传输协议中的哪一个。offers 为各成员组按声明顺序排列的有效协议，
// 由调用方通过 mooncake.MemberProtocols 计算得到。
//
// 匹配规则为按声明顺序第一个满足引擎约束的提供项——对于无约束引擎，则直接取第一个提供项。
// 现有部署沿用这一合成规则；面向混合池的新建 Binding 则在准入阶段由 ValidateBindingTransport 检查。
//
// 此处拒绝意味着没有任何成员组满足引擎所需的传输协议。没有实测条目的引擎保持保守放行，
// 遵循与单一检查相同的规则。
//
// 空的 offers 列表并不是一种传输协议结论：它代表无存储形态，Render 会出于自身原因拒绝该形态，
// 因此这里返回空协议与 nil error，而不借用该场景报错。
//
// 列表中的空字符串元素则是另一回事，会被跳过。成员组的有效协议是从 API 值查表得到的产物拼写，
// 因此未在该映射中登记的 API 值会解析为空字符串。目前不会产生空值——已有守护测试断言每个枚举值都有映射条目——
// 但无约束引擎会接受任意 offer，若未来新增第九个枚举值却漏掉映射条目，没有此过滤就会把空的传输变量传给引擎而非拒绝。
// 跳过而非直接拒绝可以让确实解析成功的其他成员组继续给出结果；若整个池子没有任何有效解析结果，则会落入下方的拒绝分支。
func MatchTransport(engine Engine, offers []string) (string, error) {
	if len(offers) == 0 {
		return "", nil
	}

	facts := engineTransportConstraint[engine]
	for _, offer := range offers {
		if offer == "" {
			continue
		}
		if facts.Required == "" || facts.Required == offer {
			return offer, nil
		}
	}

	return "", newRefusal(ReasonTransportUnsupported,
		"engine %q accepts only the %q transport and no group in this pool offers it — the groups "+
			"offer %q — so its store backend would raise at startup instead of using the cache "+
			"(measured at %s, %s). The transport belongs to the KVCacheBackend the pool names, not "+
			"to the Binding: set that backend's spec.transport.protocol to %q or one member group's "+
			"transport.protocol, which is that same transport in the API's spelling, or point this "+
			"workload at a pool that already offers it. An unset protocol is not neutral here: the "+
			"field defaults to Auto, which the backend resolves to one concrete transport rather "+
			"than to whatever an engine wants",
		engine, facts.Required, offers, facts.Version, facts.Source, facts.RequiredAPIValue)
}

// ValidateBindingTransport 拒绝以下新建 Binding：为无约束引擎配置单一传输协议，而其关联的池子却通过多种协议提供缓存块。
// 现有 Binding 保留其已渲染的配置，因此该检查属于准入阶段而非连接器合成阶段。
func ValidateBindingTransport(engine Engine, offers []string) error {
	if engineTransportConstraint[engine].Required != "" {
		return nil
	}
	unique := make([]string, 0, len(offers))
	seen := make(map[string]struct{}, len(offers))
	for _, offer := range offers {
		if offer == "" {
			continue
		}
		if _, ok := seen[offer]; !ok {
			seen[offer] = struct{}{}
			unique = append(unique, offer)
		}
	}
	if len(unique) < 2 {
		return nil
	}
	return newRefusal(ReasonTransportUnsupported,
		"engine %q declares no required transport and this pool offers %q. The engine can be "+
			"configured with only one transport; when it reads a block from a group using another, "+
			"the client reads that target segment's protocol and fails with NotSupportedTransport. "+
			"Set that backend's spec.transport.protocol and any overriding member group's "+
			"transport.protocol to one common transport, or bind this workload to a pool with one "+
			"effective transport",
		engine, unique)
}

// ConfigSourceKeys 按引擎返回会将引擎切换到本 Operator 不写入的配置来源的键，
// 包括环境变量名和命令行参数。
//
// 它们自成一类，区分它们的关键正是其危险所在：所有其他保留键都是本包所渲染设置的第二个来源，
// 因此遵循“后者覆盖前者”。而这些键不是。引擎通过互斥来源上的 if/elif/else 读取其存储配置，
// 因此这里的一个键不会只是覆盖某个值——它会直接走入另一个分支，使本包全部注入逻辑所在的分支根本得不到执行。
//
// 基于 SGLang main（66c7bc83）实测，
// python/sglang/srt/mem_cache/storage/mooncake_store/mooncake_store.py:294-314：
//
//	if extra_config and (...):                     load_from_extra_config(extra_config)
//	elif SGLANG_HICACHE_MOONCAKE_CONFIG_PATH...:   from_file()
//	else:                                          load_from_env()      <-- 本包注入的所有内容都在此分支读取
//
// 因此，如果容器设置了前两者之一，本包发出的任何 MOONCAKE_* 变量都不会被读取，
// 而此时 Pod 上却带着全套变量且注入记录显示成功。本列表存在的目的就是直接拒绝这种失败，而非仅仅记录它。
//
// 上文省略的条件是包含 master_server_address 或 client_server_address 的 extra_config。
// 对于指定权重的部署，渲染器会自行写入仅含 extra_backend_tag 的 extra_config，从而仍走 else 分支；
// 因此该参数在用户容器上会被拒绝，而由本包自行渲染。
//
// 必须按引擎键控，因为一个键只对读取它的引擎起分支选择作用。
// SGLang 的变量对 vLLM 毫无意义，因此以此拒绝 vLLM 容器是没有依据的——
// 这与自有键扫描按渲染实际写入的键进行过滤的原因相同。
//
// vLLM 自身的 MOONCAKE_CONFIG_PATH 刻意不在此列：本包会写入该变量，因此它已作为自有键被拒绝，
// 若在此重复列出，同一次冲突会收到两条错误信息。
func ConfigSourceKeys(engine Engine) (env, args []string) {
	switch engine {
	case EngineSGLang:
		return []string{"SGLANG_HICACHE_MOONCAKE_CONFIG_PATH"},
			[]string{"--hicache-storage-backend-extra-config"}
	case EngineVLLM, EngineVLLMAscend:
		return nil, nil
	}

	return nil, nil
}
