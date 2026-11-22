package worker

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

// ModelDeploymentDeclaredParallelism 表示角色作者声明的并行形态：从该角色自身的参数流
// （ExtraArgs，或在替换命令行时为其 Command，绝不同时取两者）中解析出的引擎并行度参数，
// 再加上引擎接受的以字面环境变量形式传入的那个并行度。未声明的并行度保持引擎自身的默认值 1——
// operator 不会自行构造任何并行度，因为传输文档必须与引擎保持一致，唯有作者提供的数值才是正确的。
type ModelDeploymentDeclaredParallelism struct {
	// NodeCount 保存显式指定的 --nnodes 值；0 表示未指定或无法读取。
	NodeCount        int
	TensorParallel   int
	PipelineParallel int
	DataParallel     int
	// DataParallelLocal 对应 vLLM 的 --data-parallel-size-local：分配到当前成员上的 DP 宽度份额。
	// 0 表示未声明——此时引擎会将完整宽度放在此处，且规模检查会按此相乘。声明为大于 0 的份额即是每成员的
	// 数据并行宽度——在没有编排参数（wiring flag）时，它恰好等于该成员运行的 rank 数量——因此检查改为乘以该值。
	// 显式声明的 0 同样存储为 0：这是引擎自身用于表示由外部指定 DP 的哨兵值，规模检查将其视为未声明，
	// 而环境变量优先级仍会遵循该值。
	DataParallelLocal int
	// ElasticEPMaxDataParallel 表示 vLLM 的启动 DP 容量，而非运行时并行度。
	// 0 表示未显式声明 --elastic-ep-max-dp-size。
	ElasticEPMaxDataParallel int
	// 以下并行度在当前的传输文档中没有对应字段；保留它们是为了让规模检查以及未来的任何消费方
	// 无需修改解析器即可读取完整信息。
	PrefillContextParallel int
	DecodeContextParallel  int
	// ExpertParallel 是 SGLang 的并行度。vLLM 中同名的概念是一种模式，因此会写入 Modes 中——
	// 这两种形式绝不会合并到同一个字段中。其后的四个字段为 SGLang 其余的并行度：
	// 规模检查根据它们是否存在进行分支判断，未来的消费方则可使用其具体数值。
	ExpertParallel int
	// AttentionDataParallel 是 SGLang 的 attention-DP 宽度：TP 组内的注意力分组数量，
	// 每组对应一个调度器，而 FFN 在各组之间保持张量并行。它不是数据并行乘数，规模检查不会乘以该值：
	// 这些分组共享同一个模型，因此它们占用的宽度即已被计入的 TP 宽度。
	// 1 表示未显式声明 --attn-dp-size。
	AttentionDataParallel    int
	AttentionContextParallel int
	MoEDataParallel          int
	DWDPSize                 int
	// Modes 按规范拼写记录已出现的模式参数（expert-parallel、DP attention、prefill CP）；
	// 消费方（如规模检查）会根据这些参数进行分支处理。
	Modes []string
	// Wiring 按规范拼写记录已出现的组网参数（--nnodes 及其同类参数、--data-parallel-* 集合通信参数、
	// --dist-init-addr）：出现其中任意一个都意味着部分并行宽度可能位于当前成员之外，因此规模检查将保持静默。
	Wiring []string
}

// modelDeploymentParallelFlag 表示参数表中的一行：单个参数的所有受支持拼写、所属类别
// （degree、mode 或 wiring）及其参数个数。每一行仅属于一种类别。
type modelDeploymentParallelFlag struct {
	// names 保存引擎注册的所有带短横线拼写形式，规范的长拼写排在首位：Modes、Wiring 和错误消息均记录该拼写。
	// 已注册拼写会匹配完整 token 及其 "--flag=value" 形式；单字母拼写还会匹配紧连形式（"-n2"），
	// 即 argparse 的短选项规则。
	names []string
	// abbr 保存 argparse 会解析为此行的单短横线缩写形式：它们不是已注册的拼写，而是其唯一前缀
	// （例如 "-tp" 的 "-t"），在所有受支持的 Python 版本中都会被 argparse 的单短横线分支进行前缀匹配。
	// 它们仅匹配独立 token：若紧跟 "="，不同解释器版本的行为会出现分歧——3.12 及以上版本会将 "-t=2"
	// 解析为此行，而 3.10 和 3.11 会拒绝该 token——因此带 "=" 的并行度缩写会在解析时报错，
	// 而模式缩写会直接放行（因为所有版本都会拒绝它）。紧跟裸值（"-t2"）在所有版本中同样会被拒绝，
	// 因此在解析时也会报错：引擎启动时必然发生的崩溃最好在准入阶段就予以拒绝，以便错误消息能指明已注册的拼写。
	// 每个缩写均已对照引擎完整的单短横线选项集（当前为：-q -n -r -tp -pp -dcp -pcp -dp -dpn -dpr
	// -dpl -dpa -dpp -dpb -dph -dpe -dpm -ep -sc -dc -cc -ac）验证过唯一性；若未来新增的引擎选项发生冲突，
	// 该形式会触发引擎自身的歧义拒绝错误（即显式报错方向）。
	abbr []string
	// degree 标记需要校验并存储整数值的参数。degree 参数总是需要携带参数值。
	degree bool
	// min 为并行度值的下界，除 vLLM 的本地数据并行份额（其引擎接受 0 作为哨兵值）外，
	// 其余参数下界均为 1。
	min int
	// mode 标记不带参数值且按是否出现记录到 Modes 中的参数。
	mode bool
	// wiring 标记在 Wiring 中按是否出现来记录的参数；takesValue 表示其后是否跟随值
	// token，扫描时必须跳过该值以保持参数流对齐。
	wiring     bool
	takesValue bool
}

// modelDeploymentParallelFlags 是参数识别表，API 支持的每个引擎各占一项。各行
// 依据引擎自身的解析器（vllm/engine/arg_utils.py、
// sglang/srt/arg_groups/fields/parallel.py）构建；未配置条目的引擎默认全部
// 解析为 1 且不报错，而新增支持的参数只需添加一行，无需修改解析器。
var modelDeploymentParallelFlags = map[string][]modelDeploymentParallelFlag{
	workercore.ModelDeploymentEngineVLLM: {
		{names: []string{"--tensor-parallel-size", "-tp"}, abbr: []string{"-t"}, degree: true, min: 1},
		{names: []string{"--pipeline-parallel-size", "-pp"}, degree: true, min: 1},
		{names: []string{"--data-parallel-size", "-dp"}, degree: true, min: 1},
		{names: []string{"--data-parallel-size-local", "-dpl"}, degree: true, min: 0},
		{names: []string{"--elastic-ep-max-dp-size"}, degree: true, min: 1},
		{names: []string{"--prefill-context-parallel-size", "-pcp"}, abbr: []string{"-pc"}, degree: true, min: 1},
		{names: []string{"--decode-context-parallel-size", "-dcp"}, degree: true, min: 1},
		{names: []string{"--enable-expert-parallel", "-ep"}, abbr: []string{"-e"}, mode: true},
		{names: []string{"--nnodes", "-n"}, wiring: true, takesValue: true},
		{names: []string{"--node-rank", "-r"}, wiring: true, takesValue: true},
		{names: []string{"--master-addr"}, wiring: true, takesValue: true},
		{names: []string{"--master-port"}, wiring: true, takesValue: true},
		{names: []string{"--data-parallel-rank", "-dpn"}, wiring: true, takesValue: true},
		{names: []string{"--data-parallel-start-rank", "-dpr"}, wiring: true, takesValue: true},
		{names: []string{"--data-parallel-address", "-dpa"}, wiring: true, takesValue: true},
		{names: []string{"--data-parallel-rpc-port", "-dpp"}, wiring: true, takesValue: true},
		{names: []string{"--data-parallel-backend", "-dpb"}, wiring: true, takesValue: true},
		{names: []string{"--headless"}, wiring: true},
		{names: []string{"--data-parallel-hybrid-lb", "-dph"}, wiring: true},
		{names: []string{"--data-parallel-external-lb", "-dpe"}, wiring: true},
		{names: []string{"--data-parallel-multi-port-external-lb", "-dpm"}, wiring: true},
	},
	workercore.ModelDeploymentEngineSGLang: {
		{names: []string{"--tp-size", "--tensor-parallel-size"}, degree: true, min: 1},
		{names: []string{"--pp-size", "--pipeline-parallel-size"}, degree: true, min: 1},
		{names: []string{"--dp-size", "--data-parallel-size"}, degree: true, min: 1},
		{names: []string{"--attn-dp-size", "--attention-data-parallel-size"}, degree: true, min: 1},
		{names: []string{"--ep-size", "--expert-parallel-size", "--ep"}, degree: true, min: 1},
		{names: []string{"--dcp-size", "--decode-context-parallel-size"}, degree: true, min: 1},
		{names: []string{"--attn-cp-size", "--attention-context-parallel-size"}, degree: true, min: 1},
		{names: []string{"--moe-dp-size", "--moe-data-parallel-size"}, degree: true, min: 1},
		{names: []string{"--dwdp-size"}, degree: true, min: 1},
		{names: []string{"--enable-dp-attention"}, mode: true},
		{names: []string{"--enable-prefill-cp"}, mode: true},
		{names: []string{"--nnodes"}, wiring: true, takesValue: true},
		{names: []string{"--node-rank"}, wiring: true, takesValue: true},
		{names: []string{"--dist-init-addr", "--nccl-init-addr"}, wiring: true, takesValue: true},
	},
}

// ParseModelDeploymentDeclaredParallelism 按角色所用引擎的方式读取其参数配置：支持
// 两种传值形式、重复出现时后者覆盖前者、仅在引擎自身解析器会改写时支持下划线拼写、
// 唯一前缀正常解析而歧义前缀透传交由引擎拒绝、支持引擎自身可解析的单短横线缩写及
// 单字母注册拼写的拼接传值、单独的 "--" 终止参数识别，以及像 argparse 那样将负数
// 作为参数值消费。数值按引擎自身的整数字符集解析。仅当已知并行度参数缺少值、
// 非整数、越界或低于下限时，以及作为有效 DP 来源的 VLLM_DP_SIZE 无法解析时才会
// 报错；其余所有 token 均由引擎自行处理并直接透传，从而避免未知参数在此处产生
// 错误结果。
//
// DP 环境变量的优先级与引擎一致（parallel.py：当 data_parallel_size 大于 1 或
// 声明的本地大小为 0 时走 CLI 路径且不读取该环境变量；否则由环境变量提供并行度），
// 因此同时指定两处 DP 时，中继与引擎不会得出不同的数值。
func ParseModelDeploymentDeclaredParallelism(
	engine string, extraArgs []string, env []workercore.ModelDeploymentEnvVar,
) (ModelDeploymentDeclaredParallelism, error) {
	reading, err := scanModelDeploymentParallelism(engine, extraArgs, env)
	return reading.declared, err
}

// modelDeploymentParallelReading 表示单个参数流的解析结果：供传输文档读取的数值记录，
// 以及供状态读取使用的存在性记录，即该参数流自身声明了哪些并行度。
type modelDeploymentParallelReading struct {
	declared ModelDeploymentDeclaredParallelism
	// present 按状态结构体字段顺序标记参数流自身声明了哪些并行度槽位；参数流从未提及
	// 的槽位保持缺失，而上述数值默认值会刻意抹除这一事实。
	present [modelDeploymentDegreeSlotCount]bool
}

// modelDeploymentDegreeSlot 按状态结构体的声明顺序标识一个并行度字段。
type modelDeploymentDegreeSlot int

const (
	modelDeploymentSlotTensorParallel modelDeploymentDegreeSlot = iota
	modelDeploymentSlotPipelineParallel
	modelDeploymentSlotDataParallel
	modelDeploymentSlotDataParallelLocal
	modelDeploymentSlotPrefillContextParallel
	modelDeploymentSlotDecodeContextParallel
	modelDeploymentSlotExpertParallel
	modelDeploymentSlotAttentionContextParallel
	modelDeploymentSlotMoEDPSize
	modelDeploymentSlotDWDPSize
	modelDeploymentDegreeSlotCount
)

func scanModelDeploymentParallelism(
	engine string, extraArgs []string, env []workercore.ModelDeploymentEnvVar,
) (modelDeploymentParallelReading, error) {
	reading := modelDeploymentParallelReading{}
	declared := &reading.declared
	*declared = ModelDeploymentDeclaredParallelism{
		TensorParallel:           1,
		PipelineParallel:         1,
		DataParallel:             1,
		PrefillContextParallel:   1,
		DecodeContextParallel:    1,
		ExpertParallel:           1,
		AttentionDataParallel:    1,
		AttentionContextParallel: 1,
		MoEDataParallel:          1,
		DWDPSize:                 1,
	}

	table := modelDeploymentParallelFlags[engine]

	localZeroDeclared := false

	for i := 0; i < len(extraArgs); i++ {
		token := extraArgs[i]
		if token == "--" {
			break
		}

		flag, value, hasValue, ok, err := matchModelDeploymentParallelFlag(engine, table, token)
		if err != nil {
			return reading, err
		}
		if !ok {
			continue
		}
		canonical := flag.names[0]

		switch {
		case flag.mode:
			// 模式参数不接受值；若显式附带了值，则由引擎自行拒绝，因此不予记录直接透传。
			if hasValue {
				continue
			}
			if !slices.Contains(declared.Modes, canonical) {
				declared.Modes = append(declared.Modes, canonical)
			}
		case flag.wiring:
			// 不接收值的连线参数若附带了值，则由引擎自行拒绝，因此不予记录直接透传。
			if hasValue && !flag.takesValue {
				continue
			}
			if !slices.Contains(declared.Wiring, canonical) {
				declared.Wiring = append(declared.Wiring, canonical)
			}
			if flag.takesValue && !hasValue && i+1 < len(extraArgs) &&
				modelDeploymentConsumableValue(extraArgs[i+1]) {
				i++
				value = extraArgs[i]
			}
			if canonical == "--nnodes" {
				declared.NodeCount, _ = modelDeploymentEngineInt(value)
			}
		case flag.degree:
			if !hasValue {
				if i+1 >= len(extraArgs) || !modelDeploymentConsumableValue(extraArgs[i+1]) {
					return reading, fmt.Errorf("the %s declaration has no value", canonical)
				}
				i++
				value = extraArgs[i]
			}

			degree, err := modelDeploymentEngineInt(value)
			switch {
			case errors.Is(err, errModelDeploymentIntRange):
				return reading, fmt.Errorf("the %s value %q is out of range", canonical, value)
			case err != nil:
				return reading, fmt.Errorf("the %s value %q is not an integer", canonical, value)
			}
			if degree < flag.min {
				return reading, fmt.Errorf("the %s value %d is below %d", canonical, degree, flag.min)
			}

			switch canonical {
			case "--tensor-parallel-size", "--tp-size":
				declared.TensorParallel = degree
				reading.present[modelDeploymentSlotTensorParallel] = true
			case "--pipeline-parallel-size", "--pp-size":
				declared.PipelineParallel = degree
				reading.present[modelDeploymentSlotPipelineParallel] = true
			case "--data-parallel-size", "--dp-size":
				declared.DataParallel = degree
				reading.present[modelDeploymentSlotDataParallel] = true
			case "--attn-dp-size", "--attention-data-parallel-size":
				// 注意力并行宽度不是状态中的并行度。它在传输文档中没有对应槽位，
				// 因此仅用于负载均衡判定，绝不参与规模校验的乘法计算。
				declared.AttentionDataParallel = degree
			case "--data-parallel-size-local":
				declared.DataParallelLocal = degree
				reading.present[modelDeploymentSlotDataParallelLocal] = true
				localZeroDeclared = degree == 0
			case "--elastic-ep-max-dp-size":
				declared.ElasticEPMaxDataParallel = degree
			case "--prefill-context-parallel-size":
				declared.PrefillContextParallel = degree
				reading.present[modelDeploymentSlotPrefillContextParallel] = true
			case "--decode-context-parallel-size", "--dcp-size":
				declared.DecodeContextParallel = degree
				reading.present[modelDeploymentSlotDecodeContextParallel] = true
			case "--ep-size":
				declared.ExpertParallel = degree
				reading.present[modelDeploymentSlotExpertParallel] = true
			case "--attn-cp-size":
				declared.AttentionContextParallel = degree
				reading.present[modelDeploymentSlotAttentionContextParallel] = true
			case "--moe-dp-size":
				declared.MoEDataParallel = degree
				reading.present[modelDeploymentSlotMoEDPSize] = true
			case "--dwdp-size":
				declared.DWDPSize = degree
				reading.present[modelDeploymentSlotDWDPSize] = true
			}
		}
	}

	if engine != workercore.ModelDeploymentEngineVLLM {
		return reading, nil
	}

	// 引擎仅在 CLI 未驱动数据并行时才读取该环境变量；此处遵循相同逻辑，而不自行
	// 对两种来源排定优先级。
	if declared.DataParallel > 1 || localZeroDeclared {
		return reading, nil
	}
	raw, ok := modelDeploymentEnvValue(env, "VLLM_DP_SIZE")
	if !ok {
		return reading, nil
	}
	degree, err := modelDeploymentEngineInt(raw)
	switch {
	case errors.Is(err, errModelDeploymentIntRange):
		return reading, fmt.Errorf("the VLLM_DP_SIZE value %q is out of range", raw)
	case err != nil:
		return reading, fmt.Errorf("the VLLM_DP_SIZE value %q is not an integer", raw)
	}
	if degree < 1 {
		return reading, fmt.Errorf("the VLLM_DP_SIZE value %d is below 1", degree)
	}
	declared.DataParallel = degree
	reading.present[modelDeploymentSlotDataParallel] = true

	return reading, nil
}

// modelDeploymentDegreeSlots 将数值并行度映射到其状态字段：包含用于提示消息的
// 规范拼写、读取数值的字段以及写入的目标状态字段。
var modelDeploymentDegreeSlots = [modelDeploymentDegreeSlotCount]struct {
	name string
	of   func(*ModelDeploymentDeclaredParallelism) int
	set  func(*workercore.ModelDeploymentParallelismDeclaredStatus, *int32)
}{
	modelDeploymentSlotTensorParallel: {
		"--tensor-parallel-size",
		func(p *ModelDeploymentDeclaredParallelism) int { return p.TensorParallel },
		func(s *workercore.ModelDeploymentParallelismDeclaredStatus, d *int32) { s.TensorParallel = d },
	},
	modelDeploymentSlotPipelineParallel: {
		"--pipeline-parallel-size",
		func(p *ModelDeploymentDeclaredParallelism) int { return p.PipelineParallel },
		func(s *workercore.ModelDeploymentParallelismDeclaredStatus, d *int32) { s.PipelineParallel = d },
	},
	modelDeploymentSlotDataParallel: {
		"--data-parallel-size",
		func(p *ModelDeploymentDeclaredParallelism) int { return p.DataParallel },
		func(s *workercore.ModelDeploymentParallelismDeclaredStatus, d *int32) { s.DataParallel = d },
	},
	modelDeploymentSlotDataParallelLocal: {
		"--data-parallel-size-local",
		func(p *ModelDeploymentDeclaredParallelism) int { return p.DataParallelLocal },
		func(s *workercore.ModelDeploymentParallelismDeclaredStatus, d *int32) { s.DataParallelLocal = d },
	},
	modelDeploymentSlotPrefillContextParallel: {
		"--prefill-context-parallel-size",
		func(p *ModelDeploymentDeclaredParallelism) int { return p.PrefillContextParallel },
		func(s *workercore.ModelDeploymentParallelismDeclaredStatus, d *int32) { s.PrefillContextParallel = d },
	},
	modelDeploymentSlotDecodeContextParallel: {
		"--decode-context-parallel-size",
		func(p *ModelDeploymentDeclaredParallelism) int { return p.DecodeContextParallel },
		func(s *workercore.ModelDeploymentParallelismDeclaredStatus, d *int32) { s.DecodeContextParallel = d },
	},
	modelDeploymentSlotExpertParallel: {
		"--ep-size",
		func(p *ModelDeploymentDeclaredParallelism) int { return p.ExpertParallel },
		func(s *workercore.ModelDeploymentParallelismDeclaredStatus, d *int32) { s.ExpertParallel = d },
	},
	modelDeploymentSlotAttentionContextParallel: {
		"--attn-cp-size",
		func(p *ModelDeploymentDeclaredParallelism) int { return p.AttentionContextParallel },
		func(s *workercore.ModelDeploymentParallelismDeclaredStatus, d *int32) { s.AttentionContextParallel = d },
	},
	modelDeploymentSlotMoEDPSize: {
		"--moe-dp-size",
		func(p *ModelDeploymentDeclaredParallelism) int { return p.MoEDataParallel },
		func(s *workercore.ModelDeploymentParallelismDeclaredStatus, d *int32) { s.MoEDPSize = d },
	},
	modelDeploymentSlotDWDPSize: {
		"--dwdp-size",
		func(p *ModelDeploymentDeclaredParallelism) int { return p.DWDPSize },
		func(s *workercore.ModelDeploymentParallelismDeclaredStatus, d *int32) { s.DWDPSize = d },
	},
}

// statusDegrees 将解析结果的存在性记录转换为状态结构体：每个已声明的并行度对应一个
// 指针，保留包括显式 1 和显式本地 0 在内的精确值。若某个并行度虽被宿主机 int 接受
// 但超出状态字段可容纳范围，则直接报错并指明对应参数，拒绝整份解析结果，而不回绕
// 成角色从未声明的数值。
func (r modelDeploymentParallelReading) statusDegrees() (
	workercore.ModelDeploymentParallelismDeclaredStatus, string,
) {
	var out workercore.ModelDeploymentParallelismDeclaredStatus
	for slot, table := range modelDeploymentDegreeSlots {
		if !r.present[slot] {
			continue
		}
		degree := table.of(&r.declared)
		if degree > math.MaxInt32 || degree < math.MinInt32 {
			return workercore.ModelDeploymentParallelismDeclaredStatus{},
				fmt.Sprintf("the %s value %d is beyond the status degree fields", table.name, degree)
		}
		value := int32(degree)
		table.set(&out, &value)
	}

	return out, ""
}

// ReadModelDeploymentRoleParallelism 读取单个角色自身参数流的状态：即其 ExtraArgs
// （沿用与传输解析器相同的引擎规则），且仅在引擎会读取环境变量时才读取角色的
// 环境变量。仅当参数流声明了某并行度时该并行度才存在；负载均衡形态由参数流自身的
// 连线参数推导得出；无法读取的参数流会使所有并行度保持缺失，并在来源字段中说明原因。
//
// 已替换命令行的角色不在此读取。被替换后的 argv 属于角色自身配置——operator 并未
// 为其合成参数，因此状态不会从中提取并行度，而是将来源标记为非托管；在准入必须与
// 实际运行内容一致的场景下，传输校验仍会读取该配置。
func ReadModelDeploymentRoleParallelism(
	engine string, role *workercore.ModelDeploymentRole,
) workercore.ModelDeploymentRoleParallelismStatus {
	if len(role.Command) > 0 {
		return modelDeploymentParallelismUnreadable(modelDeploymentReasonUnmanaged)
	}

	reading, err := scanModelDeploymentParallelism(engine, role.ExtraArgs, role.Env)
	if err != nil {
		return modelDeploymentParallelismUnreadable(err.Error())
	}
	declared, overflow := reading.statusDegrees()
	if overflow != "" {
		return modelDeploymentParallelismUnreadable(overflow)
	}

	loadBalance, balanceReason := modelDeploymentLoadBalance(engine,
		ModelDeploymentEffectiveRoleKind(role), role.Env, reading.declared)

	return workercore.ModelDeploymentRoleParallelismStatus{
		Declared:    declared,
		Modes:       modelDeploymentModeMap(reading.declared.Modes),
		LoadBalance: loadBalance,
		Source: workercore.ModelDeploymentParallelismSourceStatus{
			Kind:             workercore.ModelDeploymentParallelismSourceKindExtraArgs,
			Complete:         true,
			UnreadableReason: balanceReason,
		},
	}
}

// modelDeploymentParallelismUnreadable 表示未能产出可信解析结果的参数流返回值：
// 无任何已声明内容、无任何推导结果，并在接口要求的位置携带原因。
func modelDeploymentParallelismUnreadable(reason string) workercore.ModelDeploymentRoleParallelismStatus {
	return workercore.ModelDeploymentRoleParallelismStatus{
		LoadBalance: workercore.ModelDeploymentLoadBalanceUnknown,
		Source: workercore.ModelDeploymentParallelismSourceStatus{
			Kind:             workercore.ModelDeploymentParallelismSourceKindUnknown,
			UnreadableReason: reason,
		},
	}
}

// modelDeploymentBalanceShapes 是用于判定负载均衡形态的 vLLM 连线参数。rank 系列
// 参数表示通过 rank 推导的外部负载均衡：携带指定数据并行 rank 启动的成员由进程外部
// 进行负载均衡。此处未列出的引擎均采用内部负载均衡。
var modelDeploymentBalanceShapes = map[string]workercore.ModelDeploymentLoadBalance{
	"--data-parallel-external-lb":            workercore.ModelDeploymentLoadBalanceExternal,
	"--data-parallel-rank":                   workercore.ModelDeploymentLoadBalanceExternal,
	"--data-parallel-hybrid-lb":              workercore.ModelDeploymentLoadBalanceHybrid,
	"--data-parallel-multi-port-external-lb": workercore.ModelDeploymentLoadBalanceMultiPort,
}

// modelDeploymentSGLangRustServerEnv 是使 SGLang 进入 Rust-server 模式的环境变量，
// 该模式下每个注意力 DP 组都会托管各自独立的监听器。
const modelDeploymentSGLangRustServerEnv = "SGLANG_RUST_SERVER"

// modelDeploymentSGLangExternalDP 报告 SGLang 角色是否声明了外部注意力数据并行形态：
// 即在 Rust server 下跨成员启动的注意力 DP。
//
// 这三个条件共同构成同一形态，缺一不可。大于 1 的注意力宽度决定存在多个注意力组；
// 大于 1 的节点数决定这些组分布在不同成员上；Rust server 则为每个组提供独立的
// 监听器，使其可以从外部访问。若未启用 Rust server，各组共享同一个前端，并在组内
// 自行完成负载均衡。
//
// 角色必须是 server 角色。prefill 或 decode 角色各自通过自身链路提供服务，该链路的
// 负载均衡由链路自身决定：在 server 角色上表现为外部均衡的启动方式并不能说明
// prefill 如何均衡，因此分离式角色保持未分类状态。
//
// 参数值按引擎的方式读取，因为引擎会拒绝无法解析的值，operator 若基于此类值
// 做判定只能是猜测。引擎会拒绝的值不参与判定，由引擎自行中止启动。
func modelDeploymentSGLangExternalDP(
	engine string, kind workercore.ModelDeploymentRoleKind,
	env []workercore.ModelDeploymentEnvVar, declared ModelDeploymentDeclaredParallelism,
) bool {
	if engine != workercore.ModelDeploymentEngineSGLang {
		return false
	}
	if kind != workercore.ModelDeploymentRoleKindServer {
		return false
	}
	if declared.AttentionDataParallel < 2 || declared.NodeCount < 2 {
		return false
	}
	value, ok := modelDeploymentEnvValue(env, modelDeploymentSGLangRustServerEnv)
	if !ok {
		return false
	}
	return modelDeploymentEngineBool(value)
}

// modelDeploymentEngineBool 按 SGLang 自身的环境变量读取器方式解析布尔值：
// 仅该读取器视为 true 的拼写（不区分大小写）返回 true。其余所有值均返回 false，
// 与引擎自身遇到未知拼写时的静默处理一致。
func modelDeploymentEngineBool(value string) bool {
	switch strings.ToLower(value) {
	case modelDeploymentTrueValue, "1":
		return true
	default:
		return false
	}
}

// modelDeploymentLoadBalance 根据参数流的连线参数推导负载均衡形态。指向同一形态的
// 参数相互一致（指定 rank 的同时配置外部负载均衡器仍属于 External）；指向不同
// 形态的参数则无法推导，并会指出冲突之处。
//
// SGLang 的判定自成一类，因为 SGLang 的均衡方式不是单个参数，而是启动配置声明的
// 整体形态。上方的映射表无法识别它：表中不包含 SGLang 的连线参数，且其自身参数
// 也不在表中参与判定，因此两个来源不会针对同一引擎产生冲突。
//
// 角色类型也是结果的一部分，因为同一形态对 server 角色和分离式角色含义不同：
// prefill 或 decode 角色通过各自链路提供服务，在 server 角色上会采用外部均衡的
// 启动配置并不能说明该链路如何提供服务。
func modelDeploymentLoadBalance(
	engine string, kind workercore.ModelDeploymentRoleKind,
	env []workercore.ModelDeploymentEnvVar, declared ModelDeploymentDeclaredParallelism,
) (workercore.ModelDeploymentLoadBalance, string) {
	if modelDeploymentSGLangExternalDP(engine, kind, env, declared) {
		return workercore.ModelDeploymentLoadBalanceExternal, ""
	}
	wiring := declared.Wiring
	shapes := map[workercore.ModelDeploymentLoadBalance][]string{}
	for _, flag := range wiring {
		if shape, ok := modelDeploymentBalanceShapes[flag]; ok {
			shapes[shape] = append(shapes[shape], flag)
		}
	}
	startRank := slices.Contains(wiring, "--data-parallel-start-rank")
	if startRank && len(shapes[workercore.ModelDeploymentLoadBalanceExternal]) > 0 {
		shapes[workercore.ModelDeploymentLoadBalanceHybrid] = append(
			shapes[workercore.ModelDeploymentLoadBalanceHybrid], "--data-parallel-start-rank",
		)
	}
	if len(shapes) == 0 {
		// headless rank 属于 Internal 组。仅当本地组小于全局组时，start rank 才会
		// 选择外部路由。
		if startRank && !slices.Contains(wiring, "--headless") {
			local := declared.DataParallelLocal
			if local > 0 && local < declared.DataParallel {
				if local == 1 {
					return workercore.ModelDeploymentLoadBalanceExternal, ""
				}
				return workercore.ModelDeploymentLoadBalanceHybrid, ""
			}
			if local == 0 && slices.Contains(wiring, "--nnodes") && declared.NodeCount != 1 {
				if declared.NodeCount <= 0 {
					return workercore.ModelDeploymentLoadBalanceUnknown,
						"nnodes must be a readable positive integer to derive start-rank routing"
				}
				return workercore.ModelDeploymentLoadBalanceUnknown,
					"start-rank routing requires an explicit data-parallel-size-local when nnodes exceeds 1"
			}
		}
		return workercore.ModelDeploymentLoadBalanceInternal, ""
	}
	if len(shapes) == 1 {
		for shape := range shapes {
			if shape == workercore.ModelDeploymentLoadBalanceHybrid {
				if declared.DataParallelLocal == 1 {
					return workercore.ModelDeploymentLoadBalanceExternal, ""
				}
				if declared.DataParallelLocal == declared.DataParallel {
					return workercore.ModelDeploymentLoadBalanceInternal, ""
				}
			}
			return shape, ""
		}
	}

	var flags []string
	for _, named := range shapes {
		flags = append(flags, named...)
	}
	slices.Sort(flags)

	return workercore.ModelDeploymentLoadBalanceUnknown,
		fmt.Sprintf("the balance flags %s derive to different shapes", strings.Join(flags, " and "))
}

// modelDeploymentModeKeys 定义引擎模式参数对应的状态键名，即已声明字段所用
// 术语体系中的规范概念。此处未列出的模式参数保留其规范参数拼写作为键名，
// 从而确保观测到的模式不会从 map 中丢失。
var modelDeploymentModeKeys = map[string]string{
	"--enable-expert-parallel": "expertParallel",
	"--enable-dp-attention":    "dpAttention",
	"--enable-prefill-cp":      "prefillContextParallel",
}

// modelDeploymentModeMap 将扫描得到的规范模式拼写转换为状态 map，其中键的
// 存在即表示布尔真值，而键不存在并不等同于 false。
func modelDeploymentModeMap(modes []string) map[string]bool {
	if len(modes) == 0 {
		return nil
	}

	out := make(map[string]bool, len(modes))
	for _, mode := range modes {
		key, ok := modelDeploymentModeKeys[mode]
		if !ok {
			key = mode
		}
		out[key] = true
	}

	return out
}

// matchModelDeploymentParallelFlag 按引擎自身解析器的方式将单个 token 与引擎参数表
// 进行匹配：vLLM 会在匹配前将长参数键中的下划线改写为短横线；完全匹配的已注册拼写
// 在两种传值形式下均直接胜出；其次匹配该行列表中的独立单短横线缩写；否则长参数键
// 按唯一前缀缩写解析——且按拼写数而非动作数计数，因为即使两个选项字符串属于同一
// action，argparse 仍会判定同时匹配两者的前缀存在歧义。紧跟在单字母已注册拼写后的
// token（如 "-n2"）会拆分为该拼写及其余部分作为参数值。有歧义或未知的 token 报告
// 未匹配并透传交由引擎判断。有两类 token 会直接报错而非透传：一是带 "=" 的并行度
// 缩写，argparse 仅从 Python 3.12 起才为缩写的单短横线参数键拆分 "="，而 3.10 和
// 3.11 会拒绝该 token——此处无法获知镜像内的 Python 版本，因此宁可拒绝也不猜测
// 写入引擎未必能运行的文档；二是带拼接值的缩写（如 "-t2"、"-tp2"），所有受支持的
// argparse 在引擎启动时都会拒绝此类写法，此处提前拒绝以便错误消息能够指明已注册的
// 规范拼写。
func matchModelDeploymentParallelFlag(
	engine string, table []modelDeploymentParallelFlag, token string,
) (modelDeploymentParallelFlag, string, bool, bool, error) {
	var zero modelDeploymentParallelFlag

	if !strings.HasPrefix(token, "-") {
		return zero, "", false, false, nil
	}

	key, value, hasValue := token, "", false
	if i := strings.IndexByte(token, '='); i >= 0 {
		key, value, hasValue = token[:i], token[i+1:], true
	}

	long := strings.HasPrefix(key, "--")
	if long && engine == workercore.ModelDeploymentEngineVLLM {
		key = modelDeploymentVLLMFlagKey(key)
	}

	for _, flag := range table {
		if slices.Contains(flag.names, key) {
			return flag, value, hasValue, true, nil
		}
	}

	for _, flag := range table {
		if !slices.Contains(flag.abbr, key) {
			continue
		}
		if hasValue && flag.degree {
			return zero, "", false, false, fmt.Errorf(
				"the %s abbreviation %q is accepted by argparse only from Python 3.12 "+
					"and refused before, so the engine's reading depends on the image; "+
					"use the registered %s",
				key, token, flag.names[0],
			)
		}
		if !hasValue {
			return flag, "", false, true, nil
		}
		// 携带参数值的模式缩写在所有版本上都会被引擎自身拒绝，因此不予记录直接透传。
	}

	if !long {
		if !hasValue && len(token) > 2 {
			for _, flag := range table {
				if slices.Contains(flag.names, token[:2]) {
					return flag, token[2:], true, true, nil
				}
			}
			for _, flag := range table {
				if slices.Contains(flag.abbr, token[:2]) {
					return zero, "", false, false, fmt.Errorf(
						"the %s abbreviation %q carries a glued value, which argparse "+
							"refuses on every supported Python version; use the registered %s",
						token[:2], token, flag.names[0],
					)
				}
			}
		}
		return zero, "", false, false, nil
	}

	spellings := 0
	matched := -1
	for i, flag := range table {
		for _, name := range flag.names {
			if strings.HasPrefix(name, key) {
				spellings++
				matched = i
			}
		}
	}
	if spellings != 1 {
		return zero, "", false, false, nil
	}

	return table[matched], value, hasValue, true, nil
}

// modelDeploymentConsumableValue 报告需带值参数后面的 token 是否可作为其参数值。
// argparse 会消费任何看起来不像选项的 token——而对没有数字型选项字符串的解析器
// （两个引擎的参数表均无此类选项）而言，形如负数的 token 不算选项，因此 "-2" 在
// 此处与引擎中一样会被当作值消费。
func modelDeploymentConsumableValue(token string) bool {
	if token == "" || token[0] != '-' {
		return true
	}

	return modelDeploymentLooksLikeNegativeNumber(token)
}

// modelDeploymentLooksLikeNegativeNumber 对应 argparse 自身的判断正则：^-\d+$ 或 ^-\d*\.\d+$。
func modelDeploymentLooksLikeNegativeNumber(token string) bool {
	if len(token) < 2 || token[0] != '-' {
		return false
	}

	rest := token[1:]
	if modelDeploymentAllDigits(rest) {
		return true
	}

	integer, fraction, found := strings.Cut(rest, ".")
	return found && (integer == "" || modelDeploymentAllDigits(integer)) &&
		modelDeploymentAllDigits(fraction)
}

func modelDeploymentAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}

	return true
}

var (
	// errModelDeploymentIntSyntax 标记引擎自身的 int() 转换会视为非整数而拒绝的并行度值。
	errModelDeploymentIntSyntax = errors.New("not an integer")
	// errModelDeploymentIntRange 标记超出宿主机 int 范围的并行度值，引擎的任意精度 int()
	// 虽能接受，但任何实际部署都不可能使用如此大的数值。
	errModelDeploymentIntRange = errors.New("out of range")
)

// modelDeploymentEngineInt 使用引擎自身的 int() 字符集解析并行度值：忽略首尾空白，
// 允许一个前导符号，且数字之间可包含单个下划线——" 3 " 解析为 3，"1_0" 解析为 10，
// 而 "_3" 和 "3_" 不属于有效整数。
func modelDeploymentEngineInt(raw string) (int, error) {
	s := strings.TrimSpace(raw)
	for i := 0; i < len(s); i++ {
		if s[i] != '_' {
			continue
		}
		if i == 0 || i == len(s)-1 ||
			s[i-1] < '0' || s[i-1] > '9' || s[i+1] < '0' || s[i+1] > '9' {
			return 0, errModelDeploymentIntSyntax
		}
	}

	n, err := strconv.Atoi(strings.ReplaceAll(s, "_", ""))
	if err != nil {
		if errors.Is(err, strconv.ErrRange) {
			return 0, errModelDeploymentIntRange
		}
		return 0, errModelDeploymentIntSyntax
	}

	return n, nil
}

// modelDeploymentEnvValue 按后写覆盖先写的顺序读取字面环境变量条目，这与容器运行时
// 对重复变量名的处理顺序一致。
func modelDeploymentEnvValue(env []workercore.ModelDeploymentEnvVar, name string) (string, bool) {
	for i := len(env) - 1; i >= 0; i-- {
		if env[i].Name == name {
			return env[i].Value, true
		}
	}

	return "", false
}
