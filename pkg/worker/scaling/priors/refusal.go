package priors

import (
	"fmt"
	"sort"
	"strings"
)

// RefusalReason 是解析被拒绝时有界原因枚举集合。
//
// 原因集合是有界的，从而保证基于拒绝信息构造的 status 字段或 condition 消息不会随 catalog 规模线性膨胀。
type RefusalReason string

const (
	// RefusalCapacityPriorUnresolved 表示无法推导出容量先验。
	//
	// 它涵盖 profile 与部署不匹配、profile 匹配但不存在可行负载点，以及角色不需要容量等情况。
	RefusalCapacityPriorUnresolved RefusalReason = "CapacityPriorUnresolved"

	// RefusalInsufficientLoadCoverage 表示实测负载曲线过于稀疏，不足以确定容量。
	//
	// 它涵盖实测负载档位少于两个的 profile，以及缺乏容量边界依据的 profile。
	RefusalInsufficientLoadCoverage RefusalReason = "InsufficientLoadCoverage"

	// RefusalIncompatibleMetric 表示 profile 未测量角色所需的指标。
	//
	// 它涵盖吞吐量分子与角色要求不符的 profile、未测量所需延迟统计量的 profile，以及缓存约定无法确定的 profile。
	RefusalIncompatibleMetric RefusalReason = "IncompatibleMetric"
)

// maxRefusalDetail 限制拒绝详情中最多可携带的键数量。
const maxRefusalDetail = 8

// RefusalError 说明未能解析出容量先验的原因。
//
// 它实现了 error 接口，因此无法处理拒绝的调用方可原样向上返回，而需要上报状态的调用方可读取其 Reason。
type RefusalError struct {
	// Reason 是解析被拒绝的有界原因。
	Reason RefusalReason `json:"reason"`

	// Message 用一句话说明缺失的内容。
	Message string `json:"message"`

	// Role 是本次解析所针对的角色。
	Role string `json:"role,omitempty"`

	// Shape 是本次解析所涉及的规格形状（若已找到）。
	Shape string `json:"shape,omitempty"`

	// Detail 携带缺失的具体依据信息。
	//
	// 其大小是有界的，因为它最终会写入 status 字段。
	Detail map[string]string `json:"detail,omitempty"`
}

// Error 返回拒绝说明消息。
//
// 实现该方法使拒绝结果可在需要 error 的位置直接返回。
func (r *RefusalError) Error() string {
	if r == nil {
		return ""
	}
	if r.Message != "" {
		return fmt.Sprintf("%s: %s", r.Reason, r.Message)
	}
	return string(r.Reason)
}

// newRefusal 构造一个带有有界详情映射表的拒绝错误。
func newRefusal(reason RefusalReason, message string, detail map[string]string) *RefusalError {
	refusal := &RefusalError{
		Reason:  reason,
		Message: message,
		Detail:  map[string]string{},
	}
	for key, value := range detail {
		if len(refusal.Detail) >= maxRefusalDetail {
			break
		}
		refusal.Detail[key] = value
	}
	if len(refusal.Detail) == 0 {
		refusal.Detail = nil
	}
	return refusal
}

// identityMismatch 按稳定顺序列出存在差异的标识字段。
//
// 顺序是固定的，因此对同一组数据进行两次比较时会报告相同的首个差异字段。
//
// 请求侧代表部署，部署未声明的字段不构成约束，例如未声明量化方式的部署不会与任何测量结果产生冲突。
func identityMismatch(want, got Identity) []string {
	mismatched := make([]string, 0, 9)
	appendIf := func(name string, equal bool) {
		if !equal {
			mismatched = append(mismatched, name)
		}
	}
	// 模型通过规范化形式进行比较，因为部署声明模型时可能省略组织名而数据源会携带组织名，且同一配置存在多种拼写方式。
	appendIf("model.name", ModelNameMatches(want.Model.Name, got.Model.Name))
	appendIf("model.revision", RevisionMatches(want.Model.Revision, got.Model.Revision))
	appendIf("model.tokenizer", TokenizerMatches(want.Model.Tokenizer, got.Model.Tokenizer))
	if got.Model.Quantization != "" {
		appendIf("model.quantization", CanonicalQuantization(want.Model.Quantization) == CanonicalQuantization(got.Model.Quantization))
	}
	// 引擎按系列进行比较，因为部署指定运行时的镜像而数据源指定测量时的镜像，两者虽属同一引擎，但 tag 或摘要可能不同。
	appendIf("engine.image", engineImageMatches(want.Engine.Image, got.Engine.Image))
	// 仅当双方均声明引擎构建版本时才进行比较，因为 catalog 记录的是实测结果而非配置，未指定构建版本的部署并未对其运行的构建版本作出限制。
	if want.Engine.Build != "" && got.Engine.Build != "" {
		appendIf("engine.build", want.Engine.Build == got.Engine.Build)
	}
	appendIf("hardware.accelerator", NormalizeHardwareIdentity(want.Hardware).Accelerator == NormalizeHardwareIdentity(got.Hardware).Accelerator)
	// 仅当部署声明了显存容量和形态规格时才进行比较，因为如果部署的 InstanceType 未指明显存大小，则可与任意显存的测量结果匹配。
	if got.Hardware.MemoryGB > 0 {
		appendIf("hardware.memoryGB", want.Hardware.MemoryGB == got.Hardware.MemoryGB)
	}
	if got.Hardware.FormFactor != "" {
		appendIf("hardware.formFactor", want.Hardware.FormFactor == got.Hardware.FormFactor)
	}
	return mismatched
}

// engineImageMatches 判断两个引擎镜像是否属于同一引擎系列。
//
// 若镜像完全相同则直接匹配，因为数据源与部署指定相同镜像时，其一致程度已超出引擎系列本身。
//
// 若共享同一引擎系列则允许匹配，因为同一引擎构建版本的 tag 和摘要写法多样，比较两种拼写无法判断其背后的引擎。
//
// 若镜像未指明任何已知系列则拒绝匹配，因为本 operator 无法归类的镜像不应与测量结果进行匹配。
func engineImageMatches(want, got string) bool {
	if want == got {
		return true
	}
	wantFamily, gotFamily := EngineFamily(want), EngineFamily(got)
	return wantFamily != "" && wantFamily == gotFamily
}

// servingConfigMismatch 列出存在差异的服务配置字段。
//
// 仅比较 profile 声明的字段，因为目录是测量结果而非配置：未声明任何
// 服务设置的 profile 不施加约束，而声明了某项设置的 profile 仅按该项
// 进行比较。
func servingConfigMismatch(want, got ServingConfig) []string {
	mismatched := make([]string, 0, 6)
	appendIf := func(name, stated, deployed string) {
		if stated == "" {
			return
		}
		if stated != deployed {
			mismatched = append(mismatched, name)
		}
	}
	appendIf("servingConfig.router", want.Router, got.Router)
	appendIf("servingConfig.kvCacheDType", want.KVCacheDType, got.KVCacheDType)
	appendIf("servingConfig.offload", want.Offload, got.Offload)
	appendIf("servingConfig.prefixCaching", want.PrefixCaching, got.PrefixCaching)
	appendIf("servingConfig.speculativeDecoding", want.SpeculativeDecoding, got.SpeculativeDecoding)
	if conflicts := launchParameterConflicts(want, got); len(conflicts) > 0 {
		mismatched = append(mismatched, "servingConfig.launchParameters")
	}
	// 不比较生效的启动参数，因为它们反映的是引擎对启动参数的实际处理结果，
	// 而非部署所请求的配置。
	return mismatched
}

// launchParameterConflicts 返回部署传入但 profile 中记录为不同值的标志。
//
// 仅比较 profile 中记录的标志而非完整列表，因为部署声明的是其设置的标志，
// 而数据源记录的是其运行使用的标志，两者在对方未提及的所有标志上都会存在差异。
//
// 同时读取生效的默认值，因为引擎自行选择的默认值即部署省略该标志时
// 实际运行使用的值。
//
// 不列出具体的值，因为用户填写的值属于其自身配置。
func launchParameterConflicts(want, got ServingConfig) []string {
	recorded := map[string]string{}
	launch, err := ParseLaunchParameters(want.LaunchParameters)
	if err != nil {
		return []string{"the catalog records unreadable launch arguments"}
	}
	for _, parameter := range launch {
		recorded[parameter.Flag] = parameter.Value
	}
	for flag, value := range want.EffectiveDefaults {
		if _, found := recorded[flag]; !found {
			recorded[flag] = value
		}
	}
	deployment, err := ParseLaunchParameters(got.LaunchParameters)
	if err != nil {
		return []string{"the deployment carries unreadable launch arguments"}
	}
	var conflicts []string
	for _, parameter := range deployment {
		prior, known := recorded[parameter.Flag]
		if !known || prior == parameter.Value {
			continue
		}
		conflicts = append(conflicts, parameter.Flag)
	}
	sort.Strings(conflicts)
	return conflicts
}

// topologyMismatch 列出存在差异的拓扑字段。
func topologyMismatch(want, got Topology) []string {
	mismatched := make([]string, 0, 7)
	appendIf := func(name string, equal bool) {
		if !equal {
			mismatched = append(mismatched, name)
		}
	}
	appendIf("topology.gpusPerReplica", want.GPUsPerReplica == got.GPUsPerReplica)
	appendIf("topology.tensorParallel", want.TensorParallel == got.TensorParallel)
	appendIf("topology.pipelineParallel", want.PipelineParallel == got.PipelineParallel)
	appendIf("topology.dataParallel", want.DataParallel == got.DataParallel)
	appendIf("topology.localDataParallel", want.LocalDataParallel == got.LocalDataParallel)
	appendIf("topology.contextParallel", want.ContextParallel == got.ContextParallel)
	appendIf("topology.expertParallel", want.ExpertParallel == got.ExpertParallel)
	return mismatched
}

// formatMismatch 将不匹配的身份字段格式化为拒绝详情。
func formatMismatch(mismatched []string) string {
	if len(mismatched) == 0 {
		return ""
	}
	sorted := append([]string(nil), mismatched...)
	sort.Strings(sorted)
	return strings.Join(sorted, ",")
}
