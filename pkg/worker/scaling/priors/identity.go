package priors

import (
	"strings"

	"gpustack.ai/gpustack/pkg/nodefeature"
)

// UnspecifiedRevision 是未固定权重提交的标识所携带的 revision 值。
//
// 它使用具名值而非空字段，因为空字段表示记录未提供信息，而具名值则表明数据源明确说明了未固定提交。
const UnspecifiedRevision = "unspecified"

// quantizationAliases 将量化拼写形式映射到其规范标记。
//
// 该映射表是封闭的，因为未收录的拼写是本 operator 无法担保的格式，而未经担保的配方并非测量运行时所采用的配方。
//
// 键采用规范化拼写，因此 "fp8_e4m3" 和 "fp8-e4m3" 会命中同一条目。
// "w8a8-fp8" 与 "w8a8" 保持区分，因为仅权重量化的 8 位方案与 8 位浮点方案属于不同的量化配方。
var quantizationAliases = map[string]string{
	"fp8":         "fp8",
	"fp8-e4m3":    "fp8",
	"fp8-e4m3fn":  "fp8",
	"w8a8-fp8":    "fp8",
	"bf16":        "bf16",
	"bfloat16":    "bf16",
	"fp16":        "fp16",
	"float16":     "fp16",
	"half":        "fp16",
	"fp4":         "fp4",
	"nvfp4":       "fp4",
	"mxfp4":       "fp4",
	"int8":        "int8",
	"w8a8":        "int8",
	"smoothquant": "int8",
	"int4":        "int4",
	"w4a16":       "int4",
	"awq":         "awq",
	"gptq":        "gptq",
}

// acceleratorSpecs 将加速器系列映射到无需显式声明即可推断的已知属性。
//
// 键为 pkg/nodefeature 中的系列名称，因此加速器表解析出的名称唯一对应一种加速器。
//
// 显存容量和形态规格（form factor）仅为默认值而非绝对事实，因为部署会自行声明显存容量，数据源也会自行声明形态规格。默认值仅用于填充记录中留空的字段。
var acceleratorSpecs = map[string]struct {
	memoryGB   int
	formFactor string
}{
	"amd-mi325x":       {memoryGB: 256, formFactor: "OAM Module"},
	"amd-mi300x":       {memoryGB: 192, formFactor: "OAM Module"},
	"amd-mi350x":       {memoryGB: 288, formFactor: "OAM Module"},
	"amd-mi355x":       {memoryGB: 288, formFactor: "OAM Module"},
	"nvidia-h100":      {memoryGB: 80, formFactor: "SXM"},
	"nvidia-h200":      {memoryGB: 140, formFactor: "SXM"},
	"nvidia-b200":      {memoryGB: 192, formFactor: "SXM"},
	"nvidia-b300":      {memoryGB: 288, formFactor: "SXM"},
	"nvidia-gb200":     {memoryGB: 186, formFactor: "SXM"},
	"nvidia-gb300":     {memoryGB: 288, formFactor: "SXM"},
	"nvidia-a100-80gb": {memoryGB: 80, formFactor: "SXM"},
	"nvidia-a100-40gb": {memoryGB: 40, formFactor: "SXM"},
}

// canonicalModelNames 将公开基准测试的内部模型 ID 映射到存放该模型的已验证 Hugging Face 仓库。
//
// 基准测试通常使用短 ID 命名模型，而部署则使用仓库名命名，因此需要该映射让 catalog 统一标识双方指向的同一个模型。
//
// 键均为小写，因为写入与查询时所使用的大小写可能不同。
var canonicalModelNames = map[string]string{
	"dsr1":        "deepseek-ai/DeepSeek-R1-0528",
	"dsv4":        "deepseek-ai/DeepSeek-V4-Pro",
	"glm5":        "zai-org/GLM-5",
	"glm5.1":      "zai-org/GLM-5",
	"glm5.2":      "zai-org/GLM-5.2",
	"glm5.3":      "zai-org/GLM-5.2",
	"kimik2.5":    "moonshotai/Kimi-K2.5",
	"kimik2.6":    "moonshotai/Kimi-K2.5",
	"kimik3":      "moonshotai/Kimi-K3",
	"llama70b":    "meta-llama/Llama-3.3-70B-Instruct",
	"minimaxm3":   "MiniMaxAI/MiniMax-M3",
	"qwen3.5":     "Qwen/Qwen3.5-397B-A17B",
	"qwen3.8next": "Qwen/Qwen3.8-Flash-Next",
	"gptoss120b":  "openai/gpt-oss-120b",
}

// CanonicalModelName 返回模型的规范名称。
//
// 它会映射公开基准测试发布的短 ID，因为针对 "dsr1" 的测量对应于特定模型，catalog 必须按部署所用的方式命名该模型。
//
// 它会原样保留仓库名称，因为仓库名称本身已唯一标识其包含的模型。
func CanonicalModelName(name string) string {
	trimmed := strings.TrimSpace(name)
	if canonical, known := canonicalModelNames[strings.ToLower(trimmed)]; known {
		return canonical
	}
	return trimmed
}

// CanonicalQuantization 返回量化格式的规范标记。
//
// 它会将指向同一配方的不同拼写合并，因为 "bfloat16" 和 "bf16" 表示同一配方，仅比较拼写差异无法反映其背后的权重类型。
//
// 它会原样保留未知拼写，因为本 operator 无法识别的配方不应被合并到其他配方中。
func CanonicalQuantization(quantization string) string {
	key := normalizeSpelling(quantization)
	if canonical, found := quantizationAliases[key]; found {
		return canonical
	}
	return key
}

// NormalizeHardwareIdentity 返回硬件标识的规范形式。
//
// 它通过 pkg/nodefeature 的加速器表解析加速器型号，因为同一款加速器可能分别以产品线名称和部件号进入 catalog。
//
// 当显存和形态规格为空时，它会根据规范加速器信息进行填充，因为本 operator 已知的加速器具有固定的显存和形态规格；若记录中已声明这些字段则予以保留，以部署实际声明为准。
//
// 它会原样保留未知加速器，因为本 operator 无法识别的加速器不应被合并到其他型号中。
func NormalizeHardwareIdentity(identity HardwareIdentity) HardwareIdentity {
	identity.Accelerator = strings.TrimSpace(identity.Accelerator)
	family := nodefeature.ResolveAcceleratorFamily(identity.Accelerator)
	if family == "" {
		return identity
	}
	identity.Accelerator = family
	spec, known := acceleratorSpecs[family]
	if !known {
		return identity
	}
	if identity.MemoryGB == 0 {
		identity.MemoryGB = spec.memoryGB
	}
	if identity.FormFactor == "" {
		identity.FormFactor = spec.formFactor
	}
	return identity
}

// EngineFamily 返回镜像所属的服务引擎系列。
//
// 若镜像未指明任何已知系列，则返回空字符串，因为本 operator 无法归类的镜像不应与部署进行匹配。
//
// 它仅解析镜像名称而不看 tag，因为引擎系列由镜像本身决定，tag 或摘要无法体现这一点。
func EngineFamily(image string) string {
	name := strings.ToLower(image)
	switch {
	case strings.Contains(name, "sglang"):
		return "sglang"
	case strings.Contains(name, "vllm"):
		return "vllm"
	default:
		return ""
	}
}

// canonicalRevision 返回 revision 的规范形式。
//
// 它会为未固定提交的 revision 赋予明确名称，因为记录标明 "unspecified" 表示明确未固定提交，而留空则未作任何说明。
func canonicalRevision(revision string) string {
	revision = strings.TrimSpace(revision)
	if revision == "" {
		return UnspecifiedRevision
	}
	return revision
}

// RevisionMatches 判断两个 revision 是否一致。
//
// 任意一侧为未固定 revision 时均视为匹配，因为数据源未固定权重提交与部署未固定提交在语义上一致。
func RevisionMatches(want, got string) bool {
	want, got = canonicalRevision(want), canonicalRevision(got)
	return want == got || want == UnspecifiedRevision || got == UnspecifiedRevision
}

// TokenizerMatches 判断两个 tokenizer 标识是否一致。
//
// 任意一侧为未指定 tokenizer 时均视为匹配，因为未指定 tokenizer 的数据源默认使用模型自带的 tokenizer 提供服务。
func TokenizerMatches(want, got string) bool {
	want, got = strings.TrimSpace(want), strings.TrimSpace(got)
	if want == "" || got == "" {
		return true
	}
	return want == got || want == UnspecifiedRevision || got == UnspecifiedRevision
}

// ModelNameMatches 判断两个模型名称是否指向同一模型。
//
// 仅允许单侧省略组织名，因为若一个短名称同时是两个不同模型的后缀，则无法唯一确定所指模型。
func ModelNameMatches(want, got string) bool {
	want, got = CanonicalModelName(want), CanonicalModelName(got)
	if want == "" || got == "" {
		return false
	}
	if strings.EqualFold(want, got) {
		return true
	}
	if !strings.Contains(got, "/") && strings.EqualFold(modelBareName(want), got) {
		return true
	}
	if !strings.Contains(want, "/") && strings.EqualFold(modelBareName(got), want) {
		return true
	}
	return false
}

// modelBareName 返回模型名称中除去组织名之后的部分。
func modelBareName(name string) string {
	if _, bare, found := strings.Cut(name, "/"); found {
		return bare
	}
	return name
}

// normalizeSpelling 返回拼写形式的可比较规范格式。
//
// 它将拼写转为小写并统一分隔符，因为 "FP8_E4M3" 和 "fp8-e4m3" 表示同一配方。
func normalizeSpelling(spelling string) string {
	spelling = strings.ToLower(strings.TrimSpace(spelling))
	spelling = strings.ReplaceAll(spelling, "_", "-")
	return strings.Join(strings.Fields(spelling), " ")
}
