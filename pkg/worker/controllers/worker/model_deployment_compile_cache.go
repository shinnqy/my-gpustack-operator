package worker

import (
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"

	core "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"

	worker "gpustack.ai/gpustack/api/worker/v1"
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/utils/json"
	"gpustack.ai/gpustack/pkg/utils/stringx"
)

const (
	// modelDeploymentCompileCacheIdentityDigits 是权重身份在子目录名中保留的十六进制位数：
	// 取权重自身内容地址的前 128 位，在共享同一缓存根目录的部署之间足以避免碰撞，同时保持路径简短。
	modelDeploymentCompileCacheIdentityDigits = 32

	// modelDeploymentCompileCacheHashDigits 是编译因子摘要在叶目录名中保留的位数（32 位）。
	// 发生碰撞最多多一次冷启动而不会产出错误结果，叶目录也是管理员清理因参数变更遗留缓存的粒度。
	modelDeploymentCompileCacheHashDigits = 8

	// modelDeploymentCompileCacheCPUManufacturer 是非加速实例类型角色使用的厂商目录段，
	// 因为纯 CPU 资源池不属于任何硬件厂商。
	modelDeploymentCompileCacheCPUManufacturer = "cpu"

	// modelDeploymentCompileCacheDigestPrefix 是本项目所有内容寻址摘要统一携带的算法前缀。
	modelDeploymentCompileCacheDigestPrefix = "sha256:"

	// modelDeploymentCompileCacheOCIDigestLength 是 OCI 摘要的十六进制长度，用于区分按摘要固定的
	// 镜像引用与可变的 tag 引用。
	modelDeploymentCompileCacheOCIDigestLength = 64
)

// ModelDeploymentCompileCacheInput 包含计算角色宿主机编译缓存子目录路径所需的全部输入。
//
// 渲染器会将制品解析为 ModelDeploymentArtifactRender 并传入；未指定制品的部署由引擎自行下载权重，
// 此时模型名称是唯一可用的身份标识。
type ModelDeploymentCompileCacheInput struct {
	// Deployment 是正在渲染的部署对象。
	Deployment *workercore.ModelDeployment

	// Role 是当前副本所属的 Deployment.Spec.Roles 条目。
	Role *workercore.ModelDeploymentRole

	// InstanceType 是该角色 Pod 准入所使用的实例类型，用于确定编译产物所属的硬件厂商。
	InstanceType *worker.InstanceType

	// Artifact 是调谐器为该部署解析出的权重交付信息；未指定制品的部署为 nil。
	Artifact *ModelDeploymentArtifactRender
}

// IMPORTANT: 宿主机编译缓存隔离的入口
// 把宿主机缓存目录按“不能共享的维度”划成 3 层：
// <宿主机根目录> / <硬件厂商>-<引擎> / <模型权重身份> / tp<TP>-pp<PP>-dp<DP>-ep<EP>-cp<PCP>_<DCP>-<hash8>
//
//	示例：     / nvidia-vllm      / m-a1b2c3...    / tp4-pp1-dp1-ep1-cp1_1-9f8e7d6c
//
// BuildModelDeploymentCompileCacheSubPath 返回 ModelDeployment 角色在
// DefaultModelDeploymentCompileCacheHostDir 下的相对子目录路径：
//
//	<manufacturer>-<engine>/<model-identity>/tp<TP>-pp<PP>-dp<DP>-ep<EP>-cp<PCP>_<DCP>-<hash8>
//
// 路径各段按不能共享编译产物的维度依次隔离：运行编译器的硬件厂商、驱动编译的引擎、参与编译的
// 权重、生成核函数所针对的并行拓扑，以及其余所有影响生成代码的因素摘要。
//
// 中间两层目录便于按粒度清理：删除模型或并行拓扑目录会清理其下全部缓存，删除单个叶目录则精确
// 清理因标志变更而失效的产物，这也是引擎缓存加载器能容忍的最小粒度（vLLM 的 standalone compile
// 适配器依赖清单脚本列出的文件，底层缺失单个文件会导致加载失败）。
//
// 注意：该映射与 vLLM（VLLM_CACHE_ROOT，含 vllm-ascend）及 SGLang（SGLANG_CACHE_DIR）组织
// 编译图与核函数缓存的方式耦合。若后续版本更改缓存目录结构、环境变量名或影响编译的命令行参数，
// 需同步更新本函数及参数表。
func BuildModelDeploymentCompileCacheSubPath(in ModelDeploymentCompileCacheInput) (string, error) {
	if in.Deployment == nil {
		return "", errors.New("the compilation cache path needs the deployment")
	}
	if in.Role == nil {
		return "", errors.New("the compilation cache path needs the role")
	}
	if in.InstanceType == nil {
		return "", errors.New("the compilation cache path needs the instance type")
	}

	manufacturer, err := modelDeploymentCompileCacheManufacturer(in.InstanceType)
	if err != nil {
		return "", err
	}
	engine, ok := ModelDeploymentEngineUpstreamName(in.Deployment.Spec.Engine.Name)
	if !ok {
		return "", fmt.Errorf("unsupported engine %q", in.Deployment.Spec.Engine.Name)
	}
	identity, err := modelDeploymentCompileCacheModelIdentity(in)
	if err != nil {
		return "", err
	}

	declared, err := ParseModelDeploymentDeclaredParallelism(
		in.Deployment.Spec.Engine.Name, ModelDeploymentRoleArgs(in.Role), in.Role.Env,
	)
	if err != nil {
		return "", fmt.Errorf("the compilation cache path cannot read the parallel degrees: %w", err)
	}
	digest, err := modelDeploymentCompileCacheHash(in, declared)
	if err != nil {
		return "", err
	}

	shape := fmt.Sprintf("tp%d-pp%d-dp%d-ep%d-cp%d_%d",
		modelDeploymentCompileCacheDegree(declared.TensorParallel),
		modelDeploymentCompileCacheDegree(declared.PipelineParallel),
		modelDeploymentCompileCacheDegree(declared.DataParallel),
		modelDeploymentCompileCacheDegree(declared.ExpertParallel),
		modelDeploymentCompileCacheDegree(declared.PrefillContextParallel),
		modelDeploymentCompileCacheDegree(declared.DecodeContextParallel),
	)

	return fmt.Sprintf("%s-%s/%s/%s-%s", manufacturer, engine, identity, shape,
		digest[:modelDeploymentCompileCacheHashDigits]), nil
}

// modelDeploymentCompileCacheManufacturer 返回生成该目录内编译产物的硬件厂商。
// 不归属任何厂商的资源池视为 CPU 池；若命名为 "generic"，一旦有真实厂商接入就会产生碰撞。
func modelDeploymentCompileCacheManufacturer(it *worker.InstanceType) (string, error) {
	if !it.Spec.Acceleratable {
		return modelDeploymentCompileCacheCPUManufacturer, nil
	}

	manufacturer := strings.ToLower(strings.TrimSpace(it.Status.Detail.Manufacturer))
	if manufacturer == "" {
		return "", fmt.Errorf(
			"instance type %q is acceleratable but has no manufacturer in status", it.Name,
		)
	}

	return manufacturer, nil
}

// modelDeploymentCompileCacheModelIdentity 生成标识权重的路径段，采用与 KV 存储键相同的
// "m-" 加前导十六进制位格式：指向同一份权重的两个部署落入同一标识，不同权重绝不共享编译产物。
func modelDeploymentCompileCacheModelIdentity(in ModelDeploymentCompileCacheInput) (string, error) {
	if in.Artifact != nil {
		return modelDeploymentCompileCacheRenderIdentity(in)
	}

	name := strings.TrimSpace(in.Deployment.Spec.Model.Name)
	if name == "" {
		return "", errors.New("the deployment names no model, so the weights have no identity")
	}

	return modelDeploymentCompileCacheIdentityHex(
		stringx.SumBySHA256(name),
	), nil
}

// modelDeploymentCompileCacheRenderIdentity 根据调谐器为渲染解析出的交付结果推导权重身份，
// 凡由渲染路径调用时均走此分支。
func modelDeploymentCompileCacheRenderIdentity(in ModelDeploymentCompileCacheInput) (string, error) {
	a := in.Artifact

	switch a.Delivery {
	case workercore.ModelDeploymentModelDeliveryNode, workercore.ModelDeploymentModelDeliveryEngine:
		switch {
		case a.ManifestDigest != "":
			hexDigits := strings.ToLower(strings.TrimPrefix(
				a.ManifestDigest, modelDeploymentCompileCacheDigestPrefix,
			))
			if len(hexDigits) < modelDeploymentCompileCacheIdentityDigits ||
				!modelDeploymentCompileCacheAllHex(hexDigits) {
				return "", fmt.Errorf("the artifact manifest digest %q is not a hexadecimal address",
					a.ManifestDigest)
			}

			return modelDeploymentCompileCacheIdentityHex(hexDigits), nil
		case a.ArtifactUID != "":
			return modelDeploymentCompileCacheIdentityHex(
				stringx.SumBySHA256(a.ArtifactUID),
			), nil
		case a.Repository != "" || a.Revision != "":
			return modelDeploymentCompileCacheIdentityHex(stringx.SumBySHA256(
				a.Hub + "/" + a.Repository + "@" + a.Revision,
			)), nil
		}

		// 尚未解析出任何元数据的 hub 制品仍对应其声明的模型名称，指向同一仓库的两个角色运行相同权重。
		return modelDeploymentCompileCacheIdentityHex(
			stringx.SumBySHA256(in.Deployment.Spec.Model.Name),
		), nil

	case workercore.ModelDeploymentModelDeliveryImage:
		if digest, ok := modelDeploymentCompileCacheOCIDigest(a.ImageReference); ok {
			return modelDeploymentCompileCacheIdentityHex(digest), nil
		}

		return modelDeploymentCompileCacheIdentityHex(
			stringx.SumBySHA256(a.ImageReference),
		), nil

	case workercore.ModelDeploymentModelDeliveryPVC:
		artifactName := a.ArtifactName
		if artifactName == "" {
			artifactName = ModelDeploymentArtifactName(in.Deployment)
		}

		return modelDeploymentCompileCacheIdentityHex(stringx.SumBySHA256(
			in.Deployment.Namespace + "/" + artifactName + "/" + a.ArtifactUID + "/" +
				a.ClaimName + "/" + a.Path,
		)), nil
	}

	return "", fmt.Errorf("unsupported model delivery %q", a.Delivery)
}

// modelDeploymentCompileCacheIdentityHex 将十六进制地址截断到目录保留长度，并加上 "m-" 前缀。
func modelDeploymentCompileCacheIdentityHex(hexDigits string) string {
	if len(hexDigits) > modelDeploymentCompileCacheIdentityDigits {
		hexDigits = hexDigits[:modelDeploymentCompileCacheIdentityDigits]
	}

	return "m-" + hexDigits
}

// modelDeploymentCompileCacheOCIDigest 提取镜像引用中固定的十六进制摘要；若引用的是 tag 则返回 false。
// tag 是可变的，因此按完整字符串求哈希，而不当作不可变内容地址信任。
func modelDeploymentCompileCacheOCIDigest(reference string) (string, bool) {
	_, digest, found := strings.Cut(reference, "@")
	if !found || !strings.HasPrefix(digest, modelDeploymentCompileCacheDigestPrefix) {
		return "", false
	}

	hexDigits := strings.ToLower(strings.TrimPrefix(digest, modelDeploymentCompileCacheDigestPrefix))
	if len(hexDigits) != modelDeploymentCompileCacheOCIDigestLength ||
		!modelDeploymentCompileCacheAllHex(hexDigits) {
		return "", false
	}

	return hexDigits, true
}

func modelDeploymentCompileCacheAllHex(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}

	return true
}

// modelDeploymentCompileCacheDegree 将声明的并行度规整为目录名使用的数值：未声明时按引擎默认 1
// 处理，使省略参数与显式写 1 命中同一份缓存。
func modelDeploymentCompileCacheDegree(v int) int {
	if v <= 0 {
		return 1
	}

	return v
}

// modelDeploymentCompileCacheHashSubject 是叶目录摘要的编码对象。使用结构体而非 map，
// 因为 JSON 会按字段声明顺序序列化结构体，从而保证同一组输入在每次运行与每次构建中字节完全一致。
type modelDeploymentCompileCacheHashSubject struct {
	Image       string                                 `json:"image"`
	Parallelism modelDeploymentCompileCacheParallelism `json:"parallelism"`
	Flags       []string                               `json:"flags"`
	Env         []string                               `json:"env"`
}

// modelDeploymentCompileCacheParallelism 是参与摘要计算的并行拓扑。各引擎并行度均填充默认值 1，
// 因为编译器针对引擎实际运行的拓扑生成核函数，缺省与显式声明 1 表示同一拓扑；NodeCount 保留原始值，
// 因为 0 与 1 对多机启动意味着不同的配置事实，不属于可借用的引擎默认值。
type modelDeploymentCompileCacheParallelism struct {
	TP        int      `json:"tp"`
	PP        int      `json:"pp"`
	DP        int      `json:"dp"`
	DPL       int      `json:"dpl"`
	PCP       int      `json:"pcp"`
	DCP       int      `json:"dcp"`
	EP        int      `json:"ep"`
	AttnCP    int      `json:"attnCP"`
	MoEDP     int      `json:"moeDP"`
	DWDP      int      `json:"dwdp"`
	NodeCount int      `json:"nodeCount"`
	Modes     []string `json:"modes"`
}

// modelDeploymentCompileCacheHash 计算角色中所有可能改变引擎编译产物的因素的十六进制摘要。
//
// 这是一份封闭清单，也正是其核心安全属性：漏掉影响编译的参数可能导致复用错误产物，而把不影响编译
// 的参数纳入则会让每次调参都付出冷启动代价。新增或移除影响编译的参数都必须显式维护本清单。
func modelDeploymentCompileCacheHash(
	in ModelDeploymentCompileCacheInput, declared ModelDeploymentDeclaredParallelism,
) (string, error) {
	image := strings.TrimSpace(in.Role.Image)
	if image == "" {
		image = strings.TrimSpace(in.Deployment.Spec.Engine.Image)
	}
	modes := slices.Clone(declared.Modes)
	slices.Sort(modes)

	subject := modelDeploymentCompileCacheHashSubject{
		Image: image,
		Parallelism: modelDeploymentCompileCacheParallelism{
			TP:        modelDeploymentCompileCacheDegree(declared.TensorParallel),
			PP:        modelDeploymentCompileCacheDegree(declared.PipelineParallel),
			DP:        modelDeploymentCompileCacheDegree(declared.DataParallel),
			DPL:       modelDeploymentCompileCacheDegree(declared.DataParallelLocal),
			PCP:       modelDeploymentCompileCacheDegree(declared.PrefillContextParallel),
			DCP:       modelDeploymentCompileCacheDegree(declared.DecodeContextParallel),
			EP:        modelDeploymentCompileCacheDegree(declared.ExpertParallel),
			AttnCP:    modelDeploymentCompileCacheDegree(declared.AttentionContextParallel),
			MoEDP:     modelDeploymentCompileCacheDegree(declared.MoEDataParallel),
			DWDP:      modelDeploymentCompileCacheDegree(declared.DWDPSize),
			NodeCount: declared.NodeCount,
			Modes:     modes,
		},
		Flags: modelDeploymentCompileCacheFlags(ModelDeploymentRoleArgs(in.Role)),
		Env:   modelDeploymentCompileCacheEnv(in.Role.Env),
	}

	canonical, err := json.Marshal(subject)
	if err != nil {
		return "", fmt.Errorf("the compilation cache path cannot encode its factors: %w", err)
	}

	return stringx.SumBytesBySHA256(canonical), nil
}

// modelDeploymentCompileCacheFlag 表示参数表中的一条记录，描述该参数是否带值以及是否消费多个连续 token。
type modelDeploymentCompileCacheFlag struct {
	// valued 标记参数值会直接影响生成代码的选项；开关型选项不带值，避免吞掉其后的独立 token。
	valued bool

	// multi 标记消费后续所有非选项 token 的带值参数，与 vLLM 解析捕获尺寸列表的规则一致。
	multi bool
}

// modelDeploymentCompileCacheOptimizationFlag 是 vLLM 指定编译优化级别的短选项，同时作为表内规范键。
const modelDeploymentCompileCacheOptimizationFlag = "-O"

// modelDeploymentCompileCacheQuantizationFlag 是 --quantization 的短选项拼写。
const modelDeploymentCompileCacheQuantizationFlag = "-q"

// modelDeploymentCompileCacheFlagTable 是影响编译的参数表，以目录哈希使用的规范参数名为键。
//
// 表中每一项都会改变编译器看到的输入：核函数所针对的数据类型与量化方案、决定图形状的上下文长度、
// 编译与图捕获开关、注意力与推测解码后端，以及在构图追踪前改写模型结构的覆盖项。服务运行参数
// （如并发上限、显存占比、日志级别）按设计不入表，它们不改变任何核函数，入表只会让调参触发无谓冷启动。
var modelDeploymentCompileCacheFlagTable = map[string]modelDeploymentCompileCacheFlag{
	// 数据类型与量化。
	"--dtype":                   {valued: true},
	"--quantization":            {valued: true},
	"--quantization-param-path": {valued: true},
	"--kv-cache-dtype":          {valued: true},
	"--moe-runner-backend":      {valued: true},
	// 上下文长度。
	"--max-model-len":  {valued: true},
	"--context-length": {valued: true},
	// 编译与图捕获。
	"--compilation-config":                      {valued: true},
	modelDeploymentCompileCacheOptimizationFlag: {valued: true},
	"--enforce-eager":                           {},
	"--cudagraph-capture-sizes":                 {valued: true, multi: true},
	"--max-cudagraph-capture-size":              {valued: true},
	"--additional-config":                       {valued: true},
	"--enable-torch-compile":                    {},
	"--torch-compile-max-bs":                    {valued: true},
	"--cuda-graph-max-bs":                       {valued: true},
	"--cuda-graph-bs":                           {valued: true, multi: true},
	"--disable-cuda-graph":                      {},
	"--disable-cuda-graph-padding":              {},
	"--piecewise-cuda-graph":                    {},
	"--piecewise-cuda-graph-max-tokens":         {valued: true},
	"--piecewise-cuda-graph-tokens":             {valued: true, multi: true},
	"--piecewise-cuda-graph-compiler":           {valued: true},
	// 注意力与推测解码。
	"--attention-backend":               {valued: true},
	"--attention-config":                {valued: true},
	"--prefill-attention-backend":       {valued: true},
	"--decode-attention-backend":        {valued: true},
	"--triton-attention-reduce-in-fp32": {},
	"--all2all-backend":                 {valued: true},
	"--speculative-config":              {valued: true},
	"--speculative-algorithm":           {valued: true},
	"--speculative-num-steps":           {valued: true},
	"--speculative-eagle-topk":          {valued: true},
	"--speculative-num-draft-tokens":    {valued: true},
	// 模型结构覆盖。
	"--trust-remote-code":        {},
	"--hf-overrides":             {valued: true},
	"--json-model-override-args": {valued: true},
	"--rope-scaling":             {valued: true},
	"--rope-theta":               {valued: true},
	"--config":                   {valued: true},
}

// modelDeploymentCompileCacheFlagAliases 将引擎接受的别名映射到参数表中的规范名称。
var modelDeploymentCompileCacheFlagAliases = map[string]string{
	modelDeploymentCompileCacheQuantizationFlag: "--quantization",
}

// modelDeploymentCompileCacheFlags 提取角色参数流中参与编译缓存定键的 "key=value" 列表并排序。
//
// 同名参数按 argparse 语义以后写覆盖先写：引擎实际使用最后出现的值，缓存也必须对应最后的值。
// 排序保证无关参数的顺序不影响结果，避免调整参数排列顺序时静默重建未变更的缓存目录。
func modelDeploymentCompileCacheFlags(args []string) []string {
	pairs := map[string]string{}

	for i := 0; i < len(args); i++ {
		// 单独的 "--" 结束选项解析，其后的所有参数均交给程序本身处理。
		if args[i] == "--" {
			break
		}
		key, values, consumed, ok := modelDeploymentCompileCacheReadFlag(args, i)
		if !ok {
			continue
		}
		i += consumed
		pairs[key] = strings.Join(values, ",")
	}

	out := make([]string, 0, len(pairs))
	for key, value := range pairs {
		out = append(out, key+"="+value)
	}
	slices.Sort(out)

	return out
}

// modelDeploymentCompileCacheReadFlag 将参数流中的单个 token 按参数表解析，返回规范键、解析出的值列表
// 以及额外消费的后续 token 数量。
func modelDeploymentCompileCacheReadFlag(
	args []string, i int,
) (string, []string, int, bool) {
	token := args[i]
	// 单个 "-" 在命令行中表示标准流而非选项标志。
	if !strings.HasPrefix(token, "-") || token == "-" {
		return "", nil, 0, false
	}

	key, value, hasValue := token, "", false
	if eq := strings.IndexByte(token, '='); eq >= 0 {
		key, value, hasValue = token[:eq], token[eq+1:], true
	}

	canonical, glued, ok := modelDeploymentCompileCacheFlagKey(key)
	if !ok {
		return "", nil, 0, false
	}
	if !hasValue && glued != "" {
		value, hasValue = glued, true
	}

	// 带点号的键按首个点号之前的基础参数名查表："--compilation-config.level=3" 是
	// --compilation-config 的字段值，而不是表里未登记的未知参数。
	row := modelDeploymentCompileCacheFlagTable[canonical]
	if base, _, dotted := strings.Cut(canonical, "."); dotted {
		row = modelDeploymentCompileCacheFlagTable[base]
	}
	if !row.valued {
		if hasValue {
			// 开关参数携带值会被引擎自身拒绝，直接跳过不记录，避免为无法运行的命令行生成缓存键。
			return "", nil, 0, false
		}

		return canonical, []string{"true"}, 0, true
	}

	values, consumed := []string{}, 0
	switch {
	case hasValue:
		values = []string{value}
	case row.multi:
		for i+1+consumed < len(args) && modelDeploymentConsumableValue(args[i+1+consumed]) {
			consumed++
			values = append(values, args[i+1+consumed-1])
		}
	case i+1 < len(args) && modelDeploymentConsumableValue(args[i+1]):
		consumed = 1
		values = []string{args[i+1]}
	}
	if len(values) == 0 {
		// 带值参数缺失值时同样会被引擎拒绝。为其分配独立空值键而非折叠进上一次合法值，
		// 避免错误命令行误读其他配置编译出的缓存。
		values = []string{""}
	}

	return canonical, values, consumed, true
}

// modelDeploymentCompileCacheFlagKey 将参数键解析为表内规范名，并报告 argparse 会识别为紧贴其后的值。
//
// 两条归一化规则均遵循引擎自身实现：vLLM 会将长选项首个点号之前的下划线改写为连字符，并将带点号的
// 条目归并到点号前的基础参数下，因此 "--compilation_config.level=3" 与 "--compilation-config.level=3"
// 归为同一拼写；带点号键保留点号后缀，因为覆盖不同字段属于不同变更，彼此不能覆盖。短选项按 argparse
// 自身规则解析而非长选项规则（例如 "-q" 恒为量化选项，无论长选项前缀如何）。
func modelDeploymentCompileCacheFlagKey(key string) (canonical, glued string, ok bool) {
	if !strings.HasPrefix(key, "--") {
		return modelDeploymentCompileCacheShortFlag(key)
	}

	name := modelDeploymentVLLMFlagKey(key)
	if base, _, dotted := strings.Cut(name, "."); dotted {
		if _, known := modelDeploymentCompileCacheFlagTable[base]; !known {
			return "", "", false
		}

		return name, "", true
	}

	if alias, aliased := modelDeploymentCompileCacheFlagAliases[name]; aliased {
		return alias, "", true
	}
	if _, known := modelDeploymentCompileCacheFlagTable[name]; known {
		return name, "", true
	}
	// 按 argparse 的前缀匹配规则在本表内解析缩写。必须这样做：否则把 "--dtype" 简写为 "--dt" 时
	// 会因未识别而被忽略，进而误与其他 dtype 共享编译目录。
	//
	// 在本表中存在歧义的前缀保持未解析，这是更安全的方向：引擎注册的参数比本表更多，此处就存在歧义
	// 的前缀在引擎启动时必然被拒绝，强行猜测反而会为无法运行的标志建立缓存键。带点号的键不支持缩写，
	// vLLM 将其作为完整字段路径处理。
	if !strings.Contains(name, ".") {
		if canonical, unique := modelDeploymentCompileCacheFlagByPrefix(name); unique {
			return canonical, "", true
		}
	}

	return "", "", false
}

// modelDeploymentCompileCacheFlagByPrefix 将缩写前缀唯一解析为表中的参数名。
func modelDeploymentCompileCacheFlagByPrefix(name string) (string, bool) {
	matched := ""
	for candidate := range modelDeploymentCompileCacheFlagTable {
		if !strings.HasPrefix(candidate, name) {
			continue
		}
		if matched != "" {
			return "", false
		}
		matched = candidate
	}

	return matched, matched != ""
}

// modelDeploymentCompileCacheShortFlag 解析单连字符短选项。"-O3" 与 "-qfp8" 将值直接紧贴在 token
// 尾部（符合 argparse 短选项规则），而 "-O" 与 "-q" 则从下一个 token 读取值。
func modelDeploymentCompileCacheShortFlag(key string) (string, string, bool) {
	optimization := modelDeploymentCompileCacheOptimizationFlag
	quantization := modelDeploymentCompileCacheQuantizationFlag

	switch key {
	case optimization:
		return optimization, "", true
	case quantization:
		return modelDeploymentCompileCacheFlagAliases[quantization], "", true
	}

	if rest := strings.TrimPrefix(key, optimization); rest != key {
		if modelDeploymentAllDigits(rest) {
			return optimization, rest, true
		}

		return "", "", false
	}
	if rest := strings.TrimPrefix(key, quantization); rest != key && rest != "" {
		return modelDeploymentCompileCacheFlagAliases[quantization], rest, true
	}

	return "", "", false
}

// modelDeploymentCompileCacheEnvNames 是会影响引擎编译产物的环境变量清单：包括选择编译路径的开关、
// 注意力后端、FlashAttention 版本以及核函数目标架构列表。
//
// 引擎通过环境变量读取的其他配置（如 Hub token、缓存根目录、代理地址）只改变数据读取来源，
// 不改变生成的编译产物，若纳入清单会导致每次切换地址都触发冷启动。
var modelDeploymentCompileCacheEnvNames = []string{
	"VLLM_USE_V1",
	"VLLM_ATTENTION_BACKEND",
	"VLLM_USE_STANDALONE_COMPILE",
	"VLLM_USE_MEGA_AOT_ARTIFACT",
	"VLLM_FLASH_ATTN_VERSION",
	"SGLANG_ENABLE_TORCH_COMPILE",
	"TRITON_ATTN_NUM_STAGES",
	"TORCH_CUDA_ARCH_LIST",
}

// modelDeploymentCompileCacheEnv 返回单个角色环境变量中参与编译缓存定键的 "NAME=VALUE" 列表并排序。
// 同名变量按容器运行时语义以后写覆盖先写读取。
func modelDeploymentCompileCacheEnv(env []workercore.ModelDeploymentEnvVar) []string {
	var out []string
	for _, name := range modelDeploymentCompileCacheEnvNames {
		if value, ok := modelDeploymentEnvValue(env, name); ok {
			out = append(out, name+"="+value)
		}
	}
	slices.Sort(out)

	return out
}

// modelDeploymentCompileCacheVolumes 返回受管角色编译缓存对应的 HostPath（DirectoryOrCreate）卷及挂载项。
func modelDeploymentCompileCacheVolumes(subPath string) (core.Volume, core.VolumeMount) {
	return core.Volume{
			Name: ModelDeploymentCompileCacheVolumeName,
			VolumeSource: core.VolumeSource{HostPath: &core.HostPathVolumeSource{
				Path: path.Join(DefaultModelDeploymentCompileCacheHostDir, subPath),
				Type: ptr.To(core.HostPathDirectoryOrCreate),
			}},
		}, core.VolumeMount{
			Name:      ModelDeploymentCompileCacheVolumeName,
			MountPath: ModelDeploymentCompileCacheMountPath,
		}
}

// modelDeploymentCompileCacheMountOverridden 报告现有容器挂载是否已覆盖 ModelDeploymentCompileCacheMountPath
// （重合、位于其内部或包含其外层）；若是，则 operator 让出默认宿主机编译缓存卷，改由用户卷承载缓存。
func modelDeploymentCompileCacheMountOverridden(mounts []core.VolumeMount) bool {
	return slices.ContainsFunc(mounts, func(m core.VolumeMount) bool {
		return modelDeploymentCompileCachePathsOverlap(m.MountPath, ModelDeploymentCompileCacheMountPath)
	})
}

func modelDeploymentCompileCachePathsOverlap(a, b string) bool {
	a, b = path.Clean(a), path.Clean(b)

	return a == b || strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}

// modelDeploymentCompileCacheDefaultedEnv 返回将引擎编译缓存指向
// ModelDeploymentCompileCacheMountPath 的默认环境变量。
//
// 仅设置缓存根目录。operator 不指定引擎在其内部创建的子目录，因为 vLLM 会自行按 rank
// 和 DP rank 划分目录树，而 SGLang 不会：越过它们的目录划分会让两个 rank 写入同一目录
// 并互相覆盖生成的代码。没有此类环境变量的引擎不会获得任何设置，以免给出错误配置。
func modelDeploymentCompileCacheDefaultedEnv(engine string) []core.EnvVar {
	switch engine {
	case workercore.ModelDeploymentEngineVLLM:
		return []core.EnvVar{{Name: "VLLM_CACHE_ROOT", Value: ModelDeploymentCompileCacheMountPath}}
	case workercore.ModelDeploymentEngineSGLang:
		return []core.EnvVar{{Name: "SGLANG_CACHE_DIR", Value: ModelDeploymentCompileCacheMountPath}}
	}

	return nil
}
