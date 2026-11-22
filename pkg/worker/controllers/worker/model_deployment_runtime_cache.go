package worker

import (
	"path"

	core "k8s.io/api/core/v1"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

// 下表列出了引擎启动缓存所使用的环境变量名，以及各自占用的缓存挂载子目录。
//
// 引擎在服务第一个 token 之前会打开多个缓存，它们默认均位于容器的可写层中。
// Pod 的 ephemeral-storage 限制会计量该可写层，因此在该层持续增长的缓存会驱逐
// 正在写入的 Pod；而放在由主机支撑的缓存挂载点上的相同字节数则完全不计入该限制。
//
// 下面七个环境变量是 vLLM v0.32.0 中这些缓存所使用的独立根目录：
//
//   - HF_HUB_CACHE 是 huggingface_hub 下载模型权重与 tokenizer 文件的位置。
//     在已记录的驱逐事件中，正是该缓存填满了容器可写层，因此本表主要为此而设。
//   - HF_XET_CACHE 是 Xet 分块缓存的存放位置。hf_xet 会先于 HF_HOME 读取该变量，
//     因此分块缓存会随 hub 缓存一同落入挂载目录，而认证令牌仍留在本地。
//   - PIP_CACHE_DIR 是 pip 的下载与 wheel 缓存目录。
//   - TORCH_HOME 是 torch 存放其 hub checkpoint 与缓存状态的根目录。
//   - FLASHINFER_WORKSPACE_BASE 是 FlashInfer 构建其 JIT 工作区的基准目录。
//     该工作区保存了引擎启动期间编译生成的 attention 后端源码与编译后内核。
//     FlashInfer 会在该基准目录下追加 ".cache/flashinfer"，因此实际目录结构位于
//     <mount>/flashinfer/.cache/flashinfer。
//   - TORCHINDUCTOR_CACHE_DIR 是 torch 存放 inductor 所编译内核的位置。其默认值
//     是临时目录下的一个子目录，位于容器的可写层中。
//   - TRITON_CACHE_DIR 是 triton 存放已编译内核的位置。其默认值位于主目录下，
//     在缓存根目录之外而非其中。
//
// HUB 认证令牌被刻意排除在迁移之外。huggingface_hub 将其令牌与数据集缓存保存在
// HF_HOME 下（未指定时跟随 XDG_CACHE_HOME）；若将该根目录指向挂载目录，会把
// 凭据带入由主机支撑的目录中，导致挂载同一路径的每个 Pod 都能读取。因此仅迁移
// 体积庞大的缓存子树，HF_HOME 保留在容器自身的文件系统中，使写入其中的令牌随 Pod
// 一同销毁。数据集缓存也随之留在本地：推理服务副本下载的是权重，而非数据集。
//
// 以上均不是引擎自身的缓存根目录。引擎自身的缓存根目录为 VLLM_CACHE_ROOT 或
// SGLANG_CACHE_DIR，由 modelDeploymentCompileCacheDefaultedEnv 渲染，本表不作重复。
//
// NOTE: 此映射关系与各库存放缓存的位置及其读取的环境变量紧耦合。如果未来版本的
// vLLM、torch、triton、flashinfer 或 Hugging Face 库更改了默认位置或变量名，请相应更新本表。
const (
	// modelDeploymentHFHubCacheEnv 是 huggingface_hub 下载权重和 tokenizer 的目录变量。
	modelDeploymentHFHubCacheEnv = "HF_HUB_CACHE"

	// modelDeploymentHFXetCacheEnv 是 Xet 分块缓存的存放位置；hf_xet 会先于 HF_HOME 读取它。
	modelDeploymentHFXetCacheEnv = "HF_XET_CACHE"

	// modelDeploymentPIPCacheDirEnv 是 pip 的下载与 wheel 缓存目录变量。
	modelDeploymentPIPCacheDirEnv = "PIP_CACHE_DIR"

	// modelDeploymentTorchHomeEnv 是 torch 存放其 hub checkpoint 与缓存状态的根目录变量。
	modelDeploymentTorchHomeEnv = "TORCH_HOME"

	// modelDeploymentFlashInferWorkspaceBaseEnv 是 FlashInfer 构建其 JIT 工作区的基准目录变量。
	modelDeploymentFlashInferWorkspaceBaseEnv = "FLASHINFER_WORKSPACE_BASE"

	// modelDeploymentTorchInductorCacheDirEnv 是 torch 存放 inductor 所编译内核的目录变量。
	modelDeploymentTorchInductorCacheDirEnv = "TORCHINDUCTOR_CACHE_DIR"

	// modelDeploymentTritonCacheDirEnv 是 triton 存放已编译内核的目录变量。
	modelDeploymentTritonCacheDirEnv = "TRITON_CACHE_DIR"
)

// modelDeploymentRuntimeCache 表示一个缓存环境变量名及其在缓存挂载点下占用的子目录。
type modelDeploymentRuntimeCache struct {
	// Name 是库读取的环境变量名。
	Name string

	// SubDir 是该变量渲染后指向缓存挂载点下的子目录。
	SubDir string
}

// modelDeploymentRuntimeCaches 是引擎启动期间打开的各缓存表，按渲染顺序排列。
// 顺序固定，以确保每次渲染出的环境变量列表完全一致。
var modelDeploymentRuntimeCaches = []modelDeploymentRuntimeCache{
	{Name: modelDeploymentHFHubCacheEnv, SubDir: "hf-hub"},
	{Name: modelDeploymentHFXetCacheEnv, SubDir: "xet"},
	{Name: modelDeploymentPIPCacheDirEnv, SubDir: "pip"},
	{Name: modelDeploymentTorchHomeEnv, SubDir: "torch"},
	{Name: modelDeploymentFlashInferWorkspaceBaseEnv, SubDir: "flashinfer"},
	{Name: modelDeploymentTorchInductorCacheDirEnv, SubDir: "inductor"},
	{Name: modelDeploymentTritonCacheDirEnv, SubDir: "triton"},
}

// modelDeploymentRuntimeCacheEnvNames 是缓存表中的环境变量名列表，供判断
// 哪些环境变量仅由 operator 提供默认值的注册表使用。
var modelDeploymentRuntimeCacheEnvNames = []string{
	modelDeploymentHFHubCacheEnv,
	modelDeploymentHFXetCacheEnv,
	modelDeploymentPIPCacheDirEnv,
	modelDeploymentTorchHomeEnv,
	modelDeploymentFlashInferWorkspaceBaseEnv,
	modelDeploymentTorchInductorCacheDirEnv,
	modelDeploymentTritonCacheDirEnv,
}

// modelDeploymentRuntimeCacheDefaultedEnv 返回将引擎启动缓存指向主机支撑的
// 缓存挂载点的默认环境变量条目。
//
// 这里提供的是默认值而非所有权接管。若某个角色显式设置了其中任一变量名，则保留
// 其自有值：operator 仅负责填充默认值，而挂载在 ModelDeploymentCompileCacheMountPath
// 的卷使该默认值生效。
//
// 对于 operator 不认识的引擎，不返回任何条目，以免给出错误路径。这些变量名属于
// 引擎可能携带也可能不携带的库，operator 无法为其担保的引擎设置这些变量等同于
// 对未知引擎做出不实假定。
func modelDeploymentRuntimeCacheDefaultedEnv(engine string) []core.EnvVar {
	switch engine {
	case workercore.ModelDeploymentEngineVLLM, workercore.ModelDeploymentEngineSGLang:
	default:
		return nil
	}

	env := make([]core.EnvVar, 0, len(modelDeploymentRuntimeCaches))
	for _, c := range modelDeploymentRuntimeCaches {
		env = append(env, core.EnvVar{
			Name:  c.Name,
			Value: path.Join(ModelDeploymentCompileCacheMountPath, c.SubDir),
		})
	}

	return env
}
