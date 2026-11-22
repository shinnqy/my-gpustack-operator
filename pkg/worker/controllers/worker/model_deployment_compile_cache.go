package worker

import (
	core "k8s.io/api/core/v1"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

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
