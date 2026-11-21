package worker

import (
	core "k8s.io/api/core/v1"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

// modelDeploymentCompileCacheDefaultedEnv returns the defaulted environment variable
// pointing the engine's compilation cache to ModelDeploymentCompileCacheMountPath.
//
// IT IS THE CACHE ROOT ALONE. The operator does not name the directory an engine builds inside
// it, because vLLM partitions that tree by rank and DP rank itself and SGLang does not: writing
// past their partitioning would put two ranks in one directory and have them overwrite each
// other's generated code. An engine with no such variable gets nothing rather than a wrong one.
func modelDeploymentCompileCacheDefaultedEnv(engine string) []core.EnvVar {
	switch engine {
	case workercore.ModelDeploymentEngineVLLM:
		return []core.EnvVar{{Name: "VLLM_CACHE_ROOT", Value: ModelDeploymentCompileCacheMountPath}}
	case workercore.ModelDeploymentEngineSGLang:
		return []core.EnvVar{{Name: "SGLANG_CACHE_DIR", Value: ModelDeploymentCompileCacheMountPath}}
	}

	return nil
}
