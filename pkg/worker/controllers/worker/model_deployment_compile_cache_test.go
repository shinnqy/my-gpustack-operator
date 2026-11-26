package worker

import (
	"testing"

	"github.com/stretchr/testify/require"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

// 配置故意保留空格，渲染到容器命令时，整个 JSON 应占一个参数。
const testSpeculativeNgramConfig = `{"method":"ngram", "num_speculative_tokens":4, "prompt_lookup_min":2, "prompt_lookup_max":5}`

func speculativeCompileCachePath(t *testing.T, engine string, args ...string) string {
	t.Helper()
	md := &workercore.ModelDeployment{}
	md.Spec.Engine.Name = engine
	md.Spec.Model.Name = "Qwen/Qwen2.5-72B-Instruct"
	md.Spec.Roles = []workercore.ModelDeploymentRole{{Name: "server", ExtraArgs: args}}
	subPath, err := BuildModelDeploymentCompileCacheSubPath(ModelDeploymentCompileCacheInput{
		Deployment: md, Role: &md.Spec.Roles[0], InstanceType: newRenderInstanceType(),
	})
	require.NoError(t, err)
	return subPath
}

func TestModelDeploymentCompileCacheCompilationFactorsIsolateTheCache(t *testing.T) {
	cases := []struct {
		name   string
		engine string
		before []string
		after  []string
	}{
		{
			name:   "the speculative decoding configuration",
			engine: workercore.ModelDeploymentEngineVLLM,
			after:  []string{"--speculative-config", testSpeculativeNgramConfig},
		},
		{
			name:   "SGLang 改变草稿步数",
			engine: workercore.ModelDeploymentEngineSGLang,
			before: []string{"--speculative-algorithm", "EAGLE", "--speculative-num-steps", "2"},
			after:  []string{"--speculative-algorithm", "EAGLE", "--speculative-num-steps", "4"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := speculativeCompileCachePath(t, tc.engine, tc.before...)
			after := speculativeCompileCachePath(t, tc.engine, tc.after...)
			require.NotEqual(t, before, after)
		})
	}
}

func TestModelDeploymentCompileCacheEquivalentSpellingsShareTheCache(t *testing.T) {
	engine := workercore.ModelDeploymentEngineVLLM
	separate := speculativeCompileCachePath(t, engine, "--speculative-config", testSpeculativeNgramConfig)
	inline := speculativeCompileCachePath(t, engine, "--speculative_config="+testSpeculativeNgramConfig)
	require.Equal(t, separate, inline)
}
