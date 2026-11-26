package worker

import (
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/worker/scaling/priors"
)

func TestModelDeploymentScalerCapacityPriorRefusals(t *testing.T) {
	// 内置目录记录了两种引擎在四张 B300 上以 TP=4 服务这个 FP4 模型时的容量。
	instance := newRenderInstanceType()
	instance.Name = "b300"
	instance.Status.Detail.Product = "NVIDIA B300"
	client := newModelDeploymentClient(instance)
	scaler := &workercore.ModelDeploymentScaler{}
	scaler.Spec.Algorithm = workercore.ModelDeploymentScalerAlgorithmSpec{
		Type: workercore.ModelDeploymentScalerAlgorithmTypeTokenThroughput,
		TokenThroughput: &workercore.ModelDeploymentScalerTokenThroughputSpec{
			LatencyQuantile:        ptr.To(resource.MustParse("0.90")),
			TTFTTargetMilliseconds: ptr.To(resource.MustParse("10000")),
			ITLTargetMilliseconds:  ptr.To(resource.MustParse("1000")),
		},
		CapacityPrior: &workercore.ModelDeploymentScalerCapacityPriorSpec{},
	}
	cases := []struct {
		name   string
		engine string
		args   []string
		want   string
	}{
		{
			name: "speculative decoding", engine: workercore.ModelDeploymentEngineVLLM,
			args: []string{"--speculative-config", testSpeculativeNgramConfig},
			want: "catalog cannot establish settings from --speculative-config",
		},
		{
			name: "SGLang 投机算法", engine: workercore.ModelDeploymentEngineSGLang,
			args: []string{"--speculative-algorithm=EAGLE"},
			want: "catalog cannot establish settings from --speculative-algorithm",
		},
		{
			name: "缺少 dtype 的值", engine: workercore.ModelDeploymentEngineVLLM,
			args: []string{"--dtype"},
			want: "--dtype requires a value",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			md := &workercore.ModelDeployment{}
			md.Annotations = map[string]string{workercore.ModelDeploymentScalerPrecisionAnnotationKey: "fp4"}
			md.Spec.Model.Name = "deepseek-ai/DeepSeek-V4-Pro"
			md.Spec.Engine.Name = tc.engine
			md.Spec.Roles = []workercore.ModelDeploymentRole{{
				Name: "server", Kind: workercore.ModelDeploymentRoleKindServer, InstanceType: instance.Name,
				Resources: &workercore.ModelDeploymentRoleResources{Accelerator: ptr.To(resource.MustParse("4"))},
				ExtraArgs: []string{"--tensor-parallel-size=4"},
			}}
			baseline, err := ResolveModelDeploymentScalerCapacityPriors(t.Context(), client, scaler, md)
			require.NoError(t, err)
			require.Len(t, baseline.Status, 1)
			require.Len(t, baseline.Status[0].Fields, 1)
			require.Equal(t, workercore.ModelDeploymentScalerCapacityFieldSourceCatalog, baseline.Status[0].Fields[0].Source)

			role := &md.Spec.Roles[0]
			role.ExtraArgs = append(role.ExtraArgs, tc.args...)
			resolved, err := ResolveModelDeploymentScalerCapacityPriors(t.Context(), client, scaler, md)
			var refusal *priors.RefusalError
			require.ErrorAs(t, err, &refusal)
			require.Equal(t, priors.RefusalCapacityPriorUnresolved, refusal.Reason)
			require.Equal(t, `role "server": `+tc.want, refusal.Message)
			require.Nil(t, resolved)
		})
	}
}

func TestModelDeploymentScalerPriorFlags(t *testing.T) {
	cases := []struct {
		engine string
		args   []string
		want   map[string]string
	}{
		{
			engine: workercore.ModelDeploymentEngineVLLM,
			args:   []string{"--kv_cache_dtype=auto", "-qfp8", "--enable-prefix-caching", "--no-enable-prefix-caching", "--port", "8000"},
			want:   map[string]string{"--kv-cache-dtype": "auto", "--quantization": "fp8", "--no-enable-prefix-caching": "true"},
		},
		{
			engine: workercore.ModelDeploymentEngineSGLang,
			args:   []string{"--tokenizer-path", "org/model", "--disable-radix-cache"},
			want:   map[string]string{"--tokenizer": "org/model", "--disable-radix-cache": "true"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.engine, func(t *testing.T) {
			flags, err := modelDeploymentScalerPriorFlags(tc.engine, tc.args)
			require.NoError(t, err)
			require.Equal(t, tc.want, flags)
		})
	}
}
