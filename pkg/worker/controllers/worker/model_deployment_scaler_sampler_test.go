package worker

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	kueuepodconst "sigs.k8s.io/kueue/pkg/controller/jobs/pod/constants"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/worker/scaling/algorithm"
)

// scalerSamplerVLLMExposition 是一份 vLLM 指标响应，包含按 engine 标签上报的请求数、
// token 计数器和延迟直方图。
const scalerSamplerVLLMExposition = `# TYPE vllm:num_requests_running gauge
vllm:num_requests_running{engine="0",model_name="Qwen/Qwen2.5-7B-Instruct"} 4.0
# TYPE vllm:num_requests_waiting gauge
vllm:num_requests_waiting{engine="0",model_name="Qwen/Qwen2.5-7B-Instruct"} 2.0
# TYPE vllm:kv_cache_usage_perc gauge
vllm:kv_cache_usage_perc{engine="0",model_name="Qwen/Qwen2.5-7B-Instruct"} 0.35
# TYPE vllm:prompt_tokens_total counter
vllm:prompt_tokens_total{engine="0",model_name="Qwen/Qwen2.5-7B-Instruct"} 120000.0
# TYPE vllm:generation_tokens_total counter
vllm:generation_tokens_total{engine="0",model_name="Qwen/Qwen2.5-7B-Instruct"} 48000.0
# TYPE vllm:prefix_cache_hits_total counter
vllm:prefix_cache_hits_total{engine="0",model_name="Qwen/Qwen2.5-7B-Instruct"} 30000.0
# TYPE vllm:prefix_cache_queries_total counter
vllm:prefix_cache_queries_total{engine="0",model_name="Qwen/Qwen2.5-7B-Instruct"} 60000.0
# TYPE vllm:time_to_first_token_seconds histogram
vllm:time_to_first_token_seconds_bucket{engine="0",model_name="Qwen/Qwen2.5-7B-Instruct",le="0.5"} 40.0
vllm:time_to_first_token_seconds_bucket{engine="0",model_name="Qwen/Qwen2.5-7B-Instruct",le="1.0"} 50.0
vllm:time_to_first_token_seconds_bucket{engine="0",model_name="Qwen/Qwen2.5-7B-Instruct",le="+Inf"} 56.0
vllm:time_to_first_token_seconds_sum{engine="0",model_name="Qwen/Qwen2.5-7B-Instruct"} 20.0
vllm:time_to_first_token_seconds_count{engine="0",model_name="Qwen/Qwen2.5-7B-Instruct"} 56.0
# TYPE vllm:inter_token_latency_seconds histogram
vllm:inter_token_latency_seconds_bucket{engine="0",model_name="Qwen/Qwen2.5-7B-Instruct",le="0.01"} 1200.0
vllm:inter_token_latency_seconds_bucket{engine="0",model_name="Qwen/Qwen2.5-7B-Instruct",le="0.05"} 4800.0
vllm:inter_token_latency_seconds_bucket{engine="0",model_name="Qwen/Qwen2.5-7B-Instruct",le="+Inf"} 5428.0
vllm:inter_token_latency_seconds_sum{engine="0",model_name="Qwen/Qwen2.5-7B-Instruct"} 31.09642936099135
vllm:inter_token_latency_seconds_count{engine="0",model_name="Qwen/Qwen2.5-7B-Instruct"} 5428.0
`

// scalerSamplerVLLMSecondExposition 是同一引擎 14 秒后的响应：它完成了更多工作，
// 命中前缀缓存的 token 也有所增加。
const scalerSamplerVLLMSecondExposition = `# TYPE vllm:num_requests_running gauge
vllm:num_requests_running{engine="0",model_name="Qwen/Qwen2.5-7B-Instruct"} 7.0
# TYPE vllm:num_requests_waiting gauge
vllm:num_requests_waiting{engine="0",model_name="Qwen/Qwen2.5-7B-Instruct"} 3.0
# TYPE vllm:kv_cache_usage_perc gauge
vllm:kv_cache_usage_perc{engine="0",model_name="Qwen/Qwen2.5-7B-Instruct"} 0.42
# TYPE vllm:prompt_tokens_total counter
vllm:prompt_tokens_total{engine="0",model_name="Qwen/Qwen2.5-7B-Instruct"} 132000.0
# TYPE vllm:generation_tokens_total counter
vllm:generation_tokens_total{engine="0",model_name="Qwen/Qwen2.5-7B-Instruct"} 51600.0
# TYPE vllm:prefix_cache_hits_total counter
vllm:prefix_cache_hits_total{engine="0",model_name="Qwen/Qwen2.5-7B-Instruct"} 33000.0
# TYPE vllm:prefix_cache_queries_total counter
vllm:prefix_cache_queries_total{engine="0",model_name="Qwen/Qwen2.5-7B-Instruct"} 64000.0
# TYPE vllm:time_to_first_token_seconds histogram
vllm:time_to_first_token_seconds_bucket{engine="0",model_name="Qwen/Qwen2.5-7B-Instruct",le="0.5"} 110.0
vllm:time_to_first_token_seconds_bucket{engine="0",model_name="Qwen/Qwen2.5-7B-Instruct",le="1.0"} 130.0
vllm:time_to_first_token_seconds_bucket{engine="0",model_name="Qwen/Qwen2.5-7B-Instruct",le="+Inf"} 140.0
vllm:time_to_first_token_seconds_sum{engine="0",model_name="Qwen/Qwen2.5-7B-Instruct"} 61.0
vllm:time_to_first_token_seconds_count{engine="0",model_name="Qwen/Qwen2.5-7B-Instruct"} 140.0
# TYPE vllm:inter_token_latency_seconds histogram
vllm:inter_token_latency_seconds_bucket{engine="0",model_name="Qwen/Qwen2.5-7B-Instruct",le="0.01"} 1500.0
vllm:inter_token_latency_seconds_bucket{engine="0",model_name="Qwen/Qwen2.5-7B-Instruct",le="0.05"} 5350.0
vllm:inter_token_latency_seconds_bucket{engine="0",model_name="Qwen/Qwen2.5-7B-Instruct",le="+Inf"} 6000.0
vllm:inter_token_latency_seconds_sum{engine="0",model_name="Qwen/Qwen2.5-7B-Instruct"} 33.09642936099135
vllm:inter_token_latency_seconds_count{engine="0",model_name="Qwen/Qwen2.5-7B-Instruct"} 6000.0
`

// scalerSamplerSGLangExposition 是一份带张量、流水线和专家并行 rank 标签的 SGLang 指标响应，
// 两个 rank 重复报告相同的计数。
//
// 第二个 rank 刻意提供不同的直方图。如果把两个 rank 都计入，token 数会翻倍，延迟分布也会发生变化，
// 测试可据此发现重复采样。
const scalerSamplerSGLangExposition = `# TYPE sglang:num_running_reqs gauge
sglang:num_running_reqs{engine_type="unified",model_name="Qwen/Qwen2.5-7B-Instruct",moe_ep_rank="0",pp_rank="0",tp_rank="0"} 4.0
sglang:num_running_reqs{engine_type="unified",model_name="Qwen/Qwen2.5-7B-Instruct",moe_ep_rank="0",pp_rank="0",tp_rank="1"} 4.0
# TYPE sglang:num_queue_reqs gauge
sglang:num_queue_reqs{engine_type="unified",model_name="Qwen/Qwen2.5-7B-Instruct",moe_ep_rank="0",pp_rank="0",tp_rank="0"} 2.0
sglang:num_queue_reqs{engine_type="unified",model_name="Qwen/Qwen2.5-7B-Instruct",moe_ep_rank="0",pp_rank="0",tp_rank="1"} 2.0
# TYPE sglang:token_usage gauge
sglang:token_usage{engine_type="unified",model_name="Qwen/Qwen2.5-7B-Instruct",moe_ep_rank="0",pp_rank="0",tp_rank="0"} 0.38
# TYPE sglang:prompt_tokens_total counter
sglang:prompt_tokens_total{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="0"} 100000.0
sglang:prompt_tokens_total{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="1"} 100000.0
# TYPE sglang:generation_tokens_total counter
sglang:generation_tokens_total{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="0"} 40000.0
sglang:generation_tokens_total{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="1"} 40000.0
# TYPE sglang:prefill_effective_tokens_total counter
sglang:prefill_effective_tokens_total{mode="input",model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="0"} 60000.0
sglang:prefill_effective_tokens_total{mode="input",model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="1"} 60000.0
sglang:prefill_effective_tokens_total{mode="device_hit",model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="0"} 20000.0
sglang:prefill_effective_tokens_total{mode="device_hit",model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="1"} 20000.0
sglang:prefill_effective_tokens_total{mode="host_hit",model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="0"} 5000.0
sglang:prefill_effective_tokens_total{mode="host_hit",model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="1"} 5000.0
sglang:prefill_effective_tokens_total{mode="storage_hit",model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="0"} 1000.0
sglang:prefill_effective_tokens_total{mode="storage_hit",model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="1"} 1000.0
# TYPE sglang:time_to_first_token_seconds histogram
sglang:time_to_first_token_seconds_bucket{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="0",le="0.5"} 40.0
sglang:time_to_first_token_seconds_bucket{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="0",le="1.0"} 50.0
sglang:time_to_first_token_seconds_bucket{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="0",le="+Inf"} 56.0
sglang:time_to_first_token_seconds_sum{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="0"} 20.0
sglang:time_to_first_token_seconds_count{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="0"} 56.0
sglang:time_to_first_token_seconds_bucket{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="1",le="0.5"} 0.0
sglang:time_to_first_token_seconds_bucket{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="1",le="1.0"} 0.0
sglang:time_to_first_token_seconds_bucket{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="1",le="+Inf"} 400.0
sglang:time_to_first_token_seconds_sum{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="1"} 300.0
sglang:time_to_first_token_seconds_count{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="1"} 400.0
# TYPE sglang:inter_token_latency_seconds histogram
sglang:inter_token_latency_seconds_bucket{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="0",le="0.01"} 1200.0
sglang:inter_token_latency_seconds_bucket{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="0",le="0.05"} 4800.0
sglang:inter_token_latency_seconds_bucket{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="0",le="+Inf"} 5428.0
sglang:inter_token_latency_seconds_sum{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="0"} 15.15720793402943
sglang:inter_token_latency_seconds_count{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="0"} 5428.0
sglang:inter_token_latency_seconds_bucket{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="1",le="0.01"} 1200.0
sglang:inter_token_latency_seconds_bucket{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="1",le="0.05"} 4800.0
sglang:inter_token_latency_seconds_bucket{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="1",le="+Inf"} 5428.0
sglang:inter_token_latency_seconds_sum{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="1"} 15.15720793402943
sglang:inter_token_latency_seconds_count{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="1"} 5428.0
`

// scalerSamplerSGLangSecondExposition 是同一 SGLang 引擎 19 秒后的指标响应。
const scalerSamplerSGLangSecondExposition = `# TYPE sglang:num_running_reqs gauge
sglang:num_running_reqs{engine_type="unified",model_name="Qwen/Qwen2.5-7B-Instruct",moe_ep_rank="0",pp_rank="0",tp_rank="0"} 7.0
sglang:num_running_reqs{engine_type="unified",model_name="Qwen/Qwen2.5-7B-Instruct",moe_ep_rank="0",pp_rank="0",tp_rank="1"} 7.0
# TYPE sglang:num_queue_reqs gauge
sglang:num_queue_reqs{engine_type="unified",model_name="Qwen/Qwen2.5-7B-Instruct",moe_ep_rank="0",pp_rank="0",tp_rank="0"} 3.0
sglang:num_queue_reqs{engine_type="unified",model_name="Qwen/Qwen2.5-7B-Instruct",moe_ep_rank="0",pp_rank="0",tp_rank="1"} 3.0
# TYPE sglang:token_usage gauge
sglang:token_usage{engine_type="unified",model_name="Qwen/Qwen2.5-7B-Instruct",moe_ep_rank="0",pp_rank="0",tp_rank="0"} 0.44
# TYPE sglang:prompt_tokens_total counter
sglang:prompt_tokens_total{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="0"} 112000.0
sglang:prompt_tokens_total{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="1"} 112000.0
# TYPE sglang:generation_tokens_total counter
sglang:generation_tokens_total{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="0"} 43600.0
sglang:generation_tokens_total{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="1"} 43600.0
# TYPE sglang:prefill_effective_tokens_total counter
sglang:prefill_effective_tokens_total{mode="input",model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="0"} 78000.0
sglang:prefill_effective_tokens_total{mode="input",model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="1"} 78000.0
sglang:prefill_effective_tokens_total{mode="device_hit",model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="0"} 26000.0
sglang:prefill_effective_tokens_total{mode="device_hit",model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="1"} 26000.0
sglang:prefill_effective_tokens_total{mode="host_hit",model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="0"} 7000.0
sglang:prefill_effective_tokens_total{mode="host_hit",model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="1"} 7000.0
sglang:prefill_effective_tokens_total{mode="storage_hit",model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="0"} 2000.0
sglang:prefill_effective_tokens_total{mode="storage_hit",model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="1"} 2000.0
# TYPE sglang:time_to_first_token_seconds histogram
sglang:time_to_first_token_seconds_bucket{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="0",le="0.5"} 110.0
sglang:time_to_first_token_seconds_bucket{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="0",le="1.0"} 130.0
sglang:time_to_first_token_seconds_bucket{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="0",le="+Inf"} 140.0
sglang:time_to_first_token_seconds_sum{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="0"} 61.0
sglang:time_to_first_token_seconds_count{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="0"} 140.0
sglang:time_to_first_token_seconds_bucket{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="1",le="0.5"} 0.0
sglang:time_to_first_token_seconds_bucket{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="1",le="1.0"} 0.0
sglang:time_to_first_token_seconds_bucket{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="1",le="+Inf"} 900.0
sglang:time_to_first_token_seconds_sum{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="1"} 700.0
sglang:time_to_first_token_seconds_count{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="1"} 900.0
# TYPE sglang:inter_token_latency_seconds histogram
sglang:inter_token_latency_seconds_bucket{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="0",le="0.01"} 1500.0
sglang:inter_token_latency_seconds_bucket{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="0",le="0.05"} 5350.0
sglang:inter_token_latency_seconds_bucket{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="0",le="+Inf"} 6000.0
sglang:inter_token_latency_seconds_sum{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="0"} 17.15720793402943
sglang:inter_token_latency_seconds_count{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="0"} 6000.0
sglang:inter_token_latency_seconds_bucket{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="1",le="0.01"} 1500.0
sglang:inter_token_latency_seconds_bucket{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="1",le="0.05"} 5400.0
sglang:inter_token_latency_seconds_bucket{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="1",le="+Inf"} 6000.0
sglang:inter_token_latency_seconds_sum{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="1"} 17.15720793402943
sglang:inter_token_latency_seconds_count{model_name="Qwen/Qwen2.5-7B-Instruct",pp_rank="0",tp_rank="1"} 6000.0
`

// scalerSamplerPod 构造与渲染结果一致的就绪引擎 Pod，包含角色标签、指标监听端口和 Operator 生成的主容器。
func scalerSamplerPod(name, role string, uid types.UID, ordinal int) core.Pod {
	return core.Pod{
		ObjectMeta: meta.ObjectMeta{
			Name:      name,
			Namespace: "default",
			UID:       uid,
			Labels: map[string]string{
				modelDeploymentLabelKeyComponent:   role,
				modelDeploymentReplicaOrdinalLabel: strconv.Itoa(ordinal),
			},
			Annotations: map[string]string{
				"prometheus.io/port":                    "8000",
				"prometheus.io/scheme":                  "http",
				"prometheus.io/path":                    "/metrics",
				"prometheus.io/scrape":                  modelDeploymentEndpointEligibleValue,
				kueuepodconst.GroupTotalCountAnnotation: "1",
			},
		},
		Spec: core.PodSpec{
			Containers: []core.Container{{
				Name:  modelDeploymentMainContainerName,
				Ports: []core.ContainerPort{{Name: "http", ContainerPort: 8000}},
			}},
		},
		Status: core.PodStatus{
			PodIP: "10.0.0.1",
			Conditions: []core.PodCondition{{
				Type:   core.PodReady,
				Status: core.ConditionTrue,
			}},
			ContainerStatuses: []core.ContainerStatus{{
				Name:         modelDeploymentMainContainerName,
				Ready:        true,
				RestartCount: 0,
			}},
		},
	}
}

// scalerSamplerTarget 构造 Scaler 要调节的目标部署。
func scalerSamplerTarget(uid types.UID, engine string, roles ...workercore.ModelDeploymentRole) *workercore.ModelDeployment {
	return &workercore.ModelDeployment{
		ObjectMeta: meta.ObjectMeta{Name: "target", Namespace: "default", UID: uid},
		Spec: workercore.ModelDeploymentSpec{
			Engine: workercore.ModelDeploymentEngine{Name: engine},
			Roles:  roles,
		},
	}
}

// scalerSamplerRole 构造目标部署中的一个角色。
func scalerSamplerRole(name string, kind workercore.ModelDeploymentRoleKind, replicas int32) workercore.ModelDeploymentRole {
	return workercore.ModelDeploymentRole{Name: name, Kind: kind, Replicas: replicas}
}

// scalerSamplerScaler 构造针对目标部署的 Scaler。
func scalerSamplerScaler(uid types.UID, intervalSeconds int32) *workercore.ModelDeploymentScaler {
	return &workercore.ModelDeploymentScaler{
		ObjectMeta: meta.ObjectMeta{Name: "scaler", Namespace: "default", UID: uid},
		Spec: workercore.ModelDeploymentScalerSpec{
			TargetRef: workercore.ModelDeploymentScalerTargetReference{Name: "target"},
			Sampling:  workercore.ModelDeploymentScalerSamplingSpec{IntervalSeconds: intervalSeconds},
		},
	}
}

// TestModelDeploymentScalerSampler_TwoPassRates 对同一角色采样两次，并用两个引擎各自导出的计数验证速率。
func TestModelDeploymentScalerSampler_TwoPassRates(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		// engine 是目标部署使用的引擎。
		engine string
		// first 和 second 分别是两次采样返回的指标文本。
		first  string
		second string
		// window 是两次采样之间的时间间隔。
		window time.Duration
		// wantPromptTokensPerSecond 是预期的未命中缓存的输入 token 速率。
		wantPromptTokensPerSecond float64
		// wantGenerationTokensPerSecond 是预期的生成 token 速率。
		wantGenerationTokensPerSecond float64
		// wantTTFTMilliseconds 和 wantITLMilliseconds 是预期的首 token 延迟和 token 间延迟，
		// 单位为毫秒。
		wantTTFTMilliseconds float64
		wantITLMilliseconds  float64
		// wantRunning 和 wantWaiting 分别是预期的运行中请求数与等待请求数。
		wantRunning float64
		wantWaiting float64
		// wantKVCacheUsage 是预期的 KV 缓存使用率。
		wantKVCacheUsage float64
	}{
		{
			name:                          "vLLM counts the prompt tokens the prefix cache did not serve",
			engine:                        workercore.ModelDeploymentEngineVLLM,
			first:                         scalerSamplerVLLMExposition,
			second:                        scalerSamplerVLLMSecondExposition,
			window:                        14 * time.Second,
			wantPromptTokensPerSecond:     9000.0 / 14.0,
			wantGenerationTokensPerSecond: 3600.0 / 14.0,
			wantTTFTMilliseconds:          1000.0,
			wantITLMilliseconds:           50.0,
			wantRunning:                   7,
			wantWaiting:                   3,
			wantKVCacheUsage:              0.42,
		},
		{
			name:                          "SGLang counts one rank and reads the effective input tokens",
			engine:                        workercore.ModelDeploymentEngineSGLang,
			first:                         scalerSamplerSGLangExposition,
			second:                        scalerSamplerSGLangSecondExposition,
			window:                        19 * time.Second,
			wantPromptTokensPerSecond:     18000.0 / 19.0,
			wantGenerationTokensPerSecond: 3600.0 / 19.0,
			wantTTFTMilliseconds:          1000.0,
			wantITLMilliseconds:           50.0,
			wantRunning:                   7,
			wantWaiting:                   3,
			wantKVCacheUsage:              0.44,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			pod := scalerSamplerPod("engine-0", "server", types.UID("pod-uid"), 0)
			target := scalerSamplerTarget(types.UID("target-uid"), tc.engine, scalerSamplerRole("server", workercore.ModelDeploymentRoleKindServer, 1))
			// 为使第二次调用实际抓取指标，设置的最短采样间隔要短于两次调用的时间差。
			scaler := scalerSamplerScaler(types.UID("scaler-uid"), 10)
			body := tc.first
			sampler := &modelDeploymentScalerSampler{
				Scrape: func(context.Context, *core.Pod) ([]byte, error) { return []byte(body), nil },
			}

			start := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
			first := sampler.SampleRoleMetrics(context.Background(), scaler, target, []core.Pod{pod}, start)
			require.Contains(t, first, "server")
			assert.Equal(t, algorithm.SignalFresh, first["server"].GaugeState,
				"the first pass reads the request gauges")
			assert.Equal(t, algorithm.SignalMissing, first["server"].ThroughputState,
				"the first pass has no window to read a rate from")
			assert.False(t, first["server"].CompleteCoverage,
				"the first pass cannot cover a rate it has no window for")

			body = tc.second

			second := sampler.SampleRoleMetrics(context.Background(), scaler, target, []core.Pod{pod}, start.Add(tc.window))
			require.Contains(t, second, "server")
			observation := second["server"]

			assert.Equal(t, algorithm.SignalFresh, observation.GaugeState)
			assert.Equal(t, algorithm.SignalFresh, observation.ThroughputState)
			assert.Equal(t, algorithm.SignalFresh, observation.LatencyState)
			assert.True(t, observation.CompleteCoverage,
				"one ready Pod answering twice covers the role")
			assert.InDelta(t, tc.wantRunning, observation.RunningRequests, 1e-9)
			assert.InDelta(t, tc.wantWaiting, observation.WaitingRequests, 1e-9)
			assert.InDelta(t, tc.wantKVCacheUsage, observation.KVCacheUsage, 1e-9)
			assert.InDelta(t, tc.wantPromptTokensPerSecond, observation.PromptTokensPerSecond, 1e-6)
			assert.InDelta(t, tc.wantGenerationTokensPerSecond, observation.GenerationTokensPerSecond, 1e-6)
			assert.InDelta(t, tc.wantTTFTMilliseconds, observation.TTFTMilliseconds, 1e-6)
			assert.InDelta(t, tc.wantITLMilliseconds, observation.ITLMilliseconds, 1e-6)
			assert.Equal(t, int32(1), observation.ReadyReplicas)
			assert.Equal(t, int32(1), observation.CurrentReplicas)
			assert.Equal(t, algorithm.RoleKindServer, observation.Kind)
		})
	}
}

// TestModelDeploymentScalerRunningRequestsShadowThenActive 验证同一负载在 Shadow 中只形成建议，切换到 Active 后才写回副本数。
func TestModelDeploymentScalerRunningRequestsShadowThenActive(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(scalerSamplerVLLMSecondExposition))
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	md := scalerTestDeployment("target", "md-uid", map[string]int32{"server": 1})
	md.Spec.Model.Name = "Qwen/Qwen2.5-7B-Instruct"
	md.Spec.Engine.Name = workercore.ModelDeploymentEngineVLLM
	md.Spec.Roles[0].Kind = workercore.ModelDeploymentRoleKindServer
	pod := scalerSamplerPod("engine-0", "server", "pod-uid", 0)
	pod.Namespace = md.Namespace
	pod.OwnerReferences = []meta.OwnerReference{*meta.NewControllerRef(md, workercore.SchemeGroupVersion.WithKind("ModelDeployment"))}
	pod.Labels[modelDeploymentLabelKeyName] = modelDeploymentLabelValueName
	pod.Labels[modelDeploymentLabelKeyInstance] = md.Name
	addr := server.Listener.Addr().(*net.TCPAddr)
	pod.Status.PodIP = addr.IP.String()
	pod.Annotations["prometheus.io/port"] = strconv.Itoa(addr.Port)
	pod.Spec.Containers[0].Ports[0].ContainerPort = int32(addr.Port)

	scaler := scalerTestScaler("scaler", "scaler-uid", workercore.ModelDeploymentScalerModeShadow)
	scaler.Spec.Sampling.IntervalSeconds = 5
	scaler.Spec.Roles[0].MaxReplicas = 3
	scaler.Spec.Algorithm.Proportional.RoleTargets[0].Target = resource.MustParse("4")
	client := scalerTestClient(t, md, scaler, &pod)
	now := time.Unix(1000, 0)
	clock := func() time.Time { return now }
	r := &ModelDeploymentScalerReconciler{
		Client: client, clock: clock,
		sampler: &modelDeploymentScalerSampler{Clock: clock, HTTPClient: server.Client()},
	}

	// 这份指标响应有 7 个正在运行的请求。每个副本的目标是 4，因此一个就绪副本应扩到两个。
	reconcileScaler(t, r, scaler.Name)
	shadow := scalerTestGetScaler(t, client, scaler.Name)
	require.Len(t, shadow.Status.Roles, 1)
	require.EqualValues(t, 2, shadow.Status.Roles[0].RawRecommendedReplicas)
	require.EqualValues(t, 2, shadow.Status.Roles[0].ConstrainedReplicas)
	target := scalerTestGetDeployment(t, client)
	require.EqualValues(t, 1, target.Spec.Roles[0].Replicas)
	require.Empty(t, target.Annotations[workercore.ModelDeploymentScalerBindingAnnotationKey])
	require.Empty(t, target.Annotations[workercore.ModelDeploymentScalerDecisionAnnotationKey])

	shadow.Spec.Mode = workercore.ModelDeploymentScalerModeActive
	require.NoError(t, client.Update(t.Context(), shadow))
	now = now.Add(5 * time.Second)
	reconcileScaler(t, r, scaler.Name)
	target = scalerTestGetDeployment(t, client)
	require.EqualValues(t, 2, target.Spec.Roles[0].Replicas)
	binding, ok, err := readScalerBinding(target)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, string(scaler.UID), binding.ScalerUID)
	active := scalerTestGetScaler(t, client, scaler.Name)
	require.NotNil(t, active.Status.LastCommit)
	require.NotNil(t, active.Status.PendingCohort)
	require.Equal(t, active.Status.LastCommit.DecisionID, active.Status.PendingCohort.DecisionID)
	require.EqualValues(t, 1, active.Status.PendingCohort.BaselineVector["server"])
	require.EqualValues(t, 2, active.Status.PendingCohort.TargetVector["server"])
}
