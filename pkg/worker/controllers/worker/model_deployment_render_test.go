package worker

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	worker "gpustack.ai/gpustack/api/worker/v1"
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/systemmeta"
)

// newRenderDeployment 构造用于渲染测试的基础单角色部署，各用例通过 mutate 按需改写。
func newRenderDeployment(mutate ...func(*workercore.ModelDeployment)) *workercore.ModelDeployment {
	md := &workercore.ModelDeployment{
		ObjectMeta: meta.ObjectMeta{Name: "qwen", Namespace: "team-a", UID: "md-uid"},
		Spec: workercore.ModelDeploymentSpec{
			Model: workercore.ModelDeploymentModel{Name: "Qwen/Qwen2.5-72B-Instruct"},
			Engine: workercore.ModelDeploymentEngine{
				Name:  workercore.ModelDeploymentEngineVLLM,
				Image: "vllm/vllm-openai:v0.25.1",
			},
			KVCache: &workercore.ModelDeploymentKVCache{PoolRef: core.LocalObjectReference{Name: "shared-kv"}},
			Roles: []workercore.ModelDeploymentRole{{
				Name:         "server",
				Replicas:     2,
				InstanceType: "h20-8x",
				Image:        "vllm/vllm-openai:v0.25.1",
			}},
		},
	}
	for _, m := range mutate {
		m(md)
	}

	return md
}

// newRenderInstanceType 构造用于资源推导与渲染测试的加速型 InstanceType（单卡配比 16 CPU、64Gi 内存）。
func newRenderInstanceType(mutate ...func(*worker.InstanceType)) *worker.InstanceType {
	it := &worker.InstanceType{
		ObjectMeta: meta.ObjectMeta{Name: "h20-8x"},
		Spec: workercore.InstanceTypeSpec{
			Acceleratable: true,
			UnitResources: workercore.InstanceTypeUnitResources{CPU: "16", RAM: "64Gi"},
			LocalStorage:  "512Gi",
		},
		Status: workercore.InstanceTypeStatus{
			// 刻意不使用 FormatLocalQueueName("h20-8x")，以验证队列入口直接读取自 Status.Entrance 而非名称推导。
			Entrance: "queue-for-h20-8x",
			Detail: workercore.InstanceTypeDetail{
				Manufacturer: nodefeature.ManufacturerNVIDIA,
				InstanceTypeAcceleratorDetail: workercore.InstanceTypeAcceleratorDetail{
					RuntimeVersion:  "12.9",
					RuntimeVersions: []string{"12.9"},
				},
			},
		},
	}
	for _, m := range mutate {
		m(it)
	}

	return it
}

// TestRenderModelDeploymentPod_Command 验证受管模式下整条启动命令的装配顺序与监听地址默认填充规则。
func TestRenderModelDeploymentPod_Command(t *testing.T) {
	testCases := []struct {
		name        string
		engine      string
		extraArgs   []string
		connector   ModelDeploymentConnectorRender
		wantCommand []string
	}{
		{
			name:   "vllm base command, model positional, listen address filled",
			engine: workercore.ModelDeploymentEngineVLLM,
			wantCommand: []string{
				"vllm", "serve", "Qwen/Qwen2.5-72B-Instruct",
				"--host", "0.0.0.0", "--port", "8000",
			},
		},
		{
			name:   "sglang base command names the model through a flag, and is moved off loopback",
			engine: workercore.ModelDeploymentEngineSGLang,
			wantCommand: []string{
				"python3", "-m", "sglang.launch_server", "--model-path", "Qwen/Qwen2.5-72B-Instruct",
				"--enable-metrics",
				"--host", "0.0.0.0", "--port", "8000",
			},
		},
		{
			name:      "vLLM 将带空格的 speculative JSON 作为一个参数传入",
			engine:    workercore.ModelDeploymentEngineVLLM,
			extraArgs: []string{"--speculative-config", testSpeculativeNgramConfig},
			wantCommand: []string{
				"vllm", "serve", "Qwen/Qwen2.5-72B-Instruct",
				"--speculative-config", testSpeculativeNgramConfig,
				"--host", "0.0.0.0", "--port", "8000",
			},
		},
		{
			name:      "SGLang 接收原生 speculative 参数",
			engine:    workercore.ModelDeploymentEngineSGLang,
			extraArgs: []string{"--speculative-algorithm", "EAGLE", "--speculative-num-steps=4"},
			wantCommand: []string{
				"python3", "-m", "sglang.launch_server", "--model-path", "Qwen/Qwen2.5-72B-Instruct",
				"--enable-metrics", "--speculative-algorithm", "EAGLE", "--speculative-num-steps=4",
				"--host", "0.0.0.0", "--port", "8000",
			},
		},
		{
			name:      "connector arguments land before the user's, and the address after both",
			engine:    workercore.ModelDeploymentEngineVLLM,
			connector: ModelDeploymentConnectorRender{Args: []string{`--kv-transfer-config={}`}},
			extraArgs: []string{"--max-model-len=32768"},
			wantCommand: []string{
				"vllm", "serve", "Qwen/Qwen2.5-72B-Instruct",
				`--kv-transfer-config={}`,
				"--max-model-len=32768",
				"--host", "0.0.0.0", "--port", "8000",
			},
		},
		{
			name:      "a role's own port is kept and only the host is filled",
			engine:    workercore.ModelDeploymentEngineVLLM,
			extraArgs: []string{"--port", "9100"},
			wantCommand: []string{
				"vllm", "serve", "Qwen/Qwen2.5-72B-Instruct",
				"--port", "9100",
				"--host", "0.0.0.0",
			},
		},
		{
			name:      "the equals spelling counts as already set",
			engine:    workercore.ModelDeploymentEngineSGLang,
			extraArgs: []string{"--host=127.0.0.1"},
			wantCommand: []string{
				"python3", "-m", "sglang.launch_server", "--model-path", "Qwen/Qwen2.5-72B-Instruct",
				"--enable-metrics",
				"--host=127.0.0.1",
				"--port", "8000",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			md := newRenderDeployment(func(md *workercore.ModelDeployment) {
				md.Spec.Engine.Name = tc.engine
				md.Spec.Roles[0].ExtraArgs = tc.extraArgs
			})

			pod, err := renderModelDeploymentPod(context.Background(), ModelDeploymentRenderInput{
				Deployment:   md,
				Role:         &md.Spec.Roles[0],
				InstanceType: newRenderInstanceType(),
				Connector:    tc.connector,
			})
			require.NoError(t, err)

			assert.Equal(t, tc.wantCommand, pod.Spec.Containers[0].Command)
		})
	}
}

// TestDeriveModelDeploymentResources 验证整卡、无卡、逻辑切分、硬件分区及未就绪详情场景下的宿主机资源推导。
func TestDeriveModelDeploymentResources(t *testing.T) {
	testCases := []struct {
		name      string
		resources *workercore.ModelDeploymentRoleResources
		instType  func(*worker.InstanceType)

		wantCPU, wantRAM string
		wantErr          string
	}{
		{
			name:      "whole cards scale the unit resources by the card count",
			resources: &workercore.ModelDeploymentRoleResources{Accelerator: ptr.To(resource.MustParse("4"))},
			wantCPU:   "64", wantRAM: "256Gi",
		},
		{
			name:    "no accelerator at all still sizes one unit's worth of host",
			wantCPU: "16", wantRAM: "64Gi",
		},
		{
			name: "a memory slice takes that percentage of one card's worth",
			resources: &workercore.ModelDeploymentRoleResources{
				Accelerator:                       ptr.To(resource.MustParse("1")),
				AcceleratorSlicedMemoryPercentage: 50,
			},
			instType: func(it *worker.InstanceType) { it.Status.Detail.SlicedDetail.Logical.Count = 128 },
			wantCPU:  "8", wantRAM: "32Gi",
		},
		{
			name: "a bare compute slice copies across, so the host follows the same fraction",
			resources: &workercore.ModelDeploymentRoleResources{
				Accelerator:                      ptr.To(resource.MustParse("1")),
				AcceleratorSlicedCoresPercentage: 25,
			},
			instType: func(it *worker.InstanceType) { it.Status.Detail.SlicedDetail.Logical.Count = 128 },
			wantCPU:  "4", wantRAM: "16Gi",
		},
		{
			name: "a partition profile is anchored on the share of VRAM it occupies",
			resources: &workercore.ModelDeploymentRoleResources{
				Accelerator:                   ptr.To(resource.MustParse("1")),
				AcceleratorPartitionedProfile: "3g.40gb",
			},
			instType: func(it *worker.InstanceType) {
				it.Status.Detail.Memory = "80Gi"
				it.Status.Detail.SlicedDetail.Physical.Profiles = []workercore.AcceleratorSlicedPhysicalDetailProfile{
					{Name: "3g.40gb", MemoryMib: 40 << 10, Count: 2},
				}
			},
			wantCPU: "8", wantRAM: "32Gi",
		},
		{
			name: "a slice against an uncomputed detail is retryable, never a whole card",
			resources: &workercore.ModelDeploymentRoleResources{
				Accelerator:                       ptr.To(resource.MustParse("1")),
				AcceleratorSlicedMemoryPercentage: 50,
			},
			instType: func(it *worker.InstanceType) { it.Status.Detail.Manufacturer = "" },
			wantErr:  "not ready yet",
		},
		{
			name: "a partition the detail cannot size yet is retryable too",
			resources: &workercore.ModelDeploymentRoleResources{
				Accelerator:                   ptr.To(resource.MustParse("1")),
				AcceleratorPartitionedProfile: "3g.40gb",
			},
			instType: func(it *worker.InstanceType) {
				it.Status.Detail.SlicedDetail.Physical.Profiles = []workercore.AcceleratorSlicedPhysicalDetailProfile{
					{Name: "3g.40gb", MemoryMib: 0},
				}
			},
			wantErr: "cannot be sized",
		},
		{
			name:      "an unparseable unit is reported rather than rendered as zero",
			resources: &workercore.ModelDeploymentRoleResources{Accelerator: ptr.To(resource.MustParse("1"))},
			instType:  func(it *worker.InstanceType) { it.Spec.UnitResources.CPU = "not-a-quantity" },
			wantErr:   "invalid CPU unit",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			it := newRenderInstanceType()
			if tc.instType != nil {
				tc.instType(it)
			}
			role := &workercore.ModelDeploymentRole{Name: "server", Resources: tc.resources}

			ress, err := deriveModelDeploymentResources(role, it)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)

				return
			}

			require.NoError(t, err)
			qtyEqual(t, qty(tc.wantCPU), ress.CPU, "cpu")
			qtyEqual(t, qty(tc.wantRAM), ress.RAM, "ram")
			qtyEqual(t, qty("15Gi"), ress.LocalStorage, "local storage")
		})
	}
}

// TestRenderModelDeploymentPod_TemplateCarriesNoGroupMetadataOrHash 验证模板渲染与打标（stamp）两阶段边界：
// 模板阶段不携带任何 Kueue 组元数据、ordinal 或 spec 哈希，打标后统一补齐。
func TestRenderModelDeploymentPod_TemplateCarriesNoGroupMetadataOrHash(t *testing.T) {
	md := newRenderDeployment()
	role := &md.Spec.Roles[0]

	template, err := renderModelDeploymentPodTemplate(context.Background(), ModelDeploymentRenderInput{
		Deployment:   md,
		Role:         role,
		InstanceType: newRenderInstanceType(),
	})
	require.NoError(t, err)

	assert.NotContains(t, template.Labels, "kueue.x-k8s.io/pod-group-name")
	assert.NotContains(t, template.Labels, modelDeploymentReplicaOrdinalLabel)
	assert.NotContains(t, template.Annotations, "kueue.x-k8s.io/pod-group-total-count")
	assert.NotContains(t, template.Annotations, "kueue.x-k8s.io/role-hash")
	assert.NotContains(t, template.Annotations, modelDeploymentPodSpecHashAnnotation)

	assert.Equal(t, "qwen-server-", template.GenerateName)
	require.Len(t, template.Spec.Containers, 1)
	require.Len(t, template.OwnerReferences, 1)
	assert.True(t, systemmeta.MatchResource(template, ModelDeploymentResourceType))

	stampModelDeploymentPod(template, md, role, 0, 0)

	assert.Equal(t, modelDeploymentReplicaGroupName(md, role.Name, 0),
		template.Labels["kueue.x-k8s.io/pod-group-name"],
		"每个副本应拥有独立分组名")
	assert.Equal(t, "1", template.Annotations["kueue.x-k8s.io/pod-group-total-count"],
		"单成员副本的分组总数应为 1")
	assert.Equal(t, "0", template.Labels[modelDeploymentReplicaOrdinalLabel],
		"打标时应写入副本 ordinal 标签")
	assert.Equal(t, "server", template.Annotations["kueue.x-k8s.io/role-hash"],
		"role-hash 应保持为角色名")
	assert.Contains(t, template.Annotations, modelDeploymentPodSpecHashAnnotation)
}

// TestRenderModelDeploymentPodTemplate_RendersTwoReplicasOfOneRoleIdentical 验证同一角色两次渲染的模板
// 序列化字节完全一致，确保副本身份不会泄漏进模板阶段。
func TestRenderModelDeploymentPodTemplate_RendersTwoReplicasOfOneRoleIdentical(t *testing.T) {
	testCases := []struct {
		name  string
		input func(*testing.T) ModelDeploymentRenderInput
	}{
		{
			name: "a server role",
			input: func(t *testing.T) ModelDeploymentRenderInput {
				md := newRenderDeployment()

				return ModelDeploymentRenderInput{
					Deployment: md, Role: &md.Spec.Roles[0], InstanceType: newRenderInstanceType(),
				}
			},
		},
		{
			name: "a direct decoder with a native routing sidecar",
			input: func(t *testing.T) ModelDeploymentRenderInput {
				md := newRenderDeployment(func(md *workercore.ModelDeployment) {
					md.Spec.Router = &workercore.ModelDeploymentRouter{Name: workercore.ModelDeploymentRouterLLMD}
					md.Spec.Roles[0].Kind = workercore.ModelDeploymentRoleKindDecode
					md.Spec.Roles[0].Ports = []workercore.ModelDeploymentPort{{
						Protocol: core.ProtocolTCP, Port: 8000,
					}}
				})

				return ModelDeploymentRenderInput{
					Deployment: md, Role: &md.Spec.Roles[0], InstanceType: newRenderInstanceType(),
					Connector: ModelDeploymentConnectorRender{
						Args: []string{"--kv-transfer-config", `{}`}, KVTransfer: true, RoutingSidecar: true,
					},
					NativeSidecar: true,
				}
			},
		},
		{
			name: "a take-over role",
			input: func(t *testing.T) ModelDeploymentRenderInput {
				md := newRenderDeployment(func(md *workercore.ModelDeployment) {
					md.Spec.Roles[0].Command = []string{"/bin/my-server", "--flag"}
				})

				return ModelDeploymentRenderInput{
					Deployment: md, Role: &md.Spec.Roles[0], InstanceType: newRenderInstanceType(),
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			first, err := renderModelDeploymentPodTemplate(context.Background(), tc.input(t))
			require.NoError(t, err)
			second, err := renderModelDeploymentPodTemplate(context.Background(), tc.input(t))
			require.NoError(t, err)

			require.Len(t, first.Spec.Containers, 1)

			firstJSON, err := json.Marshal(first)
			require.NoError(t, err)
			secondJSON, err := json.Marshal(second)
			require.NoError(t, err)

			assert.Equal(t, string(firstJSON), string(secondJSON))
		})
	}
}
