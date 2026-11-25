package worker

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/nodefeature"
)

// connectorInputForKind 构造带指定角色类型的连接器合成输入夹具，用于按引擎与角色验证 KV 交接输出。
func connectorInputForKind(
	engine, manufacturer string, kind workercore.ModelDeploymentRoleKind,
) ModelDeploymentConnectorInput {
	return ModelDeploymentConnectorInput{
		Engine:              engine,
		Manufacturer:        manufacturer,
		Kind:                kind,
		Domain:              "team-a-shared",
		MasterServerAddress: "shared-kv-master.gpustack-system.svc:50051",
		Protocols:           []string{"tcp"},
		// 该夹具默认声明了成对的拆分角色，以便验证拆分形态下的连接器输出。
		Disaggregated: true,
	}
}

// connectorPredicateDeployment 构造携带 router 和单个指定类型角色的 ModelDeployment 夹具。
func connectorPredicateDeployment(
	router, engine string, kind workercore.ModelDeploymentRoleKind,
) *workercore.ModelDeployment {
	return newRenderDeployment(func(md *workercore.ModelDeployment) {
		md.Spec.Engine.Name = engine
		md.Spec.Router = &workercore.ModelDeploymentRouter{Name: router}
		md.Spec.Roles[0].Kind = kind
	})
}

// complementaryRole 返回 prefill/decode 配对中的另一半角色；Server 角色没有对应配对，第二返回值为 false。
func complementaryRole(kind workercore.ModelDeploymentRoleKind) (workercore.ModelDeploymentRole, bool) {
	switch kind {
	case workercore.ModelDeploymentRoleKindPrefill:
		return workercore.ModelDeploymentRole{Name: "decode", Kind: workercore.ModelDeploymentRoleKindDecode}, true
	case workercore.ModelDeploymentRoleKindDecode:
		return workercore.ModelDeploymentRole{Name: "prefill", Kind: workercore.ModelDeploymentRoleKindPrefill}, true
	default:
		return workercore.ModelDeploymentRole{}, false
	}
}

// routedModelDeployment 构造挂接 llm-d-router 且包含 prefill 与 decode 两个角色的 ModelDeployment 夹具。
func routedModelDeployment(mutate ...func(*workercore.ModelDeployment)) *workercore.ModelDeployment {
	md := newRenderDeployment(func(md *workercore.ModelDeployment) {
		decode := md.Spec.Roles[0]
		md.Spec.Router = &workercore.ModelDeploymentRouter{Name: workercore.ModelDeploymentRouterLLMD}
		md.Spec.Roles[0].Name = "prefill"
		md.Spec.Roles[0].Kind = workercore.ModelDeploymentRoleKindPrefill
		decode.Name = "decode"
		decode.Kind = workercore.ModelDeploymentRoleKindDecode
		md.Spec.Roles = append(md.Spec.Roles, decode)
	})
	for _, m := range mutate {
		m(md)
	}

	return md
}

func TestSynthesizeModelDeploymentConnector_KVTransfer(t *testing.T) {
	in := connectorInputForKind(
		workercore.ModelDeploymentEngineVLLM, nodefeature.ManufacturerNVIDIA,
		workercore.ModelDeploymentRoleKindPrefill,
	)
	in.KVTransfer = true
	in.PublishKVEvents = true
	in.KVEventsHost = "qwen-prefill.team-a.svc"

	got, err := SynthesizeModelDeploymentConnector(in)
	require.NoError(t, err)
	assert.True(t, got.KVTransfer)
	require.Len(t, got.Args, 4)
	assert.JSONEq(t, `{
		"kv_connector":"MultiConnector","kv_role":"kv_producer",
		"kv_connector_extra_config":{"connectors":[
			{"kv_connector":"MooncakeConnector","kv_role":"kv_producer",
			 "kv_connector_extra_config":{"mooncake_protocol":"tcp"}},
			{"kv_connector":"MooncakeStoreConnector","kv_role":"kv_both"}
		]}
	}`, got.Args[1])
	assert.Contains(t, got.Env, core.EnvVar{
		Name: "VLLM_MOONCAKE_BOOTSTRAP_PORT", Value: "8998",
	})
	assert.Equal(t, []core.ContainerPort{
		{Name: "mc-bootstrap", Protocol: core.ProtocolTCP, ContainerPort: 8998},
		{Name: "kv-events", Protocol: core.ProtocolTCP, ContainerPort: 5557},
		{Name: "kv-replay", Protocol: core.ProtocolTCP, ContainerPort: 5558},
	}, got.Ports)
}

func TestSynthesizeModelDeploymentConnector_SGLangPureTransfer(t *testing.T) {
	in := connectorInputForKind(
		workercore.ModelDeploymentEngineSGLang, nodefeature.ManufacturerNVIDIA,
		workercore.ModelDeploymentRoleKindPrefill,
	)
	in.Domain = ""
	in.MasterServerAddress = ""
	in.Protocols = nil
	in.KVTransfer = true

	got, err := SynthesizeModelDeploymentConnector(in)
	require.NoError(t, err)
	assert.Empty(t, got.Env)
	assert.NotContains(t, got.Args, "--hicache-storage-backend")
	assert.Contains(t, got.Args, "--disaggregation-mode")
	assert.True(t, got.KVTransfer)
}

// TestModelDeploymentUsesKVTransfer 锁定引擎侧直传通路的启用规则：该通路服务于所有已准入的
// router 与引擎组合，且仅在成对声明了 Prefill 与 Decode 两个角色时启用；单边角色会被前方
// router 按未拆分形态路由，引擎侧也应保持一致，不开启等待对端的直传通路。
func TestModelDeploymentUsesKVTransfer(t *testing.T) {
	cases := []struct {
		name         string
		router       string
		engine       string
		kind         workercore.ModelDeploymentRoleKind
		manufacturer string
		// lone 为 true 表示该用例仅声明了单边角色，缺少成对的另一半。
		lone bool
		want bool
	}{
		{
			name: "the picker in front of vllm", router: workercore.ModelDeploymentRouterLLMD,
			engine:       workercore.ModelDeploymentEngineVLLM,
			kind:         workercore.ModelDeploymentRoleKindPrefill,
			manufacturer: nodefeature.ManufacturerNVIDIA, want: true,
		},
		{
			// picker 可前置任意支持的引擎，在 SGLang 下两半由其自身的拆分参数渲染。
			name: "the picker in front of sglang", router: workercore.ModelDeploymentRouterLLMD,
			engine:       workercore.ModelDeploymentEngineSGLang,
			kind:         workercore.ModelDeploymentRoleKindPrefill,
			manufacturer: nodefeature.ManufacturerNVIDIA, want: true,
		},
		{
			name: "each project's own router", router: workercore.ModelDeploymentRouterVLLM,
			engine:       workercore.ModelDeploymentEngineVLLM,
			kind:         workercore.ModelDeploymentRoleKindDecode,
			manufacturer: nodefeature.ManufacturerNVIDIA, want: true,
		},
		{
			name: "and the gateway", router: workercore.ModelDeploymentRouterSGLang,
			engine:       workercore.ModelDeploymentEngineSGLang,
			kind:         workercore.ModelDeploymentRoleKindDecode,
			manufacturer: nodefeature.ManufacturerNVIDIA, want: true,
		},
		{
			// Server 角色不属于拆分角色，不存在配对对象。
			name: "a server role under any of them", router: workercore.ModelDeploymentRouterLLMD,
			engine:       workercore.ModelDeploymentEngineVLLM,
			kind:         workercore.ModelDeploymentRoleKindServer,
			manufacturer: nodefeature.ManufacturerNVIDIA,
		},
		{
			// Ascend 上走 picker 时会携带 decode proxy，由此中继该连接器所需的握手信息。
			name:         "on Ascend under the picker",
			router:       workercore.ModelDeploymentRouterLLMD,
			engine:       workercore.ModelDeploymentEngineVLLM,
			kind:         workercore.ModelDeploymentRoleKindPrefill,
			manufacturer: nodefeature.ManufacturerAscend, want: true,
		},
		{
			// vLLM router 自行驱动配对，其握手协议会被 Ascend 连接器拒绝，因此保持关闭。
			name:         "on Ascend under the vllm router",
			router:       workercore.ModelDeploymentRouterVLLM,
			engine:       workercore.ModelDeploymentEngineVLLM,
			kind:         workercore.ModelDeploymentRoleKindPrefill,
			manufacturer: nodefeature.ManufacturerAscend,
		},
		{
			// SGLang 使用自身的 bootstrap 注册表，而非 vLLM-Ascend 的 MooncakeConnectorV1，
			// 因此在 Ascend 上配合 sglang-gateway 时与 NVIDIA/ROCm 表现一致。
			name:         "SGLang on Ascend under the gateway",
			router:       workercore.ModelDeploymentRouterSGLang,
			engine:       workercore.ModelDeploymentEngineSGLang,
			kind:         workercore.ModelDeploymentRoleKindPrefill,
			manufacturer: nodefeature.ManufacturerAscend, want: true,
		},
		{
			// 仅声明单边角色时不开启直传通路，前方 router 已将其按未拆分形态转发。
			name: "a lone prefill under the vllm router", lone: true,
			router:       workercore.ModelDeploymentRouterVLLM,
			engine:       workercore.ModelDeploymentEngineVLLM,
			kind:         workercore.ModelDeploymentRoleKindPrefill,
			manufacturer: nodefeature.ManufacturerNVIDIA,
		},
		{
			name: "a lone decode under the picker", lone: true,
			router:       workercore.ModelDeploymentRouterLLMD,
			engine:       workercore.ModelDeploymentEngineVLLM,
			kind:         workercore.ModelDeploymentRoleKindDecode,
			manufacturer: nodefeature.ManufacturerNVIDIA,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			md := connectorPredicateDeployment(c.router, c.engine, c.kind)
			if other, half := complementaryRole(c.kind); half && !c.lone {
				// 该行验证的是成对角色，因此补齐互补的另一半角色。
				md.Spec.Roles = append(md.Spec.Roles, other)
			}
			assert.Equal(t, c.want,
				modelDeploymentUsesKVTransfer(md, &md.Spec.Roles[0], c.manufacturer))
		})
	}

	unrouted := connectorPredicateDeployment(workercore.ModelDeploymentRouterLLMD,
		workercore.ModelDeploymentEngineVLLM, workercore.ModelDeploymentRoleKindPrefill)
	unrouted.Spec.Router = nil
	assert.False(t, modelDeploymentUsesKVTransfer(
		unrouted, &unrouted.Spec.Roles[0], nodefeature.ManufacturerNVIDIA,
	),
		"a half with nothing routing between the halves has nobody to pair with either")
}

func TestModelDeploymentConnector_RoutedPairWithoutKVCacheUsesKVTransferOnly(t *testing.T) {
	md := routedModelDeployment(func(md *workercore.ModelDeployment) {
		md.Spec.KVCache = nil
	})
	cli := newModelDeploymentClient(md, newRenderInstanceType())

	_, err := reconcileModelDeployment(t, cli)
	require.NoError(t, err)

	byRole := map[string]string{}
	for _, pod := range replicaPods(t, cli) {
		container := pod.Spec.Containers[0]
		at := slices.Index(container.Command, "--kv-transfer-config")
		require.GreaterOrEqual(t, at, 0, "%s carries no direct transfer configuration", pod.Name)
		byRole[modelDeploymentPodRole(&pod)] = container.Command[at+1]
		for _, env := range container.Env {
			assert.NotEqual(t, "MOONCAKE_CONFIG_PATH", env.Name,
				"a deployment without kvCache must not receive shared-store configuration")
		}
	}

	require.Len(t, byRole, 2)
	assert.JSONEq(t, `{
		"kv_connector":"MooncakeConnector","kv_role":"kv_producer",
		"kv_connector_extra_config":{"mooncake_protocol":"tcp"}
	}`, byRole["prefill"])
	assert.JSONEq(t, `{
		"kv_connector":"MooncakeConnector","kv_role":"kv_consumer",
		"kv_connector_extra_config":{"mooncake_protocol":"tcp"}
	}`, byRole["decode"])
}
