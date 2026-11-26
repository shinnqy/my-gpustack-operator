package inject

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
)

// envNames 返回按顺序排列的渲染环境名称。
func envNames(env []core.EnvVar) []string {
	out := make([]string, 0, len(env))
	for i := range env {
		out = append(out, env[i].Name)
	}
	return out
}

// envValue 按名称返回渲染后的变量。
func envValue(t *testing.T, env []core.EnvVar, name string) core.EnvVar {
	t.Helper()

	for i := range env {
		if env[i].Name == name {
			return env[i]
		}
	}
	require.Failf(t, "variable not rendered", "%q is not among %v", name, envNames(env))
	return core.EnvVar{}
}

// TestRender_VLLMFamilyVehicleIsAFile 涵盖了那些接收文件的引擎的整个渲染过程，每行对应一个角色。它声明的是
// 最终的结果，而非构建该结果的调用过程。
func TestRender_VLLMFamilyVehicleIsAFile(t *testing.T) {
	// wantConnector 因推理引擎而异，这就是将其作为数据传递的意义所在：这两种推理引擎共享同一个渲染器和
	// 车辆，但不共享连接器注册表；此处仅有一个预期值即可通过，而其中一种则可能无法启动。参见
	// vllmConnectorFor。
	testCases := []struct {
		name          string
		engine        Engine
		role          Role
		wantKVRole    string
		wantConnector string
	}{
		{name: "vllm, no role", engine: EngineVLLM, role: RoleNone, wantKVRole: "kv_both", wantConnector: "MooncakeStoreConnector"},
		{name: "vllm, prefill", engine: EngineVLLM, role: RolePrefill, wantKVRole: "kv_producer", wantConnector: "MooncakeStoreConnector"},
		{name: "vllm, decode", engine: EngineVLLM, role: RoleDecode, wantKVRole: "kv_consumer", wantConnector: "MooncakeStoreConnector"},
		{name: "vllm-ascend, no role", engine: EngineVLLMAscend, role: RoleNone, wantKVRole: "kv_both", wantConnector: "AscendStoreConnector"},
		{name: "vllm-ascend, prefill", engine: EngineVLLMAscend, role: RolePrefill, wantKVRole: "kv_producer", wantConnector: "AscendStoreConnector"},
		{name: "vllm-ascend, decode", engine: EngineVLLMAscend, role: RoleDecode, wantKVRole: "kv_consumer", wantConnector: "AscendStoreConnector"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := Render(Input{
				Engine: tc.engine, Role: tc.role, Connection: testConnectionFor(tc.engine),
			})
			require.NoError(t, err)

			assert.Equal(t, []core.EnvVar{
				{Name: "MOONCAKE_CONFIG_PATH", Value: "/etc/gpustack/kvcache/mooncake.json"},
			}, result.Env, "the variable names the file; the configuration is never in it")

			require.Len(t, result.Args, 2)
			assert.Equal(t, "--kv-transfer-config", result.Args[0])
			assert.JSONEq(t,
				`{"kv_connector":"`+tc.wantConnector+`","kv_role":"`+tc.wantKVRole+`"}`,
				result.Args[1])

			require.Len(t, result.Volumes, 1)
			volume := result.Volumes[0]
			require.NotNil(t, volume.DownwardAPI,
				"the file is projected from the Pod's own annotation, so the webhook creates no object")
			require.Len(t, volume.DownwardAPI.Items, 1)
			assert.Equal(t, "mooncake.json", volume.DownwardAPI.Items[0].Path)
			require.NotNil(t, volume.DownwardAPI.Items[0].FieldRef)
			assert.Equal(t, "metadata.annotations['kvcache.gpustack.ai/client-config']",
				volume.DownwardAPI.Items[0].FieldRef.FieldPath)

			assert.Equal(t, []core.VolumeMount{
				{Name: volume.Name, MountPath: "/etc/gpustack/kvcache", ReadOnly: true},
			}, result.VolumeMounts)

			assert.Contains(t, result.PodAnnotations, "kvcache.gpustack.ai/client-config",
				"the projection and the annotation it reads are two halves of one thing")
		})
	}
}

func TestRender_SGLangCarriesTheResolvedConnection(t *testing.T) {
	conn := testConnection()
	result, err := Render(Input{Engine: EngineSGLang, Connection: conn})
	require.NoError(t, err)

	assert.Equal(t, conn.MasterAddress, envValue(t, result.Env, "MOONCAKE_MASTER").Value)
	assert.Equal(t, conn.Protocol, envValue(t, result.Env, "MOONCAKE_PROTOCOL").Value)
	assert.Equal(t, "P2PHANDSHAKE", envValue(t, result.Env, "MOONCAKE_TE_META_DATA_SERVER").Value)

	device := envValue(t, result.Env, "MOONCAKE_DEVICE")
	assert.Equal(t, "", device.Value)
	assert.Nil(t, device.ValueFrom, "empty and written, not omitted")
}

// TestRender_TenantGoesToEveryEngineThatReadsOne 记录了每个推理引擎在复用领域所使用的车辆。vLLM 系列从文件
// 中读取 tenant_id；SGLang 则读取 MOONCAKE_TENANT_ID。
func TestRender_TenantGoesToEveryEngineThatReadsOne(t *testing.T) {
	const domain = "team-a-chat"

	for _, engine := range Engines() {
		t.Run(string(engine), func(t *testing.T) {
			result, err := Render(Input{
				Engine: engine, Domain: domain, Connection: testConnectionFor(engine),
			})
			require.NoError(t, err)

			assert.True(t, result.TenantInjected, "the renderer reports the action it took")
			switch engine {
			case EngineSGLang:
				assert.Equal(t, domain, envValue(t, result.Env, "MOONCAKE_TENANT_ID").Value,
					"the reuse domain is what the engine is told to write under")
			default:
				assert.Equal(t, domain, renderedConfig(t, Input{
					Engine: engine, Domain: domain, Connection: testConnectionFor(engine),
				})["tenant_id"], "the reuse domain is written into the file the engine reads")
			}
		})
	}
}

func TestRender_VLLMKVTransferComposesPointToPointAndStore(t *testing.T) {
	for _, tc := range []struct {
		name      string
		role      Role
		pointRole string
		storeRole string
	}{
		{name: "prefill", role: RolePrefill, pointRole: "kv_producer", storeRole: "kv_both"},
		{name: "decode", role: RoleDecode, pointRole: "kv_consumer", storeRole: "kv_consumer"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := Render(Input{
				Engine: EngineVLLM, Role: tc.role, Domain: "team-a-chat",
				Connection: testConnection(), KVTransfer: true,
			})
			require.NoError(t, err)
			require.Len(t, result.Args, 2)
			assert.Equal(t, vllmTransferConfigArg, result.Args[0])
			assert.JSONEq(t, fmt.Sprintf(`{
				"kv_connector":"MultiConnector",
				"kv_role":%q,
				"kv_connector_extra_config":{"connectors":[
					{"kv_connector":"MooncakeConnector","kv_role":%q,
					 "kv_connector_extra_config":{"mooncake_protocol":"tcp"}},
					{"kv_connector":"MooncakeStoreConnector","kv_role":%q}
				]}
			}`, tc.pointRole, tc.pointRole, tc.storeRole), result.Args[1])
			assert.True(t, result.KVTransfer)
			for _, port := range result.Ports {
				assert.Empty(t, utilvalidation.IsValidPortName(port.Name),
					"rendered port %q must be accepted by the Kubernetes API", port.Name)
			}
		})
	}
}

func TestRender_VLLMKVTransferWithoutStore(t *testing.T) {
	result, err := Render(Input{
		Engine: EngineVLLM, Role: RolePrefill, KVTransfer: true,
	})
	require.NoError(t, err)
	require.Len(t, result.Args, 2)
	assert.Equal(t, vllmTransferConfigArg, result.Args[0])
	assert.JSONEq(t, `{
		"kv_connector":"MooncakeConnector",
		"kv_role":"kv_producer",
		"kv_connector_extra_config":{"mooncake_protocol":"tcp"}
	}`, result.Args[1])
	assert.True(t, result.KVTransfer)
	assert.False(t, result.TenantInjected)
	assert.NotContains(t, envNames(result.Env), vllmConfigPathEnv)
	assert.Empty(t, result.Volumes)
	assert.Empty(t, result.VolumeMounts)
	assert.Empty(t, result.PodAnnotations)
	assert.Contains(t, envNames(result.Env), "VLLM_MOONCAKE_BOOTSTRAP_PORT")
}
