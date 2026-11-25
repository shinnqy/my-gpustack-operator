package worker

import (
	"context"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

// TestRenderModelDeploymentService_TargetFollowsTheEnginesPort 验证角色通过命令行参数指定引擎监听端口时，
// 容器端口、Service targetPort 与探针一致跟随该端口，而 Service 对外端口与发布地址保持不变。
func TestRenderModelDeploymentService_TargetFollowsTheEnginesPort(t *testing.T) {
	testCases := []struct {
		name           string
		engine         string
		extraArgs      []string
		command        []string
		ports          []workercore.ModelDeploymentPort
		directDecode   bool
		wantPort       int32
		wantTargetPort int32
		wantEnginePort int32
		wantProbes     bool
	}{
		{
			name:     "no_port_flag_leaves_the_default",
			wantPort: 8000, wantTargetPort: 8000, wantEnginePort: 8000, wantProbes: true,
		},
		{
			name:      "a_flag_sharing_a_stem_moves_nothing",
			extraArgs: []string{"--pooler-config", "{}"},
			wantPort:  8000, wantTargetPort: 8000, wantEnginePort: 8000, wantProbes: true,
		},
		{
			name:      "an_explicit_port_is_followed",
			extraArgs: []string{"--port", "9100"},
			wantPort:  8000, wantTargetPort: 9100, wantEnginePort: 9100, wantProbes: true,
		},
		{
			name:      "an_inline_port_is_followed",
			extraArgs: []string{"--port=9100"},
			wantPort:  8000, wantTargetPort: 9100, wantEnginePort: 9100, wantProbes: true,
		},
		{
			name:      "an_abbreviated_port_is_followed",
			extraArgs: []string{"--por", "9100"},
			wantPort:  8000, wantTargetPort: 9100, wantEnginePort: 9100, wantProbes: true,
		},
		{
			name:      "an_abbreviated_inline_port_is_followed",
			extraArgs: []string{"--por=9100"},
			wantPort:  8000, wantTargetPort: 9100, wantEnginePort: 9100, wantProbes: true,
		},
		{
			name:      "the_last_spelling_wins",
			extraArgs: []string{"--port", "9000", "--por", "9100"},
			wantPort:  8000, wantTargetPort: 9100, wantEnginePort: 9100, wantProbes: true,
		},
		{
			// "--po" 在 vLLM 下因存在 --pooler-config 而有歧义，在 SGLang 下可唯一解析。
			name: "sglang_reads_its_own_abbreviation", engine: workercore.ModelDeploymentEngineSGLang,
			extraArgs: []string{"--po", "9100"},
			wantPort:  8000, wantTargetPort: 9100, wantEnginePort: 9100, wantProbes: true,
		},
		{
			name:      "a_declared_port_that_agrees_is_gated_on_it",
			ports:     []workercore.ModelDeploymentPort{{Port: 9100, Protocol: core.ProtocolTCP}},
			extraArgs: []string{"--port", "9100"},
			wantPort:  9100, wantTargetPort: 9100, wantEnginePort: 9100, wantProbes: true,
		},
		{
			// 接管模式角色不渲染探针，但 Service 仍需转发到引擎监听端口。
			name:     "a_take_over_command_is_followed",
			command:  []string{"vllm", "serve", "m", "--port", "9100"},
			wantPort: 8000, wantTargetPort: 9100, wantEnginePort: 9100,
		},
		{
			// direct-decode 形态由路由代理占用对外端口，角色的 --port 指定后方引擎端口。
			name:         "a_direct_decoder_keeps_the_proxy_in_front",
			extraArgs:    []string{"--port", "9100"},
			directDecode: true,
			wantPort:     8000, wantTargetPort: 8000, wantEnginePort: 9100, wantProbes: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			md := newRenderDeployment(func(md *workercore.ModelDeployment) {
				if tc.engine != "" {
					md.Spec.Engine.Name = tc.engine
				}
				md.Spec.Roles[0].ExtraArgs = tc.extraArgs
				md.Spec.Roles[0].Command = tc.command
				md.Spec.Roles[0].Ports = tc.ports
				if tc.directDecode {
					md.Spec.Router = &workercore.ModelDeploymentRouter{Name: workercore.ModelDeploymentRouterLLMD}
					md.Spec.Roles[0].Kind = workercore.ModelDeploymentRoleKindDecode
				}
			})
			in := ModelDeploymentRenderInput{
				Deployment: md, Role: &md.Spec.Roles[0], InstanceType: newRenderInstanceType(),
				NativeSidecar: true,
			}
			if tc.directDecode {
				in.Connector = ModelDeploymentConnectorRender{
					Args: []string{"--kv-transfer-config", `{}`}, KVTransfer: true, RoutingSidecar: true,
				}
			}
			pod, err := renderModelDeploymentPod(context.Background(), in)
			require.NoError(t, err)

			svcs := renderModelDeploymentServices(md, nil, nil)
			require.Len(t, svcs, 2)
			for _, svc := range svcs {
				require.Len(t, svc.Spec.Ports, 1, svc.Name)
				assert.Equal(t, tc.wantPort, svc.Spec.Ports[0].Port, svc.Name)
				assert.Equal(t, tc.wantTargetPort, svc.Spec.Ports[0].TargetPort.IntVal, svc.Name)
			}
			assert.Equal(t, "http://qwen.team-a.svc:"+strconv.Itoa(int(tc.wantPort)), modelDeploymentEndpoint(md))

			main := pod.Spec.Containers[len(pod.Spec.Containers)-1]
			require.Equal(t, modelDeploymentMainContainerName, main.Name)
			assert.Equal(t, tc.wantEnginePort, main.Ports[0].ContainerPort)
			served := false
			for _, c := range append(slices.Clone(pod.Spec.InitContainers), pod.Spec.Containers...) {
				for _, p := range c.Ports {
					served = served || p.ContainerPort == tc.wantTargetPort
				}
			}
			assert.True(t, served, "Pod 中应有容器开放 Service 指向的目标端口")

			if !tc.wantProbes {
				assert.Nil(t, main.StartupProbe)
				assert.Nil(t, main.ReadinessProbe)
				assert.Nil(t, main.LivenessProbe)

				return
			}
			for _, probe := range []*core.Probe{main.StartupProbe, main.ReadinessProbe, main.LivenessProbe} {
				require.NotNil(t, probe)
				assert.Equal(t, tc.wantTargetPort, probe.HTTPGet.Port.IntVal,
					"探针应检查 Service 转发的目标端口")
			}
		})
	}
}
