package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	ctrlrecord "k8s.io/client-go/tools/record"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"

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

func TestRenderModelDeploymentRoleService_ExternalDPPublishesEveryRankOfAQualifiedGroup(t *testing.T) {
	ctx := t.Context()
	md := newRenderDeployment()
	md.Spec.KVCache = nil
	role := &md.Spec.Roles[0]
	role.ReplicaSize = 2
	role.ExtraArgs = []string{"--data-parallel-size", "2", "--data-parallel-rank", "$(GPUSTACK_MEMBER_INDEX)"}
	cli := newModelDeploymentClient(md, newRenderInstanceType())
	r := &ModelDeploymentReconciler{
		Client: cli, APIReader: cli, Recorder: ctrlrecord.NewFakeRecorder(64),
		// 返回与本次转发探测绑定的请求号，让控制器沿正常路径判定这组成员可服务。
		groupForwardFetch: func(_ context.Context, _ string, body []byte) ([]byte, error) {
			var request struct {
				ID string `json:"request_id"`
			}
			if err := json.Unmarshal(body, &request); err != nil {
				return nil, err
			}
			return fmt.Appendf(nil, `{"id":"cmpl-%s","choices":[{"finish_reason":"length"}]}`, request.ID), nil
		},
	}
	_, err := reconcileModelDeploymentWith(t, r)
	require.NoError(t, err)
	pods := replicaPods(t, cli)
	require.Len(t, pods, 4)
	for i := range pods {
		pods[i].Status.PodIP = "10.0.0." + strconv.Itoa(i+1)
		pods[i].Status.Conditions = []core.PodCondition{{Type: core.PodReady, Status: core.ConditionTrue}}
		require.NoError(t, cli.Status().Update(ctx, &pods[i]))
	}
	_, err = reconcileModelDeploymentWith(t, r)
	require.NoError(t, err)

	svc := new(core.Service)
	require.NoError(t, cli.Get(ctx, ctrlcli.ObjectKey{Namespace: md.Namespace, Name: md.Name + "-" + role.Name}, svc))
	require.Equal(t, modelDeploymentEndpointEligibleValue, svc.Spec.Selector[modelDeploymentLabelKeyEndpointEligible])
	require.NotContains(t, svc.Spec.Selector, modelDeploymentMemberIndexLabel)
	selector := labels.SelectorFromSet(svc.Spec.Selector)
	pods = replicaPods(t, cli)
	require.Len(t, pods, 4)
	for _, pod := range pods {
		require.True(t, selector.Matches(labels.Set(pod.Labels)), pod.Name)
	}

	// 同组一名成员失去就绪状态后，整组退出角色 Service，另一份副本的两个 rank 仍可接收请求。
	member := new(core.Pod)
	require.NoError(t, cli.Get(ctx, ctrlcli.ObjectKeyFromObject(&pods[0]), member))
	member.Status.Conditions[0].Status = core.ConditionFalse
	require.NoError(t, cli.Status().Update(ctx, member))
	_, err = reconcileModelDeploymentWith(t, r)
	require.NoError(t, err)
	require.NoError(t, cli.Get(ctx, ctrlcli.ObjectKeyFromObject(svc), svc))
	selector = labels.SelectorFromSet(svc.Spec.Selector)
	pods = replicaPods(t, cli)
	require.Len(t, pods, 4)
	for _, pod := range pods {
		otherGroup := pod.Labels[modelDeploymentReplicaOrdinalLabel] != member.Labels[modelDeploymentReplicaOrdinalLabel]
		assert.Equal(t, otherGroup, selector.Matches(labels.Set(pod.Labels)), pod.Name)
	}
}

func TestRenderModelDeploymentRoleService_EligibilityInLeaderAndMixedModes(t *testing.T) {
	md := newRenderDeployment()
	role := &md.Spec.Roles[0]
	role.ReplicaSize = 2
	leader := renderModelDeploymentRoleService(md, role, nil, nil, false)
	assert.Equal(t, "0", leader.Spec.Selector[modelDeploymentMemberIndexLabel])
	assert.Equal(t, modelDeploymentEndpointEligibleValue, leader.Spec.Selector[modelDeploymentLabelKeyEndpointEligible])

	mixed := renderModelDeploymentRoleService(md, role, nil, nil, true)
	assert.Equal(t, modelDeploymentAPIAnsweringValue, mixed.Spec.Selector[modelDeploymentLabelKeyAPIAnswering])
	assert.Equal(t, modelDeploymentEndpointEligibleValue, mixed.Annotations[modelDeploymentEligibilityActiveAnnotation])
	assert.NotContains(t, mixed.Spec.Selector, modelDeploymentLabelKeyEndpointEligible)
	assert.NotContains(t, mixed.Spec.Selector, modelDeploymentMemberIndexLabel)
}
