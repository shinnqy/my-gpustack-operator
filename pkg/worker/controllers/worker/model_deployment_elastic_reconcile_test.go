package worker

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clitesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	ctrlinterceptor "sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/deviceplugin"
	"gpustack.ai/gpustack/pkg/kubeclients/kubernetes/scheme"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/utils/json"
	"gpustack.ai/gpustack/pkg/worker/elasticengine"
)

// 测试提供传输层响应和 Kubernetes 对象。成员身份、实际宽度和操作结果均由生产观测器、原生客户端、
// 记录存储和调谐入口计算。
type elasticConvergenceFixture struct {
	md           *workercore.ModelDeployment
	reconciler   *ModelDeploymentReconciler
	nativeWidth  atomic.Int32
	scaleCalls   atomic.Int32
	forwardCalls atomic.Int32
	rayMalformed atomic.Bool
	forwardsFail atomic.Bool
	scaleStatus  atomic.Int32
}

func newElasticConvergenceFixture(t *testing.T) *elasticConvergenceFixture {
	t.Helper()
	md := newRenderDeployment()
	md.Generation = 1
	md.Spec.KVCache = nil
	md.Spec.Roles[0].Replicas = 1
	md.Spec.Roles[0].Resources = &workercore.ModelDeploymentRoleResources{Accelerator: resource.NewQuantity(1, resource.DecimalSI)}
	md.Spec.Roles[0].ElasticEP = &workercore.ModelDeploymentRoleElasticEP{Width: 2}
	md.ResourceVersion = "1"
	cli := ctrlfake.NewClientBuilder().WithScheme(scheme.Scheme).
		WithObjectTracker(clitesting.NewObjectTracker(scheme.Scheme, scheme.Codecs.UniversalDecoder())).
		WithObjects(md, newRenderInstanceType()).
		WithStatusSubresource(&workercore.ModelDeployment{}, &workercore.Devices{}).
		WithIndex(&core.Pod{}, "spec.nodeName", func(obj ctrlcli.Object) []string { return []string{obj.(*core.Pod).Spec.NodeName} }).
		WithIndex(&core.Pod{}, modelDeploymentDrainIndexPodUID, func(obj ctrlcli.Object) []string { return []string{string(obj.GetUID())} }).Build()
	f := &elasticConvergenceFixture{md: md, reconciler: &ModelDeploymentReconciler{Client: cli, APIReader: cli}}
	f.nativeWidth.Store(2)
	f.scaleStatus.Store(http.StatusOK)
	_, err := f.reconciler.convergeModelDeployment(context.Background(), md)
	require.NoError(t, err)
	f.startMembers(t)
	observer := newModelDeploymentElasticObserver(cli, cli, nil, nil)
	observer.exec = func(ctx context.Context, head *core.Pod, container string, argv []string) (string, error) {
		if f.rayMalformed.Load() {
			return `{}`, nil
		}
		pods, err := f.reconciler.elasticLiveMembers(ctx, md)
		if err != nil {
			return "", err
		}
		nodes := []elasticRayNodeWire{}
		actors := []elasticRayActorWire{}
		groups := []elasticRayPGWire{}
		for i := range pods {
			pod := &pods[i]
			id := fmt.Sprintf("%02x", i+1)
			node := elasticRayNodeWire{
				NodeIDHex: ptr.To(id), Alive: ptr.To(true), NodeManagerAddress: "10.0.0.1",
				Labels:    map[string]string{modelDeploymentElasticPodUIDLabelKey: string(pod.UID)},
				Resources: map[string]string{},
			}
			if modelDeploymentPodRole(pod) != modelDeploymentElasticHeadName(md) {
				node.Resources["GPU"] = "1"
				ordinal, valid := modelDeploymentPodOrdinal(pod)
				if !valid {
					return "", fmt.Errorf("fixture member has no ordinal")
				}
				if ordinal < int(f.nativeWidth.Load()) {
					pg := fmt.Sprintf("b%02x", ordinal)
					groups = append(groups, elasticRayPGWire{
						PlacementGroupIDHex: ptr.To(pg), Name: ptr.To(fmt.Sprintf("dp_rank_%d", ordinal)),
						State: ptr.To(elasticRayPGCreated), StateName: "CREATED", Bundles: []elasticRayBundleWire{},
					})
					actors = append(actors, elasticRayActorWire{
						ActorIDHex: ptr.To(fmt.Sprintf("a%02x", ordinal)), State: ptr.To(elasticRayActorAlive),
						StateName: "ALIVE", ClassName: modelDeploymentElasticActorClassEngineCore, NodeIDHex: id, PlacementGroupIDHex: pg,
					})
				}
			}
			nodes = append(nodes, node)
		}
		raw, err := json.Marshal(map[string]any{
			"schema_version": 1, "ray_version": "2.56.1", "gcs_address": modelDeploymentElasticDefaultGCSAddress,
			"collected_at_ms": 1790847987000, "nodes": nodes, "actors": actors,
			"placement_groups": groups, "errors": []elasticRayErrorWire{},
		})
		return string(raw), err
	}
	f.reconciler.elasticObserver = observer
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch req.URL.Path {
		case "/v1/completions":
			if req.Header.Get("X-data-parallel-rank") == "-1" {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = fmt.Fprintf(w, `{"error":{"message":"data_parallel_rank -1 is out of range [0, %d).","type":"BadRequestError","param":null,"code":400}}`, f.nativeWidth.Load())
				return
			}
			f.forwardCalls.Add(1)
			if f.forwardsFail.Load() {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			_, _ = w.Write([]byte(`{"id":"cmpl-1","object":"text_completion","choices":[{"index":0,"text":" "}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`))
		case "/is_scaling_elastic_ep":
			_, _ = w.Write([]byte(`{"is_scaling_elastic_ep":false}`))
		case "/scale_elastic_ep":
			f.scaleCalls.Add(1)
			var payload struct {
				Target int `json:"new_data_parallel_size"`
			}
			if json.NewDecoder(req.Body).Decode(&payload) != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			op, err := newElasticOperationStore(cli).Read(req.Context(), md.Namespace, md.Name, md.UID)
			if err != nil || op.State != elasticStateCommandSent || !op.CommandSent || op.Width.Target != payload.Target || op.Generation != md.Generation {
				t.Errorf("scale requested without a matching persisted intent: op=%+v, err=%v", op, err)
				w.WriteHeader(http.StatusConflict)
				return
			}
			code := int(f.scaleStatus.Load())
			if code == http.StatusOK {
				f.nativeWidth.Store(int32(payload.Target))
			}
			w.WriteHeader(code)
			if code == http.StatusOK {
				_, _ = w.Write([]byte(`{"message":"accepted"}`))
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	f.reconciler.elasticClient = func(*core.Pod) (*elasticengine.Client, error) { return elasticengine.New(server.URL, time.Second) }
	return f
}

func (f *elasticConvergenceFixture) startMembers(t *testing.T) {
	t.Helper()
	cli := f.reconciler.Client
	pods := new(core.PodList)
	require.NoError(t, cli.List(context.Background(), pods, ctrlcli.InNamespace(f.md.Namespace)))
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.UID != "" {
			continue
		}
		pod.UID = types.UID("runtime-" + pod.Name)
		require.NoError(t, cli.Update(context.Background(), pod))
		pod.Status.PodIP = fmt.Sprintf("10.0.0.%d", i+1)
		pod.Status.ContainerStatuses = []core.ContainerStatus{{Name: modelDeploymentMainContainerName, ContainerID: "container-" + string(pod.UID), State: core.ContainerState{Running: &core.ContainerStateRunning{}}}}
		require.NoError(t, cli.Status().Update(context.Background(), pod))
	}
}

func (f *elasticConvergenceFixture) observation(t *testing.T) modelDeploymentElasticStatus {
	t.Helper()
	cm := new(core.ConfigMap)
	require.NoError(t, f.reconciler.Client.Get(context.Background(), ctrlcli.ObjectKey{Namespace: f.md.Namespace, Name: modelDeploymentElasticObservationName(f.md)}, cm))
	var status modelDeploymentElasticStatus
	require.NoError(t, json.Unmarshal([]byte(cm.Data["observation.json"]), &status))
	return status
}

func TestElasticConvergenceObservesNativeWorld(t *testing.T) {
	cases := []struct {
		name                        string
		malformedRay, failedForward bool
		effective                   bool
	}{
		{name: "native ranks and every forward agree", effective: true},
		{name: "malformed Ray document holds", malformedRay: true},
		{name: "unserved native rank holds", failedForward: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newElasticConvergenceFixture(t)
			f.rayMalformed.Store(tc.malformedRay)
			f.forwardsFail.Store(tc.failedForward)
			_, err := f.reconciler.convergeModelDeployment(context.Background(), f.md)
			require.NoError(t, err)
			status := f.observation(t)
			require.Equal(t, tc.effective, status.Effective.Known, status.Reason)
			if tc.effective {
				require.Equal(t, 2, status.Effective.Value)
				require.GreaterOrEqual(t, f.forwardCalls.Load(), int32(2))
			}
			require.Zero(t, f.scaleCalls.Load())
			pods := new(core.PodList)
			require.NoError(t, f.reconciler.Client.List(context.Background(), pods))
			require.Len(t, pods.Items, 3)
		})
	}
}

func TestElasticConvergenceResumesSentRecordWithoutReplay(t *testing.T) {
	for _, state := range []elasticOperationState{elasticStateCommandSent, elasticStateAwaitingWidth, elasticStateNativeDone} {
		t.Run(string(state), func(t *testing.T) {
			f := newElasticConvergenceFixture(t)
			f.md.Spec.Roles[0].ElasticEP.Width = 4
			f.md.Generation++
			require.NoError(t, f.reconciler.Client.Update(context.Background(), f.md))
			_, err := f.reconciler.convergeModelDeployment(context.Background(), f.md)
			require.NoError(t, err)
			f.startMembers(t)
			pods, err := f.reconciler.elasticLiveMembers(context.Background(), f.md)
			require.NoError(t, err)
			op := &elasticOperation{Name: f.md.Name, Namespace: f.md.Namespace, DeploymentUID: f.md.UID, Generation: f.md.Generation, Width: elasticWidth{Old: 2, Target: 4}, State: state, CommandSent: true, CommandIntent: "scale to 4"}
			for i := range pods {
				pod := &pods[i]
				id := elasticCapturedIdentity{Name: pod.Name, UID: pod.UID}
				if modelDeploymentPodRole(pod) == modelDeploymentElasticHeadName(f.md) {
					op.Head = id
				} else {
					op.Members = append(op.Members, id)
					if modelDeploymentOrdinalOrFloor(pod) == 0 {
						op.Master = id
					}
				}
			}
			_, err = newElasticOperationStore(f.reconciler.Client).Create(context.Background(), op)
			require.NoError(t, err)
			for range 2 {
				_, err = f.reconciler.convergeModelDeployment(context.Background(), f.md)
				require.NoError(t, err)
			}
			require.Zero(t, f.scaleCalls.Load(), "a sent record cannot repeat an ambiguous mutation")
			f.nativeWidth.Store(4)
			_, err = f.reconciler.convergeModelDeployment(context.Background(), f.md)
			require.NoError(t, err)
			status := f.observation(t)
			require.True(t, status.Effective.Known, status.Reason)
			require.Equal(t, 4, status.StableWidth)
			require.Equal(t, elasticStateCompleted, status.State)
			after, err := f.reconciler.elasticLiveMembers(context.Background(), f.md)
			require.NoError(t, err)
			require.True(t, slices.EqualFunc(pods, after, func(a, b core.Pod) bool { return a.UID == b.UID && a.Name == b.Name }))
			require.Zero(t, f.scaleCalls.Load())
		})
	}
}

func (f *elasticConvergenceFixture) admitMembers(t *testing.T) {
	t.Helper()
	cli := f.reconciler.Client
	pods := new(core.PodList)
	require.NoError(t, cli.List(context.Background(), pods, ctrlcli.InNamespace(f.md.Namespace)))
	for i := range pods.Items {
		pod := &pods.Items[i]
		if modelDeploymentPodRole(pod) != "server" || pod.Spec.NodeName != "" {
			continue
		}
		ordinal, valid := modelDeploymentPodOrdinal(pod)
		require.True(t, valid)
		pod.Spec.NodeName = fmt.Sprintf("node-%d", ordinal)
		cardID := "GPU-" + pod.Spec.NodeName
		devices := &workercore.Devices{
			ObjectMeta: metav1.ObjectMeta{Name: pod.Spec.NodeName, UID: types.UID("devices-" + pod.Spec.NodeName)},
			Spec: workercore.DevicesSpec{Groups: []workercore.DevicesGroup{{
				ID: "gpu-0", Manufacturer: "NVIDIA", Name: "H20",
				Accelerators: []workercore.Accelerator{{ID: cardID, Index: 0}},
			}}},
		}
		allocation := deviceplugin.ContainerAllocation{Devices: workercore.DevicesStatus{
			Groups: []workercore.DevicesAllocationGroup{{
				ID: "gpu-0", Manufacturer: "NVIDIA", Accelerators: []workercore.AcceleratorAllocation{{
					ID: cardID, Index: 0, Mode: workercore.DeviceAllocationModeExclusive, Allocated: nodefeature.ResourceMaxUnits,
				}},
			}},
		}}
		raw, err := json.Marshal(deviceplugin.PodAllocations{modelDeploymentMainContainerName: allocation})
		require.NoError(t, err)
		pod.Annotations[deviceplugin.AllocatedAcceleratorAnnoKey] = string(raw)
		require.NoError(t, cli.Update(context.Background(), pod))
		status, _, err := deviceplugin.BuildDesiredStatusStrict(logr.Discard(), devices, &core.PodList{Items: []core.Pod{*pod}})
		require.NoError(t, err)
		devices.Status = status
		require.NoError(t, cli.Create(context.Background(), devices))
		workload := &kueue.Workload{
			ObjectMeta: metav1.ObjectMeta{
				Name: "wl-" + pod.Name, Namespace: f.md.Namespace, UID: types.UID("wl-" + string(pod.UID)),
				OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: pod.Name, UID: pod.UID}},
			},
			Spec: kueue.WorkloadSpec{PodSets: []kueue.PodSet{{
				Name: "server", Count: 1, Template: core.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: pod.Labels}, Spec: *pod.Spec.DeepCopy()},
			}}},
			Status: kueue.WorkloadStatus{
				Conditions: []metav1.Condition{
					{Type: kueue.WorkloadQuotaReserved, Status: metav1.ConditionTrue, Reason: "Reserved", LastTransitionTime: metav1.Now()},
					{Type: kueue.WorkloadAdmitted, Status: metav1.ConditionTrue, Reason: "Admitted", LastTransitionTime: metav1.Now()},
				},
				Admission: &kueue.Admission{ClusterQueue: "gpu-queue", PodSetAssignments: []kueue.PodSetAssignment{{
					Name: "server", Count: ptr.To(int32(1)), ResourceUsage: pod.Spec.Containers[0].Resources.Requests.DeepCopy(),
				}}},
			},
		}
		require.NoError(t, cli.Create(context.Background(), workload))
	}
}

func (f *elasticConvergenceFixture) publishLedgers(t *testing.T) {
	t.Helper()
	devices := new(workercore.DevicesList)
	require.NoError(t, f.reconciler.Client.List(context.Background(), devices))
	for i := range devices.Items {
		dev := &devices.Items[i]
		pods := new(core.PodList)
		require.NoError(t, f.reconciler.Client.List(context.Background(), pods, ctrlcli.MatchingFields{"spec.nodeName": dev.Name}))
		status, _, err := deviceplugin.BuildDesiredStatusStrict(logr.Discard(), dev, pods)
		require.NoError(t, err)
		dev.Status = status
		require.NoError(t, f.reconciler.Client.Status().Update(context.Background(), dev))
	}
}

// 路由观测会沿 Pod、ReplicaSet、Deployment 逐级检查属主，再读取这个 Router 的服务成员列表。
func elasticTestRouterObjects(md *workercore.ModelDeployment) []ctrlcli.Object {
	deployment := &apps.Deployment{ObjectMeta: metav1.ObjectMeta{
		Namespace: md.Namespace, Name: md.Name + "-router", UID: "test-router-deployment",
		OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(md, workercore.SchemeGroupVersion.WithKind("ModelDeployment"))},
	}}
	rs := &apps.ReplicaSet{ObjectMeta: metav1.ObjectMeta{
		Namespace: md.Namespace, Name: "test-router-rs", UID: "test-router-rs",
		OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(deployment, apps.SchemeGroupVersion.WithKind("Deployment"))},
	}}
	pod := &core.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: md.Namespace, Name: "router-zero", UID: "test-router-pod",
			Labels: map[string]string{
				modelDeploymentLabelKeyName:     modelDeploymentLabelValueName,
				modelDeploymentLabelKeyInstance: md.Name,
				modelDeploymentRouterLabelKey:   md.Spec.Router.Name,
			},
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(rs, apps.SchemeGroupVersion.WithKind("ReplicaSet"))},
		},
		Status: core.PodStatus{PodIP: "10.0.9.1"},
	}
	return []ctrlcli.Object{deployment, rs, pod}
}

func TestElasticConvergenceCompletesTwoFourTwo(t *testing.T) {
	for _, trial := range []struct {
		name            string
		generationDrift bool
	}{
		{name: "unchanged generation"},
		{name: "generation drift before accelerator return", generationDrift: true},
	} {
		t.Run(trial.name, func(t *testing.T) {
			f := newElasticConvergenceFixture(t)
			f.admitMembers(t)
			masterBefore := new(core.Pod)
			initial, err := f.reconciler.elasticLiveMembers(context.Background(), f.md)
			require.NoError(t, err)
			for _, pod := range initial {
				if modelDeploymentPodRole(&pod) == "server" && modelDeploymentOrdinalOrFloor(&pod) == 0 {
					masterBefore = pod.DeepCopy()
				}
			}
			require.NotEmpty(t, masterBefore.UID)
			f.md.Spec.Router = &workercore.ModelDeploymentRouter{Name: "llm-d-router"}
			f.md.Spec.Roles[0].ElasticEP.Width = 4
			f.md.Generation++
			require.NoError(t, f.reconciler.Client.Update(context.Background(), f.md))
			_, err = f.reconciler.convergeModelDeployment(context.Background(), f.md)
			require.NoError(t, err)
			f.startMembers(t)
			f.admitMembers(t)
			for range 2 {
				_, err = f.reconciler.convergeModelDeployment(context.Background(), f.md)
				require.NoError(t, err)
			}
			require.Equal(t, 4, f.observation(t).StableWidth, f.observation(t).Reason)
			require.Equal(t, int32(1), f.scaleCalls.Load())
			for _, obj := range elasticTestRouterObjects(f.md) {
				if wanted, ok := obj.(*apps.Deployment); ok {
					actual := new(apps.Deployment)
					require.NoError(t, f.reconciler.Client.Get(context.Background(), ctrlcli.ObjectKeyFromObject(wanted), actual))
					actual.UID = wanted.UID
					require.NoError(t, f.reconciler.Client.Update(context.Background(), actual))
					continue
				}
				require.NoError(t, f.reconciler.Client.Create(context.Background(), obj))
			}
			f.reconciler.servingViewFetch = func(context.Context, string) ([]byte, error) {
				return []byte(`{"router":{"boot_generation":1,"now_ms":1},"registry_revision":1,"workers":[]}`), nil
			}
			collector := newModelDeploymentDrainCollector(f.reconciler.Client, f.reconciler.APIReader, nil, nil)
			collector.exec = func(context.Context, *core.Pod, string, []string) (string, error) {
				return "vllm:num_requests_running\t0\nvllm:num_requests_waiting\t0\n", nil
			}
			f.reconciler.drainReader = collector
			f.md.Spec.Roles[0].ElasticEP.Width = 2
			f.md.Generation++
			require.NoError(t, f.reconciler.Client.Update(context.Background(), f.md))
			// 发出缩容请求，再观察 actor 退出。此刻已发布的账本仍包含两张待归还的卡。
			for range 2 {
				_, err = f.reconciler.convergeModelDeployment(context.Background(), f.md)
				require.NoError(t, err)
			}
			require.Equal(t, int32(2), f.scaleCalls.Load(), f.observation(t).Reason)
			current, err := f.reconciler.elasticLiveMembers(context.Background(), f.md)
			require.NoError(t, err)
			require.Len(t, current, 3, "actor-free captured workers should be deleted; master and CPU head survive")
			require.Equal(t, 4, f.observation(t).StableWidth, "deletion is not a confirmed allocation return")
			if trial.generationDrift {
				f.md.Generation++
				require.NoError(t, f.reconciler.Client.Update(context.Background(), f.md))
				_, err = f.reconciler.convergeModelDeployment(context.Background(), f.md)
				require.NoError(t, err)
				require.Equal(t, int32(2), f.scaleCalls.Load())
				_, err = newElasticOperationStore(f.reconciler.Client).Read(
					context.Background(), f.md.Namespace, f.md.Name, f.md.UID,
				)
				require.NoError(t, err, "unreturned accelerator accounting retains the old intent")
				require.Equal(t, 4, f.observation(t).StableWidth)
			}
			f.publishLedgers(t)
			_, err = f.reconciler.convergeModelDeployment(context.Background(), f.md)
			require.NoError(t, err)
			require.Equal(t, 2, f.observation(t).StableWidth, f.observation(t).Reason)
			require.Equal(t, elasticStateCompleted, f.observation(t).State)
			masterAfter := new(core.Pod)
			require.NoError(t, f.reconciler.Client.Get(context.Background(), ctrlcli.ObjectKeyFromObject(masterBefore), masterAfter))
			require.Equal(t, masterBefore.UID, masterAfter.UID)
			require.Equal(t, masterBefore.Spec, masterAfter.Spec)
			require.Equal(t, "2", masterAfter.Annotations[ModelDeploymentElasticMasterBootWidthAnnotation])
			require.Equal(t, int32(2), f.scaleCalls.Load())
		})
	}
}

func TestElasticDeletionRetainsUnprovenCapturedMember(t *testing.T) {
	for _, failure := range []string{"unknown actor", "held actor", "replacement", "allocation changed", "generation changed", "late update"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			f := newElasticConvergenceFixture(t)
			f.admitMembers(t)
			pods, err := f.reconciler.elasticLiveMembers(ctx, f.md)
			require.NoError(t, err)
			var target *core.Pod
			for i := range pods {
				if modelDeploymentPodRole(&pods[i]) == "server" && modelDeploymentOrdinalOrFloor(&pods[i]) == 1 {
					target = &pods[i]
				}
			}
			require.NotNil(t, target)
			release, err := f.reconciler.captureModelDeploymentElasticRelease(ctx, f.md, []core.Pod{*target})
			require.NoError(t, err)
			op := &elasticOperation{
				DeploymentUID: f.md.UID, Generation: f.md.Generation,
				Workers: []elasticCapturedIdentity{{Name: target.Name, UID: target.UID}}, Release: release,
			}
			ray := modelDeploymentElasticObservation{ActorFree: map[types.UID]modelDeploymentElasticActorFreeFact{
				target.UID: {ActorFree: true},
			}}
			switch failure {
			case "unknown actor":
				delete(ray.ActorFree, target.UID)
			case "held actor":
				ray.ActorFree[target.UID] = modelDeploymentElasticActorFreeFact{}
			case "replacement":
				require.NoError(t, f.reconciler.Client.Delete(ctx, target))
				target.UID = "replacement-uid"
				target.ResourceVersion = ""
				require.NoError(t, f.reconciler.Client.Create(ctx, target))
			case "allocation changed":
				target.Annotations[deviceplugin.AllocatedAcceleratorAnnoKey] = "{}"
				require.NoError(t, f.reconciler.Client.Update(ctx, target))
			case "generation changed":
				f.md.Generation++
				require.NoError(t, f.reconciler.Client.Update(ctx, f.md))
			case "late update":
				f.reconciler.Client = ctrlinterceptor.NewClient(f.reconciler.Client.(ctrlcli.WithWatch), ctrlinterceptor.Funcs{
					Delete: func(ctx context.Context, cli ctrlcli.WithWatch, obj ctrlcli.Object, opts ...ctrlcli.DeleteOption) error {
						options := new(ctrlcli.DeleteOptions).ApplyOptions(opts)
						require.NotNil(t, options.Preconditions)
						require.Equal(t, target.UID, *options.Preconditions.UID)
						require.Equal(t, obj.GetResourceVersion(), *options.Preconditions.ResourceVersion)
						fresh := new(core.Pod)
						require.NoError(t, cli.Get(ctx, ctrlcli.ObjectKeyFromObject(obj), fresh))
						fresh.Labels["concurrent-update"] = "true"
						require.NoError(t, cli.Update(ctx, fresh))
						return cli.Delete(ctx, obj, opts...)
					},
				})
			}
			err = f.reconciler.elasticDeleteCaptured(ctx, f.md, op, ray)
			require.Error(t, err)
			retained := new(core.Pod)
			require.NoError(t, f.reconciler.Client.Get(ctx, ctrlcli.ObjectKeyFromObject(target), retained))
			require.Equal(t, target.UID, retained.UID)
			workloads := new(kueue.WorkloadList)
			require.NoError(t, f.reconciler.Client.List(ctx, workloads))
			require.Len(t, workloads.Items, 2, "retirement cannot delete quota after an unproven Pod deletion")
		})
	}
}

// TestElasticConvergenceRetriesARefusedScaleCommand 验证一次明确拒绝的后续处理。记录保留引擎的错误，
// 按退避周期重发，次数耗尽后把最终拒绝同时写入观测和部署条件。spec 回到已证明的旧宽度后，
// 常规恢复流程仍能处理这份记录。
func TestElasticConvergenceRetriesARefusedScaleCommand(t *testing.T) {
	ctx := context.Background()
	f := newElasticConvergenceFixture(t)
	f.scaleStatus.Store(http.StatusInternalServerError)
	now := time.Now()
	f.reconciler.clock = func() time.Time { return now }

	recordDoc := func(t *testing.T) map[string]any {
		t.Helper()
		record := new(core.ConfigMap)
		require.NoError(t, f.reconciler.Client.Get(ctx, ctrlcli.ObjectKey{
			Namespace: f.md.Namespace, Name: elasticOperationRecordName(f.md.UID),
		}, record))
		var doc map[string]any
		require.NoError(t, json.Unmarshal([]byte(record.Data[elasticOperationDataKey]), &doc))
		return doc
	}
	liveCondition := func(t *testing.T) (status, reason, message string) {
		t.Helper()
		live := new(workercore.ModelDeployment)
		require.NoError(t, f.reconciler.Client.Get(ctx, ctrlcli.ObjectKeyFromObject(f.md), live))
		for _, condition := range live.Status.Conditions {
			if condition.Type == string(ModelDeploymentConditionElasticResize) {
				return string(condition.Status), condition.Reason, condition.Message
			}
		}
		t.Fatal("missing ElasticResize condition")
		return "", "", ""
	}

	f.md.Spec.Roles[0].ElasticEP.Width = 4
	f.md.Generation++
	require.NoError(t, f.reconciler.Client.Update(ctx, f.md))
	_, err := f.reconciler.convergeModelDeployment(ctx, f.md)
	require.NoError(t, err)
	f.startMembers(t)
	f.admitMembers(t)

	// 第一次发送收到明确拒绝。检查记录和观测里都保留引擎的原始原因。
	_, err = f.reconciler.convergeModelDeployment(ctx, f.md)
	require.NoError(t, err)
	require.Equal(t, int32(1), f.scaleCalls.Load(), f.observation(t).Reason)
	doc := recordDoc(t)
	require.Contains(t, doc["scaleLastError"], "status 500",
		"the record must keep the engine's own refusal")
	require.Equal(t, float64(1), doc["scaleAttempts"])
	require.Contains(t, f.observation(t).Reason, "status 500", f.observation(t).Reason)
	require.Equal(t, "CommandSent", doc["state"], "a refused command is not an unreported width")

	// 退避间隔内再次调谐应继续等待，并保留上次拒绝的原因。
	_, err = f.reconciler.convergeModelDeployment(ctx, f.md)
	require.NoError(t, err)
	require.Equal(t, int32(1), f.scaleCalls.Load())
	require.Contains(t, f.observation(t).Reason, "status 500", f.observation(t).Reason)
	status, reason, _ := liveCondition(t)
	require.NotEqual(t, "False", status,
		"a refusal still inside its retry bound is not terminal")
	require.Equal(t, "NoRefusal", reason)

	// 每跨过一个退避间隔允许再试一次，直到次数耗尽。
	for _, step := range []struct {
		advance  time.Duration
		attempts float64
	}{
		{20 * time.Second, 2}, {31 * time.Second, 3}, {61 * time.Second, 4}, {121 * time.Second, 5},
	} {
		now = now.Add(step.advance)
		_, err = f.reconciler.convergeModelDeployment(ctx, f.md)
		require.NoError(t, err)
		doc = recordDoc(t)
		require.Equal(t, step.attempts, doc["scaleAttempts"], f.observation(t).Reason)
		require.Contains(t, doc["scaleLastError"], "status 500")
	}
	require.Equal(t, int32(5), f.scaleCalls.Load())

	// 拒绝次数耗尽后进入 Abandoned。观测原因和部署条件都应携带引擎的错误。
	_, err = f.reconciler.convergeModelDeployment(ctx, f.md)
	require.NoError(t, err)
	require.Equal(t, "Abandoned", recordDoc(t)["state"], f.observation(t).Reason)
	require.Contains(t, f.observation(t).Reason, "status 500", f.observation(t).Reason)
	status, reason, message := liveCondition(t)
	require.Equal(t, "False", status, "a spent retry bound is a terminal refusal")
	require.Equal(t, "ScaleRefused", reason)
	require.Contains(t, message, "status 500")

	// 进入终态后继续调谐，也应保留原拒绝并维持相同的发送次数。
	_, err = f.reconciler.convergeModelDeployment(ctx, f.md)
	require.NoError(t, err)
	require.Equal(t, int32(5), f.scaleCalls.Load())
	require.Equal(t, "Abandoned", recordDoc(t)["state"])
	status, _, message = liveCondition(t)
	require.Equal(t, "False", status)
	require.Contains(t, message, "status 500")

	// spec 回到已证明的旧宽度时，仍有两个多余成员占用准入资源。
	// 恢复流程把旧记录换成仅处理它们退场的记录，并清除先前的拒绝状态。
	f.md.Spec.Roles[0].ElasticEP.Width = 2
	f.md.Generation++
	require.NoError(t, f.reconciler.Client.Update(ctx, f.md))
	_, err = f.reconciler.convergeModelDeployment(ctx, f.md)
	require.NoError(t, err)
	recovered, readErr := newElasticOperationStore(f.reconciler.Client).Read(
		ctx, f.md.Namespace, f.md.Name, f.md.UID,
	)
	require.NoError(t, readErr, "recovery keeps the retirement the excess members owe: %v",
		f.observation(t).Reason)
	require.Equal(t, elasticStateReleased, recovered.State)
	require.Equal(t, elasticWidth{Old: 4, Target: 2}, recovered.Width)
	require.Len(t, recovered.Workers, 2)
	observed := f.observation(t)
	require.Equal(t, 2, observed.StableWidth)
	status, reason, _ = liveCondition(t)
	require.Equal(t, "True", status, "recovery leaves no refusal standing")
	require.Equal(t, "NoRefusal", reason)
	require.Equal(t, int32(5), f.scaleCalls.Load(), "recovery proves; it issues no command")
}
