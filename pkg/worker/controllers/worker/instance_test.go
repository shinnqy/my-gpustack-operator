package worker

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	ctrlinterceptor "sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	kueuectrlconst "sigs.k8s.io/kueue/pkg/controller/constants"

	worker "gpustack.ai/gpustack/api/worker/v1"
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/deviceplugin"
	"gpustack.ai/gpustack/pkg/kubeclients/kubernetes/scheme"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/systemmeta"
)

// qty 将测试数据里的字符串转换成 Kubernetes 资源量。
func qty(s string) resource.Quantity { return resource.MustParse(s) }

// qtyEqual 按数值比较 Quantity，使相等关系与使用十进制还是二进制单位无关。
func qtyEqual(t *testing.T, want, got resource.Quantity, name string) {
	t.Helper()
	assert.Zerof(t, want.Cmp(got), "%s: want %s, got %s", name, want.String(), got.String())
}

// assertResourceList 同时比较键集合和每个资源的数量，防止多写或漏写一项。
func assertResourceList(t *testing.T, want, got core.ResourceList, label string) {
	t.Helper()
	keys := func(rl core.ResourceList) []core.ResourceName {
		out := make([]core.ResourceName, 0, len(rl))
		for k := range rl {
			out = append(out, k)
		}
		return out
	}

	assert.Equalf(t, len(want), len(got),
		"%s: expected %d entries (%v), got %d (%v)", label, len(want), keys(want), len(got), keys(got))
	for k, v := range want {
		gv, ok := got[k]
		if !assert.Truef(t, ok, "%s: missing key %s", label, k) {
			continue
		}
		qtyEqual(t, v, gv, label+"["+string(k)+"]")
	}
}

func TestGetResourceRequirements(t *testing.T) {
	cases := []struct {
		name                               string
		cpu, ram, storage                  string
		acceleratable                      bool
		withGeneral, withGeneralOvercommit bool
		wantLimits, wantRequests           core.ResourceList
	}{
		{
			name: "general only, overcommit off — requests mirror limits",
			cpu:  "4", ram: "16Gi", storage: "32Gi",
			withGeneral: true,
			wantLimits: core.ResourceList{
				core.ResourceCPU:              qty("4"),
				core.ResourceMemory:           qty("16Gi"),
				core.ResourceEphemeralStorage: qty("32Gi"),
			},
			wantRequests: core.ResourceList{
				core.ResourceCPU:              qty("4"),
				core.ResourceMemory:           qty("16Gi"),
				core.ResourceEphemeralStorage: qty("32Gi"),
			},
		},
		{
			name: "general only, overcommit on, non-acceleratable — 800m / 128Mi bases",
			cpu:  "4", ram: "16Gi", storage: "32Gi",
			withGeneral: true, withGeneralOvercommit: true,
			wantLimits: core.ResourceList{
				core.ResourceCPU:              qty("4"),
				core.ResourceMemory:           qty("16Gi"),
				core.ResourceEphemeralStorage: qty("32Gi"),
			},
			wantRequests: core.ResourceList{
				core.ResourceCPU:              qty("3200m"), // 800m × 4
				core.ResourceMemory:           qty("2Gi"),   // 128Mi × 16
				core.ResourceEphemeralStorage: qty("4Gi"),   // 128Mi × 32
			},
		},
		{
			// 小于 800m 基数的 CPU limit 必须仍能提交给 API 服务器，request 不能超过 limit。
			name: "general only, overcommit on, non-acceleratable — sub-base CPU clamps to limit",
			cpu:  "100m", ram: "16Gi", storage: "32Gi",
			withGeneral: true, withGeneralOvercommit: true,
			wantLimits: core.ResourceList{
				core.ResourceCPU:              qty("100m"),
				core.ResourceMemory:           qty("16Gi"),
				core.ResourceEphemeralStorage: qty("32Gi"),
			},
			wantRequests: core.ResourceList{
				core.ResourceCPU:              qty("100m"), // 800m × 1 超过 limit，取 100m。
				core.ResourceMemory:           qty("2Gi"),
				core.ResourceEphemeralStorage: qty("4Gi"),
			},
		},
		{
			// Value() 会把小数核数向上取整，结果仍需受 limit 约束。
			name: "general only, overcommit on, non-acceleratable — fractional CPU clamps to limit",
			cpu:  "1500m", ram: "16Gi", storage: "32Gi",
			withGeneral: true, withGeneralOvercommit: true,
			wantLimits: core.ResourceList{
				core.ResourceCPU:              qty("1500m"),
				core.ResourceMemory:           qty("16Gi"),
				core.ResourceEphemeralStorage: qty("32Gi"),
			},
			wantRequests: core.ResourceList{
				core.ResourceCPU:              qty("1500m"), // 800m × 2 超过 limit，取 1500m。
				core.ResourceMemory:           qty("2Gi"),
				core.ResourceEphemeralStorage: qty("4Gi"),
			},
		},
		{
			name: "general only, overcommit on, acceleratable — CPU uses 100m base",
			cpu:  "16", ram: "64Gi", storage: "128Gi",
			acceleratable: true,
			withGeneral:   true, withGeneralOvercommit: true,
			wantLimits: core.ResourceList{
				core.ResourceCPU:              qty("16"),
				core.ResourceMemory:           qty("64Gi"),
				core.ResourceEphemeralStorage: qty("128Gi"),
			},
			wantRequests: core.ResourceList{
				core.ResourceCPU:              qty("1600m"), // 100m × 16
				core.ResourceMemory:           qty("8Gi"),   // 128Mi × 64
				core.ResourceEphemeralStorage: qty("16Gi"),  // 128Mi × 128
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			resources := &workercore.InstanceResources{CPU: qty(c.cpu), RAM: qty(c.ram), LocalStorage: qty(c.storage)}
			instType := &worker.InstanceType{Spec: workercore.InstanceTypeSpec{Acceleratable: c.acceleratable}}
			rr := getResourceRequirements(resources, instType, c.withGeneral, c.withGeneralOvercommit, false, false)
			assertResourceList(t, c.wantLimits, rr.Limits, "limits")
			assertResourceList(t, c.wantRequests, rr.Requests, "requests")
		})
	}
}

func TestGetResourceRequirementsExclusive(t *testing.T) {
	instType := &worker.InstanceType{Spec: workercore.InstanceTypeSpec{Acceleratable: true}}
	instType.Status.Detail.Manufacturer = nodefeature.ManufacturerNVIDIA
	resources := &workercore.InstanceResources{Accelerator: ptr.To(qty("2"))}
	rr := getResourceRequirements(resources, instType, false, false, true, false)
	want := core.ResourceList{"nvidia.com/gpu": qty("2")}
	assertResourceList(t, want, rr.Requests, "requests")
	assertResourceList(t, want, rr.Limits, "limits")
}

func buildInstanceClient(objs ...ctrlcli.Object) ctrlcli.Client {
	return ctrlfake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithStatusSubresource(&workercore.Instance{}).
		WithObjects(objs...).
		Build()
}

func reconcileInstance(t *testing.T, cli ctrlcli.Client, namespace, name string) (ctrlreconcile.Result, error) {
	t.Helper()
	r := &InstanceReconciler{Client: cli, APIReader: cli}
	return r.Reconcile(context.Background(),
		ctrlreconcile.Request{NamespacedName: ctrlcli.ObjectKey{Namespace: namespace, Name: name}})
}

// newReadyInstance 构造引用给定规格的实例，状态已标为 Ready，当前还没有对应的 Pod。
func newReadyInstance(namespace, name, instType string) *workercore.Instance {
	return &workercore.Instance{
		ObjectMeta: meta.ObjectMeta{Namespace: namespace, Name: name},
		Spec:       workercore.InstanceSpec{Type: instType},
		Status:     workercore.InstanceStatus{Phase: InstancePhaseReady},
	}
}

// TestInstanceReconciler_NoPublishedEntranceRequeues 检查 CPU 规格尚未发布队列入口时的第一轮协调。
// 没有队列标签的 Pod 会绕过 Kueue。此时应让实例重新排队，集群中仍然查不到 Pod。
func TestInstanceReconciler_NoPublishedEntranceRequeues(t *testing.T) {
	const typeName = "cpu-type"
	inst := newReadyInstance("default", "inst", typeName)
	// 使用 CPU 规格，排除加速器详情未就绪的影响，单独验证入口等待。
	it := &worker.InstanceType{
		ObjectMeta: meta.ObjectMeta{Name: typeName},
		Spec:       workercore.InstanceTypeSpec{Acceleratable: false},
	}
	cq := &kueue.ClusterQueue{ObjectMeta: meta.ObjectMeta{Name: typeName}}
	cli := buildInstanceClient(inst, it, cq)

	res, err := reconcileInstance(t, cli, "default", "inst")
	require.NoError(t, err)
	assert.Positive(t, res.RequeueAfter, "reconcile requeues while no entrance is published")

	pod := &core.Pod{}
	err = cli.Get(context.Background(), ctrlcli.ObjectKey{Namespace: "default", Name: "inst"}, pod)
	assert.True(t, kerrors.IsNotFound(err),
		"no Pod may be created before the type publishes the queue it belongs to")
}

// TestInstanceReconciler_EntranceRequeueTerminates 接着验证等待能结束，规格控制器发布 CPU 规格的入口后，
// 下一轮协调必须创建 Pod，并使用刚发布的入口。
func TestInstanceReconciler_EntranceRequeueTerminates(t *testing.T) {
	const typeName = "cpu-type"
	ctx := context.Background()

	inst := newReadyInstance("default", "inst", typeName)
	// 补齐容器资源和工作卷，因为第二轮协调会实际执行 Pod 渲染。
	inst.Spec.InstanceTemplate = workercore.InstanceTemplate{
		Image: "img",
		Resources: &workercore.InstanceResources{
			CPU:          qty("1"),
			RAM:          qty("2Gi"),
			LocalStorage: qty("10Gi"),
		},
	}
	inst.Spec.Volume = workercore.InstanceVolume{
		Ephemeral: &workercore.InstanceEphemeralVolume{Capacity: qty("10Gi")},
	}
	it := &worker.InstanceType{
		ObjectMeta: meta.ObjectMeta{Name: typeName},
		Spec:       workercore.InstanceTypeSpec{Acceleratable: false},
	}
	cq := &kueue.ClusterQueue{ObjectMeta: meta.ObjectMeta{Name: typeName}}
	cli := buildInstanceClient(inst, it, cq)

	// 第一轮，入口尚未发布，实例应重新排队。
	res, err := reconcileInstance(t, cli, "default", "inst")
	require.NoError(t, err)
	require.Positive(t, res.RequeueAfter, "reconcile requeues while no entrance is published")

	// 调用真实的规格状态计算函数得到入口，检查规格发布与 Pod 消费两个步骤是否能衔接。
	coreIT := &workercore.InstanceType{
		ObjectMeta: meta.ObjectMeta{Name: typeName},
		Spec:       workercore.InstanceTypeSpec{Acceleratable: false},
	}
	st := (&InstanceTypeReconciler{Client: cli, APIReader: cli}).computeStatus(ctx, coreIT, cq, false)
	require.NotEmpty(t, st.Entrance,
		"a CPU-only type reaches an entrance on the same pass an accelerated one does")

	require.NoError(t, cli.Get(ctx, ctrlcli.ObjectKey{Name: typeName}, it))
	it.Status = st
	require.NoError(t, cli.Update(ctx, it))

	// 第二轮应能查到 Pod，并看到它带着规格发布的队列名。
	_, err = reconcileInstance(t, cli, "default", "inst")
	require.NoError(t, err)

	pod := &core.Pod{}
	require.NoError(t, cli.Get(ctx, ctrlcli.ObjectKey{Namespace: "default", Name: "inst"}, pod),
		"the requeue ends once the type publishes an entrance")
	assert.Equal(t, st.Entrance, pod.Labels[kueuectrlconst.QueueLabel],
		"the Pod is queued at the entrance the type published")
}

// TestConvertPodFromInstance_QueueLabelIsThePublishedEntrance 故意让发布的入口名与从规格名推算的值不同。
// 两者在健康集群通常相同，这样构造输入才能检测出渲染器错误地重新推算了入口。
func TestConvertPodFromInstance_QueueLabelIsThePublishedEntrance(t *testing.T) {
	const typeName = "h20-8x"
	inst := &workercore.Instance{
		ObjectMeta: meta.ObjectMeta{Namespace: "default", Name: "inst"},
		Spec: workercore.InstanceSpec{
			Type: typeName,
			InstanceTemplate: workercore.InstanceTemplate{
				Image: "img",
				Resources: &workercore.InstanceResources{
					CPU:          qty("1"),
					RAM:          qty("2Gi"),
					LocalStorage: qty("10Gi"),
				},
			},
			Volume: workercore.InstanceVolume{
				Ephemeral: &workercore.InstanceEphemeralVolume{Capacity: qty("10Gi")},
			},
		},
	}
	it := &worker.InstanceType{
		ObjectMeta: meta.ObjectMeta{Name: typeName},
		Status:     workercore.InstanceTypeStatus{Entrance: "queue-for-h20-8x"},
	}

	cli := buildInstanceClient(inst, it)
	r := &InstanceReconciler{Client: cli, APIReader: cli}
	pod := r.convertPodFromInstance(context.Background(), inst, it, nil)

	require.NotNil(t, pod)
	assert.Equal(t, "queue-for-h20-8x", pod.Labels[kueuectrlconst.QueueLabel],
		"the label must be the entrance the type published, not one derived from its name")
	assert.NotEqual(t, nodefeature.FormatLocalQueueName(typeName),
		pod.Labels[kueuectrlconst.QueueLabel],
		"the fixture spells the two differently so this test can discriminate")
}

const (
	// instanceServiceType 使用 CPU 规格，让用例直接进入 Service 生命周期。
	instanceServiceType = "cpu-type"
	// instanceServiceUID 是实例的 UID。测试客户端不会自动分配它，而 Service 用它选择 Pod。
	instanceServiceUID = types.UID("11111111-2222-3333-4444-555555555555")
	// instanceServiceObjectUID 在第一次启动后写入 Service，之后用来判断重启是否复用了同一个对象。
	instanceServiceObjectUID = types.UID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
)

// instancePort 构造给定端口号的 TCP 端口。
func instancePort(p int32) workercore.InstancePort {
	return workercore.InstancePort{Port: p, Protocol: core.ProtocolTCP}
}

// newInstanceServiceFixture 同时构造实例、已发布入口的规格及其队列，让测试能直接进入创建 Pod 的分支。
func newInstanceServiceFixture(ports ...workercore.InstancePort) (*workercore.Instance,
	*worker.InstanceType, *kueue.ClusterQueue,
) {
	inst := &workercore.Instance{
		ObjectMeta: meta.ObjectMeta{Namespace: "default", Name: "inst", UID: instanceServiceUID},
		Spec: workercore.InstanceSpec{
			Type: instanceServiceType,
			InstanceTemplate: workercore.InstanceTemplate{
				Image: "img",
				Resources: &workercore.InstanceResources{
					CPU:          qty("1"),
					RAM:          qty("2Gi"),
					LocalStorage: qty("10Gi"),
				},
				Ports: ports,
			},
			Volume: workercore.InstanceVolume{
				Ephemeral: &workercore.InstanceEphemeralVolume{Capacity: qty("10Gi")},
			},
		},
	}
	instType := &worker.InstanceType{
		ObjectMeta: meta.ObjectMeta{Name: instanceServiceType},
		Spec:       workercore.InstanceTypeSpec{Acceleratable: false},
		Status: workercore.InstanceTypeStatus{
			Entrance: nodefeature.FormatLocalQueueName(instanceServiceType),
		},
	}
	cq := &kueue.ClusterQueue{ObjectMeta: meta.ObjectMeta{Name: instanceServiceType}}
	return inst, instType, cq
}

// newInstanceServiceFixtureService 构造已有的实例 Service，带上集群原先为它分配的 NodePort。
func newInstanceServiceFixtureService(inst *workercore.Instance,
	nodePort int32,
) *core.Service {
	svc := &core.Service{
		ObjectMeta: meta.ObjectMeta{
			Namespace: inst.Namespace,
			Name:      inst.Name,
			UID:       instanceServiceObjectUID,
		},
		Spec: core.ServiceSpec{
			Type:     core.ServiceTypeNodePort,
			Selector: map[string]string{deviceplugin.InstancePartOfLabelKey: string(inst.UID)},
			Ports: []core.ServicePort{{
				Name:       "tcp-8080",
				Port:       8080,
				Protocol:   core.ProtocolTCP,
				TargetPort: intstr.FromInt32(8080),
				NodePort:   nodePort,
			}},
		},
	}
	kubemeta.ControlOnWithoutBlock(svc, inst, workercore.SchemeGroupVersionKind("Instance"))
	return svc
}

// buildInstanceServiceClient 在单元测试中补上 API 服务器分配 NodePort 的行为，
// 新端口获得一个编号，已有编号的端口保持原值，否则控制器只能反复等待端口分配。
func buildInstanceServiceClient(objs ...ctrlcli.Object) ctrlcli.Client {
	next := int32(30000)
	allocate := func(obj ctrlcli.Object) {
		svc, ok := obj.(*core.Service)
		if !ok {
			return
		}
		for i := range svc.Spec.Ports {
			if svc.Spec.Ports[i].NodePort != 0 {
				continue
			}
			next++
			svc.Spec.Ports[i].NodePort = next
		}
	}
	return ctrlinterceptor.NewClient(buildInstanceClient(objs...).(ctrlcli.WithWatch),
		ctrlinterceptor.Funcs{
			Create: func(ctx context.Context, c ctrlcli.WithWatch, obj ctrlcli.Object, opts ...ctrlcli.CreateOption) error {
				allocate(obj)
				return c.Create(ctx, obj, opts...)
			},
			Update: func(ctx context.Context, c ctrlcli.WithWatch, obj ctrlcli.Object, opts ...ctrlcli.UpdateOption) error {
				allocate(obj)
				return c.Update(ctx, obj, opts...)
			},
		})
}

// setInstancePodRunning 写入 kubelet 在 Pod 就绪时会报告的状态，使控制器能发布实例阶段和访问端口。
func setInstancePodRunning(t *testing.T, cli ctrlcli.Client) {
	t.Helper()
	ctx := context.Background()
	pod := new(core.Pod)
	require.NoError(t, cli.Get(ctx, ctrlcli.ObjectKey{Namespace: "default", Name: "inst"}, pod))
	pod.Status = core.PodStatus{
		Phase:  core.PodRunning,
		PodIP:  "10.244.1.5",
		PodIPs: []core.PodIP{{IP: "10.244.1.5"}},
		Conditions: []core.PodCondition{
			{Type: core.PodScheduled, Status: core.ConditionTrue},
			{Type: core.PodInitialized, Status: core.ConditionTrue},
			{Type: core.PodReady, Status: core.ConditionTrue},
		},
	}
	require.NoError(t, cli.Status().Update(ctx, pod))
}

// serviceNodePorts 按端口名读取 NodePort，供停止前后比较。
func serviceNodePorts(svc *core.Service) map[string]int32 {
	ports := make(map[string]int32, len(svc.Spec.Ports))
	for i := range svc.Spec.Ports {
		ports[svc.Spec.Ports[i].Name] = svc.Spec.Ports[i].NodePort
	}
	return ports
}

// getInstanceService 读取当前实例的 Service，若 Service 已消失则立即使测试失败。
func getInstanceService(t *testing.T, cli ctrlcli.Client) *core.Service {
	t.Helper()
	svc := new(core.Service)
	require.NoError(t, cli.Get(context.Background(),
		ctrlcli.ObjectKey{Namespace: "default", Name: "inst"}, svc),
		"the Service belongs to the Instance, so it outlives the Pod")
	return svc
}

// getInstance 读取本用例正在协调的 Instance。
func getInstance(t *testing.T, cli ctrlcli.Client) *workercore.Instance {
	t.Helper()
	inst := new(workercore.Instance)
	require.NoError(t, cli.Get(context.Background(),
		ctrlcli.ObjectKey{Namespace: "default", Name: "inst"}, inst))
	return inst
}

// reconcileInstanceUntilSettled 连续协调到无需重新排队。一次更新或删除之后，可能还要再读一次对象才能确认最终状态。
func reconcileInstanceUntilSettled(t *testing.T, cli ctrlcli.Client) {
	t.Helper()
	for range 5 {
		res, err := reconcileInstance(t, cli, "default", "inst")
		require.NoError(t, err)
		if res.RequeueAfter == 0 && !res.Requeue {
			return
		}
	}
	t.Fatal("reconcile did not settle within 5 passes")
}

// updateInstanceSpec 在测试中模拟用户修改已存在实例的 spec。
func updateInstanceSpec(t *testing.T, cli ctrlcli.Client, edit func(*workercore.Instance)) {
	t.Helper()
	inst := getInstance(t, cli)
	edit(inst)
	require.NoError(t, cli.Update(context.Background(), inst))
}

// TestInstanceReconciler_ServicePreservedAcrossStopAndRestart 检查停止、恢复后用户仍能使用原 NodePort。
// Service 的所有者是 Instance，选择器依据实例 UID。旧 Pod 消失后，原来的 Service 因此还能选中新 Pod。
// 同时核对 Service UID 和 NodePort，可检测到删除后碰巧分回相同端口的情况。
func TestInstanceReconciler_ServicePreservedAcrossStopAndRestart(t *testing.T) {
	ctx := context.Background()
	inst, instType, cq := newInstanceServiceFixture(instancePort(8080))
	cli := buildInstanceServiceClient(inst, instType, cq)

	// 先让控制器创建 Pod 和 Service，随后报告 Pod 就绪，再次协调以发布端口。
	reconcileInstanceUntilSettled(t, cli)
	setInstancePodRunning(t, cli)
	reconcileInstanceUntilSettled(t, cli)

	svc := getInstanceService(t, cli)
	allocated := serviceNodePorts(svc)
	require.Equal(t, map[string]int32{"tcp-8080": 30001}, allocated,
		"the API server allocates a node port for the Instance's port")

	// Service 的所有者应指向 Instance。若指向 Pod，停止 Pod 时垃圾回收也会删除 Service。
	owner := kubemeta.GetControllerOfNoCopy(svc)
	require.NotNil(t, owner, "the Service names a controller owner")
	assert.Equal(t, "Instance", owner.Kind, "the owner is the Instance")
	assert.Equal(t, workercore.SchemeGroupVersionKind("Instance").GroupVersion().String(),
		owner.APIVersion, "and it is named in the Instance's own API version")
	assert.Equal(t, inst.Name, owner.Name)
	assert.Equal(t, instanceServiceUID, owner.UID)
	assert.True(t, ptr.Deref(owner.Controller, false), "as the controller owner")

	// 选择器只需要 part-of 标签。若连队列标签也复制进去，修改实例规格后可能选不中新 Pod。
	assert.Equal(t, map[string]string{
		deviceplugin.InstancePartOfLabelKey: string(instanceServiceUID),
	}, svc.Spec.Selector, "the Service selects its Pod by the part-of label alone")
	assert.Equal(t, core.ServiceTypeNodePort, svc.Spec.Type)

	running := getInstance(t, cli)
	require.Len(t, running.Status.Ports, 1, "a running Instance publishes the port it is reached on")
	firstNodePort := running.Status.Ports[0].NodePort

	// 记录 API 服务器赋予 Service 的 UID。重启后如创建了另一个 Service，就能看出身份变化。
	svc.UID = instanceServiceObjectUID
	require.NoError(t, cli.Update(ctx, svc))

	// 停止时 Pod 消失，Service 继续保留端口，Instance 暂停公布访问地址。
	updateInstanceSpec(t, cli, func(inst *workercore.Instance) { inst.Spec.Stop = true })
	reconcileInstanceUntilSettled(t, cli)

	err := cli.Get(ctx, ctrlcli.ObjectKey{Namespace: "default", Name: "inst"}, &core.Pod{})
	assert.True(t, kerrors.IsNotFound(err), "stopping deletes the backing Pod")

	stopped := getInstanceService(t, cli)
	assert.Equal(t, allocated, serviceNodePorts(stopped),
		"stopping must not release the node port the user connects to")
	assert.Equal(t, instanceServiceObjectUID, stopped.UID, "and it must not replace the Service")

	stoppedInst := getInstance(t, cli)
	assert.Equal(t, InstancePhaseStopped, stoppedInst.Status.Phase)
	assert.Empty(t, stoppedInst.Status.Ports, "a stopped Instance serves nothing, so it reports no port")

	// 恢复后应重新创建 Pod，保留下来的 Service 自然选中新 Pod。
	updateInstanceSpec(t, cli, func(inst *workercore.Instance) { inst.Spec.Stop = false })
	reconcileInstanceUntilSettled(t, cli)

	pod := new(core.Pod)
	require.NoError(t, cli.Get(ctx, ctrlcli.ObjectKey{Namespace: "default", Name: "inst"}, pod),
		"starting builds a Pod again")
	for key, value := range getInstanceService(t, cli).Spec.Selector {
		assert.Equal(t, value, pod.Labels[key], "the retained Service selects the new Pod")
	}

	setInstancePodRunning(t, cli)
	reconcileInstanceUntilSettled(t, cli)

	restarted := getInstanceService(t, cli)
	assert.Equal(t, instanceServiceObjectUID, restarted.UID, "a restart reuses the Service it kept")
	assert.Equal(t, allocated, serviceNodePorts(restarted),
		"rather than publishing a new address for the user to chase")

	restartedInst := getInstance(t, cli)
	require.Len(t, restartedInst.Status.Ports, 1)
	assert.Equal(t, firstNodePort, restartedInst.Status.Ports[0].NodePort,
		"the address given before the stop is the one reported after it")
	assert.Equal(t, InstancePhaseReady, restartedInst.Status.Phase)
}

// TestInstanceReconciler_DeletingInstanceDeletesServiceAndPod 检查删除实例时也会回收一直保留到此时的 Service 及 NodePort。
func TestInstanceReconciler_DeletingInstanceDeletesServiceAndPod(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name string

		// 首次观察到删除意图时，哪些子对象仍然存在。
		withPod bool
		withSvc bool

		// 只有已经没有子对象时，第一轮才能释放终结器。
		wantReleasedFirstPass bool
	}{
		{
			name:    "a running instance loses its Pod and its Service",
			withPod: true, withSvc: true,
		},
		{
			// 已停止的实例只有保留的 Service。必须先删除它，再在后续协调确认资源已清理后释放终结器。
			name:    "a stopped instance loses its retained Service before the finalizer is released",
			withSvc: true,
		},
		{
			name:                  "an instance owning nothing is released at once",
			wantReleasedFirstPass: true,
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			inst, instType, cq := newInstanceServiceFixture(instancePort(8080))
			// 提前构造带删除时间戳和终结器的实例。测试客户端会拒绝通过普通更新设置删除时间戳。
			now := meta.Now()
			inst.DeletionTimestamp = &now
			systemmeta.Lock(inst)

			objs := []ctrlcli.Object{inst, instType, cq}
			if c.withPod {
				objs = append(objs, &core.Pod{ObjectMeta: meta.ObjectMeta{
					Namespace: inst.Namespace, Name: inst.Name,
					OwnerReferences: []meta.OwnerReference{{
						APIVersion: "worker.gpustack.ai/v1alpha1",
						Kind:       "Instance",
						Name:       inst.Name,
						UID:        inst.UID,
						Controller: ptr.To(true),
					}},
				}})
			}
			if c.withSvc {
				objs = append(objs, newInstanceServiceFixtureService(inst, 30001))
			}
			cli := buildInstanceServiceClient(objs...)

			// 第一轮删除实例仍然拥有的子对象。
			_, err := reconcileInstance(t, cli, "default", "inst")
			require.NoError(t, err)

			svcErr := cli.Get(ctx, ctrlcli.ObjectKey{Namespace: "default", Name: "inst"}, &core.Service{})
			podErr := cli.Get(ctx, ctrlcli.ObjectKey{Namespace: "default", Name: "inst"}, &core.Pod{})
			if c.withSvc {
				assert.True(t, kerrors.IsNotFound(svcErr), "the Service is deleted with the Instance")
			} else {
				assert.True(t, kerrors.IsNotFound(svcErr), "an Instance that owned none has none to clean up")
			}
			if c.withPod {
				assert.True(t, kerrors.IsNotFound(podErr), "the backing Pod is deleted with the Instance")
			}

			instErr := cli.Get(ctx, ctrlcli.ObjectKey{Namespace: "default", Name: "inst"},
				&workercore.Instance{})
			if !c.wantReleasedFirstPass {
				// 即使删除请求已经发出，仍要保留实例的终结器，直到下一轮确认子对象清理完毕。
				require.NoError(t, instErr, "the Instance is not released before its Service is gone")
				got := new(workercore.Instance)
				require.NoError(t, cli.Get(ctx, ctrlcli.ObjectKey{Namespace: "default", Name: "inst"}, got))
				assert.Contains(t, got.Finalizers, systemmeta.LockedResourceFinalizer)
			}

			// 再次协调，确认已无子对象后释放终结器，实例才能最终消失。
			_, err = reconcileInstance(t, cli, "default", "inst")
			require.NoError(t, err)
			assert.True(t, kerrors.IsNotFound(
				cli.Get(ctx, ctrlcli.ObjectKey{Namespace: "default", Name: "inst"}, &workercore.Instance{}),
			),
				"the Instance is released once nothing it owns is left")
		})
	}
}
