package nvidia

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clienttesting "k8s.io/client-go/testing"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/deviceplugin"
	"gpustack.ai/gpustack/pkg/kubeclients/kubernetes/scheme"
	"gpustack.ai/gpustack/pkg/nodefeature"
)

// registrationRecorder 代替 kubelet 接收插件注册。后续 RPC 通过注册的真实 Unix 套接字往返。
type registrationRecorder struct {
	pluginapi.UnimplementedRegistrationServer
	registrations chan *pluginapi.RegisterRequest
}

func (r *registrationRecorder) Register(ctx context.Context, req *pluginapi.RegisterRequest) (*pluginapi.Empty, error) {
	select {
	case r.registrations <- req:
		return &pluginapi.Empty{}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestExclusiveDeviceLifecycle(t *testing.T) {
	const node = "node-a"
	const gpu = "GPU-0001"
	const token = "group-a:" + gpu + ":0000"
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	devs := &workercore.Devices{
		ObjectMeta: meta.ObjectMeta{Name: node},
		Spec: workercore.DevicesSpec{Groups: []workercore.DevicesGroup{{
			ID: "group-a", Manufacturer: Manufacturer,
			Accelerators: []workercore.Accelerator{{ID: gpu}},
		}}},
	}
	pod := &core.Pod{
		ObjectMeta: meta.ObjectMeta{Name: "exclusive", Namespace: "default", UID: "uid-exclusive"},
		Spec: core.PodSpec{NodeName: node, Containers: []core.Container{{
			Name: "main", Resources: core.ResourceRequirements{
				Requests: core.ResourceList{"nvidia.com/gpu": resource.MustParse("1")},
				Limits:   core.ResourceList{"nvidia.com/gpu": resource.MustParse("1")},
			},
		}}},
	}
	// Devices 中有 uint64 字段。使用普通对象存储，避免假客户端模拟字段所有权时无法表示这种数值。
	tracker := clienttesting.NewObjectTracker(scheme.Scheme, scheme.Codecs.UniversalDecoder())
	client := ctrlfake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjectTracker(tracker).
		WithStatusSubresource(devs).WithObjects(devs, pod).
		WithIndex(&core.Pod{}, deviceplugin.IndexingPodsByNodeName, func(o ctrlcli.Object) []string {
			return []string{o.(*core.Pod).Spec.NodeName}
		}).Build()
	rec := &deviceplugin.DevicesReconciler{NodeName: node, Client: client}
	s := &server{ResourceServer: deviceplugin.ResourceServer{
		Manufacturer: Manufacturer, AllocationMode: workercore.DeviceAllocationModeExclusive, Reconciler: rec,
	}}
	t.Setenv(deviceplugin.InjectionStrategyEnv(Manufacturer), "envvar")
	var err error
	s.injection, err = deviceplugin.NewInjectionResolver(injectionConfig)
	require.NoError(t, err)
	s.Responder = s

	// Unix 套接字有路径长度限制，使用短目录名，避免 macOS 的临时目录加上测试名后超长。
	dir, err := os.MkdirTemp("", "dp8-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	kubeSocket := filepath.Join(dir, "kubelet.sock")
	listener, err := net.Listen("unix", kubeSocket)
	require.NoError(t, err)
	reg := &registrationRecorder{registrations: make(chan *pluginapi.RegisterRequest, 1)}
	kubelet := grpc.NewServer()
	pluginapi.RegisterRegistrationServer(kubelet, reg)
	go func() { _ = kubelet.Serve(listener) }()
	defer kubelet.Stop()

	done := make(chan error, 1)
	go func() { done <- s.Start(ctx, kubeSocket) }()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("设备插件退出失败：%v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("设备插件在上下文取消后仍未退出")
		}
	}()

	var registration *pluginapi.RegisterRequest
	select {
	case registration = <-reg.registrations:
	case <-ctx.Done():
		t.Fatal("未收到设备插件注册", ctx.Err())
	}
	require.Equal(t, "nvidia.com/gpu", registration.GetResourceName())
	require.True(t, registration.GetOptions().GetGetPreferredAllocationAvailable())
	conn, err := grpc.NewClient("unix://"+filepath.Join(dir, registration.GetEndpoint()), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	plugin := pluginapi.NewDevicePluginClient(conn)

	stream, err := plugin.ListAndWatch(ctx, &pluginapi.Empty{})
	require.NoError(t, err)
	inventory, err := stream.Recv()
	require.NoError(t, err)
	require.Len(t, inventory.GetDevices(), 1)
	card := inventory.GetDevices()[0]
	require.Equal(t, token, card.GetID())
	require.Equal(t, pluginapi.Healthy, card.GetHealth())

	hint, err := plugin.GetPreferredAllocation(ctx, &pluginapi.PreferredAllocationRequest{
		ContainerRequests: []*pluginapi.ContainerPreferredAllocationRequest{{
			AvailableDeviceIDs: []string{card.GetID()}, AllocationSize: 1,
		}},
	})
	require.NoError(t, err)
	require.Len(t, hint.GetContainerResponses(), 1)
	ids := hint.GetContainerResponses()[0].GetDeviceIDs()
	require.Equal(t, []string{token}, ids)
	resp, err := plugin.Allocate(ctx, &pluginapi.AllocateRequest{
		ContainerRequests: []*pluginapi.ContainerAllocateRequest{{DevicesIds: ids}},
	})
	require.NoError(t, err)
	require.Len(t, resp.GetContainerResponses(), 1)
	require.Equal(t, gpu, resp.GetContainerResponses()[0].GetEnvs()["NVIDIA_VISIBLE_DEVICES"])
	require.NoError(t, client.Get(ctx, ctrlcli.ObjectKeyFromObject(pod), pod))
	claims, err := deviceplugin.AllocatedAcceleratorsOf(pod)
	require.NoError(t, err)
	require.Equal(t, []string{token}, claims["main"].DeviceIDs)

	// 模拟设备管理器重启。新的控制器只有持久注解可读，也应能扣除整卡，并在 Pod 删除后恢复满额。
	restarted := &deviceplugin.DevicesReconciler{NodeName: node, Client: client}
	checkLedger := func(mode workercore.DeviceAllocationMode, remaining int32) {
		t.Helper()
		key := ctrlcli.ObjectKeyFromObject(devs)
		_, err := restarted.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		require.NoError(t, err)
		require.NoError(t, client.Get(ctx, key, devs))
		require.Len(t, devs.Status.Groups, 1)
		require.Len(t, devs.Status.Groups[0].Accelerators, 1)
		entry := devs.Status.Groups[0].Accelerators[0]
		require.Equal(t, gpu, entry.ID)
		require.Equal(t, mode, entry.Mode)
		require.Equal(t, remaining, entry.Remaining)
	}
	checkLedger(workercore.DeviceAllocationModeExclusive, 0)
	require.NoError(t, client.Delete(ctx, pod))
	checkLedger(workercore.DeviceAllocationModeNone, nodefeature.ResourceMaxUnits)
}

const (
	testGPUUUID0 = "GPU-aaaa0000-0000-0000-0000-000000000000"
	testGPUUUID1 = "GPU-bbbb1111-1111-1111-1111-111111111111"
)

// redirectLogicalSliceDirs 将逻辑切片主机路径（包括 vgpulock 目录）指向用于测试的临时目录。
func redirectLogicalSliceDirs(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	origLib, origPods, origLock := deviceplugin.OperatorLibDir, deviceplugin.OperatorPodsDir, hostVgpuLockPath
	deviceplugin.OperatorLibDir = filepath.Join(root, "lib")
	deviceplugin.OperatorPodsDir = filepath.Join(root, "pods")
	hostVgpuLockPath = filepath.Join(root, "vgpulock")
	t.Cleanup(func() {
		deviceplugin.OperatorLibDir = origLib
		deviceplugin.OperatorPodsDir = origPods
		hostVgpuLockPath = origLock
	})
	return root
}

// nvidiaDevices 构建了一个固定结构，该结构包含一组（runtimeVersion 控制 cuda 目录，内存控制 VRAM
// 限制），其中包含了给定的 GPU UUIDs。
func nvidiaDevices(runtimeVersion string, memoryMiB uint64, uuids ...string) *workercore.Devices {
	accels := make([]workercore.Accelerator, len(uuids))
	for i, u := range uuids {
		accels[i] = workercore.Accelerator{ID: u, Index: uint32(i)}
	}
	return &workercore.Devices{
		Spec: workercore.DevicesSpec{
			Groups: []workercore.DevicesGroup{{
				ID:             "a10g",
				Manufacturer:   Manufacturer,
				Memory:         memoryMiB,
				RuntimeVersion: runtimeVersion,
				Accelerators:   accels,
			}},
		},
	}
}

// slicedPod构建切片容器，要求实现独立的计算与VRAM资源分配，分配器现在会读取“
// .sliced.cores-percentage”（SM）和“.sliced.memory-percentage”（每加速器VRAM）这些指标。
func slicedPod(uid, ctrName string, coresPercent, memPercent int64) (*core.Pod, *core.Container) {
	coresRes := nodefeature.GetAcceleratableSlicedCoresPercentageResourceName(Manufacturer)
	memPctRes := nodefeature.GetAcceleratableSlicedMemoryPercentageResourceName(Manufacturer)
	pod := &core.Pod{
		ObjectMeta: meta.ObjectMeta{Name: ctrName + "-pod", UID: types.UID(uid)},
		Spec: core.PodSpec{
			Containers: []core.Container{{
				Name: ctrName,
				Resources: core.ResourceRequirements{
					Limits: core.ResourceList{
						coresRes:  *resource.NewQuantity(coresPercent, resource.DecimalSI),
						memPctRes: *resource.NewQuantity(memPercent, resource.DecimalSI),
					},
				},
			}},
		},
	}
	return pod, &pod.Spec.Containers[0]
}

func newSlicedServer() *server {
	return &server{
		ResourceServer: deviceplugin.ResourceServer{
			Manufacturer:   Manufacturer,
			AllocationMode: workercore.DeviceAllocationModeSliced,
		},
	}
}

func TestGetSlicedContainerAllocateResponse(t *testing.T) {
	redirectLogicalSliceDirs(t)
	s := newSlicedServer()
	// 测试卡有 24,576 MiB 显存。算力请求为 10% SM，显存请求为 25%，两项独立。
	devs := nvidiaDevices("12.4", 24576, testGPUUUID0)
	pod, ctr := slicedPod("pod-uid-1", "train", 10, 25)
	allocated := map[deviceplugin.Resource]int32{{Group: "a10g", Device: testGPUUUID0}: 1}

	resp, err := s.GetContainerAllocateResponse(context.Background(), pod, ctr, devs, allocated)
	require.NoError(t, err)

	// Envs.
	assert.Equal(t, testGPUUUID0, resp.Envs["NVIDIA_VISIBLE_DEVICES"])
	assert.Equal(t, "10", resp.Envs["CUDA_DEVICE_SM_LIMIT"]) // .sliced.cores-percentage
	assert.Equal(t, "/tmp/vgpu/cudevshr.cache", resp.Envs["CUDA_DEVICE_MEMORY_SHARED_CACHE"])
	assert.Equal(t, "0", resp.Envs["LIBCUDA_LOG_LEVEL"]) // 默认降低 HAMi-core 的日志级别。CUDA 与 NVML 的设备序号需要一致，CUDA_DEVICE_MEMORY_LIMIT_<i> 才
	// 能限制预期的卡。
	assert.Equal(t, "PCI_BUS_ID", resp.Envs["CUDA_DEVICE_ORDER"])
	// 向下取整后，显存限额为 24,576 × 25% = 6,144 MiB。
	assert.Equal(t, "6144m", resp.Envs["CUDA_DEVICE_MEMORY_LIMIT_0"])
	_, hasSecond := resp.Envs["CUDA_DEVICE_MEMORY_LIMIT_1"]
	assert.False(t, hasSecond, "single card must not emit LIMIT_1")

	// osx.MkdirAll 强制将工作目录权限设为 0777，不受 umask 影响。
	podWorkDir := deviceplugin.PodWorkDir("pod-uid-1", "train")
	for _, dir := range []string{hostVgpuLockPath, podWorkDir, filepath.Join(podWorkDir, "tmp/vgpu")} {
		info, statErr := os.Stat(dir)
		require.NoErrorf(t, statErr, "dir %s", dir)
		assert.Equalf(t, os.FileMode(0o777), info.Mode().Perm(), "mode of %s", dir)
	}

	// Mounts.
	libDir := filepath.Join(deviceplugin.OperatorLibDir, "nvidia")
	byCtr := make(map[string]*deviceplugin.Mount, len(resp.Mounts))
	for _, m := range resp.Mounts {
		byCtr[m.ContainerPath] = m
	}
	cases := []struct {
		ctrPath, hostPath string
		readOnly          bool
	}{
		{ctrVgpuLockPath, hostVgpuLockPath, false},
		{ctrLdPreloadPath, filepath.Join(libDir, "ld.so.preload"), true},
		{ctrVgpuLibPath, filepath.Join(libDir, "cuda-12/libvgpu.so"), true},
		{ctrVgpuCacheDir, filepath.Join(podWorkDir, "tmp/vgpu"), false},
		{ctrDevShmPath, ctrDevShmPath, false},
	}
	for _, c := range cases {
		m, ok := byCtr[c.ctrPath]
		require.Truef(t, ok, "missing mount for %s", c.ctrPath)
		assert.Equalf(t, c.hostPath, m.HostPath, "host path for %s", c.ctrPath)
		assert.Equalf(t, c.readOnly, m.ReadOnly, "readOnly for %s", c.ctrPath)
	}
}

// 没有内存维度（既无 .sliced.memory-percentage 也无 .sliced.memory-mib）的切片容器必须被拒绝，而不
// 是默默接受整个加速器。
func TestGetSlicedContainerAllocateResponse_NoMemoryRequest(t *testing.T) {
	redirectLogicalSliceDirs(t)
	s := newSlicedServer()
	devs := nvidiaDevices("12.4", 24576, testGPUUUID0)
	pod := &core.Pod{
		ObjectMeta: meta.ObjectMeta{UID: types.UID("uid-x")},
		Spec:       core.PodSpec{Containers: []core.Container{{Name: "train"}}},
	}
	_, err := s.GetContainerAllocateResponse(context.Background(), pod, &pod.Spec.Containers[0], devs,
		map[deviceplugin.Resource]int32{{Group: "a10g", Device: testGPUUUID0}: 1})
	require.Error(t, err)
}

// 声明了默认变量之一的切片容器会保留其自身值：分配器不得在其之上进行注入操作——安静的日志级别是调试
// 的出口途径，设备顺序则决定了工作负载自身的调用方式，即 CUDA 根据此方式对卡片进行编号。
func TestGetSlicedContainerAllocateResponse_RespectsContainerDeclaredEnv(t *testing.T) {
	cases := []struct{ name, declared string }{
		{"LIBCUDA_LOG_LEVEL", "3"},
		{"CUDA_DEVICE_ORDER", "FASTEST_FIRST"},
	}
	for _, c := range cases {
		redirectLogicalSliceDirs(t)
		s := newSlicedServer()
		devs := nvidiaDevices("12.4", 24576, testGPUUUID0)
		pod, ctr := slicedPod("pod-uid-"+c.name, "train", 10, 25)
		ctr.Env = []core.EnvVar{{Name: c.name, Value: c.declared}}
		allocated := map[deviceplugin.Resource]int32{{Group: "a10g", Device: testGPUUUID0}: 1}

		resp, err := s.GetContainerAllocateResponse(context.Background(), pod, ctr, devs, allocated)
		require.NoErrorf(t, err, "declared %s", c.name)

		_, injected := resp.Envs[c.name]
		assert.Falsef(t, injected, "must not override a container-declared %s", c.name)
	}
}

// 同一份 3g.40gb 分配应在容器响应、Pod 注解、设备账本和所有权标记中指向同一分区。
// 测试替换集群客户端和 NVML 驱动，使用正式的 Allocate、账本重建及回收入口。
func TestPartitionedAllocationLifecycle(t *testing.T) {
	const node = "node-mig"
	const profile = "3g.40gb"
	const owner = "main"
	redirectLogicalSliceDirs(t)

	devs := migDevices(profile, 3, 4, testGPUUUID0)
	devs.Name = node
	group := &devs.Spec.Groups[0]
	group.Memory = 81920
	physical := &group.Accelerators[0].Status.PhysicalSliced
	physical.Count = 2
	physical.Profiles[0].Count = 2
	physical.Profiles[0].MemoryMib = 40960
	physical.Profiles[0].Placements = []workercore.AcceleratorPlacement{{Start: 0, Length: 4}, {Start: 4, Length: 4}}

	resources := core.ResourceList{
		"nvidia.com/gpu.partitioned":             resource.MustParse("1"),
		"nvidia.com/gpu.partitioned.mig-3g.40gb": resource.MustParse("1"),
		"nvidia.com/gpu.partitioned.units":       resource.MustParse("800000"),
	}
	pod := &core.Pod{
		ObjectMeta: meta.ObjectMeta{Name: "partition-a", Namespace: "default", UID: "uid-mig"},
		Spec: core.PodSpec{NodeName: node, Containers: []core.Container{{
			Name: owner, Resources: core.ResourceRequirements{Requests: resources, Limits: resources.DeepCopy()},
		}}},
	}
	tracker := clienttesting.NewObjectTracker(scheme.Scheme, scheme.Codecs.UniversalDecoder())
	cli := ctrlfake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjectTracker(tracker).
		WithStatusSubresource(devs).WithObjects(devs, pod).
		WithIndex(&core.Pod{}, deviceplugin.IndexingPodsByNodeName, func(obj ctrlcli.Object) []string {
			return []string{obj.(*core.Pod).Spec.NodeName}
		}).Build()
	drv := newFakeMigDriver()
	drv.possible[testGPUUUID0] = []migPlacement{{Start: 0, Length: 4}, {Start: 4, Length: 4}}

	allocate := func() string {
		t.Helper()
		// 每次使用一个新设备插件，第二次调用只能凭 Pod 注解、磁盘标记和驱动状态恢复分配。
		s := &server{
			ResourceServer: deviceplugin.ResourceServer{
				Manufacturer: Manufacturer, AllocationMode: workercore.DeviceAllocationModePartitioned,
				Reconciler: &deviceplugin.DevicesReconciler{NodeName: node, Client: cli},
			},
			mig: drv,
		}
		s.Responder = s
		response, err := s.Allocate(t.Context(), &deviceplugin.AllocateRequest{
			ContainerRequests: []*deviceplugin.ContainerAllocateRequest{{DevicesIds: []string{"h100:" + testGPUUUID0 + ":0000"}}},
		})
		require.NoError(t, err)
		require.Len(t, response.ContainerResponses, 1)
		return response.ContainerResponses[0].Envs["NVIDIA_VISIBLE_DEVICES"]
	}

	visible := allocate()
	path := markerPath(string(pod.UID), owner, testGPUUUID0)
	marker, err := parseMarker(path)
	require.NoError(t, err)
	require.NotEmpty(t, visible)
	require.Equal(t, marker.MigUUID, visible)
	require.Equal(t, profile, marker.Profile)
	require.NoError(t, cli.Get(t.Context(), ctrlcli.ObjectKeyFromObject(pod), pod))
	allocations, err := deviceplugin.AllocatedAcceleratorsOf(pod)
	require.NoError(t, err)
	require.Len(t, allocations[owner].Devices.Groups, 1)
	cards := allocations[owner].Devices.Groups[0].Accelerators
	require.Len(t, cards, 1)
	require.Equal(t, visible, cards[0].AllocatedPhysicalID)
	require.Equal(t, int32(800000), cards[0].Allocated)
	require.Equal(t, []workercore.AcceleratorPlacement{{Start: marker.Start, Length: marker.Length}}, cards[0].AllocatedPhysicalPlacements)
	require.Equal(t, visible, allocate())
	require.Equal(t, 1, drv.createCalls)

	rec := &deviceplugin.DevicesReconciler{NodeName: node, Client: cli}
	checkRoom := func(want int32) {
		t.Helper()
		key := ctrlcli.ObjectKeyFromObject(devs)
		_, err := rec.Reconcile(t.Context(), ctrl.Request{NamespacedName: key})
		require.NoError(t, err)
		require.NoError(t, cli.Get(t.Context(), key, devs))
		require.Len(t, devs.Status.Groups, 1)
		require.Len(t, devs.Status.Groups[0].Accelerators, 1)
		require.Equal(t, []workercore.AcceleratorProfileCount{{Name: profile, Count: want}},
			devs.Status.Groups[0].Accelerators[0].RemainingProfiles)
	}
	checkRoom(1)
	r := newReclaimer(drv, deviceplugin.OperatorPodsDir, ctrl.Log, liveClaimsFrom(t.Context(), rec))
	for range reclaimMaxMisses {
		r.reconcile([]string{string(pod.UID)})
	}
	require.Empty(t, drv.destroyed)

	require.NoError(t, cli.Delete(t.Context(), pod))
	for range reclaimMaxMisses - 1 {
		r.reconcile(nil)
	}
	require.Empty(t, drv.destroyed)
	r.reconcile(nil)
	require.Equal(t, []migInstance{marker.instance()}, drv.destroyed)
	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err))
	checkRoom(2)
}
