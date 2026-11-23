package nvidia

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
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
