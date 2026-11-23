package deviceplugin

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubeclients/kubernetes/scheme"
	"gpustack.ai/gpustack/pkg/nodefeature"
)

// workloadContainer 是测试中持有加速器的容器名。预留按 Pod UID 和容器名定位，
// 测试直接写入预留时也应使用同一名称。
const workloadContainer = "main"

// reservedWorkload 读回测试中主容器的预留。
func reservedWorkload(r *DevicesReconciler, podUID types.UID) (workercore.DevicesStatus, bool) {
	return r.reservedDevices(podUID, workloadContainer)
}

type stubResponder struct{}

func (stubResponder) GetContainerAllocateResponse(
	context.Context, *core.Pod, *core.Container, *workercore.Devices, map[Resource]int32,
) (*ContainerAllocateResponse, error) {
	return &ContainerAllocateResponse{}, nil
}

// nodeFixture 按节点名给 Pod 建索引，使 Allocate 可以从同节点的 Pod 中找到正在申请设备的容器。
func nodeFixture(objs ...ctrlcli.Object) ctrlcli.WithWatch {
	return ctrlfake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(objs...).
		WithIndex(&core.Pod{}, IndexingPodsByNodeName, func(obj ctrlcli.Object) []string {
			return []string{obj.(*core.Pod).Spec.NodeName}
		}).
		Build()
}

// TestResourceServer_Allocate_RecordsReservation 验证 Allocate 将设备 ID、
// 下标等记入按 Pod 和容器区分的进程内预留，供边车复用同一张物理卡。
func TestResourceServer_Allocate_RecordsReservation(t *testing.T) {
	const nodeName = "node-7"
	resName := nodefeature.GetAcceleratableResourceName(nodefeature.ManufacturerNVIDIA, workercore.DeviceAllocationModeSliced)

	devs := &workercore.Devices{
		ObjectMeta: meta.ObjectMeta{Name: nodeName},
		Spec: workercore.DevicesSpec{
			Groups: []workercore.DevicesGroup{{
				ID:           "grp-0",
				Manufacturer: nodefeature.ManufacturerNVIDIA,
				Accelerators: []workercore.Accelerator{{
					ID:    "dev-0",
					Index: 0,
				}},
			}},
		},
	}
	pod := &core.Pod{
		ObjectMeta: meta.ObjectMeta{Name: "p", Namespace: "default", UID: "pod-uid-7"},
		Spec: core.PodSpec{
			NodeName: nodeName,
			Containers: []core.Container{{
				Name: "main",
				Resources: core.ResourceRequirements{
					Limits: core.ResourceList{resName: resource.MustParse("1")},
				},
			}},
		},
	}

	cli := nodeFixture(devs, pod)

	rec := &DevicesReconciler{NodeName: nodeName, Client: cli}
	s := &ResourceServer{
		Manufacturer:   nodefeature.ManufacturerNVIDIA,
		AllocationMode: workercore.DeviceAllocationModeSliced,
		Reconciler:     rec,
		Responder:      stubResponder{},
	}

	_, err := s.Allocate(context.Background(), &AllocateRequest{
		ContainerRequests: []*ContainerAllocateRequest{{
			DevicesIds: []string{"grp-0:dev-0:0000"},
		}},
	})
	require.NoError(t, err)

	got, ok := reservedWorkload(rec, "pod-uid-7")
	require.True(t, ok, "workload Allocate must record an in-process reservation")
	require.Len(t, got.Groups, 1)
	require.Len(t, got.Groups[0].Accelerators, 1)
	assert.Equal(t, "dev-0", got.Groups[0].Accelerators[0].ID)
}

// 共享请求每张物理卡只取一个令牌。即使 kubelet 忽略选卡建议，Allocate 也要在预留和写注解前核对。
func TestResourceServer_Allocate_SharedNeedsDistinctCards(t *testing.T) {
	const nodeName = "node-sh"
	const share = int32(nodefeature.ResourceMaxUnits / nodefeature.SharedResourceMaxSize)
	devs := &workercore.Devices{
		ObjectMeta: meta.ObjectMeta{Name: nodeName},
		Spec: workercore.DevicesSpec{Groups: []workercore.DevicesGroup{{
			ID: "grp-0", Manufacturer: nodefeature.ManufacturerNVIDIA,
			Accelerators: []workercore.Accelerator{{ID: "dev-0", Index: 0}, {ID: "dev-1", Index: 1}},
		}}},
	}
	// 使用插件实际向 kubelet 公布的 ID，取同一卡上的两个不同令牌。
	first := Resource{Group: "grp-0", Device: "dev-0"}
	second := Resource{Group: "grp-0", Device: "dev-1"}
	a := first.DeviceIDs(workercore.DeviceAllocationModeShared, 0)
	b := second.DeviceIDs(workercore.DeviceAllocationModeShared, 0)
	require.Len(t, a, nodefeature.SharedResourceMaxSize)
	require.Len(t, b, nodefeature.SharedResourceMaxSize)
	cases := []struct {
		name      string
		mode      workercore.DeviceAllocationMode
		deviceIDs []string
		wantError []string
	}{
		{
			name: "shared tokens on two distinct accelerators are allocated", mode: workercore.DeviceAllocationModeShared,
			deviceIDs: []string{a[0], b[0]},
		},
		{
			name: "two shared tokens on one accelerator are refused", mode: workercore.DeviceAllocationModeShared,
			deviceIDs: []string{a[0], a[1]}, wantError: []string{"2 distinct accelerators", "grp-0:dev-0", "2 of its tokens"},
		},
		{
			name: "multiple repeated cards name the same first card", mode: workercore.DeviceAllocationModeShared,
			deviceIDs: []string{b[0], b[1], a[0], a[1]}, wantError: []string{"4 distinct accelerators", "grp-0:dev-0"},
		},
		{
			name: "the shared rule does not reject sliced tokens", mode: workercore.DeviceAllocationModeSliced,
			deviceIDs: first.DeviceIDs(workercore.DeviceAllocationModeSliced, 2),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resName := nodefeature.GetAcceleratableResourceName(nodefeature.ManufacturerNVIDIA, tc.mode)
			pod := &core.Pod{
				ObjectMeta: meta.ObjectMeta{Name: "p", Namespace: "default", UID: "uid-sh"},
				Spec: core.PodSpec{NodeName: nodeName, Containers: []core.Container{{
					Name: workloadContainer, Resources: core.ResourceRequirements{Limits: core.ResourceList{
						resName: *resource.NewQuantity(int64(len(tc.deviceIDs)), resource.DecimalSI),
					}},
				}}},
			}
			cli := nodeFixture(devs.DeepCopy(), pod)
			rec := &DevicesReconciler{NodeName: nodeName, Client: cli}
			s := &ResourceServer{
				Manufacturer: nodefeature.ManufacturerNVIDIA, AllocationMode: tc.mode, Reconciler: rec, Responder: stubResponder{},
			}

			_, err := s.Allocate(t.Context(), &AllocateRequest{
				ContainerRequests: []*ContainerAllocateRequest{{DevicesIds: tc.deviceIDs}},
			})
			got := new(core.Pod)
			require.NoError(t, cli.Get(t.Context(), ctrlcli.ObjectKeyFromObject(pod), got))
			_, annotated := got.Annotations[AllocatedAcceleratorAnnoKey]
			ledger, reserved := reservedWorkload(rec, pod.UID)
			if len(tc.wantError) > 0 {
				require.Error(t, err)
				assert.Equal(t, grpccodes.FailedPrecondition, grpcstatus.Code(err))
				for _, part := range tc.wantError {
					assert.Contains(t, grpcstatus.Convert(err).Message(), part)
				}
				assert.False(t, annotated)
				assert.False(t, reserved)
				return
			}
			require.NoError(t, err)
			assert.True(t, annotated)
			require.True(t, reserved)
			if tc.mode == workercore.DeviceAllocationModeShared {
				require.Len(t, ledger.Groups, 1)
				cards := ledger.Groups[0].Accelerators
				require.Len(t, cards, 2)
				assert.ElementsMatch(t, []string{"dev-0", "dev-1"}, []string{cards[0].ID, cards[1].ID})
				for _, card := range cards {
					assert.Equal(t, share, card.Allocated)
				}
			}
		})
	}
}
