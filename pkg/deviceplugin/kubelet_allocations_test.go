package deviceplugin

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	podresources "k8s.io/kubelet/pkg/apis/podresources/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/utils/json"
)

func TestDevicesReconciler_KubeletConflictRefusesRebuildAndAllocate(t *testing.T) {
	for _, tc := range []struct {
		name           string
		annotationCard string
		runtimeID      string
		missingPod     bool
		unavailable    bool
		wantError      string
	}{
		{name: "stale annotation after restart", annotationCard: "dev-1", runtimeID: "grp-0:dev-0:0000", wantError: "allocation identity conflict"},
		{name: "missing annotation", runtimeID: "grp-0:dev-0:0000", wantError: "allocation identity conflict"},
		{name: "unknown accelerator", annotationCard: "dev-0", runtimeID: "grp-0:unknown:0000", wantError: "absent from inventory"},
		{name: "invalid token", annotationCard: "dev-0", runtimeID: "invalid", wantError: "parse kubelet device ID"},
		{name: "uncached runtime pod", missingPod: true, runtimeID: "grp-0:dev-0:0000", wantError: "uncached pod"},
		{name: "unavailable kubelet", annotationCard: "dev-0", unavailable: true, wantError: "verify kubelet device assignments"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			const node = "node-bookkeeping"
			devs := twoCardDevices(node, workercore.DeviceAllocationModeExclusive)
			pod := concurrentAllocatePod(node, "served", "uid-served", workercore.DeviceAllocationModeExclusive, 1)
			pod.Status.Phase = core.PodRunning
			if tc.annotationCard != "" {
				pod.Annotations = wholeCardAnnotation(t, tc.annotationCard)
			}
			objects := []ctrlcli.Object{devs}
			if !tc.missingPod {
				objects = append(objects, pod)
			}
			cli := nodeFixture(objects...)
			rec := &DevicesReconciler{NodeName: node, Client: cli, kubeletPods: func(context.Context) ([]*podresources.PodResources, error) {
				return wholeCardRuntime(pod, tc.runtimeID), nil
			}}
			if tc.unavailable {
				rec.kubeletPods = kubeletUnavailable
			}
			// 用没有内存预留的新协调器模拟进程重启，验证仅靠持久化注解和 kubelet 的实际占用就能拒绝冲突。
			for range 2 {
				_, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: node}})
				require.ErrorContains(t, err, tc.wantError)
				got := new(workercore.Devices)
				require.NoError(t, cli.Get(ctx, ctrlcli.ObjectKeyFromObject(devs), got))
				require.Equal(t, devs.Status, got.Status, "conflict must not publish a guessed ledger")
				server := &ResourceServer{Manufacturer: nodefeature.ManufacturerNVIDIA, AllocationMode: workercore.DeviceAllocationModeExclusive, Reconciler: rec, Responder: stubResponder{}}
				_, err = server.Allocate(ctx, &AllocateRequest{ContainerRequests: []*ContainerAllocateRequest{{DevicesIds: []string{"grp-0:dev-1:0000"}}}})
				require.Equal(t, grpccodes.FailedPrecondition, grpcstatus.Code(err))
				require.Contains(t, err.Error(), tc.wantError)
			}
			if !tc.missingPod {
				got := new(core.Pod)
				require.NoError(t, cli.Get(ctx, ctrlcli.ObjectKeyFromObject(pod), got))
				require.Equal(t, pod.Annotations, got.Annotations, "refusal must preserve all durable claims")
			}
		})
	}
}

func wholeCardAnnotation(t *testing.T, cards ...string) map[string]string {
	t.Helper()
	ids := make([]string, 0, len(cards))
	accelerators := make([]workercore.AcceleratorAllocation, 0, len(cards))
	for _, card := range cards {
		index := uint32(0)
		if card == "dev-1" {
			index = 1
		}
		ids = append(ids, "grp-0:"+card+":0000")
		accelerators = append(accelerators, workercore.AcceleratorAllocation{
			ID: card, Index: index, Mode: workercore.DeviceAllocationModeExclusive, Allocated: nodefeature.ResourceMaxUnits,
		})
	}
	encoded, err := json.Marshal(PodAllocations{workloadContainer: {
		DeviceIDs: ids,
		Devices: workercore.DevicesStatus{Groups: []workercore.DevicesAllocationGroup{{
			ID: "grp-0", Manufacturer: nodefeature.ManufacturerNVIDIA,
			Accelerators: accelerators,
		}}},
	}})
	require.NoError(t, err)
	return map[string]string{AllocatedAcceleratorAnnoKey: string(encoded)}
}

func wholeCardRuntime(pod *core.Pod, id string) []*podresources.PodResources {
	return []*podresources.PodResources{{Name: pod.Name, Namespace: pod.Namespace, Containers: []*podresources.ContainerResources{{
		Name: workloadContainer, Devices: []*podresources.ContainerDevices{{ResourceName: string(nodefeature.GetAcceleratableResourceName(nodefeature.ManufacturerNVIDIA, workercore.DeviceAllocationModeExclusive)), DeviceIds: []string{id}}},
	}}}}
}

func kubeletUnavailable(context.Context) ([]*podresources.PodResources, error) {
	return nil, errors.New("dial unix: no such file or directory")
}
