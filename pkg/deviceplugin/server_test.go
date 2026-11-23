package deviceplugin

import (
	"context"
	"errors"
	"testing"
	"time"

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

// TestResourceServer_Allocate_Sliced_RecordsUnits 用于验证容器中每个加速器对应的“.sliced.units”分
// 配记录是否符合账本中的分配情况——即每个加速器的账本所反映的即为实际容量，节点设备准入检查会拒绝那
// 些已承诺的单元数量超过该容量的加速器，而非单纯的注入令牌数量。
func TestResourceServer_Allocate_Sliced_RecordsUnits(t *testing.T) {
	const nodeName = "node-6"
	resName := nodefeature.GetAcceleratableResourceName(nodefeature.ManufacturerNVIDIA, workercore.DeviceAllocationModeSliced)
	unitsName := nodefeature.GetAcceleratableSlicedUnitsResourceName(nodefeature.ManufacturerNVIDIA)
	wantUnits := int32(60 * nodefeature.ResourceMaxUnits / 100) // 60%的槽位 = 一张卡单位的60%

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
		ObjectMeta: meta.ObjectMeta{Name: "p", Namespace: "default"},
		Spec: core.PodSpec{
			NodeName: nodeName,
			Containers: []core.Container{{
				Name: "c",
				Resources: core.ResourceRequirements{
					Limits: core.ResourceList{
						resName:   resource.MustParse("1"),
						unitsName: *resource.NewQuantity(int64(wantUnits), resource.DecimalSI),
					},
				},
			}},
		},
	}

	cli := nodeFixture(devs, pod)

	s := &ResourceServer{
		Manufacturer:   nodefeature.ManufacturerNVIDIA,
		AllocationMode: workercore.DeviceAllocationModeSliced,
		Reconciler:     &DevicesReconciler{NodeName: nodeName, Client: cli},
		Responder:      stubResponder{},
	}

	_, err := s.Allocate(context.Background(), &AllocateRequest{
		ContainerRequests: []*ContainerAllocateRequest{{
			DevicesIds: []string{"grp-0:dev-0:0000"},
		}},
	})
	require.NoError(t, err)

	got := new(core.Pod)
	require.NoError(t, cli.Get(context.Background(), ctrlcli.ObjectKeyFromObject(pod), got))
	allocated, err := allocatedStatusOf(got)
	require.NoError(t, err)
	require.Len(t, allocated.Groups, 1)
	require.Len(t, allocated.Groups[0].Accelerators, 1)
	acc := allocated.Groups[0].Accelerators[0]
	assert.Equal(t, workercore.DeviceAllocationModeSliced, acc.Mode)
	assert.Equal(t, wantUnits, acc.Allocated, "real per-card committed units, not the token count")
}

// echoActuatorResponder 会准确报告插件所选择的放置位置，并将其发布到预留中，因此测试可以观察到插件自
// 身的决策，而非预设的决策。
type echoActuatorResponder struct {
	stubResponder
	rec *DevicesReconciler
}

// GetPhysicalSlicedVisibilityResponse 完成了该功能，因此这个假对象仍能满足其要求；使用它的动作测试从
// 不采用侧车分支。
func (echoActuatorResponder) GetPhysicalSlicedVisibilityResponse(
	_ context.Context, _ *core.Pod, _ *core.Container, _ *workercore.Devices,
	_ map[Resource]int32, _ string,
) (*ContainerAllocateResponse, error) {
	return nil, errors.New("echoActuatorResponder serves no visibility request")
}

func (r echoActuatorResponder) ActuatePhysicalSliced(
	_ context.Context, pod *core.Pod, ctr *core.Container, _ *workercore.Devices,
	allocated map[Resource]int32, profile string,
) (*PhysicalSlicedAllocation, error) {
	reserved, _ := r.rec.reservedDevices(pod.UID, ctr.Name)
	placements := make(Placements, len(allocated))
	for i := range reserved.Groups {
		grp := &reserved.Groups[i]
		for j := range grp.Accelerators {
			acc := &grp.Accelerators[j]
			placements[Resource{Group: grp.ID, Device: acc.ID}] = acc.AllocatedPhysicalPlacements
		}
	}
	return &PhysicalSlicedAllocation{
		Profile:    profile,
		Placements: placements,
		Response:   &ContainerAllocateResponse{},
		Rollback:   func() {},
	}, nil
}

// partitionedCard 在指定的放置位置构建了一个分区加速器，用于提供放置决策所读取的检测时间能力。
func partitionedCard(
	id string, index uint32, profile string, placements ...workercore.AcceleratorPlacement,
) workercore.Accelerator {
	return workercore.Accelerator{
		ID: id, Index: index,
		Status: workercore.AcceleratorStatus{
			PhysicalSliced: workercore.AcceleratorPhysicalSliced{
				Count: int32(len(placements)),
				Profiles: []workercore.AcceleratorPhysicalSlicedProfile{
					{Name: profile, Count: int32(len(placements)), Placements: placements},
				},
			},
		},
	}
}

// partitionedDevices 将加速器封装到节点设备的单个 NVIDIA 组中。
func partitionedDevices(nodeName string, cards ...workercore.Accelerator) *workercore.Devices {
	return &workercore.Devices{
		ObjectMeta: meta.ObjectMeta{Name: nodeName},
		Spec: workercore.DevicesSpec{
			Groups: []workercore.DevicesGroup{{
				ID:           "grp-0",
				Manufacturer: nodefeature.ManufacturerNVIDIA,
				Accelerators: cards,
			}},
		},
	}
}

// partitionPod 构建一个 Pod，该 Pod 请求一个分区规格——即加速器密钥加上每个分区的密钥——当 units 大于
// 0 时，Pod webhook 会合并生成 ".partitioned.units" 文件。
func partitionPod(nodeName, name, uid, profile string, units int64) *core.Pod {
	limits := core.ResourceList{
		nodefeature.GetAcceleratableResourceName(nodefeature.ManufacturerNVIDIA, workercore.DeviceAllocationModePartitioned): resource.MustParse("1"),
		nodefeature.GetAcceleratablePartitionedProfileResourceName(nodefeature.ManufacturerNVIDIA, profile):                  resource.MustParse("1"),
	}
	if units > 0 {
		limits[nodefeature.GetAcceleratablePartitionedUnitsResourceName(nodefeature.ManufacturerNVIDIA)] = *resource.NewQuantity(units, resource.DecimalSI)
	}
	return &core.Pod{
		ObjectMeta: meta.ObjectMeta{Name: name, Namespace: "default", UID: types.UID(uid)},
		Spec: core.PodSpec{
			NodeName:   nodeName,
			Containers: []core.Container{{Name: workloadContainer, Resources: core.ResourceRequirements{Limits: limits}}},
		},
	}
}

// partitionServer 通过预置的执行器，以命令行方式连接分区服务器。
func partitionServer(rec *DevicesReconciler, responder ContainerAllocateResponder) *ResourceServer {
	return &ResourceServer{
		Manufacturer:   nodefeature.ManufacturerNVIDIA,
		AllocationMode: workercore.DeviceAllocationModePartitioned,
		Reconciler:     rec,
		Responder:      responder,
	}
}

// TestResourceServer_Allocate_PartitionedPublishesSelection 锁定了互斥锁窗口保证：第一次分配所选的间
// 隔会在释放节点互斥锁前的预留中显示，因此对于无法共享加速器的配置，第二次请求将作用于其他加速器，而
// 非尚未分配的加速器。
func TestResourceServer_Allocate_PartitionedPublishesSelection(t *testing.T) {
	const nodeName = "node-partition-race"
	const profile = "7g.80gb"

	whole := workercore.AcceleratorPlacement{Start: 0, Length: 8}
	devs := partitionedDevices(nodeName,
		partitionedCard("dev-0", 0, profile, whole),
		partitionedCard("dev-1", 1, profile, whole))
	first := partitionPod(nodeName, "first", "pod-first", profile, 0)
	second := partitionPod(nodeName, "second", "pod-second", profile, 0)
	// 第二个 Pod 更年轻，因此 getAllocatingPod 会将第一次调用解析为“first”。
	second.CreationTimestamp = meta.NewTime(first.CreationTimestamp.Add(time.Second))
	cli := nodeFixture(devs, first, second)

	rec := &DevicesReconciler{NodeName: nodeName, Client: cli}
	// 执行器会返回插件所选的任意内容，因此测试会观察决策过程，而非固定答案。
	s := partitionServer(rec, echoActuatorResponder{rec: rec})

	for range 2 {
		_, err := s.Allocate(context.Background(), &AllocateRequest{
			ContainerRequests: []*ContainerAllocateRequest{{DevicesIds: []string{"grp-0:dev-0:0000"}}},
		})
		require.NoError(t, err)
	}

	cardOf := func(uid string) string {
		reserved, ok := reservedWorkload(rec, types.UID(uid))
		require.True(t, ok, "pod %s must hold a reservation", uid)
		require.Len(t, reserved.Groups, 1)
		require.Len(t, reserved.Groups[0].Accelerators, 1)
		return reserved.Groups[0].Accelerators[0].ID
	}
	assert.NotEqual(t, cardOf("pod-first"), cardOf("pod-second"),
		"two whole-card partitions must land on different cards; the first publishes its selection "+
			"into the reservation before releasing the mutex")
}

// TestResourceServer_Allocate_PartitionedRetryReusesTheCard 指出，重复进行的 Allocate 操作是幂等的。
// kubelet 会重新执行 Allocate 操作，针对那个丢失检查点的容器——即容器在停止状态下重新启动——此时该容器
// 的放置状态就成为了节点占用状态的一部分。如果重新进行决策，就会被视为属于其他人的状态：整个加速器配
// 置会显示节点已耗尽，而拥有空闲兄弟节点的节点则会被分配到该加速器中，从而绕过供应商的复用标记，创建
// 第二个实例。
func TestResourceServer_Allocate_PartitionedRetryReusesTheCard(t *testing.T) {
	const profile = "7g.80gb"
	whole := workercore.AcceleratorPlacement{Start: 0, Length: 8}

	cases := []struct {
		name      string
		cards     []workercore.Accelerator
		keepAlive bool // 保留进程内预留，即无需重启设备管理器
	}{
		{
			// 一个加速器，被该容器自身实例完全占用：新的决策将报告该节点已耗尽。
			name:      "the only accelerator is reused rather than reported exhausted",
			cards:     []workercore.Accelerator{partitionedCard("dev-0", 0, profile, whole)},
			keepAlive: true,
		},
		{
			// 存在一个免费的兄弟节点：新的决策将将其放置在那里，并创建第二个实例，使第一个节点成为孤儿状态，直到
			// 被回收为止。
			name: "a free sibling does not attract a second instance",
			cards: []workercore.Accelerator{
				partitionedCard("dev-0", 0, profile, whole),
				partitionedCard("dev-1", 1, profile, whole),
			},
			keepAlive: true,
		},
		{
			// 设备管理器重启会清除预留资源，因此 durable 注解成为仅存的记录——而这已经足够了。
			name: "the annotation alone survives a device-manager restart",
			cards: []workercore.Accelerator{
				partitionedCard("dev-0", 0, profile, whole),
				partitionedCard("dev-1", 1, profile, whole),
			},
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			const nodeName = "node-partition-retry"
			pod := partitionPod(nodeName, "p", "pod-retry", profile, 0)
			cli := nodeFixture(partitionedDevices(nodeName, c.cards...), pod)
			rec := &DevicesReconciler{NodeName: nodeName, Client: cli}
			s := partitionServer(rec, echoActuatorResponder{rec: rec})

			allocate := func() error {
				_, err := s.Allocate(context.Background(), &AllocateRequest{
					ContainerRequests: []*ContainerAllocateRequest{{DevicesIds: []string{"grp-0:dev-0:0000"}}},
				})
				return err
			}
			require.NoError(t, allocate())
			first, ok := reservedWorkload(rec, "pod-retry")
			require.True(t, ok)
			require.Len(t, first.Groups[0].Accelerators, 1)
			firstCard := first.Groups[0].Accelerators[0].ID

			if !c.keepAlive {
				rec.releaseReservation("pod-retry", workloadContainer)
			}

			require.NoError(t, allocate(), "a retry must not report the node exhausted")
			second, ok := reservedWorkload(rec, "pod-retry")
			require.True(t, ok)
			require.Len(t, second.Groups, 1, "the retry must not add a second card")
			require.Len(t, second.Groups[0].Accelerators, 1)
			assert.Equal(t, firstCard, second.Groups[0].Accelerators[0].ID,
				"the retry must land back on the card the first allocation used")
		})
	}
}

// TestResourceServer_Allocate_PartitionedRejectsWhenTheNodeIsFull 指出这种拒绝意味着什么：是节点没有
// 空间，而非加速器没有空间。该消息直接说明了分区规格，因此操作员无需猜测。
func TestResourceServer_Allocate_PartitionedRejectsWhenTheNodeIsFull(t *testing.T) {
	const nodeName = "node-partition-full"
	const profile = "7g.80gb"

	devs := partitionedDevices(nodeName,
		partitionedCard("dev-0", 0, "1g.10gb", workercore.AcceleratorPlacement{Start: 0, Length: 2}))
	pod := partitionPod(nodeName, "p", "pod-full", profile, 0)
	cli := nodeFixture(devs, pod)

	rec := &DevicesReconciler{NodeName: nodeName, Client: cli}
	s := partitionServer(rec, echoActuatorResponder{rec: rec})

	_, err := s.Allocate(context.Background(), &AllocateRequest{
		ContainerRequests: []*ContainerAllocateRequest{{DevicesIds: []string{"grp-0:dev-0:0000"}}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), profile)
	_, reserved := reservedWorkload(rec, "pod-full")
	assert.False(t, reserved, "a rejected allocation holds no reservation")
}
