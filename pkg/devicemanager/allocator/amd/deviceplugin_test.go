package amd

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/deviceplugin"
	"gpustack.ai/gpustack/pkg/nodefeature"
)

// 以下每个拦截器组件都是一部分：60个 CU / 3个 SE / 1个 XCC gfx1101，相关符合性表已经过测量，因此这
// 些被排除的情况属于表自身的行，而非此处编造的数字。
var testTopology = Topology{Name: "gfx1101", CU: 60, SE: 3, SAPerSE: 2, XCC: 1}

const (
	testCardVRAMMib = 16368
	testCardUUID    = "GPU-5c88007d760374f3"

	// 账本对这两个加速器所记录的检测器参数进行了限制，其编号顺序与供应商工具的相反，因此第一个加速器具
	// 有更高的卡号。设备使用所测量的数值对，而非精确的 0.128 / 1.129，因此这里的顺序假设并不成立。
	testCardDRM  = 1
	testCardDRM2 = 0
	testRenderD  = 128
	testRenderD2 = 129
)

// redirectLogicalSliceDirs 将分阶段库和Pod工作目录指向测试程序自身的临时根目录。仅包含路径，没有文
// 件：该处理器只是组合路径，从不读取这些路径。
func redirectLogicalSliceDirs(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	origLib, origPods := deviceplugin.OperatorLibDir, deviceplugin.OperatorPodsDir
	deviceplugin.OperatorLibDir = filepath.Join(root, "lib")
	deviceplugin.OperatorPodsDir = filepath.Join(root, "pods")
	t.Cleanup(func() {
		deviceplugin.OperatorLibDir, deviceplugin.OperatorPodsDir = origLib, origPods
	})
}

// stubTopology为加速器提供了其运行的放置算法。真正的读者是一个仅存在于Linux上的cgo接缝；其上方的算
// 法必须能够在没有加速器的情况下进行测试。
func stubTopology(t *testing.T, topo Topology) {
	t.Helper()
	orig := readTopologyFn
	readTopologyFn = func(string, string) (Topology, error) { return topo, nil }
	t.Cleanup(func() { readTopologyFn = orig })
}

// redirectDevicePaths将设备节点路径指向一个临时树结构，该结构中包含了fixture加速器名称的节点。响应
// 方仅对这些路径进行统计，因此普通的文件可以替代字符设备。
func redirectDevicePaths(t *testing.T) {
	t.Helper()
	root := filepath.Join(t.TempDir(), "dev")
	require.NoError(t, os.MkdirAll(filepath.Join(root, "dri"), 0o755))

	origNode, origCard, origRender := nodeDevicePaths, cardDevicePathFormat, renderDevicePathFormat
	nodeDevicePaths = []string{filepath.Join(root, "kfd")}
	cardDevicePathFormat = filepath.Join(root, "dri", "card%d")
	renderDevicePathFormat = filepath.Join(root, "dri", "renderD%d")
	t.Cleanup(func() {
		nodeDevicePaths, cardDevicePathFormat, renderDevicePathFormat = origNode, origCard, origRender
	})

	paths := append([]string{}, nodeDevicePaths...)
	for _, minor := range []uint32{testCardDRM, testCardDRM2} {
		paths = append(paths, cardDevicePath(minor))
	}
	for _, minor := range []uint32{testRenderD, testRenderD2} {
		paths = append(paths, renderDevicePath(minor))
	}
	for _, path := range paths {
		require.NoError(t, os.WriteFile(path, nil, 0o600))
	}
}

// testDeviceSpecs是名为加速器的设备所必须满足的授予条件：首先是节点级别的计算节点，然后是每个加速
// 器各自的配对设备，按授予顺序排列。
func testDeviceSpecs(drm ...[2]uint32) []*deviceplugin.DeviceSpec {
	rw := func(path string) *deviceplugin.DeviceSpec {
		return &deviceplugin.DeviceSpec{ContainerPath: path, HostPath: path, Permissions: "rw"}
	}
	specs := make([]*deviceplugin.DeviceSpec, 0, len(nodeDevicePaths)+2*len(drm))
	for _, path := range nodeDevicePaths {
		specs = append(specs, rw(path))
	}
	for _, pair := range drm {
		specs = append(specs, rw(cardDevicePath(pair[0])), rw(renderDevicePath(pair[1])))
	}
	return specs
}

func testDevices(uuids ...string) *workercore.Devices {
	drm := [][2]uint32{{testCardDRM, testRenderD}, {testCardDRM2, testRenderD2}}
	accels := make([]workercore.Accelerator, 0, len(uuids))
	for i, uuid := range uuids {
		accel := workercore.Accelerator{
			ID:       uuid,
			Index:    uint32(i),
			Topology: workercore.DeviceTopology{PciBusID: "0000:0" + string(rune('4'+i)) + ":00.0"},
		}
		if i < len(drm) {
			accel.PhysicalIndexes = []uint32{drm[i][0], drm[i][1]}
		}
		accels = append(accels, accel)
	}
	return &workercore.Devices{
		ObjectMeta: meta.ObjectMeta{Name: "node-0"},
		Spec: workercore.DevicesSpec{
			Groups: []workercore.DevicesGroup{{
				ID:           "grp-0",
				Manufacturer: Manufacturer,
				Memory:       testCardVRAMMib,
				Accelerators: accels,
			}},
		},
	}
}

// testResource是设备组中某个加速器在账本中的位置标识。
func testResource(uuid string) deviceplugin.Resource {
	return deviceplugin.Resource{Group: "grp-0", Device: uuid}
}

func testAllocated(uuids ...string) map[deviceplugin.Resource]int32 {
	allocated := make(map[deviceplugin.Resource]int32, len(uuids))
	for _, uuid := range uuids {
		allocated[deviceplugin.Resource{Group: "grp-0", Device: uuid}] = 1
	}
	return allocated
}

// testContainer 构建容器时会请求一个槽位：代表加速器计算能力的 coresPct 以及其 VRAM 的 memPct。百
// 分比为0则省略该请求，这就是默认设置的实际应用方式。
func testContainer(coresPct, memPct int64, envs ...core.EnvVar) *core.Container {
	limits := core.ResourceList{
		nodefeature.GetAcceleratableResourceName(Manufacturer, workercore.DeviceAllocationModeSliced): resource.MustParse("1"),
	}
	if coresPct > 0 {
		limits[nodefeature.GetAcceleratableSlicedCoresPercentageResourceName(Manufacturer)] = *resource.NewQuantity(coresPct, resource.DecimalSI)
	}
	if memPct > 0 {
		limits[nodefeature.GetAcceleratableSlicedMemoryPercentageResourceName(Manufacturer)] = *resource.NewQuantity(memPct, resource.DecimalSI)
	}
	return &core.Container{
		Name:      "main",
		Env:       envs,
		Resources: core.ResourceRequirements{Limits: limits},
	}
}

func testPod() *core.Pod {
	return &core.Pod{ObjectMeta: meta.ObjectMeta{Name: "p", Namespace: "default", UID: types.UID("pod-uid")}}
}

func TestPlaceLogicalSliced(t *testing.T) {
	cases := []struct {
		name     string
		coresPct int64
		occupied []workercore.AcceleratorPlacement
		want     workercore.AcceleratorPlacement
		wantErr  string
	}{
		{
			// 符合性表 A的25%行：7.5 WGPs四舍五入为8，将DOWN调整为6，而简单的答案（15 CUs，“0:0-14”）会导致整
			// 个加速器保持开启状态。
			name:     "25 percent takes six WGPs, not the naive fifteen CUs",
			coresPct: 25,
			want:     workercore.AcceleratorPlacement{Start: 0, Length: 12},
		},
		{
			name:     "a second slice is packed beside the first, not on top of it",
			coresPct: 25,
			occupied: []workercore.AcceleratorPlacement{{Start: 0, Length: 12}},
			want:     workercore.AcceleratorPlacement{Start: 12, Length: 12},
		},
		{
			name:     "a hole is reused ahead of the tail",
			coresPct: 25,
			occupied: []workercore.AcceleratorPlacement{{Start: 24, Length: 12}},
			want:     workercore.AcceleratorPlacement{Start: 0, Length: 12},
		},
		{
			// 当没有进行任何请求时，SlicedCoresPercent 返回 100，同时仍会输出整个加速器掩码：它说明了容器可能
			// 达到的范围，而不是不提及该内容。
			name:     "no request at all is a whole accelerator",
			coresPct: 0,
			want:     workercore.AcceleratorPlacement{Start: 0, Length: 60},
		},
		{
			// 在一种着色器引擎下，遮罩不再具有限制作用，因此请求会被拒绝，而不是被悄然提升到无人要求或未计入的
			// 上限。
			name:     "a sub-quantum request is refused, naming the accelerator's minimum",
			coresPct: 5,
			wantErr:  "smallest slice is 9%",
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			stubTopology(t, testTopology)
			s := &server{}

			placed, err := s.PlaceLogicalSliced(
				context.Background(), testPod(), testContainer(c.coresPct, 50),
				testDevices(testCardUUID), testAllocated(testCardUUID),
				deviceplugin.Placements{testResource(testCardUUID): c.occupied},
			)

			if c.wantErr != "" {
				require.ErrorContains(t, err, c.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t,
				deviceplugin.Placements{
					testResource(testCardUUID): {c.want},
				}, placed)
		})
	}
}

// TestGetLogicalSlicedResponse_SingleCard 验证了实际允许的情况下的整体注入机制：一个加速器、一个窗
// 口、每个环境键以及每个挂载。
func TestGetLogicalSlicedResponse_SingleCard(t *testing.T) {
	redirectLogicalSliceDirs(t)
	redirectDevicePaths(t)
	s := &server{}
	pod, ctr := testPod(), testContainer(25, 25)

	resp, err := s.GetLogicalSlicedResponse(
		context.Background(), pod, ctr, testDevices(testCardUUID), testAllocated(testCardUUID),
		deviceplugin.Placements{testResource(testCardUUID): {{Start: 0, Length: 12}}},
	)
	require.NoError(t, err)

	assert.Equal(t, map[string]string{
		"AMD_VISIBLE_DEVICES":         "none",
		"ROCR_VISIBLE_DEVICES":        testCardUUID,
		"HSA_CU_MASK":                 "0:0-11",
		"VROCM_DEVICE_MEMORY_LIMIT_0": "4092",
		"VROCM_LEDGER_PATH":           ctrLedgerPath,
		"LIBVROCM_LOG_LEVEL":          "1",
	}, resp.Envs)

	assert.Equal(t, testDeviceSpecs([2]uint32{testCardDRM, testRenderD}), resp.Devices,
		"a slice reaches its accelerator through the same device nodes a whole one does")

	libDir := filepath.Join(deviceplugin.OperatorLibDir, "amd")
	ledgerDir := filepath.Join(deviceplugin.PodWorkDir(string(pod.UID), ctr.Name), "run/vrocm")
	assert.Equal(t, []*deviceplugin.Mount{
		{ContainerPath: ctrLdPreloadPath, HostPath: filepath.Join(libDir, "ld.so.preload"), ReadOnly: true},
		{ContainerPath: ctrVrocmLibPath, HostPath: filepath.Join(libDir, "libvrocm.so"), ReadOnly: true},
		{ContainerPath: ctrVrocmMonPath, HostPath: filepath.Join(libDir, "rocm-monitor"), ReadOnly: true},
		{ContainerPath: ctrVrocmCheckPath, HostPath: filepath.Join(libDir, "rocm-cumask-check"), ReadOnly: true},
		{ContainerPath: ctrLedgerDir, HostPath: ledgerDir, ReadOnly: false},
	}, resp.Mounts)
}
