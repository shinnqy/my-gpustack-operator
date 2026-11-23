package preflight

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v3"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/deviceplugin"
	"gpustack.ai/gpustack/pkg/nodefeature"
)

// 节点预检失败时，也必须先向调用方交付完整结果，再以错误结束。
func TestReport_WritesTheDocumentEvenWhenTheNodeFailed(t *testing.T) {
	grpList := device.PreflightGroupList{{
		Manufacturer: nodefeature.ManufacturerNVIDIA,
		Detection:    device.PreflightDetection{State: device.PreflightStateOK, Accelerators: 1},
		Checks: []device.PreflightCheck{{
			Accelerator: "GPU-0",
			Capability:  "mig-partitioning",
			State:       device.PreflightStateUnavailable,
			Depth:       device.PreflightDepthDeclared,
			Reason:      "nvml: function not found",
		}},
	}}
	topology := TopologyReport{
		Timestamp: time.Date(2026, 9, 20, 10, 0, 0, 0, time.UTC),
		Policy:    TopologyPolicyUnknown,
		Depth:     device.PreflightDepthDeclared,
		Note:      "no kubelet configuration this report reads names a topology manager policy",
	}

	var buf bytes.Buffer
	err := Report(&buf, grpList, NetworkReport{}, topology)

	require.Error(t, err, "an unavailable capability is a failure")
	assert.Contains(t, err.Error(), "nvml: function not found", "the driver's own words reach the caller")

	// 解码同一份 YAML 的各个部分，确认加速器失败时拓扑结果也能一起读取。
	var decoded preflightResult
	require.NoError(t, yaml.Unmarshal(buf.Bytes(), &decoded), "the document is still valid YAML")
	assert.Equal(t, grpList, decoded.Accelerators, "and it is the whole result, not a truncated one")
	assert.Equal(t, topology, decoded.Topology,
		"the topology section travels in the same document, not a second one")
}

// healthProbe 记录进入厂商探针的设备，因此测试可以直接发现误送到驱动的故障卡。
type healthProbe struct {
	calls int
	ids   []string
}

func (p *healthProbe) PreflightAccelerator(groups device.DevicesGroupList) device.PreflightGroup {
	p.calls++
	for _, group := range groups {
		for _, card := range group.Accelerators {
			p.ids = append(p.ids, card.ID)
		}
	}
	return device.PreflightGroup{}
}

// PreflightResponder 在单测中止于注入边界，使测试能按设备核对所有切片检查，而无需启动容器。
func (*healthProbe) PreflightResponder(workercore.DeviceAllocationMode) (deviceplugin.ContainerAllocateResponder, func(), error) {
	return nil, nil, errors.New("测试已到达分配响应器")
}

func TestPreflight_UnhealthyAccelerators(t *testing.T) {
	const manufacturer = nodefeature.ManufacturerAscend
	for _, tc := range []struct {
		name          string
		health        [][]bool
		wantProbed    []string
		wantUnhealthy []string
	}{
		{"同组一张故障，另一个组全部故障", [][]bool{{true, false, true}, {false}}, []string{"0/0", "0/2"}, []string{"0/1", "1/0"}},
		{"全部故障", [][]bool{{false, false}}, nil, []string{"0/0", "0/1"}},
		{"全部健康", [][]bool{{true}, {true}}, []string{"0/0", "1/0"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			probe := new(healthProbe)
			previous := supportedPreflighterCreators
			t.Cleanup(func() { supportedPreflighterCreators = previous })
			supportedPreflighterCreators = map[string]func(device.PreflighterOptions) device.AcceleratorPreflighter{
				manufacturer: func(device.PreflighterOptions) device.AcceleratorPreflighter { return probe },
			}
			var groups device.DevicesGroupList
			for i, flags := range tc.health {
				group := device.DevicesGroup{Name: "Ascend910", Manufacturer: manufacturer}
				for j, healthy := range flags {
					card := device.Accelerator{ID: fmt.Sprintf("%d/%d", i, j)}
					card.Status.Unhealthy = !healthy
					card.Status.LogicalSliced.Count = 2
					group.Accelerators = append(group.Accelerators, card)
				}
				groups = append(groups, group)
			}
			before := workercore.DevicesSpec{Groups: groups}
			original := before.DeepCopy()
			p := &Preflighter{dryRun: true, host: newHostExec(t.TempDir())}
			result := p.preflight(t.Context(), manufacturer, detection(groups, false), groups, false)

			assert.Equal(t, original, &before)
			assert.Equal(t, len(tc.wantProbed)+len(tc.wantUnhealthy), result.Detection.Accelerators)
			assert.Equal(t, tc.wantProbed, probe.ids)
			if len(tc.wantProbed) == 0 {
				assert.Zero(t, probe.calls)
			} else {
				assert.Equal(t, 1, probe.calls)
			}
			var unhealthy []string
			byCapability := make(map[string][]string)
			for _, check := range result.Checks {
				if check.Capability != capDeviceHealth {
					byCapability[check.Capability] = append(byCapability[check.Capability], check.Accelerator)
					continue
				}
				assert.Equal(t, device.PreflightStateUnavailable, check.State)
				assert.Equal(t, device.PreflightDepthDeclared, check.Depth)
				assert.Equal(t, device.PreflightModeUnnamed, check.Mode)
				assert.NotEmpty(t, check.Reason)
				unhealthy = append(unhealthy, check.Accelerator)
			}
			assert.Equal(t, tc.wantUnhealthy, unhealthy)
			for _, capability := range []string{capSlicedRuntimeLoaded, capSlicedQuotaInForce, capSidecarVisibility, capCoTenancy} {
				assert.ElementsMatch(t, tc.wantProbed, byCapability[capability], capability)
			}
		})
	}
}
