package deviceplugin

import (
	"fmt"

	core "k8s.io/api/core/v1"
)

// SlicedCoresPercent 用于读取切片容器在 ".sliced.cores-percentage" 请求中要求的每个加速器计算比例。当该值
// 缺失或非正数时，默认值为 100%（即整个加速器的计算能力）。切片可能会超额使用计算资源，因此每个切片最多可
// 请求 100% 的计算能力。该值直接决定了 HAMi-core CUDA_DEVICE_SM_LIMIT / vcann-rt aicore-quota 的配额（已
// 经是百分比形式，无需进行比例转换）。
func SlicedCoresPercent(ctr *core.Container, coresResName core.ResourceName) int {
	if q, ok := ctr.Resources.Limits[coresResName]; ok {
		if v := q.Value(); v > 0 {
			return int(v)
		}
	}
	return 100
}

// SlicedMemoryMib 根据加速器的总 VRAM（acceleratorVRAMMib）来计算分片容器所要求的每个加速器 VRAM 限制（
// MiB）。".sliced.memory-percentage"优先于绝对值".sliced.memory-mib"。其结果被限制在加速器的 VRAM 范围内。
// 如果两者均未设置，则分片分配会报错，而不是默默地暴露整个加速器的 VRAM。由于内存是不可超用的基准，因此百
// 分比规则在此生效，正如 Pod webhook 将其整合到".sliced.units"中一样——这样就能保持信用统计与实际限制的一
// 致性。
func SlicedMemoryMib(ctr *core.Container, memPctResName, memMibResName core.ResourceName, acceleratorVRAMMib int64) (int64, error) {
	if q, ok := ctr.Resources.Limits[memPctResName]; ok {
		if pct := q.Value(); pct > 0 {
			return min(acceleratorVRAMMib*pct/100, acceleratorVRAMMib), nil
		}
	}
	if q, ok := ctr.Resources.Limits[memMibResName]; ok {
		if mib := q.Value(); mib > 0 {
			return min(mib, acceleratorVRAMMib), nil
		}
	}
	return 0, fmt.Errorf("container %q has no %s or %s request", ctr.Name, memPctResName, memMibResName)
}

// ContainerEnvDeclared 用于指示容器是否在其环境列表中明确声明了名为 `name` 的环境变量，从而调用者可以跳过
// 默认值的注入，使该值具有权威性。仅会检查明确的环境项。不会检查源自 EnvFrom 的变量。
func ContainerEnvDeclared(ctr *core.Container, name string) bool {
	for i := range ctr.Env {
		if ctr.Env[i].Name == name {
			return true
		}
	}
	return false
}
