package deviceplugin

import (
	"cmp"
	"slices"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

// AllocatedAccelerator 将 kubelet 分配的某个加速器与包含其模型级信息——内存、驱动程序及运行时版本——的组绑
// 定在一起，这些信息由厂商的注入过程与加速器本身一同被读取。这两个字段都指向了它们被收集到的 Devices，因
// 此仅在该环境下才有效，且不得进行写入操作。
type AllocatedAccelerator struct {
	// Group 是加速器中的设备组。
	Group *workercore.DevicesGroup

	// Accel 就是那个加速器本身。
	Accel *workercore.Accelerator
}

// AllocatedAccelerators 返回了 kubelet 分配给某个容器的加速器，这些加速器按其检测器记录的枚举索引顺序排列。
//
// 该订单即合同规定：厂商运行时框架中容器内加速器的编号方式，与其管理库对宿主设备的编号方式一致——即使用
// PCI 总线 ID——而记录的索引也遵循同样的编号规则。因此，以位置为依据进行插入的条目（每个加速器对应的配额、
// 计算掩码）与容器使用的编号方式相符，只要以该顺序进行输出即可。遵循插入本身所列的编号顺序的厂商也可以自
// 由使用。无论哪种方式，排序都不会带来任何成本，因为两种方式都是自洽的。
//
// 无法直接遍历账本。它针对每种加速器模型保存一个组，因此仅在组内按枚举顺序对组进行遍历。而容纳两种模型加
// 速器的容器会将其混合排列。组顺序也并不属于 API 的组成部分，两个列表都被声明为以身份为键的映射。
//
// 一本没有为其加速器设置索引的账本——所有项都保持为零——便产生了账本的排列顺序，而如果没有这个，调用者将无
// 法获得这种顺序。排列是一种稳定的排序方式，因此它既不创造新的顺序，也不抛弃已有的顺序。
func AllocatedAccelerators(devs *workercore.Devices, allocated map[Resource]int32) []AllocatedAccelerator {
	accels := make([]AllocatedAccelerator, 0, len(allocated))
	for i := range devs.Spec.Groups {
		grp := &devs.Spec.Groups[i]
		for j := range grp.Accelerators {
			accel := &grp.Accelerators[j]
			res := Resource{
				Group:  grp.ID,
				Device: accel.ID,
			}
			if _, existed := allocated[res]; !existed {
				continue
			}
			accels = append(accels, AllocatedAccelerator{Group: grp, Accel: accel})
		}
	}
	slices.SortStableFunc(accels, func(a, b AllocatedAccelerator) int {
		return cmp.Compare(a.Accel.Index, b.Accel.Index)
	})

	return accels
}

// AllocatedAcceleratorIDs 返回所给定加速器的 IDs，按其被持有的顺序排列——这就是每个厂商设备可见性变量所携
// 带的值。
func AllocatedAcceleratorIDs(accels []AllocatedAccelerator) []string {
	ids := make([]string, 0, len(accels))
	for i := range accels {
		ids = append(ids, accels[i].Accel.ID)
	}

	return ids
}
