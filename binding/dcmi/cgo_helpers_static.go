// SPDX-FileCopyrightText: 2026 GPUStack, Inc.
// SPDX-License-Identifier: Apache-2.0

package dcmi

/*
#cgo LDFLAGS: -ldl
#cgo CFLAGS: -w
#include "dcmi_wrapper.h"
#include <stdlib.h>
#include "cgo_helpers.h"
*/
import "C"

import (
	"runtime"
	"unsafe"
)

// stringHeader 描述 Go 字符串在内存中的结构。
type stringHeader struct {
	Data unsafe.Pointer
	Len  int
}

// cgoAllocMap 用于登记 C 内存的清理动作，调用方持有它期间可继续使用该内存。
type cgoAllocMap struct{}

var cgoAllocsUnknown = new(cgoAllocMap)

// clen 计算字节数组从起点到第一个 NUL 的长度。
func clen(n []byte) int {
	for i := 0; i < len(n); i++ {
		if n[i] == 0 {
			return i
		}
	}
	return len(n)
}

// unpackPCharString 用 C.CString 复制字符串，并登记对应的 C.free 清理动作。
func unpackPCharString(str string) (*C.char, *cgoAllocMap) {
	allocs := new(cgoAllocMap)

	mem0 := unsafe.Pointer(C.CString(str))
	runtime.AddCleanup(allocs, func(mem0 unsafe.Pointer) {
		C.free(mem0)
	}, mem0)
	return (*C.char)(mem0), allocs
}

// packPCharString 用 C 字符指针指向的内存构造 Go 字符串，避免复制。
func packPCharString(p *C.char) (raw string) {
	if p != nil && *p != 0 {
		h := (*stringHeader)(unsafe.Pointer(&raw))
		h.Data = unsafe.Pointer(p)
		for *p != 0 {
			p = (*C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(p)) + 1)) // p++.
		}
		h.Len = int(uintptr(unsafe.Pointer(p)) - uintptr(h.Data))
	}
	return raw
}

/*
 */

// PassRef 返回此对象对应的 C 指针。
func (x *ChipInfo) PassRef() (*C.struct_dcmi_chip_info, *cgoAllocMap) {
	return (*C.struct_dcmi_chip_info)(unsafe.Pointer(x)), cgoAllocsUnknown
}

// PassRef 返回此对象对应的 C 指针。
func (info *ChipInfoV2) PassRef() (*C.struct_dcmi_chip_info_v2, *cgoAllocMap) {
	return (*C.struct_dcmi_chip_info_v2)(unsafe.Pointer(info)), cgoAllocsUnknown
}

// PassRef 返回此对象对应的 C 指针。
func (x *SocDie) PassRef() (*C.struct_dcmi_soc_die_stru, *cgoAllocMap) {
	return (*C.struct_dcmi_soc_die_stru)(unsafe.Pointer(x)), cgoAllocsUnknown
}

// PassRef 返回此对象对应的 C 指针。
func (dieId *DieId) PassRef() (*C.struct_dcmi_die_id, *cgoAllocMap) {
	return (*C.struct_dcmi_die_id)(unsafe.Pointer(dieId)), cgoAllocsUnknown
}

// PassRef 返回此对象对应的 C 指针。
func (x *EccInfo) PassRef() (*C.struct_dcmi_ecc_info, *cgoAllocMap) {
	return (*C.struct_dcmi_ecc_info)(unsafe.Pointer(x)), cgoAllocsUnknown
}

// PassRef 返回此对象对应的 C 指针。
func (ipAddr *IpAddr) PassRef() (*C.struct_dcmi_ip_addr, *cgoAllocMap) {
	return (*C.struct_dcmi_ip_addr)(unsafe.Pointer(ipAddr)), cgoAllocsUnknown
}

// PassRef 返回此对象对应的 C 指针。
func (x *HbmInfo) PassRef() (*C.struct_dcmi_hbm_info, *cgoAllocMap) {
	return (*C.struct_dcmi_hbm_info)(unsafe.Pointer(x)), cgoAllocsUnknown
}

// PassRef 返回此对象对应的 C 指针。
func (x *MemoryInfo) PassRef() (*C.struct_dcmi_memory_info, *cgoAllocMap) {
	return (*C.struct_dcmi_memory_info)(unsafe.Pointer(x)), cgoAllocsUnknown
}

// PassRef 返回此对象对应的 C 指针。
func (x *GetMemoryInfo) PassRef() (*C.struct_dcmi_get_memory_info_stru, *cgoAllocMap) {
	return (*C.struct_dcmi_get_memory_info_stru)(unsafe.Pointer(x)), cgoAllocsUnknown
}

// PassRef 返回此对象对应的 C 指针。
func (x *PcieInfo) PassRef() (*C.struct_dcmi_pcie_info, *cgoAllocMap) {
	return (*C.struct_dcmi_pcie_info)(unsafe.Pointer(x)), cgoAllocsUnknown
}

// PassRef 返回此对象对应的 C 指针。
func (info *PcieInfoAll) PassRef() (*C.struct_dcmi_pcie_info_all, *cgoAllocMap) {
	return (*C.struct_dcmi_pcie_info_all)(unsafe.Pointer(info)), cgoAllocsUnknown
}

// PassRef 返回此对象对应的 C 指针。
func (x *MultiUtilizationInfo) PassRef() (*C.struct_dcmi_multi_utilization_info, *cgoAllocMap) {
	return (*C.struct_dcmi_multi_utilization_info)(unsafe.Pointer(x)), cgoAllocsUnknown
}

// PassRef 传递 Go 对象自身的地址。进程显存和 UB ping mesh 接口会接收切片首元素及数量，并连续写入后续元素，
// 因而这里必须指向原数组。
func (x *ProcMemInfo) PassRef() (*C.struct_dcmi_proc_mem_info, *cgoAllocMap) {
	return (*C.struct_dcmi_proc_mem_info)(unsafe.Pointer(x)), cgoAllocsUnknown
}

// V2 的生成调用会为每个以结构体指针传递的参数调用相应的转换方法。生成脚本移除 cgo_helpers.go，
// 所需方法由本文件提供。通过 typedef 传入的参数由 c-for-go 直接用 unsafe.Pointer 转换，
// 因此 dcmi_urma_eid_info_t 无需额外方法。

// PassRef 返回此对象对应的 C 指针。
func (x *BoardInfo) PassRef() (*C.struct_dcmi_board_info, *cgoAllocMap) {
	return (*C.struct_dcmi_board_info)(unsafe.Pointer(x)), cgoAllocsUnknown
}

// PassRef 返回此对象对应的 C 指针。
func (x *PcieLinkBandwidthInfo) PassRef() (*C.struct_dcmi_pcie_link_bandwidth_info, *cgoAllocMap) {
	return (*C.struct_dcmi_pcie_link_bandwidth_info)(unsafe.Pointer(x)), cgoAllocsUnknown
}

// PassRef 返回此对象对应的 C 指针。
func (x *ElabelInfo) PassRef() (*C.struct_dcmi_elabel_info, *cgoAllocMap) {
	return (*C.struct_dcmi_elabel_info)(unsafe.Pointer(x)), cgoAllocsUnknown
}

// PassRef 返回此对象对应的 C 指针。
func (x *UbPingMeshOperate) PassRef() (*C.struct_dcmi_ub_ping_mesh_operate, *cgoAllocMap) {
	return (*C.struct_dcmi_ub_ping_mesh_operate)(unsafe.Pointer(x)), cgoAllocsUnknown
}

// PassRef 指向原数组的首元素，驱动会按另外传入的数量填充整组 ping mesh 响应。
func (x *UbPingMeshInfo) PassRef() (*C.struct_dcmi_ub_ping_mesh_info, *cgoAllocMap) {
	return (*C.struct_dcmi_ub_ping_mesh_info)(unsafe.Pointer(x)), cgoAllocsUnknown
}

// PassRef 返回此对象对应的 C 指针。
func (x *UbPortInfo) PassRef() (*C.struct_dcmi_ub_port_info, *cgoAllocMap) {
	return (*C.struct_dcmi_ub_port_info)(unsafe.Pointer(x)), cgoAllocsUnknown
}

// PassRef 返回此对象对应的 C 指针。
func (x *PortPktStatsInfo) PassRef() (*C.struct_dcmi_port_pkt_stats_info, *cgoAllocMap) {
	return (*C.struct_dcmi_port_pkt_stats_info)(unsafe.Pointer(x)), cgoAllocsUnknown
}
