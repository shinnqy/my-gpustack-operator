// SPDX-FileCopyrightText: 2026 GPUStack, Inc.
// SPDX-License-Identifier: Apache-2.0

package dmi

/*
#cgo LDFLAGS: -ldl
#cgo CFLAGS: -w
#include "dmi_mig_wrapper.h"
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

// 下面的转换直接交出 Go 对象的地址。GPU 实例和计算实例的落位查询通过首元素地址和独立的长度填充整个数组。
// 生成脚本移除 cgo_helpers.go，因此这里提供所需方法。cgo 会检查指针所在的整个堆对象，
// 调用时要传完整对象或数组元素，不能传含 Go 指针结构体的内部字段。

// PassRef 返回此对象对应的 C 指针。
func (x *PciInfo) PassRef() (*C.struct_nvmlPciInfo_st, *cgoAllocMap) {
	return (*C.struct_nvmlPciInfo_st)(unsafe.Pointer(x)), cgoAllocsUnknown
}

// PassRef 返回此对象对应的 C 指针。
func (x *DeviceAttributes) PassRef() (*C.struct_nvmlDeviceAttributes_st, *cgoAllocMap) {
	return (*C.struct_nvmlDeviceAttributes_st)(unsafe.Pointer(x)), cgoAllocsUnknown
}

// PassRef 返回此对象对应的 C 指针。
func (x *Utilization) PassRef() (*C.struct_nvmlUtilization_st, *cgoAllocMap) {
	return (*C.struct_nvmlUtilization_st)(unsafe.Pointer(x)), cgoAllocsUnknown
}

// PassRef 返回此对象对应的 C 指针。
func (x *Memory) PassRef() (*C.struct_nvmlMemory_st, *cgoAllocMap) {
	return (*C.struct_nvmlMemory_st)(unsafe.Pointer(x)), cgoAllocsUnknown
}

// PassRef 返回此对象对应的 C 指针。
func (x *GpuInstanceProfileInfo) PassRef() (*C.struct_nvmlGpuInstanceProfileInfo_st, *cgoAllocMap) {
	return (*C.struct_nvmlGpuInstanceProfileInfo_st)(unsafe.Pointer(x)), cgoAllocsUnknown
}

// PassRef 返回此对象对应的 C 指针。
func (x *ComputeInstanceProfileInfo) PassRef() (*C.struct_nvmlComputeInstanceProfileInfo_st, *cgoAllocMap) {
	return (*C.struct_nvmlComputeInstanceProfileInfo_st)(unsafe.Pointer(x)), cgoAllocsUnknown
}

// PassRef 指向原对象，使落位查询能从首元素开始填充整块数组。
func (x *GpuInstancePlacement) PassRef() (*C.struct_nvmlGpuInstancePlacement_st, *cgoAllocMap) {
	return (*C.struct_nvmlGpuInstancePlacement_st)(unsafe.Pointer(x)), cgoAllocsUnknown
}

// PassRef 使用原数组的地址填充计算实例的落位，与 GPU 实例查询采用相同的方式。
func (x *ComputeInstancePlacement) PassRef() (*C.struct_nvmlComputeInstancePlacement_st, *cgoAllocMap) {
	return (*C.struct_nvmlComputeInstancePlacement_st)(unsafe.Pointer(x)), cgoAllocsUnknown
}

// PassRef 返回此对象对应的 C 指针。
func (x *GpuInstanceInfo) PassRef() (*C.struct_nvmlGpuInstanceInfo_st, *cgoAllocMap) {
	return (*C.struct_nvmlGpuInstanceInfo_st)(unsafe.Pointer(x)), cgoAllocsUnknown
}

// PassRef 返回此对象对应的 C 指针。
func (x *ComputeInstanceInfo) PassRef() (*C.struct_nvmlComputeInstanceInfo_st, *cgoAllocMap) {
	return (*C.struct_nvmlComputeInstanceInfo_st)(unsafe.Pointer(x)), cgoAllocsUnknown
}
