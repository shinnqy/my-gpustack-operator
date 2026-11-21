// SPDX-FileCopyrightText: 2026 GPUStack, Inc.
// SPDX-License-Identifier: Apache-2.0

package mxsml

/*
#include <stdlib.h>
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
