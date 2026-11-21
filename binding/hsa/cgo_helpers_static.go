// SPDX-FileCopyrightText: 2026 GPUStack, Inc.
// SPDX-License-Identifier: Apache-2.0

package hsa

/*
#cgo linux LDFLAGS: -Wl,--export-dynamic -Wl,--unresolved-symbols=ignore-in-object-files
#cgo darwin LDFLAGS: -Wl,-undefined,dynamic_lookup
#include "hsa_wrapper.h"
#include <stdlib.h>
#include "cgo_helpers.h"
*/
import "C"

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
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

type IterateAgentsCallback = func(agent Agent, data unsafe.Pointer) Return

var (
	cbRegistry sync.Map // map[uintptr]IterateAgentsCallback
	cbID       atomic.Uintptr
)

func registerCallback(cb IterateAgentsCallback) uintptr {
	id := cbID.Add(1)
	cbRegistry.Store(id, cb)
	return id
}

func unregisterCallback(id uintptr) {
	cbRegistry.Delete(id)
}

//export go_hsa_iterate_agents_cb
func go_hsa_iterate_agents_cb(agent C.hsa_agent_t, data unsafe.Pointer) C.hsa_status_t {
	if data == nil {
		return C.HSA_STATUS_ERROR
	}
	id := *(*uintptr)(data)

	v, ok := cbRegistry.Load(id)
	if !ok {
		return C.HSA_STATUS_ERROR
	}

	cb := v.(func(Agent, unsafe.Pointer) Return)
	return C.hsa_status_t(cb(*(*Agent)(unsafe.Pointer(&agent)), data))
}

func hsaIterateAgents(cb IterateAgentsCallback) Return {
	if cb == nil {
		return Return(C.HSA_STATUS_ERROR_INVALID_ARGUMENT)
	}

	id := registerCallback(cb)
	defer unregisterCallback(id)

	// HSA 在本次调用中同步完成枚举。传入仅含整数的地址，回调据此查找已注册的函数。
	ret := C.call_hsa_iterate_agents(unsafe.Pointer(&id))
	return Return(ret)
}

func hsaAgentGetInfoDevice(agent Agent) (uint32, Return) {
	var device uint32
	// cDevice := (*C.uint32_t)(unsafe.Pointer(&device))
	return device, hsaAgentGetInfo(agent, AGENT_INFO_DEVICE, unsafe.Pointer(&device))
}

func hsaAgentGetInfoUUID(agent Agent) (string, Return) {
	uuid := make([]byte, 21)
	// cUuid := (*C.char)(unsafe.Pointer(&uuid[0]))
	ret := hsaAgentGetInfo(agent, AgentInfo(AMD_AGENT_INFO_UUID), unsafe.Pointer(&uuid[0]))
	if !ret.IsSuccess() {
		return "", ret
	}
	return string(uuid[:clen(uuid)]), STATUS_SUCCESS
}

func hsaAgentGetInfoProduceName(agent Agent) (string, Return) {
	name := make([]byte, 64)
	// cName := (*C.char)(unsafe.Pointer(&name[0]))
	ret := hsaAgentGetInfo(agent, AgentInfo(AMD_AGENT_INFO_PRODUCT_NAME), unsafe.Pointer(&name[0]))
	if !ret.IsSuccess() {
		return "", ret
	}
	return string(name[:clen(name)]), STATUS_SUCCESS
}

func hsaAgentGetInfoName(agent Agent) (string, Return) {
	name := make([]byte, 64)
	// cName := (*C.char)(unsafe.Pointer(&name[0]))
	ret := hsaAgentGetInfo(agent, AGENT_INFO_NAME, unsafe.Pointer(&name[0]))
	if !ret.IsSuccess() {
		return "", ret
	}
	return string(name[:clen(name)]), STATUS_SUCCESS
}

func hsaAgentGetInfoComputeUnitCount(agent Agent) (uint32, Return) {
	var cuCount uint32
	// cCuCount := (*C.uint32_t)(unsafe.Pointer(&cuCount))
	return cuCount, hsaAgentGetInfo(agent, AgentInfo(AMD_AGENT_INFO_COMPUTE_UNIT_COUNT), unsafe.Pointer(&cuCount))
}

func hsaAgentGetInfoNumShaderEngines(agent Agent) (uint32, Return) {
	var numShaderEngines uint32
	return numShaderEngines, hsaAgentGetInfo(
		agent, AgentInfo(AMD_AGENT_INFO_NUM_SHADER_ENGINES), unsafe.Pointer(&numShaderEngines),
	)
}

func hsaAgentGetInfoNumShaderArraysPerSE(agent Agent) (uint32, Return) {
	var numShaderArraysPerSE uint32
	return numShaderArraysPerSE, hsaAgentGetInfo(
		agent, AgentInfo(AMD_AGENT_INFO_NUM_SHADER_ARRAYS_PER_SE), unsafe.Pointer(&numShaderArraysPerSE),
	)
}

func hsaAgentGetInfoNumXcc(agent Agent) (uint32, Return) {
	var numXcc uint32
	return numXcc, hsaAgentGetInfo(agent, AgentInfo(AMD_AGENT_INFO_NUM_XCC), unsafe.Pointer(&numXcc))
}

func hsaAgentGetInfoAsicFamilyId(agent Agent) (uint32, Return) {
	var familyId uint32
	// cFamilyId := (*C.uint32_t)(unsafe.Pointer(&familyId))
	return familyId, hsaAgentGetInfo(agent, AgentInfo(AMD_AGENT_INFO_ASIC_FAMILY_ID), unsafe.Pointer(&familyId))
}

func hsaAgentGetBDF(agent Agent) (string, Return) {
	var bdf uint64
	ret := hsaAgentGetInfo(agent, AgentInfo(AMD_AGENT_INFO_BDFID), unsafe.Pointer(&bdf))
	if !ret.IsSuccess() {
		return "", ret
	}
	domain := (bdf >> 32) & 0xFFFFFFFF
	bus := (bdf >> 8) & 0xFF
	deviceId := (bdf >> 3) & 0x1F
	function := bdf & 0x7
	return fmt.Sprintf("%04x:%02x:%02x.%x", domain, bus, deviceId, function), STATUS_SUCCESS
}
