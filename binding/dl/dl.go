package dl

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"unsafe"
)

// #cgo LDFLAGS: -ldl
// #include <dlfcn.h>
// #include <stdlib.h>
import "C"

const (
	RTLD_LAZY     = C.RTLD_LAZY
	RTLD_NOW      = C.RTLD_NOW
	RTLD_GLOBAL   = C.RTLD_GLOBAL
	RTLD_LOCAL    = C.RTLD_LOCAL
	RTLD_NODELETE = C.RTLD_NODELETE
	RTLD_NOLOAD   = C.RTLD_NOLOAD
)

type DynamicLibrary struct {
	name  string
	flags int

	// mu 保护句柄、打开代次和符号缓存。不同 goroutine 可在加载或释放库的同时探测符号，必须在此同步，
	// 避免并发读写缓存 map 导致整个设备管理进程退出。
	mu     sync.RWMutex
	handle unsafe.Pointer
	path   string
	// generation 记录库的打开代次。关闭后重新打开可能得到相同地址，驱动也可能已升级。
	// 代次可防止旧 dlsym 结果写入新库缓存。
	generation uint64
	// caches 记录各符号的查询结果，成功和缺失都会缓存。同一次打开期间导出表保持不变。
	caches map[string]error
}

func New(name string, flags int) *DynamicLibrary {
	return (&DynamicLibrary{
		name:  name,
		flags: flags,
	}).init()
}

func (dl *DynamicLibrary) reset() {
	_ = dl.init()
}

// init 将受保护字段恢复到未打开状态。构造时对象尚未发布，关闭时调用方已持写锁，因此这里不再加锁。
func (dl *DynamicLibrary) init() *DynamicLibrary {
	dl.handle = nil
	dl.path = func() string {
		if strings.Contains(dl.name, "/") {
			return dl.name
		}
		return ""
	}()
	dl.generation++
	dl.caches = make(map[string]error)
	return dl
}

func withOSLock(action func() error) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	return action()
}

func dlError() error {
	lastErr := C.dlerror()
	if lastErr == nil {
		return nil
	}
	return errors.New(C.GoString(lastErr))
}

func (dl *DynamicLibrary) Open() error {
	name := C.CString(dl.name)
	defer C.free(unsafe.Pointer(name))

	dl.mu.Lock()
	defer dl.mu.Unlock()

	if err := withOSLock(func() error {
		handle := C.dlopen(name, C.int(dl.flags))
		if handle == nil {
			return dlError()
		}
		dl.handle = handle
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (dl *DynamicLibrary) Close() error {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	if dl.handle == nil {
		return nil
	}
	if err := withOSLock(func() error {
		if C.dlclose(dl.handle) != 0 {
			return dlError()
		}
		dl.reset()
		return nil
	}); err != nil {
		return err
	}
	return nil
}

// Lookup 检查并缓存动态库导出的符号。句柄和缓存要在同一临界区读取，耗时的 dlsym 调用在锁外执行，
// 避免串行化各厂商的热路径。
func (dl *DynamicLibrary) Lookup(symbol string) error {
	dl.mu.RLock()
	handle, generation, cachedErr, cached := dl.handle, dl.generation, error(nil), false
	if handle != nil {
		cachedErr, cached = dl.caches[symbol]
	}
	dl.mu.RUnlock()

	if handle == nil {
		return fmt.Errorf("error looking up %q: %w", symbol, errors.New("library not loaded"))
	}
	if cached {
		return cachedErr
	}

	sym := C.CString(symbol)
	defer C.free(unsafe.Pointer(sym))

	var pointer unsafe.Pointer
	lookupErr := withOSLock(func() error {
		// 先调用 dlError 清除当前线程残留的错误。
		_ = dlError()
		pointer = C.dlsym(handle, sym)
		if pointer == nil {
			return fmt.Errorf("symbol %q not found: %w", symbol, dlError())
		}
		return nil
	})

	// 仅当打开代次仍一致时写回查询结果，避免在关闭、重开期间把旧库的符号写入新缓存。
	dl.mu.Lock()
	if dl.handle == handle && dl.generation == generation {
		dl.caches[symbol] = lookupErr
	}
	dl.mu.Unlock()

	return lookupErr
}
