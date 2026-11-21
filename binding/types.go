package binding

import (
	"errors"
	"fmt"
	"sync"

	"github.com/go-logr/logr"

	"gpustack.ai/gpustack/binding/dl"
)

import "C"

// ErrLibraryNotLoaded 表示尝试操作尚未加载的库。
var ErrLibraryNotLoaded = errors.New("library not loaded")

type (
	// DynamicLibrary 抽象底层库提供的符号查询。
	DynamicLibrary interface {
		// Lookup 检查共享库是否导出指定符号。
		Lookup(string) error
	}

	// LibraryCallbackFunc 表示共享库加载后可调用的回调。
	LibraryCallbackFunc = func(DynamicLibrary) error

	// libraryOptions 保存由 LibraryOption 配置的加载参数。
	libraryOptions struct {
		flags           int
		logger          logr.Logger
		loadCallbacks   []LibraryCallbackFunc
		unloadCallbacks []LibraryCallbackFunc
	}

	// LibraryOption 用于配置动态库实例。
	LibraryOption func(*libraryOptions)
)

// WithLibraryLoadFlags 指定调用动态链接器时使用的标志。
func WithLibraryLoadFlags(flags int) LibraryOption {
	return func(o *libraryOptions) {
		o.flags = flags
	}
}

// WithLogger 设置动态库加载和查询时使用的日志器。
func WithLogger(logger logr.Logger) LibraryOption {
	return func(o *libraryOptions) {
		o.logger = logger
	}
}

type (
	Library interface {
		// Load 加载动态库。重复加载已打开的库也能正常返回。
		Load() error
		// Unload 释放动态库。对已经释放的库重复调用可正常返回。
		Unload() error
		// Lookup 判断已加载的库是否导出某个符号，调用前须先 Load。
		Lookup(string) error
		// Path 返回共享库的路径。
		Path() string
	}
	library struct {
		sync.Mutex
		lg                logr.Logger
		rc                int
		dl                *dl.DynamicLibrary
		dlPath            string
		dlLoadCallbacks   []LibraryCallbackFunc
		dlUnloadCallbacks []LibraryCallbackFunc
	}
)

// NewLibrary 用给定候选路径和选项构造库对象。
//   - 先选路径，查找时依次处理候选绝对路径、`LD_LIBRARY_PATH` 和 ldconfig 缓存，均未找到就保留第一个候选名交给加载器
func NewLibrary(paths []string, opts ...LibraryOption) Library {
	p := GetLibFromPaths(paths)

	o := &libraryOptions{
		flags:  dl.RTLD_LAZY | dl.RTLD_GLOBAL,
		logger: logr.Discard(),
	}
	for i := range opts {
		opts[i](o)
	}

	return &library{
		lg:                o.logger,
		dl:                dl.New(p, o.flags),
		dlPath:            p,
		dlLoadCallbacks:   o.loadCallbacks,
		dlUnloadCallbacks: o.unloadCallbacks,
	}
}

// Load 加载动态库。重复加载已打开的库也能正常返回。
func (l *library) Load() (err error) {
	l.Lock()
	defer l.Unlock()

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic loading library %s: %v", l.dlPath, r)
		}
		if err == nil {
			l.rc++
		}
	}()
	if l.rc > 0 {
		l.lg.Info("library already loaded, skipping", "path", l.dlPath)
		return nil
	}

	if err = l.dl.Open(); err != nil {
		l.lg.Error(err, "error loading library", "path", l.dlPath)
		return fmt.Errorf("error loading %s: %w", l.dlPath, err)
	}

	if len(l.dlLoadCallbacks) != 0 {
		l.lg.Info("library loaded, running load callback", "path", l.dlPath)
		for i := range l.dlLoadCallbacks {
			if err = l.dlLoadCallbacks[i](l.dl); err != nil {
				l.lg.Error(err, "error in load callback, unloading library", "path", l.dlPath)
				_ = l.dl.Close()
				return fmt.Errorf("error in load callback for %s: %w", l.dlPath, err)
			}
		}
	}

	l.lg.Info("library loaded", "path", l.dlPath)

	return nil
}

func (l *library) Unload() (err error) {
	l.Lock()
	defer l.Unlock()

	if l.rc == 0 {
		return ErrLibraryNotLoaded
	}
	defer func() {
		if err == nil {
			l.rc--
		}
	}()

	if l.rc != 1 {
		l.lg.Info("library still in use, skipping unload", "path", l.dlPath, "refCount", l.rc-1)
		return nil
	}

	if err = l.dl.Close(); err != nil {
		l.lg.Error(err, "error unloading library", "path", l.dlPath)
		return fmt.Errorf("error unloading %s: %w", l.dlPath, err)
	}

	if len(l.dlUnloadCallbacks) != 0 {
		l.lg.Info("library unloaded, running unload callback", "path", l.dlPath)
		for i := range l.dlUnloadCallbacks {
			if err = l.dlUnloadCallbacks[i](l.dl); err != nil {
				l.lg.Error(err, "error in unload callback", "path", l.dlPath)
				return fmt.Errorf("error in unload callback for %s: %w", l.dlPath, err)
			}
		}
	}

	l.lg.Info("library unloaded", "path", l.dlPath)

	return nil
}

func (l *library) Lookup(symbol string) error {
	if l.rc == 0 {
		return fmt.Errorf("error looking symbol %s: %w", symbol, ErrLibraryNotLoaded)
	}

	if err := l.dl.Lookup(symbol); err != nil {
		// 调用方依靠符号缺失判断驱动版本，并据此回退到旧接口。只有确实需要该符号的调用才应把返回的错误作为故障。
		l.lg.Info("symbol not found", "symbol", symbol, "path", l.dlPath)
		return fmt.Errorf("error looking up symbol %s in %s: %w", symbol, l.dlPath, err)
	}

	return nil
}

func (l *library) Path() string {
	return l.dlPath
}
