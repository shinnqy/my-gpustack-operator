package deviceplugin

import (
	"fmt"
	"os"
	"path/filepath"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

// AcceleratorInjectionPreflighter 向预检进程提供厂商分配器本身的响应器。
// 可调用通用或逻辑切片接口生成注入文件。物理分区接口会实际创建硬件实例，预检只读分区。
type AcceleratorInjectionPreflighter interface {
	// PreflightResponder 按模式构建分配响应器，将其文件写到临时位置。成功后调用方必须 defer 恢复函数，
	// 各厂商额外使用的私有目录也由该函数还原。
	PreflightResponder(mode workercore.DeviceAllocationMode) (ContainerAllocateResponder, func(), error)
}

// NewPreflightRedirect 建立临时目录，重定向通用路径和厂商传入的私有路径。返回的函数会逐项恢复并删除临时目录。
func NewPreflightRedirect(private ...*string) (root string, restore func(), err error) {
	root, err = os.MkdirTemp("", "gpustack-preflight-")
	if err != nil {
		return "", nil, fmt.Errorf("create preflight scratch directory: %w", err)
	}

	restoreShared := RedirectHostWrites(root)
	restorePrivate := redirectPrivatePaths(root, private)
	return root, func() {
		restorePrivate()
		restoreShared()
		_ = os.RemoveAll(root)
	}, nil
}

// preflightRehosts 保存活动重定向的临时路径与宿主机路径的对应关系，仅供一次性预检进程使用。
var preflightRehosts = map[string]string{}

// PreflightRehosts 返回当前映射的副本。调用方在恢复之前用它把注入中的临时路径改写为节点上可挂载的路径。
func PreflightRehosts() map[string]string {
	out := make(map[string]string, len(preflightRehosts))
	for scratch, hostPath := range preflightRehosts {
		out[scratch] = hostPath
	}
	return out
}

// redirectPrivatePaths 将各个厂商私有路径移入临时目录，保留宿主机原始路径，并返回撤销本次重定向的函数。
func redirectPrivatePaths(root string, private []*string) (restore func()) {
	// 嵌套重定向时，变量先恢复到上一层路径。挂载文件始终要映射到最初的宿主机路径。
	type moved struct {
		path      *string
		restoreTo string
		dest      string
	}

	movements := make([]moved, 0, len(private))
	for i, path := range private {
		if path == nil {
			continue
		}
		restoreTo := *path
		host := restoreTo
		if outer, redirected := preflightRehosts[host]; redirected {
			host = outer
		}
		// 用序号区分同名目录，防止不同的厂商路径映射到同一位置。
		dest := filepath.Join(root, fmt.Sprintf("%d-%s", i, filepath.Base(host)))

		*path = dest
		preflightRehosts[dest] = host
		movements = append(movements, moved{path: path, restoreTo: restoreTo, dest: dest})
	}

	return func() {
		for i := len(movements) - 1; i >= 0; i-- {
			m := movements[i]
			delete(preflightRehosts, m.dest)
			*m.path = m.restoreTo
		}
	}
}

// RedirectHostWrites 临时替换响应器写入库和 Pod 文件的两个进程级目录。只能在一次性命令或独立测试中使用，
// 避免影响正在服务 kubelet 的响应器。
func RedirectHostWrites(root string) (restore func()) {
	origLib, origPods := OperatorLibDir, OperatorPodsDir
	OperatorLibDir = filepath.Join(root, "lib")
	OperatorPodsDir = filepath.Join(root, "pods")

	return func() {
		OperatorLibDir, OperatorPodsDir = origLib, origPods
	}
}
