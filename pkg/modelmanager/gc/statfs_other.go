//go:build !linux

package gc

import "errors"

// Statfs 仅支持 Linux：该插件运行在 Linux 节点上。
func Statfs(string) (Usage, error) {
	return Usage{}, errors.New("the model cache runs on Linux only")
}
