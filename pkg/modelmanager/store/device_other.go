//go:build !linux

package store

import "errors"

// Device 仅支持 Linux：该插件运行在 Linux 节点上。
func Device(string) (string, error) {
	return "", errors.New("the model cache runs on Linux only")
}
