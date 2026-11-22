package ctrlclix

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
)

type ResourceVersion string

func (v ResourceVersion) ApplyToGet(opts *ctrlcli.GetOptions) {
	if opts.Raw == nil {
		opts.Raw = &meta.GetOptions{}
	}
	opts.Raw.ResourceVersion = string(v)
}

func (v ResourceVersion) ApplyToList(opts *ctrlcli.ListOptions) {
	if opts.Raw == nil {
		opts.Raw = &meta.ListOptions{}
	}
	opts.Raw.ResourceVersion = string(v)
}

// WithoutQuorum 给 Get 或 List 指定 resourceVersion 为 "0"。请求实际到达 API 服务器时，
// 它允许返回稍旧的视图，省去等待读取 etcd 法定多数的延迟。
// 这适用于 APIReader 或绕过 manager 缓存的类型。由 informer 缓存响应的读取直接查本地索引，
// 不会读取这个选项。
const WithoutQuorum = ResourceVersion("0")

// ToPatchOptions 将更新选项中同样适用于补丁的字段传给 Patch 调用。
func ToPatchOptions(in ctrlcli.UpdateOptions) *ctrlcli.PatchOptions {
	return &ctrlcli.PatchOptions{
		DryRun:          in.DryRun,
		FieldManager:    in.FieldManager,
		FieldValidation: in.FieldValidation,
	}
}

// Terminated 将删除宽限期设为零，立即发起终止。终结器仍可让对象保留到清理完成。
const Terminated = ctrlcli.GracePeriodSeconds(0)
