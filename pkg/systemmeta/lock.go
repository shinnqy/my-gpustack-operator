package systemmeta

import (
	"slices"
)

// LockedResourceFinalizer 是用于标识资源已被系统锁定的 finalizer。
const LockedResourceFinalizer = "gpustack.ai/controlled"

// Lock 向指定资源添加 finalizer。
//
// 如果资源已被托管，返回 true，
// 否则返回 false。
//
// 返回值以检测到的状态命名，而非操作本身，这正是其命名所要表达的含义：
// true 表示 finalizer 原本就存在且未发生变更，这与字面上理解的“已加锁”相反。
// 编辑器在调用处提示的是函数签名而非这段注释，因此若返回值按操作结果命名，
// 会误导调用方在无需写回的分支上执行更新。此前已有一个对象因此带上了一个永远不会被移除的 finalizer。
//
//	if !systemmeta.Lock(obj) {
//	    _, _ = kubeclientset.UpdateWithCtrlClient(ctx, r.Client, obj)
//	}
func Lock(obj MetaObject) (alreadyLocked bool) {
	if obj == nil {
		panic("object is nil")
	}

	fs := obj.GetFinalizers()
	if slices.Contains(fs, LockedResourceFinalizer) {
		return true
	}

	fs = append(fs, LockedResourceFinalizer)
	obj.SetFinalizers(fs)
	return false
}

// Unlock 从指定资源移除 finalizer。
//
// 如果资源未被托管，返回 true，
// 否则返回 false。
//
// 与上文的 Lock 同理，返回值同样以检测到的状态命名：
// true 表示 finalizer 原本就不存在且本次调用未移除任何内容。若按操作结果来理解，
// 语义则完全相反——真正需要写回的分支是 false 分支。
//
//	if systemmeta.Unlock(obj) {
//	    return ctrl.Result{}, nil
//	}
//	// 清理
//	_, _ = kubeclientset.UpdateWithCtrlClient(ctx, r.Client, obj)
func Unlock(obj MetaObject) (alreadyUnlocked bool) {
	if obj == nil {
		panic("object is nil")
	}

	fs := obj.GetFinalizers()
	fs2 := slices.DeleteFunc(fs, func(s string) bool {
		return s == LockedResourceFinalizer
	})
	if len(fs) == len(fs2) {
		return true
	}
	obj.SetFinalizers(fs2)
	return false
}

// IsLocked 在资源被系统锁定时返回 true。
func IsLocked(obj MetaObject) (locked bool) {
	if obj == nil {
		panic("object is nil")
	}

	fs := obj.GetFinalizers()
	return slices.IndexFunc(fs, func(s string) bool {
		return s == LockedResourceFinalizer
	}) != -1
}
