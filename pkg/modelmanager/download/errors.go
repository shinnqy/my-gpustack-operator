// Package download 从模型仓库 Hub 拉取清单中的文件，并在字节到达的同时按清单逐字节校验。
package download

import (
	"errors"
	"fmt"
)

// 下载失败的原因分类表。节点侧的 NodeModelStore 条目、Prometheus 指标以及挂载错误信息均直接使用这些原因，
// 不再对错误消息文本做额外解析。
const (
	// ReasonInvalidRequest 表示当前配置无法执行下载。
	ReasonInvalidRequest = "InvalidRequest"
	// ReasonAccessDenied 表示 Hub 拒绝了凭据或文件不可访问。
	ReasonAccessDenied = "AccessDenied"
	// ReasonSourceUnavailable 表示无法到达 Hub、Hub 失败，或文件分片反复重试后仍失败。
	ReasonSourceUnavailable = "SourceUnavailable"
	// ReasonIntegrityMismatch 表示下载内容或清单与其摘要不匹配。
	ReasonIntegrityMismatch = "IntegrityMismatch"
	// ReasonInsufficientCapacity 表示缓存磁盘已无足够空间。
	ReasonInsufficientCapacity = "InsufficientCapacity"
	// ReasonCanceled 表示已无等待挂载的 Pod 而被取消的下载尝试。
	ReasonCanceled = "Canceled"
)

// Error 是已分类的下载失败错误。其 Message 仅使用清单路径和调用方传入的 URL，
// 绝不暴露可能带有签名的重定向目标地址，也绝不携带凭据。
//
// Detail 是不包含任何租户信息的失败摘要：如 HTTP 状态码、超时、TLS 故障或节点本地状态。
// 同一摘要由解析到它的所有租户共享，因此节点对外发布的失败状态只包含 Reason 和 Detail；
// 包含仓库 URL 的完整 Message 仅输出到插件日志。
type Error struct {
	Reason  string
	Message string
	Detail  string
}

func (e *Error) Error() string { return e.Reason + ": " + e.Message }

func errorf(reason, format string, args ...any) error {
	return &Error{Reason: reason, Message: fmt.Sprintf(format, args...)}
}

func detailedf(reason, detail, format string, args ...any) error {
	return &Error{Reason: reason, Message: fmt.Sprintf(format, args...), Detail: detail}
}

// DetailOf 返回 err 错误链中首个 Error 的 Detail；不存在时返回 ""。
func DetailOf(err error) string {
	if e, ok := errors.AsType[*Error](err); ok {
		return e.Detail
	}

	return ""
}

// ReasonOf 返回 err 错误链中已分类失败的原因；未分类错误返回 SourceUnavailable，nil 返回 ""。
func ReasonOf(err error) string {
	if err == nil {
		return ""
	}
	if e, ok := errors.AsType[*Error](err); ok {
		return e.Reason
	}

	return ReasonSourceUnavailable
}
