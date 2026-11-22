package review

import (
	"context"

	authz "k8s.io/api/authorization/v1"
	authzcli "k8s.io/client-go/kubernetes/typed/authorization/v1"

	"gpustack.ai/gpustack/pkg/kubereviewself"
)

type (
	// Simple 保存用于简化权限审查的属性。
	Simple struct {
		// Namespace 是所请求操作的 Namespace。
		// 当前未区分无 Namespace 与所有 Namespace。
		Namespace string
		// Group 是资源的 API Group。
		// "*" 表示全部。
		Group string
		// Version 是资源的 API Version。
		// "*" 表示全部。
		Version string
		// Resource 是现有的资源类型之一。
		// "*" 表示全部。
		Resource string
		// Name 是用于 "get" 请求或 "delete" 删除的资源名称。
		// ""（空字符串）表示全部。
		Name string
	}
	// Simples 是 Simple 的列表。
	Simples = []Simple
)

type CreateOption func([]string, string) ([]string, string)

// WithUpdateIfExisted 检查主体是否可以使用 kubeclientset.WithUpdateIfExisted
// 对指定资源执行 kubeclientset.Create。
func WithUpdateIfExisted() CreateOption {
	return func(verbs []string, subres string) ([]string, string) {
		return append(verbs, "update"), subres
	}
}

// WithRecreateIfDuplicated 检查主体是否可以使用 kubeclientset.WithRecreateIfDuplicated
// 对指定资源执行 kubeclientset.Create。
func WithRecreateIfDuplicated() CreateOption {
	return func(verbs []string, subres string) ([]string, string) {
		return append(verbs, "delete"), subres
	}
}

// CanDoCreate 检查当前主体是否可以对指定资源执行 kubeclientset.Create。
func CanDoCreate(ctx context.Context,
	cli authzcli.SelfSubjectAccessReviewInterface, reviews Simples,
	opts ...CreateOption,
) error {
	verbs := []string{"get", "create"}
	subres := ""
	for i := range opts {
		verbs, subres = opts[i](verbs, subres)
	}

	return canDoSimple(ctx, cli, reviews, verbs, subres)
}

type UpdateOption func([]string, string) ([]string, string)

// WithCreateIfNotExisted 检查主体是否可以使用 kubeclientset.WithCreateIfNotExisted
// 对指定资源执行 kubeclientset.Update。
func WithCreateIfNotExisted() UpdateOption {
	return func(verbs []string, subres string) ([]string, string) {
		return append(verbs, "create"), subres
	}
}

// CanDoUpdate 检查当前主体是否可以对指定资源执行 kubeclientset.Update。
func CanDoUpdate(ctx context.Context,
	cli authzcli.SelfSubjectAccessReviewInterface, reviews Simples,
	opts ...UpdateOption,
) error {
	verbs := []string{"get", "update"}
	subres := ""
	for i := range opts {
		verbs, subres = opts[i](verbs, subres)
	}

	return canDoSimple(ctx, cli, reviews, verbs, subres)
}

func canDoSimple(
	ctx context.Context,
	cli authzcli.SelfSubjectAccessReviewInterface, reviews Simples,
	verbs []string, subresource string,
) error {
	advances := make(kubereviewself.Reviews, 0, len(reviews)*len(verbs))
	for i := range reviews {
		simple := &reviews[i]
		for _, verb := range verbs {
			advances = append(advances, kubereviewself.Review{
				ResourceAttributes: &authz.ResourceAttributes{
					Verb:        verb,
					Group:       simple.Group,
					Version:     simple.Version,
					Resource:    simple.Resource,
					Subresource: subresource,
					Namespace:   simple.Namespace,
					Name:        simple.Name,
				},
			})
		}
	}

	return kubereviewself.CanDo(ctx, cli, advances)
}
