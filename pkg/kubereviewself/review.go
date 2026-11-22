package kubereviewself

import (
	"context"
	"errors"
	"fmt"

	authz "k8s.io/api/authorization/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	authzcli "k8s.io/client-go/kubernetes/typed/authorization/v1"
)

type (
	// Review 保存用于权限评审的属性。
	Review = authz.SelfSubjectAccessReviewSpec
	// Reviews 是 Review 的列表。
	Reviews = []Review

	// DeniedError 表示某项 Review 目标被拒绝的错误。
	DeniedError struct {
		Review Review
	}
)

func (e DeniedError) Error() string {
	return fmt.Sprintf("denied %s", e.Review)
}

// CanDo 检查当前主体是否有权执行所有指定操作。
func CanDo(ctx context.Context, cli authzcli.SelfSubjectAccessReviewInterface, reviews Reviews) error {
	if len(reviews) == 0 {
		return errors.New("no self review to check")
	}

	for i := range reviews {
		sar := &authz.SelfSubjectAccessReview{
			Spec: reviews[i],
		}

		sar, err := cli.Create(ctx, sar, meta.CreateOptions{})
		if err != nil {
			return fmt.Errorf("create self subject access review %s: %w", reviews[i], err)
		}

		if !sar.Status.Allowed {
			return DeniedError{Review: reviews[i]}
		}
	}

	return nil
}
