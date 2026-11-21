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
	// Review holds the attributes for advanced reviewing.
	Review = authz.SelfSubjectAccessReviewSpec
	// Reviews is the list of Review.
	Reviews = []Review

	// DeniedError is an error indicate which Review target has been denied.
	DeniedError struct {
		Review Review
	}
)

func (e DeniedError) Error() string {
	return fmt.Sprintf("denied %s", e.Review)
}

// CanDo checks if the current subject can do all specified actions.
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
