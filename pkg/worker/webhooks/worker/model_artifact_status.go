package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	authn "k8s.io/api/authentication/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrladmission "sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubeclients/kubernetes"
	"gpustack.ai/gpustack/pkg/system"
)

// modelArtifactStatusRuleWriter 命名了“仅允许 worker 写入 status”的规则，以便在拒绝请求时指明触发的具体规则。
const modelArtifactStatusRuleWriter = "only the worker writes a ModelArtifact's status"

// validateModelArtifactStatus 仅放行来自 worker 自身身份的 status 子资源更新请求。
//
// 节点依据该 status 挂载租户的模型权重：即解析自 Pod 所在 namespace、具有该 UID 与 digest 的制品。
// 若租户能够写入该 status，便可伪造节点上的任意摘要。本 chart 安装的任何角色均未向租户授予该子资源权限，
// 但管理员授予的宽泛角色可能会在无意中放开权限，因此必须在此处强制执行该规则，而不能假定 RBAC 已做限制。
func (r *ModelArtifactWebhook) validateModelArtifactStatus(ctx context.Context, ma *workercore.ModelArtifact) error {
	req, err := ctrladmission.RequestFromContext(ctx)
	if err != nil {
		return kerrors.NewInternalError(fmt.Errorf("read the admission request: %w", err))
	}
	if r.worker == "" || req.UserInfo.Username != r.worker {
		return kerrors.NewInvalid(workercore.SchemeGroupVersionKind("ModelArtifact").GroupKind(), ma.Name,
			field.ErrorList{field.Forbidden(field.NewPath("status"), fmt.Sprintf(
				"%s: %q is not the worker", modelArtifactStatusRuleWriter, req.UserInfo.Username,
			))})
	}

	return nil
}

// isStatusRequest 返回 ctx 中是否携带了针对 status 子资源的准入请求。
func isStatusRequest(ctx context.Context) bool {
	req, err := ctrladmission.RequestFromContext(ctx)
	return err == nil && req.SubResource == "status"
}

// selfUsername 向 API server 查询 worker 自身请求所携带的身份，因此无论 worker 使用 ServiceAccount 还是 kubeconfig 用户运行，结果均有效。
func selfUsername(ctx context.Context) (string, error) {
	return learnUsername(ctx, system.LoopbackKubeClient.Get(), loopbackBearerToken)
}

// learnUsername 向 API server 查询 cli 的身份。Kubernetes 1.28 起提供 SelfSubjectReview；
// 对于更早版本的服务端，则改为提交 cli 所发送 bearer token 的 TokenReview，同样可返回经认证的用户名。
// 若两种方式均不可用，则 status 规则将没有任何可放行的写入方，因此发生错误时会中止 worker，而不是关闭该规则。
func learnUsername(ctx context.Context, cli kubernetes.Interface, token func() (string, error)) (string, error) {
	review, err := cli.AuthenticationV1().SelfSubjectReviews().Create(ctx, &authn.SelfSubjectReview{}, meta.CreateOptions{})
	switch {
	case err == nil:
		if review.Status.UserInfo.Username == "" {
			return "", errors.New("self subject review answered no username")
		}
		return review.Status.UserInfo.Username, nil
	case !kerrors.IsNotFound(err):
		return "", fmt.Errorf("self subject review: %w", err)
	}

	t, err := token()
	if err != nil {
		return "", fmt.Errorf("the API server serves no SelfSubjectReview (Kubernetes 1.28 and later), "+
			"and a TokenReview needs the worker's bearer token: %w", err)
	}
	tr, err := cli.AuthenticationV1().TokenReviews().Create(ctx,
		&authn.TokenReview{Spec: authn.TokenReviewSpec{Token: t}}, meta.CreateOptions{})
	if err != nil {
		return "", fmt.Errorf("token review: %w", err)
	}
	if !tr.Status.Authenticated || tr.Status.User.Username == "" {
		return "", fmt.Errorf("token review did not authenticate the worker's own token: %s", tr.Status.Error)
	}

	return tr.Status.User.Username, nil
}

// loopbackBearerToken 返回回环客户端发送的 bearer token：当从文件读取时，由于 ServiceAccount token 文件会定期轮换，因此读取文件当前内容。
func loopbackBearerToken() (string, error) {
	cfg := system.LoopbackKubeRestConfig.Get()
	if cfg.BearerTokenFile != "" {
		b, err := os.ReadFile(cfg.BearerTokenFile)
		if err != nil {
			return "", err
		}
		if t := strings.TrimSpace(string(b)); t != "" {
			return t, nil
		}
	}
	if cfg.BearerToken != "" {
		return cfg.BearerToken, nil
	}

	return "", errors.New("the worker's client authenticates without a bearer token")
}
