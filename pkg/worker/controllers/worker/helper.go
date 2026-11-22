package worker

import (
	"context"
	"strings"
	"time"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"

	"gpustack.ai/gpustack/pkg/worker/settings"
)

// redirectedImage 把集群配置的镜像命名空间和仓库应用到 Operator 选择的镜像，方便离线集群使用镜像站。
// 对于 namespace/name:tag 形式可替换首段命名空间。首段含点、含端口或等于 localhost 时表示仓库主机，应保留它。
// 对象中由用户填写的镜像按原引用使用。
func redirectedImage(ctx context.Context, image string) string {
	if cn := settings.ContainerNamespace.ShouldValue(ctx); cn != "" {
		if first, suffix, found := strings.Cut(image, "/"); found &&
			!strings.ContainsAny(first, ".:") && first != "localhost" {
			image = cn + "/" + suffix
		}
	}
	if rn := settings.ContainerRegistry.ShouldValue(ctx); rn != "" {
		image = rn + "/" + image
	}
	return image
}

// _requeueAfterConflict 供会过滤部分更新事件的控制器使用。写入冲突后等待缓存追上新版本，再主动协调一次。
var _requeueAfterConflict = ctrl.Result{RequeueAfter: time.Second}

// objectWriteResult 处理写回对象时的版本冲突和对象删除。对象已删除时结束本轮。
// 遇到冲突时由调用方决定等监听事件还是显式重新排队。更新其他对象时，仅在监听覆盖其更新和删除事件时才适用。
// 保留资源版本检查能防止旧计算覆盖新状态，例如 Kueue 驱逐负载后将准入检查重置为 Pending。
func objectWriteResult(logger logr.Logger, err error, msg string, onConflict ctrl.Result) (ctrl.Result, error) {
	if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
		logger.V(1).Info(msg+" skipped, the object changed or was deleted since it was read", "reason", err.Error())
		if apierrors.IsConflict(err) {
			return onConflict, nil
		}
		return ctrl.Result{}, nil
	}
	logger.Error(err, msg)
	return ctrl.Result{}, err
}
