package manager

import (
	"context"
	"errors"
	"net/http"

	"k8s.io/apimachinery/pkg/util/sets"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlapiutil "sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	ctrlctrl "sigs.k8s.io/controller-runtime/pkg/controller"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	ctrlmgr "sigs.k8s.io/controller-runtime/pkg/manager"

	"gpustack.ai/gpustack/pkg/webserver"
)

type (
	// CtrlManager 是对 ctrl.Manager 的封装。
	// CtrlManager 保留 controller-runtime 的客户端和缓存，并为索引注册去重。
	CtrlManager struct {
		ctrl.Manager
		options           ctrl.Options
		httpClient        *http.Client
		disableController bool
		indexedFields     sets.Set[string]
	}

	// RepeatableCtrlFieldIndexer 是对 ctrlcli.FieldIndexer 的封装。
	RepeatableCtrlFieldIndexer struct {
		ctrl.Manager
		indexedFields sets.Set[string]
	}
)

// GetHTTPClient 实现 controller manager 接口，返回系统的单例 HTTP 客户端。
func (m CtrlManager) GetHTTPClient() *http.Client {
	if m.httpClient != nil {
		return m.httpClient
	}
	return m.Manager.GetHTTPClient()
}

// Start 实现 controller manager 接口，以避免函数二义性。
func (m CtrlManager) Start(ctx context.Context) error {
	return m.Manager.Start(ctx)
}

// Add 实现 controller manager 接口，用于向管理器添加控制器。
//
// 当指定 disableController 时，Add 会跳过需要 leader election 的控制器。
func (m CtrlManager) Add(r ctrlmgr.Runnable) error {
	if m.disableController {
		// 如果缓存已禁用，跳过需要 leader election 的控制器。
		l, ok := r.(ctrlmgr.LeaderElectionRunnable)
		if ok && l.NeedLeaderElection() {
			if _, ok := r.(ctrlctrl.Controller); ok {
				return nil
			}
		}
	}
	return m.Manager.Add(r)
}

// GetFieldIndexer 实现 controller manager 接口，返回可重复的字段索引器。
//
// 如果同一字段被多次索引，GetFieldIndexer 会发出警告。
func (m CtrlManager) GetFieldIndexer() ctrlcli.FieldIndexer {
	return RepeatableCtrlFieldIndexer{
		Manager:       m.Manager,
		indexedFields: m.indexedFields,
	}
}

func (i RepeatableCtrlFieldIndexer) IndexField(ctx context.Context, obj ctrlcli.Object, field string, extractValue ctrlcli.IndexerFunc) error {
	logger := ctrllog.FromContext(ctx)
	gvk, err := ctrlapiutil.GVKForObject(obj, i.GetScheme())
	if err != nil {
		return err
	}
	key := gvk.String() + "/" + field
	if i.indexedFields.Has(key) {
		// 如果字段已被索引，则跳过。
		logger.InfoDepth(1, "field is already indexed, skipping", "field", field, "gvk", gvk)
		return nil
	}
	i.indexedFields.Insert(key)
	return i.Manager.GetFieldIndexer().IndexField(ctx, obj, field, extractValue)
}

// HostPort 返回管理器服务器监听器的主机和端口。
func (m CtrlManager) HostPort() (string, int, error) {
	ms := m.GetWebhookServer()
	if ms == nil {
		return "", 0, errors.New("manager does not have a webhook server")
	}
	s, ok := ms.(webserver.Server)
	if !ok {
		return "", 0, errors.New("invalid webhook server type")
	}
	return s.HostPort()
}

// _CtrlManagerSentinel 是一个 ctrlmgr.Runnable 实现，用于观察 ctrl.Manager 是否已启动。
// _CtrlManagerSentinel 在管理器启动时关闭信号通道。
type _CtrlManagerSentinel struct {
	done chan struct{}
}

func (s _CtrlManagerSentinel) Start(ctx context.Context) error {
	close(s.done)
	<-ctx.Done()
	return ctx.Err()
}

func (s _CtrlManagerSentinel) NeedLeaderElection() bool {
	return false
}

func (s _CtrlManagerSentinel) Done() <-chan struct{} {
	return s.done
}
