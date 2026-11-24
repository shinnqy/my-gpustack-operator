package modelmanager

import (
	"context"
	"fmt"

	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	ctrlcache "sigs.k8s.io/controller-runtime/pkg/cache"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/manager"
	"gpustack.ai/gpustack/pkg/modelmanager/peer"
	"gpustack.ai/gpustack/pkg/modelmanager/store"
	"gpustack.ai/gpustack/pkg/systemname"
	"gpustack.ai/gpustack/pkg/webserver"
)

// Config 是单个节点上 model-manager 插件的完整运行配置。
type Config struct {
	ServerConfig  *webserver.Config
	ManagerConfig *manager.Config

	NodeName   string
	KubeletDir string
	CacheRoot  string
	CSISocket  string

	// PeerSync 非空时向其他节点插件提供本节点已发布的目录树，并允许从其他节点拉取。
	PeerSync *peer.Sync
}

// Apply 构建插件实例：包含 HTTP 服务器、仅监听必要资源的 controller manager，以及基于缓存根目录的 Store。
func (c *Config) Apply(ctx context.Context) (*Manager, error) {
	srv, err := c.ServerConfig.Apply(ctx)
	if err != nil {
		return nil, err
	}
	c.ManagerConfig.WebhookServer = srv
	// 插件 RBAC 仅允许读取本节点的 NodeModelStore 与 operator Namespace 中的 ConfigMap，
	// 因此缓存精确限定监听范围；ModelArtifact 则在集群范围读取。
	c.ManagerConfig.CacheByObject = map[ctrlcli.Object]ctrlcache.ByObject{
		&workercore.NodeModelStore{}: {Field: fields.OneTermEqualSelector("metadata.name", c.NodeName)},
		&core.ConfigMap{}:            {Namespaces: map[string]ctrlcache.Config{systemname.NamespaceName: {}}},
	}
	mgr, err := c.ManagerConfig.Apply(ctx)
	if err != nil {
		return nil, err
	}

	st, err := store.Open(c.CacheRoot)
	if err != nil {
		return nil, fmt.Errorf("open the cache root %s: %w", c.CacheRoot, err)
	}

	return &Manager{
		Manager:    mgr,
		Store:      st,
		NodeName:   c.NodeName,
		KubeletDir: c.KubeletDir,
		CSISocket:  c.CSISocket,
		PeerSync:   c.PeerSync,
	}, nil
}
