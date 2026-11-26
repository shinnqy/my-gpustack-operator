package worker

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	restfake "k8s.io/client-go/rest/fake"
	"k8s.io/utils/ptr"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubeclients/kubernetes/scheme"
	"gpustack.ai/gpustack/pkg/worker/kuberess"
)

func newKVCacheBackendObject() *workercore.KVCacheBackend {
	return &workercore.KVCacheBackend{
		ObjectMeta: meta.ObjectMeta{Name: "mooncake-dram"},
		Spec: workercore.KVCacheBackendSpec{
			Type:  "Mooncake",
			Image: "example.com/mooncake:v0",
			Connection: workercore.KVCacheBackendConnection{
				Managed: &workercore.KVCacheBackendManaged{
					Leader: workercore.KVCacheBackendLeader{Replicas: ptr.To[int32](1), ElectionBackend: "None"},
					Members: []workercore.KVCacheBackendMember{{
						NodeSelector:      map[string]string{"kvcache-dram": "true"},
						Medium:            "DRAM",
						CapacityPerMember: resource.MustParse("500Gi"),
					}},
				},
			},
		},
	}
}

func newKVCacheBackendClient(objs ...ctrlcli.Object) ctrlcli.Client {
	return ctrlfake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithStatusSubresource(&workercore.KVCacheBackend{}).
		WithObjects(objs...).
		Build()
}

func reconcileKVCacheBackend(t *testing.T, cli ctrlcli.Client, name string) *workercore.KVCacheBackend {
	t.Helper()

	// 模拟 leader 尚未启动时的连接错误，渲染测试无需访问集群 DNS。
	r := &KVCacheBackendReconciler{
		Client: cli,
		AdminHTTP: restfake.CreateHTTPClient(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("connect: connection refused")
		}),
	}
	_, err := r.Reconcile(context.Background(), ctrlreconcile.Request{
		NamespacedName: ctrlcli.ObjectKey{Name: name},
	})
	require.NoError(t, err)

	got := new(workercore.KVCacheBackend)
	require.NoError(t, cli.Get(context.Background(), ctrlcli.ObjectKey{Name: name}, got))
	return got
}

// leaderObjectKeys 是渲染对象的存放位置：这是一个所有后端共享的命名空间，以拥有该空间的后端名称命名。
func leaderObjectKey(kvcb *workercore.KVCacheBackend) ctrlcli.ObjectKey {
	return ctrlcli.ObjectKey{
		Name:      kvcb.Name + "-leader",
		Namespace: kuberess.SystemNamespaceName,
	}
}

func memberObjectKey(kvcb *workercore.KVCacheBackend, group int) ctrlcli.ObjectKey {
	return ctrlcli.ObjectKey{
		Name:      fmt.Sprintf("%s-member-%d", kvcb.Name, group),
		Namespace: kuberess.SystemNamespaceName,
	}
}

// TestKVCacheBackendReconciler_RendersTheLeaderWorkload 指出，一个托管后端在其前方会有一个主节点 Deployment
// 和一个 Service，以及发布到状态中的两个地址。
func TestKVCacheBackendReconciler_RendersTheLeaderWorkload(t *testing.T) {
	kvcb := newKVCacheBackendObject()
	cli := newKVCacheBackendClient(kvcb)

	got := reconcileKVCacheBackend(t, cli, kvcb.Name)
	require.NotNil(t, got)

	ctx := context.Background()

	deploy := new(apps.Deployment)
	require.NoError(t, cli.Get(ctx, leaderObjectKey(kvcb), deploy))
	require.NotNil(t, deploy.Spec.Replicas)
	assert.Equal(t, int32(1), *deploy.Spec.Replicas)
	require.Len(t, deploy.Spec.Template.Spec.Containers, 1)
	assert.Equal(t, kvcb.Spec.Image, deploy.Spec.Template.Spec.Containers[0].Image,
		"the object's own image wins over the setting")

	svc := new(core.Service)
	require.NoError(t, cli.Get(ctx, leaderObjectKey(kvcb), svc))
	assert.Equal(t, core.ServiceTypeClusterIP, svc.Spec.Type)
	assert.Equal(t, deploy.Spec.Selector.MatchLabels, svc.Spec.Selector,
		"the Service must select what the Deployment runs")

	ds := new(apps.DaemonSet)
	require.NoError(t, cli.Get(ctx, memberObjectKey(kvcb, 0), ds),
		"the member group is rendered too; a leader with nothing to allocate against holds no cache")
	assert.Equal(t, kvcb.Spec.Connection.Managed.Members[0].NodeSelector,
		ds.Spec.Template.Spec.NodeSelector)
	assert.NotEqual(t, deploy.Spec.Selector.MatchLabels, ds.Spec.Selector.MatchLabels,
		"the member's selector must not collide with the leader's, or each would front the other's Pods")

	endpoints := make(map[string]string, len(got.Status.Endpoints))
	for _, e := range got.Status.Endpoints {
		endpoints[e.Name] = e.Address
	}
	assert.Equal(t, map[string]string{
		workercore.KVCacheBackendEndpointNameClient: "mooncake-dram-leader.gpustack-system.svc:50051",
		workercore.KVCacheBackendEndpointNameAdmin:  "mooncake-dram-leader.gpustack-system.svc:9003",
	}, endpoints, "both roles are published; a consumer handed one address cannot reach the other")
}
