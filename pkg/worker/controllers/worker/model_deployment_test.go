package worker

import (
	"context"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrlrecord "k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	ctrlinterceptor "sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubeclients/kubernetes/scheme"
)

func newModelDeploymentClient(objs ...ctrlcli.Object) ctrlcli.Client {
	// 从服务端读出的对象都会带有 ResourceVersion，因此在此处为测试夹具补齐初始版本。
	for _, obj := range objs {
		if md, ok := obj.(*workercore.ModelDeployment); ok && md.ResourceVersion == "" {
			md.ResourceVersion = "1"
		}
	}

	allocated := 0
	return ctrlfake.NewClientBuilder().
		WithInterceptorFuncs(ctrlinterceptor.Funcs{
			Create: func(ctx context.Context, next ctrlcli.WithWatch, obj ctrlcli.Object,
				opts ...ctrlcli.CreateOption,
			) error {
				switch obj.(type) {
				case *core.Pod, *kueue.Workload:
					if obj.GetUID() == "" {
						allocated++
						obj.SetUID(types.UID("created-" + strconv.Itoa(allocated)))
					}
				}
				return next.Create(ctx, obj, opts...)
			},
		}).
		WithScheme(scheme.Scheme).
		WithIndex(&core.Pod{}, "spec.nodeName", func(obj ctrlcli.Object) []string {
			pod, ok := obj.(*core.Pod)
			if !ok || pod.Spec.NodeName == "" {
				return nil
			}

			return []string{pod.Spec.NodeName}
		}).
		WithStatusSubresource(&workercore.ModelDeployment{}, &workercore.KVCachePoolBinding{},
			&workercore.Devices{}).
		WithObjects(objs...).
		Build()
}

// modelDeploymentWrites 统计单轮调谐发出的所有写操作，用于验证幂等轮次不会执行任何写入。
type modelDeploymentWrites struct {
	creates, updates, deletes, patches, statusUpdates int
}

func newCountingModelDeploymentClient(w *modelDeploymentWrites, objs ...ctrlcli.Object) ctrlcli.Client {
	return ctrlfake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithStatusSubresource(&workercore.ModelDeployment{}, &workercore.KVCachePoolBinding{},
			&workercore.Devices{}).
		WithObjects(objs...).
		WithInterceptorFuncs(ctrlinterceptor.Funcs{
			Create: func(ctx context.Context, c ctrlcli.WithWatch, obj ctrlcli.Object, opts ...ctrlcli.CreateOption) error {
				w.creates++
				return c.Create(ctx, obj, opts...)
			},
			Update: func(ctx context.Context, c ctrlcli.WithWatch, obj ctrlcli.Object, opts ...ctrlcli.UpdateOption) error {
				w.updates++
				return c.Update(ctx, obj, opts...)
			},
			Delete: func(ctx context.Context, c ctrlcli.WithWatch, obj ctrlcli.Object, opts ...ctrlcli.DeleteOption) error {
				w.deletes++
				return c.Delete(ctx, obj, opts...)
			},
			Patch: func(
				ctx context.Context, c ctrlcli.WithWatch, obj ctrlcli.Object,
				p ctrlcli.Patch, opts ...ctrlcli.PatchOption,
			) error {
				w.patches++
				return c.Patch(ctx, obj, p, opts...)
			},
			SubResourceUpdate: func(
				ctx context.Context, c ctrlcli.Client, sub string, obj ctrlcli.Object,
				opts ...ctrlcli.SubResourceUpdateOption,
			) error {
				w.statusUpdates++
				return c.Status().Update(ctx, obj, opts...)
			},
		}).
		Build()
}

func reconcileModelDeployment(t *testing.T, cli ctrlcli.Client) (ctrl.Result, error) {
	t.Helper()

	return reconcileModelDeploymentWith(t, &ModelDeploymentReconciler{
		Client: cli, APIReader: cli, Recorder: ctrlrecord.NewFakeRecorder(64),
	})
}

func reconcileModelDeploymentWith(t *testing.T, r *ModelDeploymentReconciler) (ctrl.Result, error) {
	t.Helper()

	return r.Reconcile(context.Background(), ctrlreconcile.Request{
		NamespacedName: ctrlcli.ObjectKey{Namespace: "team-a", Name: "qwen"},
	})
}

func replicaPods(t *testing.T, cli ctrlcli.Client) []core.Pod {
	t.Helper()

	podList := new(core.PodList)
	require.NoError(t, cli.List(context.Background(), podList, ctrlcli.InNamespace("team-a")))

	pods := make([]core.Pod, 0, len(podList.Items))
	for i := range podList.Items {
		if _, isRouter := podList.Items[i].Labels[modelDeploymentRouterLabelKey]; isRouter {
			continue
		}
		pods = append(pods, podList.Items[i])
	}

	return pods
}

// replicaNames 返回部署持有的全部副本名称（排序后），跳过 Router Pod。
func replicaNames(t *testing.T, cli ctrlcli.Client) []string {
	t.Helper()

	pods := replicaPods(t, cli)
	names := make([]string, 0, len(pods))
	for _, pod := range pods {
		names = append(names, pod.Name)
	}
	slices.Sort(names)

	return names
}

// TestModelDeploymentReconciler_CreatesOneReplicaPerDeclaredCount 验证单个角色按声明数量创建
// N 个独立副本 Pod，且不创建任何 Instance 对象。
func TestModelDeploymentReconciler_CreatesOneReplicaPerDeclaredCount(t *testing.T) {
	md := newRenderDeployment(func(md *workercore.ModelDeployment) {
		md.Spec.Roles[0].Replicas = 4
	})
	cli := newModelDeploymentClient(md, newRenderInstanceType())

	_, err := reconcileModelDeployment(t, cli)
	require.NoError(t, err)

	names := replicaNames(t, cli)
	require.Len(t, names, 4)
	seen := make(map[string]bool, len(names))
	for _, name := range names {
		assert.True(t, strings.HasPrefix(name, "qwen-server-"),
			"副本名应以渲染前缀开头: %s", name)
		assert.False(t, seen[name], "每个副本都应拥有独立名称: %s", name)
		seen[name] = true
	}

	instList := new(workercore.InstanceList)
	require.NoError(t, cli.List(context.Background(), instList))
	assert.Empty(t, instList.Items, "ModelDeployment 直接渲染 Pod，不创建 Instance")
}

// TestModelDeploymentReconciler_SecondPassWritesNothing 验证水平调谐的幂等性：
// 期望规格未变化时，第二轮调谐不产生任何创建、更新、删除或状态写入。
func TestModelDeploymentReconciler_SecondPassWritesNothing(t *testing.T) {
	writes := new(modelDeploymentWrites)
	cli := newCountingModelDeploymentClient(writes, newRenderDeployment(), newRenderInstanceType())

	_, err := reconcileModelDeployment(t, cli)
	require.NoError(t, err)
	require.Len(t, replicaNames(t, cli), 2)
	require.Equal(t, 4, writes.creates,
		"第一轮应创建两个副本、部署级 Service 与角色级 Service")
	require.Equal(t, 1, writes.updates, "并添加 finalizer")
	require.Equal(t, 1, writes.statusUpdates, "并上报一次 status")

	*writes = modelDeploymentWrites{}
	_, err = reconcileModelDeployment(t, cli)
	require.NoError(t, err)

	assert.Equal(t, modelDeploymentWrites{}, *writes,
		"未变更的 spec 不得发出任何 create、update、delete 或 status 写入")
}
