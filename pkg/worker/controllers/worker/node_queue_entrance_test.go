package worker

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"gpustack.ai/gpustack/pkg/kubeclients/kubernetes/scheme"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/systemmeta"
)

// TestNodeQueueEntranceReconciler_CreatesLocalQueue 检查普通命名空间中的入口是否引用 ClusterQueue，
// 并在注解中保留全名。即使测试队列带有显存记录，控制器也不能把它复制到用户可写的 LocalQueue。
func TestNodeQueueEntranceReconciler_CreatesLocalQueue(t *testing.T) {
	const cqName = "gpustack-nvidia-ln-x64"
	const ns = "team-a"

	cq := &kueue.ClusterQueue{ObjectMeta: meta.ObjectMeta{Name: cqName}}
	systemmeta.NoteResource(cq, _ClusterQueueResType, map[string]string{
		"acceleratable": "true",
		"manufacturer":  "nvidia",
		"memory":        "81920Mi",
	})
	namespace := &core.Namespace{ObjectMeta: meta.ObjectMeta{Name: ns}}

	cli := ctrlfake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(cq, namespace).
		Build()

	r := &NodeQueueEntranceReconciler{Client: cli}
	_, err := r.Reconcile(context.Background(),
		ctrlreconcile.Request{NamespacedName: ctrlcli.ObjectKey{Name: cqName}})
	require.NoError(t, err)

	lq := new(kueue.LocalQueue)
	require.NoError(t, cli.Get(context.Background(),
		ctrlcli.ObjectKey{Namespace: ns, Name: nodefeature.FormatLocalQueueName(cqName)}, lq))

	assert.Equal(t, kueue.ClusterQueueReference(cqName), lq.Spec.ClusterQueue)
	assert.Equal(t, cqName, lq.Annotations[_LocalQueueClusterQueueNameAnnoKey])

	// 检查全局队列中的显存记录没有被复制到命名空间入口。
	_, notes := systemmeta.DescribeResource(lq)
	assert.NotContains(t, notes, "memory")
}
