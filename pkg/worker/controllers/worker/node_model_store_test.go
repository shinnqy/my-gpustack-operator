package worker

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubeclients/kubernetes/scheme"
	"gpustack.ai/gpustack/pkg/modelstore"
)

func testModelNode(name string) *core.Node {
	return &core.Node{ObjectMeta: meta.ObjectMeta{Name: name, UID: types.UID("uid-" + name)}}
}

func testCSINode(name string, drivers ...string) *storage.CSINode {
	cn := &storage.CSINode{ObjectMeta: meta.ObjectMeta{Name: name}}
	for _, d := range drivers {
		cn.Spec.Drivers = append(cn.Spec.Drivers, storage.CSINodeDriver{Name: d, NodeID: name})
	}
	return cn
}

func testModelCSIDriver() *storage.CSIDriver {
	return &storage.CSIDriver{ObjectMeta: meta.ObjectMeta{Name: modelstore.DriverName}}
}

// defaultNodeModelStoreSpec 是集群默认设置合并后得到的节点缓存配置。
func defaultNodeModelStoreSpec() workercore.NodeModelStoreSpec {
	return workercore.NodeModelStoreSpec{
		Watermarks:      workercore.NodeModelStoreWatermarks{HighPercent: 80, LowPercent: 70},
		Download:        workercore.NodeModelStoreDownload{Concurrency: 8},
		PeerSyncEnabled: peerSyncTrue(),
		Hub: workercore.NodeModelStoreHub{
			HuggingFaceEndpoint: "https://huggingface.co",
			ModelScopeEndpoint:  "https://www.modelscope.cn",
		},
	}
}

func newTestNodeModelStoreEnv(t *testing.T, objs ...ctrlcli.Object) (*NodeModelStoreReconciler, ctrlcli.Client) {
	t.Helper()
	cli := ctrlfake.NewClientBuilder().WithScheme(scheme.Scheme).
		WithStatusSubresource(&workercore.NodeModelStore{}).
		WithObjects(objs...).Build()

	// 普通单测中没有 kubelet 应答。未显式提供读取函数的用例沿用 kubelet 的默认值。
	return &NodeModelStoreReconciler{Client: cli, ReadKubelet: func(context.Context, string) (*workercore.NodeModelStoreKubelet, error) {
		return nil, errors.New("no kubelet in a unit test")
	}}, cli
}

func reconcileNodeModelStore(t *testing.T, r *NodeModelStoreReconciler, node string) ctrl.Result {
	t.Helper()
	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: ctrlcli.ObjectKey{Name: node}})
	require.NoError(t, err)
	return res
}

func TestNodeModelStoreRegistrationLabelFollowsCSINode(t *testing.T) {
	nd := testModelNode("node-1")
	nd.Labels = map[string]string{"site": "west", "model.csi.gpustack.ai/registered": "true"}
	cn := testCSINode("node-1")
	r, cli := newTestNodeModelStoreEnv(t, testModelCSIDriver(), nd, cn)
	ctx := context.Background()
	check := func(want string) {
		t.Helper()
		reconcileNodeModelStore(t, r, nd.Name)
		got := new(core.Node)
		require.NoError(t, cli.Get(ctx, ctrlcli.ObjectKey{Name: nd.Name}, got))
		assert.Equal(t, want, got.Labels["model.csi.gpustack.ai/registered"])
		assert.Equal(t, "west", got.Labels["site"])
	}

	check("")
	cn.Spec.Drivers = testCSINode("node-1", modelstore.DriverName).Spec.Drivers
	require.NoError(t, cli.Update(ctx, cn))
	check("true")
	cn.Spec.Drivers = nil
	require.NoError(t, cli.Update(ctx, cn))
	check("")
	stored := new(workercore.NodeModelStore)
	require.NoError(t, cli.Get(ctx, ctrlcli.ObjectKey{Name: nd.Name}, stored),
		"the store stays for a plugin restart even while the scheduling label is absent")
}
