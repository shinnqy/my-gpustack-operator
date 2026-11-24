package worker

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	gpustack "gpustack.ai/gpustack/api/v1"
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubeclients/kubernetes/scheme"
	"gpustack.ai/gpustack/pkg/modelstore"
)

const testArtifactDigest = "sha256:1111111111111111111111111111111111111111111111111111111111111111"

// placementStore 构造持有指定状态 testArtifactDigest、且 Ready condition 为 ready 的 NodeModelStore。
func placementStore(node string, state workercore.NodeModelStoreModelState, ready meta.ConditionStatus) *workercore.NodeModelStore {
	nms := &workercore.NodeModelStore{
		ObjectMeta: meta.ObjectMeta{Name: node},
		Status: workercore.NodeModelStoreStatus{
			Models: []workercore.NodeModelStoreModel{{Digest: testArtifactDigest, State: state}},
		},
	}
	if ready != "" {
		nms.Status.Conditions = []gpustack.Condition{{Type: "Ready", Status: ready}}
	}

	return nms
}

func placementCSINode(node string, registered bool) *storage.CSINode {
	csiNode := &storage.CSINode{ObjectMeta: meta.ObjectMeta{Name: node}}
	if registered {
		csiNode.Spec.Drivers = []storage.CSINodeDriver{{Name: modelstore.DriverName, NodeID: node}}
	}

	return csiNode
}

func placementNode(node, hostname string) *core.Node {
	nd := &core.Node{ObjectMeta: meta.ObjectMeta{Name: node}}
	if hostname != "" {
		nd.Labels = map[string]string{core.LabelHostname: hostname}
	}

	return nd
}

// hotNode 返回使指定节点成为 testArtifactDigest 候选节点所需的全部对象。
func hotNode(node string) []ctrlcli.Object {
	return []ctrlcli.Object{
		placementStore(node, workercore.NodeModelStoreModelStateReady, meta.ConditionTrue),
		placementCSINode(node, true),
		placementNode(node, node),
	}
}

func TestModelPlacementPreferenceCandidates(t *testing.T) {
	cases := []struct {
		name string
		objs []ctrlcli.Object
		want []string
	}{
		{name: "every condition holds", objs: hotNode("n1"), want: []string{"n1"}},
		{
			name: "the digest is still downloading",
			objs: []ctrlcli.Object{
				placementStore("n1", workercore.NodeModelStoreModelStateDownloading, meta.ConditionTrue),
				placementCSINode("n1", true), placementNode("n1", "n1"),
			},
		},
		{
			name: "the digest failed",
			objs: []ctrlcli.Object{
				placementStore("n1", workercore.NodeModelStoreModelStateFailed, meta.ConditionTrue),
				placementCSINode("n1", true), placementNode("n1", "n1"),
			},
		},
		{
			name: "the store is not Ready",
			objs: []ctrlcli.Object{
				placementStore("n1", workercore.NodeModelStoreModelStateReady, meta.ConditionFalse),
				placementCSINode("n1", true), placementNode("n1", "n1"),
			},
		},
		{
			name: "the store has no Ready condition",
			objs: []ctrlcli.Object{
				placementStore("n1", workercore.NodeModelStoreModelStateReady, ""),
				placementCSINode("n1", true), placementNode("n1", "n1"),
			},
		},
		{
			name: "a stale store: Ready, and the driver is no longer in CSINode",
			objs: []ctrlcli.Object{
				placementStore("n1", workercore.NodeModelStoreModelStateReady, meta.ConditionTrue),
				placementCSINode("n1", false), placementNode("n1", "n1"),
			},
		},
		{
			name: "the node has no CSINode",
			objs: []ctrlcli.Object{
				placementStore("n1", workercore.NodeModelStoreModelStateReady, meta.ConditionTrue),
				placementNode("n1", "n1"),
			},
		},
		{
			name: "the Node is gone",
			objs: []ctrlcli.Object{
				placementStore("n1", workercore.NodeModelStoreModelStateReady, meta.ConditionTrue),
				placementCSINode("n1", true),
			},
		},
		{
			name: "the Node has no hostname label",
			objs: []ctrlcli.Object{
				placementStore("n1", workercore.NodeModelStoreModelStateReady, meta.ConditionTrue),
				placementCSINode("n1", true), placementNode("n1", ""),
			},
		},
		{
			name: "the hostname label differs from the Node name",
			objs: []ctrlcli.Object{
				placementStore("n1", workercore.NodeModelStoreModelStateReady, meta.ConditionTrue),
				placementCSINode("n1", true), placementNode("n1", "host-1.example"),
			},
			want: []string{"host-1.example"},
		},
		{
			name: "another digest on the node",
			objs: []ctrlcli.Object{
				&workercore.NodeModelStore{
					ObjectMeta: meta.ObjectMeta{Name: "n1"},
					Status: workercore.NodeModelStoreStatus{
						Models: []workercore.NodeModelStoreModel{
							{Digest: "sha256:" + strings.Repeat("2", 64), State: workercore.NodeModelStoreModelStateReady},
						},
						Conditions: []gpustack.Condition{{Type: "Ready", Status: meta.ConditionTrue}},
					},
				},
				placementCSINode("n1", true), placementNode("n1", "n1"),
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cli := ctrlfake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(c.objs...).Build()

			term := modelPlacementPreference(context.Background(), cli, testArtifactDigest)
			if c.want == nil {
				assert.Nil(t, term, "没有候选节点时不应添加偏好项")
				return
			}
			require.NotNil(t, term)
			assert.Equal(t, int32(modelPlacementPreferenceWeight), term.Weight)
			assert.Equal(t, []core.NodeSelectorRequirement{
				{Key: core.LabelHostname, Operator: core.NodeSelectorOpIn, Values: c.want},
			}, term.Preference.MatchExpressions)
		})
	}
}
