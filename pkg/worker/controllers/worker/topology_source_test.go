package worker

import (
	"context"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"
	nfd "sigs.k8s.io/node-feature-discovery/api/nfd/v1alpha1"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubeclients/kubernetes/scheme"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/worker/kuberess"
)

const (
	topologySourceRegionLabel = "topology.gpustack.ai/region"
	topologySourceZoneLabel   = "topology.gpustack.ai/zone"
	topologySourceRackLabel   = "topology.gpustack.ai/rack"
)

func TestTopologySourceReconcileConfigMapAppliesSnapshot(t *testing.T) {
	source := &workercore.TopologySource{ObjectMeta: meta.ObjectMeta{Name: "inventory", UID: types.UID("source")}, Spec: workercore.TopologySourceSpec{
		NodeSelector: meta.LabelSelector{MatchLabels: map[string]string{"test.gpustack.ai/topology": "enabled"}},
		Levels:       []string{topologySourceRegionLabel, topologySourceZoneLabel},
		ConfigMap: &workercore.TopologySourceConfigMap{
			ConfigMapRef: workercore.TopologySourceObjectReference{Namespace: "gpustack-system", Name: "inventory"}, Key: "snapshot.yaml",
			MaxStaleness: meta.Duration{Duration: time.Minute},
		},
	}}
	configMap := &core.ConfigMap{ObjectMeta: meta.ObjectMeta{Namespace: "gpustack-system", Name: "inventory"}, Data: map[string]string{"snapshot.yaml": "apiVersion: worker.gpustack.ai/v1alpha1\nkind: TopologySourceConfiguration\nrevision: inventory-1\nnodes:\n  node-a:\n    topology.gpustack.ai/region: region-a\n    topology.gpustack.ai/zone: zone-a\n"}}
	node := &core.Node{ObjectMeta: meta.ObjectMeta{Name: "node-a", Labels: map[string]string{"test.gpustack.ai/topology": "enabled"}}}
	cli := ctrlfake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(source, configMap, node).
		WithStatusSubresource(&workercore.TopologySource{}).Build()
	now := time.Date(2026, 9, 23, 0, 0, 0, 0, time.UTC)
	r := &TopologySourceReconciler{Client: cli, Now: func() time.Time { return now }}
	_, err := r.Reconcile(context.Background(), ctrlreconcile.Request{NamespacedName: ctrlcli.ObjectKey{Name: source.Name}})
	require.NoError(t, err)
	gotNode := new(core.Node)
	require.NoError(t, cli.Get(context.Background(), ctrlcli.ObjectKey{Name: node.Name}, gotNode))
	assert.NotContains(t, gotNode.Labels, topologySourceRegionLabel)
	assert.NotContains(t, gotNode.Labels, topologySourceZoneLabel)
	feature := new(nfd.NodeFeature)
	require.NoError(t, cli.Get(context.Background(), ctrlcli.ObjectKey{Namespace: kuberess.SystemNamespaceName, Name: topologySourceNodeFeatureName(source.UID, node.Name)}, feature))
	assert.Equal(t, map[string]string{topologySourceRegionLabel: "region-a", topologySourceZoneLabel: "zone-a"}, feature.Spec.Labels)
	assert.Equal(t, node.Name, feature.Labels[nfd.NodeFeatureObjNodeNameLabel])
	assert.Equal(t, []ctrlreconcile.Request{{NamespacedName: ctrlcli.ObjectKey{Name: source.Name}}},
		r.enqueueTopologySourcesWhenNodeFeatureChanged(context.Background(), feature))
	gotSource := new(workercore.TopologySource)
	require.NoError(t, cli.Get(context.Background(), ctrlcli.ObjectKey{Name: source.Name}, gotSource))
	assert.Equal(t, "inventory-1", gotSource.Status.LastSuccessfulRevision)
	assert.True(t, TopologySourceConditionReady.IsTrue(gotSource))

	// 第二个可写来源也选中相同层级时，协调器必须撤回自己发布的标签。解除冲突后重新发布。
	other := &workercore.TopologySource{
		ObjectMeta: meta.ObjectMeta{Name: "second-inventory", UID: "second"},
		Spec:       source.Spec,
	}
	require.NoError(t, cli.Create(t.Context(), other))
	request := ctrlreconcile.Request{NamespacedName: ctrlcli.ObjectKeyFromObject(source)}
	result, err := r.Reconcile(t.Context(), request)
	require.NoError(t, err)
	assert.Equal(t, time.Minute, result.RequeueAfter)
	require.NoError(t, cli.Get(t.Context(), request.NamespacedName, gotSource))
	assert.True(t, TopologySourceConditionOwnershipConflict.IsTrue(gotSource))
	assert.True(t, TopologySourceConditionReady.IsFalse(gotSource))
	assert.EqualValues(t, 1, gotSource.Status.ConflictedNodes)
	assertSourceNodeFeatureLabel(t, cli, source.UID, topologySourceRegionLabel, "")
	require.NoError(t, cli.Delete(t.Context(), other))
	_, err = r.Reconcile(t.Context(), request)
	require.NoError(t, err)
	assertSourceNodeFeatureLabel(t, cli, source.UID, topologySourceRegionLabel, "region-a")

	require.NoError(t, cli.Delete(context.Background(), configMap))
	now = now.Add(2 * time.Minute)
	_, err = r.Reconcile(context.Background(), ctrlreconcile.Request{NamespacedName: ctrlcli.ObjectKey{Name: source.Name}})
	require.NoError(t, err)
	require.NoError(t, cli.Get(context.Background(), ctrlcli.ObjectKey{Name: node.Name}, gotNode))
	assert.NotContains(t, gotNode.Labels, topologySourceRegionLabel)
	assert.NotContains(t, gotNode.Labels, topologySourceZoneLabel)
	assert.True(t, kerrors.IsNotFound(cli.Get(context.Background(), ctrlcli.ObjectKey{Namespace: kuberess.SystemNamespaceName, Name: topologySourceNodeFeatureName(source.UID, node.Name)}, feature)))
	require.NoError(t, cli.Get(context.Background(), ctrlcli.ObjectKey{Name: source.Name}, gotSource))
	assert.True(t, TopologySourceConditionReady.IsFalse(gotSource))
	assert.Equal(t, "Expired", TopologySourceConditionReady.GetReason(gotSource))
}

func TestTopologySourceSwitchToNodeLabelsRemovesPublishedFeatures(t *testing.T) {
	source := &workercore.TopologySource{ObjectMeta: meta.ObjectMeta{
		Name: "inventory", UID: "source", Finalizers: []string{topologySourceFinalizer},
	}, Spec: workercore.TopologySourceSpec{
		NodeSelector: meta.LabelSelector{},
		Levels:       []string{core.LabelTopologyRegion},
		NodeLabels:   &workercore.TopologySourceNodeLabels{},
	}}
	node := &core.Node{ObjectMeta: meta.ObjectMeta{Name: "node-a", Labels: map[string]string{core.LabelTopologyRegion: "region-a"}}}
	feature := &nfd.NodeFeature{ObjectMeta: meta.ObjectMeta{
		Name: topologySourceNodeFeatureName(source.UID, node.Name), Namespace: kuberess.SystemNamespaceName,
		Labels: map[string]string{nfd.NodeFeatureObjNodeNameLabel: node.Name, topologySourceUIDLabel: string(source.UID)},
	}}
	feature.Spec = *nfd.NewNodeFeatureSpec()
	feature.Spec.Labels = map[string]string{topologySourceRackLabel: "rack-a"}
	kubemeta.ControlOnWithoutBlock(feature, source, workercore.SchemeGroupVersionKind("TopologySource"))
	cli := ctrlfake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(source, node, feature).
		WithStatusSubresource(&workercore.TopologySource{}).Build()
	r := &TopologySourceReconciler{Client: cli}
	_, err := r.Reconcile(context.Background(), ctrlreconcile.Request{NamespacedName: ctrlcli.ObjectKey{Name: source.Name}})
	require.NoError(t, err)
	assert.True(t, kerrors.IsNotFound(cli.Get(context.Background(), ctrlcli.ObjectKeyFromObject(feature), new(nfd.NodeFeature))))
	gotSource := new(workercore.TopologySource)
	require.NoError(t, cli.Get(context.Background(), ctrlcli.ObjectKey{Name: source.Name}, gotSource))
	assert.NotContains(t, gotSource.Finalizers, topologySourceFinalizer)
	gotNode := new(core.Node)
	require.NoError(t, cli.Get(context.Background(), ctrlcli.ObjectKey{Name: node.Name}, gotNode))
	assert.Equal(t, node.Labels, gotNode.Labels)
}

func TestTopologySourceWebhookStalenessExpiryAndRecovery(t *testing.T) {
	var status atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Header.Get("Authorization") != "Bearer inventory-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if code := int(status.Load()); code != 0 {
			w.WriteHeader(code)
			return
		}
		_, _ = w.Write([]byte(`{"apiVersion":"worker.gpustack.ai/v1alpha1","kind":"TopologySourceConfiguration","revision":"inventory-1","nodes":{"node-a":{"topology.gpustack.ai/rack":"rack-a"}}}`))
	}))
	defer server.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	configMap := &core.ConfigMap{
		ObjectMeta: meta.ObjectMeta{Namespace: kuberess.SystemNamespaceName, Name: "ca"},
		Data:       map[string]string{"ca.crt": string(ca)},
	}
	secret := &core.Secret{
		ObjectMeta: meta.ObjectMeta{Namespace: kuberess.SystemNamespaceName, Name: "credential"},
		Data:       map[string][]byte{"token": []byte("inventory-token")},
	}
	source := &workercore.TopologySource{
		ObjectMeta: meta.ObjectMeta{Name: "inventory", UID: "source"},
		Spec: workercore.TopologySourceSpec{
			NodeSelector: meta.LabelSelector{MatchLabels: map[string]string{"test.gpustack.ai/topology": "enabled"}},
			Levels:       []string{topologySourceRackLabel},
			Webhook: &workercore.TopologySourceWebhook{
				URL: server.URL, Timeout: meta.Duration{Duration: 5 * time.Second},
				PollInterval:         meta.Duration{Duration: 10 * time.Second},
				MaxStaleness:         meta.Duration{Duration: time.Minute},
				CABundleConfigMapRef: &workercore.TopologySourceObjectReference{Namespace: configMap.Namespace, Name: configMap.Name},
				BearerTokenSecretRef: &workercore.TopologySourceObjectReference{Namespace: secret.Namespace, Name: secret.Name},
			},
		},
	}
	node := &core.Node{ObjectMeta: meta.ObjectMeta{Name: "node-a", Labels: map[string]string{"test.gpustack.ai/topology": "enabled"}}}
	cli := ctrlfake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(source, configMap, secret, node).
		WithStatusSubresource(&workercore.TopologySource{}).Build()
	now := time.Date(2026, 9, 23, 0, 0, 0, 0, time.UTC)
	r := &TopologySourceReconciler{Client: cli, Now: func() time.Time { return now }}
	request := ctrlreconcile.Request{NamespacedName: ctrlcli.ObjectKey{Name: source.Name}}

	_, err := r.Reconcile(context.Background(), request)
	require.NoError(t, err)
	assertSourceNodeFeatureLabel(t, cli, source.UID, topologySourceRackLabel, "rack-a")

	status.Store(http.StatusServiceUnavailable)
	now = now.Add(30 * time.Second)
	result, err := r.Reconcile(context.Background(), request)
	require.NoError(t, err)
	assert.Equal(t, 10*time.Second, result.RequeueAfter)
	assertSourceNodeFeatureLabel(t, cli, source.UID, topologySourceRackLabel, "rack-a")
	assertSourceReadyReason(t, cli, source.Name, "Stale")

	now = now.Add(2 * time.Minute)
	result, err = r.Reconcile(context.Background(), request)
	require.NoError(t, err)
	assert.Equal(t, 10*time.Second, result.RequeueAfter)
	assertSourceNodeFeatureLabel(t, cli, source.UID, topologySourceRackLabel, "")
	assertSourceReadyReason(t, cli, source.Name, "Expired")

	status.Store(0)
	_, err = r.Reconcile(context.Background(), request)
	require.NoError(t, err)
	assertSourceNodeFeatureLabel(t, cli, source.UID, topologySourceRackLabel, "rack-a")
	assertSourceReadyReason(t, cli, source.Name, "Observed")
}

func assertSourceNodeFeatureLabel(t *testing.T, cli ctrlcli.Client, sourceUID types.UID, key, expected string) {
	t.Helper()
	node := new(core.Node)
	require.NoError(t, cli.Get(context.Background(), ctrlcli.ObjectKey{Name: "node-a"}, node))
	assert.NotContains(t, node.Labels, key)
	feature := new(nfd.NodeFeature)
	err := cli.Get(context.Background(), ctrlcli.ObjectKey{Namespace: kuberess.SystemNamespaceName, Name: topologySourceNodeFeatureName(sourceUID, node.Name)}, feature)
	if expected == "" {
		assert.True(t, kerrors.IsNotFound(err))
		return
	}
	require.NoError(t, err)
	assert.Equal(t, expected, feature.Spec.Labels[key])
}

func assertSourceReadyReason(t *testing.T, cli ctrlcli.Client, name, expected string) {
	t.Helper()
	source := new(workercore.TopologySource)
	require.NoError(t, cli.Get(context.Background(), ctrlcli.ObjectKey{Name: name}, source))
	assert.Equal(t, expected, TopologySourceConditionReady.GetReason(source))
}
