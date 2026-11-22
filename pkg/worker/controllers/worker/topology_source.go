package worker

import (
	"context"
	"time"

	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlbuilder "sigs.k8s.io/controller-runtime/pkg/builder"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlevent "sigs.k8s.io/controller-runtime/pkg/event"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	ctrlpredicate "sigs.k8s.io/controller-runtime/pkg/predicate"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"
	nfd "sigs.k8s.io/node-feature-discovery/api/nfd/v1alpha1"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/controller"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/utils/ctrlhandlerx"
)

// TopologySourceReconciler 观测设备清单并通过 NodeFeatures 发布可写标签。
type TopologySourceReconciler struct {
	Client    ctrlcli.Client
	APIReader ctrlcli.Reader
	Now       func() time.Time
}

var _ ctrlreconcile.Reconciler = (*TopologySourceReconciler)(nil)

func (r *TopologySourceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// 在第 13 章中实现此函数。
	var result0 ctrl.Result
	var result1 error
	return result0, result1
}

func (r *TopologySourceReconciler) SetupController(_ context.Context, opts controller.SetupOptions) error {
	r.Client = opts.Manager.GetClient()
	r.APIReader = opts.Manager.GetAPIReader()
	return ctrl.NewControllerManagedBy(opts.Manager).
		Named("topologysource").
		For(&workercore.TopologySource{}).
		Watches(
			&core.Node{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFunc(time.Second, r.enqueueTopologySourcesWhenNodeChanged),
			ctrlbuilder.WithPredicates(ctrlpredicate.Funcs{
				UpdateFunc: func(e ctrlevent.UpdateEvent) bool {
					return nodeLabelsChangedIgnoringFit(e.ObjectOld.(*core.Node), e.ObjectNew.(*core.Node))
				},
			}),
		).
		Watches(
			&core.Node{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFunc(time.Second, r.enqueueWritingTopologySourcesWhenNodeChanged),
			ctrlbuilder.WithPredicates(ctrlpredicate.Funcs{
				UpdateFunc: func(ctrlevent.UpdateEvent) bool { return false },
			}),
		).
		Watches(
			&core.ConfigMap{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFunc(time.Second, r.enqueueTopologySourcesWhenConfigMapChanged),
		).
		Watches(
			&core.Secret{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFunc(time.Second, r.enqueueTopologySourcesWhenSecretChanged),
		).
		Watches(
			&nfd.NodeFeature{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFunc(time.Second, r.enqueueTopologySourcesWhenNodeFeatureChanged),
		).
		Complete(r)
}

func (r *TopologySourceReconciler) enqueueTopologySourcesWhenNodeFeatureChanged(_ context.Context, feature ctrlcli.Object) []ctrlreconcile.Request {
	owner := kubemeta.GetControllerOfNoCopy(feature)
	if owner == nil || owner.APIVersion != workercore.SchemeGroupVersion.String() || owner.Kind != "TopologySource" {
		return nil
	}
	return []ctrlreconcile.Request{{NamespacedName: ctrlcli.ObjectKey{Name: owner.Name}}}
}

func (r *TopologySourceReconciler) enqueueTopologySourcesWhenSecretChanged(ctx context.Context, secret ctrlcli.Object) []ctrlreconcile.Request {
	sources := new(workercore.TopologySourceList)
	if err := r.Client.List(ctx, sources); err != nil {
		ctrllog.FromContext(ctx).Error(err, "list topology sources for Secret change")
		return nil
	}
	reqs := make([]ctrlreconcile.Request, 0)
	for i := range sources.Items {
		source := &sources.Items[i]
		if source.Spec.Webhook == nil || !topologySourceWebhookReferencesSecret(source.Spec.Webhook, secret) {
			continue
		}
		reqs = append(reqs, ctrlreconcile.Request{NamespacedName: ctrlcli.ObjectKeyFromObject(source)})
	}
	return reqs
}

func topologySourceWebhookReferencesSecret(webhook *workercore.TopologySourceWebhook, secret ctrlcli.Object) bool {
	for _, ref := range []*workercore.TopologySourceObjectReference{webhook.BearerTokenSecretRef, webhook.TLSClientCertificateSecretRef} {
		if ref != nil && ref.Namespace == secret.GetNamespace() && ref.Name == secret.GetName() {
			return true
		}
	}
	return false
}

func (r *TopologySourceReconciler) enqueueTopologySourcesWhenNodeChanged(
	ctx context.Context,
	_ ctrlcli.Object,
) []ctrlreconcile.Request {
	sources := new(workercore.TopologySourceList)
	if err := r.Client.List(ctx, sources); err != nil {
		if !kerrors.IsNotFound(err) {
			ctrllog.FromContext(ctx).Error(err, "list topology sources for Node change")
		}
		return nil
	}
	reqs := make([]ctrlreconcile.Request, 0, len(sources.Items))
	for i := range sources.Items {
		if sources.Items[i].Spec.NodeLabels == nil {
			continue
		}
		reqs = append(reqs, ctrlreconcile.Request{NamespacedName: ctrlcli.ObjectKeyFromObject(&sources.Items[i])})
	}
	return reqs
}

func (r *TopologySourceReconciler) enqueueWritingTopologySourcesWhenNodeChanged(
	ctx context.Context,
	_ ctrlcli.Object,
) []ctrlreconcile.Request {
	sources := new(workercore.TopologySourceList)
	if err := r.Client.List(ctx, sources); err != nil {
		ctrllog.FromContext(ctx).Error(err, "list writing topology sources for Node create or delete")
		return nil
	}
	reqs := make([]ctrlreconcile.Request, 0, len(sources.Items))
	for i := range sources.Items {
		if sources.Items[i].Spec.ConfigMap == nil && sources.Items[i].Spec.Webhook == nil {
			continue
		}
		reqs = append(reqs, ctrlreconcile.Request{NamespacedName: ctrlcli.ObjectKeyFromObject(&sources.Items[i])})
	}
	return reqs
}

func (r *TopologySourceReconciler) enqueueTopologySourcesWhenConfigMapChanged(
	ctx context.Context,
	configMap ctrlcli.Object,
) []ctrlreconcile.Request {
	sources := new(workercore.TopologySourceList)
	if err := r.Client.List(ctx, sources); err != nil {
		ctrllog.FromContext(ctx).Error(err, "list topology sources for ConfigMap change")
		return nil
	}
	reqs := make([]ctrlreconcile.Request, 0)
	for i := range sources.Items {
		source := &sources.Items[i]
		matches := false
		if source.Spec.ConfigMap != nil {
			ref := source.Spec.ConfigMap.ConfigMapRef
			matches = ref.Namespace == configMap.GetNamespace() && ref.Name == configMap.GetName()
		}
		if source.Spec.Webhook != nil && source.Spec.Webhook.CABundleConfigMapRef != nil {
			ref := source.Spec.Webhook.CABundleConfigMapRef
			matches = matches || ref.Namespace == configMap.GetNamespace() && ref.Name == configMap.GetName()
		}
		if matches {
			reqs = append(reqs, ctrlreconcile.Request{NamespacedName: ctrlcli.ObjectKeyFromObject(source)})
		}
	}
	return reqs
}
