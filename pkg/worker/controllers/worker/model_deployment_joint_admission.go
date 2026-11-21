package worker

import (
	"context"
	"fmt"
	"time"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlbuilder "sigs.k8s.io/controller-runtime/pkg/builder"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlhandler "sigs.k8s.io/controller-runtime/pkg/handler"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	ctrlpredicate "sigs.k8s.io/controller-runtime/pkg/predicate"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	kueuepodconst "sigs.k8s.io/kueue/pkg/controller/jobs/pod/constants"
	kueueadmissioncheck "sigs.k8s.io/kueue/pkg/util/admissioncheck"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/controller"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/utils/ctrlclix"
	"gpustack.ai/gpustack/pkg/worker/kuberess"
)

const (
	// _JointAdmissionCheckName is the AdmissionCheck object every operator-owned ClusterQueue
	// references once it is Active, whether or not the operator derived the queue's InstanceType,
	// and _JointAdmissionControllerName claims it for the reconciler in this file: Kueue
	// routes a Workload's check to whoever declares that name, so no other controller answers it.
	//
	// BOTH ARE TAKEN FROM THE PACKAGE THAT INSTALLS THE OBJECT rather than spelled again here. Two
	// copies of a contract agree until someone renames one of them, and neither side reports the
	// disagreement: the queues go on admitting, and the only thing that notices is the deployment
	// that needed the barrier.
	_JointAdmissionCheckName = kuberess.ModelDeploymentJointAdmissionCheckName

	_JointAdmissionControllerName = kuberess.ModelDeploymentJointAdmissionControllerName

	// _JointAdmissionFieldOwner owns this controller's entries in a Workload's admissionChecks.
	_JointAdmissionFieldOwner = "worker.gpustack.ai/model-deployment-joint"

	// _JointAdmissionParkedMarker is the phrase the park message carries and the one the status
	// reader looks for.
	//
	// spec.active=false DOES NOT SAY WHO WROTE IT. Kueue deactivates a Workload of its own accord and
	// an operator can pause one group by hand, so the flag alone cannot tell the status layer that the
	// bound is what stopped this deployment. This marker is a term of the message rather than a second
	// field because the message is already the durable record of the verdict; adding a field would put
	// the same fact in two places that can disagree.
	_JointAdmissionParkedMarker = "parked rather than held"

	// _JointAdmissionUndecidedMessage is what the check says while this controller cannot tell
	// whether the Workload is one group of a multi-group deployment.
	//
	// IT MUST NOT READ AS AN ORDINARY HOLD, and the message is the only thing that can carry the
	// difference: both states are Pending, so a reader with only the state learns that something is
	// waiting and not what for. A hold names the groups being waited on and clears itself as they
	// reserve quota. This one says a read failed, which is either a group mid-rebuild -- transient,
	// and nothing to do -- or replicas that are gone for good, which an operator has to act on.
	//
	// SAYING IT IS RETRYING IS PART OF THE DIFFERENCE. Without that an operator cannot tell this
	// from a verdict that has settled, and the whole point of the state is that it has not.
	_JointAdmissionUndecidedMessage = "cannot decide yet: some of this workload's owner replicas " +
		"could not be read, so whether it is one group of a multi-group model deployment is unknown. " +
		"Holding rather than admitting on a guess, and retrying"

	// _JointAdmissionInfeasibleAfter is how long a deployment's set may fail to assemble before the
	// barrier stops holding it and parks it instead.
	//
	// THE NUMBER IS A WAIT AN OPERATOR WOULD ACCEPT, not a measurement of anything. A set that cannot
	// assemble is usually waiting for capacity, and capacity arrives on a human timescale: a node
	// rejoining, a neighboring workload finishing, a quota being raised. Half an hour is long enough
	// that none of those is cut short and short enough that a deployment nobody is watching does not
	// sit on reserved quota overnight.
	//
	// WHAT MAKES IT SAFE TO BE WRONG IS WHICH WAY IT FAILS. Too long and quota is held that could have
	// been released; too short and a deployment that would have started is parked, and the operator
	// clears it. Neither loses work, and the parked state says what to do.
	_JointAdmissionInfeasibleAfter = 30 * time.Minute
)

// ModelDeploymentJointAdmissionCheckReconciler marks the joint-admission AdmissionCheck Active.
//
// Kueue turns a ClusterQueue referencing an inactive check inactive, so a check nobody activates
// stops the queue admitting anything at all. Activation is therefore the statement that this
// controller is running, and it is made here rather than at install time for exactly that reason.
type ModelDeploymentJointAdmissionCheckReconciler struct {
	Client ctrlcli.Client
}

var _ ctrlreconcile.Reconciler = (*ModelDeploymentJointAdmissionCheckReconciler)(nil)

func (r *ModelDeploymentJointAdmissionCheckReconciler) Reconcile(
	ctx context.Context, req ctrl.Request,
) (ctrl.Result, error) {
	logger := ctrllog.FromContext(ctx)

	ac := new(kueue.AdmissionCheck)
	if err := r.Client.Get(ctx, req.NamespacedName, ac); err != nil {
		// A deleted AdmissionCheck has nothing left to activate.
		if err = ctrlcli.IgnoreNotFound(err); err != nil {
			logger.Error(err, "fetch admission check")
		}
		return ctrl.Result{}, err
	}

	if ac.Spec.ControllerName != _JointAdmissionControllerName {
		logger.V(3).Info("skip admission check not owned by this controller")
		return ctrl.Result{}, nil
	}

	if kubemeta.IsConditionTrue(ac.Status.Conditions, kueue.AdmissionCheckActive) {
		return ctrl.Result{}, nil
	}

	kubemeta.SetCondition(&ac.Status.Conditions, meta.Condition{
		Type:    kueue.AdmissionCheckActive,
		Status:  meta.ConditionTrue,
		Reason:  "Ready",
		Message: "the model deployment joint admission check controller is running",
	})
	if err := r.Client.Status().Update(ctx, ac); err != nil {
		// The predicate passes every update of this controller's check, so a conflict is followed
		// by an event for the newer version.
		return objectWriteResult(logger, err, "mark admission check active", ctrl.Result{})
	}

	logger.V(2).Info("activated model deployment joint admission check", "name", ac.Name)

	return ctrl.Result{}, nil
}

func (r *ModelDeploymentJointAdmissionCheckReconciler) SetupController(
	_ context.Context, opts controller.SetupOptions,
) error {
	r.Client = opts.Manager.GetClient()

	return ctrl.NewControllerManagedBy(opts.Manager).
		Named("modeldeploymentjointadmissioncheck").
		For(
			&kueue.AdmissionCheck{},
			ctrlbuilder.WithPredicates(ctrlpredicate.NewPredicateFuncs(func(obj ctrlcli.Object) bool {
				ac := obj.(*kueue.AdmissionCheck)

				return ac.Spec.ControllerName == _JointAdmissionControllerName
			})),
		).
		Complete(r)
}

// ModelDeploymentJointAdmissionReconciler gates every group of one ModelDeployment on the whole set
// having reserved quota.
//
// WITH ONE WORKLOAD PER GROUP, KUEUE'S OWN ATOMICITY NO LONGER COVERS THE DEPLOYMENT. A pod group is
// admitted as a unit, which is what makes a single-group deployment all-or-nothing; a deployment
// spread over two instanceTypes is two groups and two Workloads, and nothing relates them. Without
// this check a prefiller can be admitted and serving while its decoder waits for capacity that never
// arrives, which is a deployment that looks half-started and is in fact never going to finish.
//
// IT ANSWERS Ready OR Pending AND NEVER Retry. A Retry does not leave a Workload sitting on its
// reservation: Kueue EVICTS it, resetting the checks and dropping the reservation in two separate
// writes, and its scheduler then refuses to reserve again while a check reads Retry. Two siblings
// waiting on each other through Retry would trade the same quota indefinitely, each one's wait
// destroying the reservation the other was waiting to observe. Pending holds, which is the whole
// reason it is the answer here.
//
// IT NEVER REJECTS AND NEVER PREEMPTS. A deployment whose set is permanently infeasible is bounded
// elsewhere; within this controller a set that cannot assemble waits.
type ModelDeploymentJointAdmissionReconciler struct {
	Client ctrlcli.Client

	// Clock measures how long a set has failed to assemble. It is a field so the bound can be
	// exercised rather than waited on: a test that slept for the real bound would be one nobody runs,
	// and a bound nobody runs is a bound nobody has checked.
	Clock clock.PassiveClock
}

var _ ctrlreconcile.Reconciler = (*ModelDeploymentJointAdmissionReconciler)(nil)

func (r *ModelDeploymentJointAdmissionReconciler) Reconcile(
	ctx context.Context, req ctrl.Request,
) (ctrl.Result, error) {
	// Implement this function in chapter 07.
	var result0 ctrl.Result
	var result1 error
	return result0, result1
}

// modelDeploymentReplicaGroups indexes a deployment's own replicas by the pod group each one joined.
//
// THE LIST IS SCOPED BY THE DEPLOYMENT'S OWN LABEL, not by the namespace. Its callers run on a
// controller that watches every Workload in the cluster, so an unscoped list here is an unbounded
// read in a hot path -- it would cost the whole namespace on every gated Workload's event, and the
// namespaces this runs in are the ones with the most Pods in them.
//
// THE LABEL NARROWS THE READ; OWNERSHIP STILL DECIDES MEMBERSHIP. A label is a value anyone can copy
// onto a Pod, and an owner reference is not.
//
// IT RETURNS TWO INDEXES BECAUSE ITS TWO CALLERS ASK DIFFERENT QUESTIONS, and one answer would be
// wrong for one of them. "Is this group assembled" counts only replicas that are staying, since a
// Pod already asked to go is not a member the group will have. "Which Workloads own these replicas"
// counts every one, since a terminating Pod is still owned and the Workload holding it is still the
// one a sibling's event has to reach. Collapsing the two either reads a rebuilding group as complete
// or drops a Workload from the mapping while its last Pod is leaving.
// modelDeploymentReplicaGroups splits this deployment's Pods by their Kueue group, and reads each
// group's deployed shape off the same Pods.
//
// The shape is read with the same reader the other observations use, seats included, so a group
// whose members all claim one seat is not mistaken for one still assembling.
func modelDeploymentReplicaGroups(
	ctx context.Context, cli ctrlcli.Client, md *workercore.ModelDeployment,
) (all, live map[string]sets.Set[types.UID], shapes map[string]modelDeploymentReplicaShape, err error) {
	podList := new(core.PodList)
	if err = cli.List(ctx, podList,
		ctrlcli.InNamespace(md.Namespace),
		ctrlcli.MatchingLabels{modelDeploymentLabelKeyInstance: md.Name},
		ctrlclix.WithoutQuorum); err != nil {
		return nil, nil, nil, fmt.Errorf("list replicas: %w", err)
	}

	all, live = make(map[string]sets.Set[types.UID]), make(map[string]sets.Set[types.UID])
	shapes = make(map[string]modelDeploymentReplicaShape)
	members := make(map[string][]*core.Pod)
	for i := range podList.Items {
		pod := &podList.Items[i]
		if !modelDeploymentOwns(pod, md) {
			continue
		}
		group := pod.Labels[kueuepodconst.GroupNameLabel]
		if all[group] == nil {
			all[group] = sets.New[types.UID]()
		}
		members[group] = append(members[group], pod)
		all[group].Insert(pod.UID)
		if pod.DeletionTimestamp != nil {
			continue
		}
		if live[group] == nil {
			live[group] = sets.New[types.UID]()
		}
		live[group].Insert(pod.UID)
	}

	// Seats are validated with the same reader the other observations use.
	for group, groupMembers := range members {
		shapes[group] = modelDeploymentDeployedReplicaShape(
			modelDeploymentReplicaView{Members: groupMembers},
		)
	}

	return all, live, shapes, nil
}

// workloadModelDeployment resolves the deployment a Workload's replicas belong to, or nil when the
// Workload is not one of this operator's.
//
// KUEUE OWNS A GROUP'S WORKLOAD FROM ITS MEMBERS, with a plain owner reference per Pod and no
// controller reference, so the path runs Workload to Pod to ModelDeployment. Any owner is enough:
// every member of a group belongs to the same deployment by construction.
func (r *ModelDeploymentJointAdmissionReconciler) workloadModelDeployment(
	ctx context.Context, wl *kueue.Workload,
) (md *workercore.ModelDeployment, decided bool, err error) {
	// UNRESOLVABLE IS NOT THE SAME ANSWER AS NOT OURS, and collapsing them opens the barrier. A group
	// being rebuilt has its replicas deleted while its Workload still references them, so every Get
	// below misses and the walk ends with nothing -- which, read as "not one of ours", makes this
	// controller answer Ready and admit a set that is mid-teardown. The same shape arrives from an
	// informer cache that has not caught up.
	//
	// A WORKLOAD WITH NO Pod OWNER AT ALL IS GENUINELY NOT OURS, and that is the case this has to keep
	// answering, because the check is referenced from every operator-owned queue: without it every
	// foreign Workload in those queues waits forever with nothing naming the cause.
	missing := false

	for _, ref := range wl.OwnerReferences {
		if !modelDeploymentOwnerRefNamesAPod(ref) {
			continue
		}

		pod := new(core.Pod)
		err := r.Client.Get(ctx,
			ctrlcli.ObjectKey{Namespace: wl.Namespace, Name: ref.Name}, pod, ctrlclix.WithoutQuorum)
		if err != nil {
			if ctrlcli.IgnoreNotFound(err) != nil {
				return nil, false, err
			}
			missing = true

			continue
		}

		owner := meta.GetControllerOf(pod)
		if owner == nil || owner.Kind != "ModelDeployment" {
			continue
		}

		md := new(workercore.ModelDeployment)
		err = r.Client.Get(ctx,
			ctrlcli.ObjectKey{Namespace: wl.Namespace, Name: owner.Name}, md, ctrlclix.WithoutQuorum)
		if err != nil {
			if ctrlcli.IgnoreNotFound(err) != nil {
				return nil, false, err
			}
			missing = true

			continue
		}

		return md, true, nil
	}

	return nil, !missing, nil
}

// jointCheckEntry returns this controller's admission check entry on a Workload, or nil when the
// Workload does not carry it. Read off the object with no I/O.
//
// EVERY READER OF THAT ENTRY GOES THROUGH HERE, so the name is compared in one place and callers
// get the entry rather than a yes-or-no. The questions asked of it differ -- presence, how long it
// has been Pending, what its message says, and in tests which state it reached -- and a helper
// returning only a bool would leave the rest rescanning the slice for the entry they need.
func jointCheckEntry(wl *kueue.Workload) *kueue.AdmissionCheckState {
	return kueueadmissioncheck.FindAdmissionCheck(
		wl.Status.AdmissionChecks, kueue.AdmissionCheckReference(_JointAdmissionCheckName),
	)
}

// carriesJointCheck reports whether a Workload carries this controller's admission check, read off
// the object with no I/O.
//
// IT IS A NECESSARY CONDITION AND NOT A SUFFICIENT ONE, which is why nothing downstream of it is
// removed: the check is referenced from every operator-owned queue, so plenty of Workloads carry it
// that this operator did not create. What it rules out cheaply is everything in every OTHER queue.
func carriesJointCheck(wl *kueue.Workload) bool {
	return jointCheckEntry(wl) != nil
}

// jointSiblings maps a Workload that changed to the sibling replicas' workloads of the same
// deployment.
//
// WITHOUT IT THE BARRIER ONLY EVER CLOSES. A held replica's verdict is a statement about a DIFFERENT
// object: it stays Pending until the last sibling reserves quota, and that reservation is written to
// the sibling's Workload. Watching only this controller's own object delivers that event to the
// sibling alone, so the held replica is never judged again and a barrier that correctly refused a
// half-feasible set never opens when the set becomes feasible.
//
// IT HIDES IN A CLUSTER WHERE THE REPLICAS RESERVE AT NEARLY THE SAME MOMENT, which is the ordinary
// case and not a guarantee: each replica's own event then arrives after the others have already
// reserved, and every replica opens on its own watch.
func (r *ModelDeploymentJointAdmissionReconciler) jointSiblings(
	ctx context.Context, obj ctrlcli.Object,
) []ctrlreconcile.Request {
	logger := ctrllog.FromContext(ctx)

	wl, ok := obj.(*kueue.Workload)
	if !ok {
		return nil
	}

	// THE CHEAPEST FILTER RUNS FIRST AND COSTS NO READ AT ALL. This mapping is called for every
	// Workload event in the cluster, and everything below it reads objects: a Get per owner replica,
	// then a list of the namespace's Workloads. A Workload that does not carry this controller's check
	// cannot be one this barrier holds, and that is answerable from the object already in hand.
	if !carriesJointCheck(wl) {
		return nil
	}

	// An undecided resolution maps to nothing rather than requeueing: this is an event mapping, and
	// the Workload whose replicas could not be read gets its own pass through Reconcile, which is
	// where waiting belongs.
	md, _, err := r.workloadModelDeployment(ctx, wl)
	if err != nil {
		logger.Error(err, "resolve the workload's model deployment for sibling mapping")
		return nil
	}
	if md == nil || len(modelDeploymentPodGroups(md)) < 2 {
		return nil
	}

	// Every replica, terminating ones included: this maps an event to the siblings that have to see
	// it, and a Workload still holding a leaving Pod is still one of them.
	byGroup, _, _, err := modelDeploymentReplicaGroups(ctx, r.Client, md)
	if err != nil {
		logger.Error(err, "index the deployment's replicas for sibling mapping")
		return nil
	}
	members := sets.New[types.UID]()
	for _, group := range byGroup {
		members = members.Union(group)
	}

	wlList := new(kueue.WorkloadList)
	if err = r.Client.List(ctx, wlList, ctrlcli.InNamespace(md.Namespace), ctrlclix.WithoutQuorum); err != nil {
		logger.Error(err, "list workloads for sibling mapping")
		return nil
	}

	var reqs []ctrlreconcile.Request
	for i := range wlList.Items {
		sibling := &wlList.Items[i]
		// The changed Workload is left out because the watch below already enqueues it.
		if sibling.Name == wl.Name || !modelDeploymentWorkloadOwnsAny(sibling, members) {
			continue
		}
		reqs = append(reqs, ctrlreconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: sibling.Namespace, Name: sibling.Name},
		})
	}

	return reqs
}

func (r *ModelDeploymentJointAdmissionReconciler) SetupController(
	_ context.Context, opts controller.SetupOptions,
) error {
	r.Client = opts.Manager.GetClient()

	return ctrl.NewControllerManagedBy(opts.Manager).
		Named("modeldeploymentjointadmission").
		For(&kueue.Workload{}).
		Watches(&kueue.Workload{}, ctrlhandler.EnqueueRequestsFromMapFunc(r.jointSiblings)).
		Complete(r)
}
