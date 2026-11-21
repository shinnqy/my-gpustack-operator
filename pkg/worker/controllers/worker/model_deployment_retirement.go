// The retirement reservation: the one protocol that is allowed to delete a replica, and the gate
// that keeps every other path's hands off it while it runs.
//
// A reservation is a persisted decision about ONE replica. It is written into the status by
// whatever admits it, and this file carries it to its end. Everything the protocol must not get
// wrong is expressed as three-valued answers and holds: a replica that might still be serving is
// never deleted on the strength of an observation that failed to arrive.
//
// TWO DECISIONS, NEVER ONE. Hold is the interception predicate -- whether a delete naming one of
// the target's UIDs must be refused this pass, whatever the path that wants it. DeletesAllowed is
// the protocol's own permission to commit its delete. They are separate because they are separate
// questions, and a single flag would make the two indistinguishable in the status: a path refused
// because the replica is reserved looks identical to a path the protocol itself is holding, and an
// operator reading the status could not tell which happened.
//
// WITH NO RESERVATION PRESENT NOTHING HERE RUNS. The plan is empty, Hold is false and every delete
// path below behaves exactly as it did before this file existed. That is a property this task has
// to earn per path rather than assert, because it is the property that makes adding a protocol to
// a controller safe.
//
// THE STATES ARE LEVELS, NOT STEPS. Every pass re-enters at the persisted state and either stays,
// advances or aborts, so a pass that was never told what happened makes the same decision a pass
// that was. Nothing is done because a previous pass said so, and a controller restart re-enters
// rather than re-decides -- which is what persisting phaseStartedAt buys.
package worker

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	kubeapistatus "gpustack.ai/gpustack/pkg/kubeapistatus"
	"gpustack.ai/gpustack/pkg/kubemeta"
)

// THE PROTOCOL'S BUDGETS, frozen. A phase ends at the earlier of the overall deadline and its own
// budget measured from phaseStartedAt, so no phase outlives the operation and a restart cannot
// extend one.
//
// THEY ARE NOT THE PRESTOP HOOK'S. The hook carries its own settle and exit reserve
// (modelDeploymentDrainSettleSeconds and modelDeploymentDrainExitSeconds) and runs inside the
// container on SIGTERM, while these run in the controller against a replica that is still
// untouched. The two are separate mechanisms and reading either number as the other's is an easy
// and completely silent error, so they are declared here rather than borrowed from there.
const (
	// modelDeploymentRetirementWithdrawalBudget is one withdrawal phase in two steps: the target
	// leaves selection, and then the answer confirming it serves nothing is read. It is ONE budget
	// because the spec names one, and splitting it would double the time a group spends out of
	// selection for no decision that does not already exist.
	modelDeploymentRetirementWithdrawalBudget = 30 * time.Second

	// modelDeploymentRetirementDrainBudget is how long the protocol waits for the engine's own
	// in-flight queues to reach zero, with every member still running.
	modelDeploymentRetirementDrainBudget = 240 * time.Second

	modelDeploymentRetirementDisaggregatedGraceWindow = 60 * time.Second

	// modelDeploymentRetirementSettleBudget is how long the protocol waits for the accelerator and
	// the quota to be released after the delete, which is a convergence rather than a decision.
	modelDeploymentRetirementSettleBudget = 30 * time.Second

	// modelDeploymentRetirementOverallBudget bounds the whole operation.
	modelDeploymentRetirementOverallBudget = 300 * time.Second
)

// modelDeploymentRetirementPlan is what one pass learned about the single reservation in flight.
//
// A zero plan is the no-reservation plan, and it is what every delete path sees when no operation
// is running: Hold false, DeletesAllowed true, nothing held.
type modelDeploymentRetirementPlan struct {
	// Reservation is the state this pass ends at, to be persisted with the rest of the status. It
	// is nil both when there is no operation and when a completed one has just been cleared, and
	// the two are told apart by Clear.
	Reservation *workercore.ModelDeploymentRetirementStatus

	// Target is the replica the reservation names, resolved against the Pods as they exist now.
	Target modelDeploymentReplicaView

	// held is the set of member UIDs the reservation claims. It is separate from Target so a path
	// can ask about one Pod without rebuilding the grouping, which is the question every delete
	// site actually asks.
	held sets.Set[types.UID]

	// Hold is whether a delete naming a held UID must be refused this pass.
	Hold bool

	// DeletesAllowed is the protocol's own permission to commit its delete. It is true at Deleting
	// and Settling and false everywhere the protocol is still withdrawing or draining.
	DeletesAllowed bool

	// Clear is that a completed operation has ended and the reservation is to be removed.
	Clear bool

	// InFlight is that the operation is still running, so the pass must come back to advance it.
	// Nothing else wakes a deployment whose replica is merely waiting for a queue to empty.
	InFlight bool

	// Deletes is the target the protocol itself is deleting this pass. It is empty at every other
	// state, including the ones where DeletesAllowed is true but the delete was already issued on
	// an earlier pass.
	Deletes []core.Pod
}

// holds reports whether a Pod is one of the operation's targets.
func (p modelDeploymentRetirementPlan) holds(uid types.UID) bool {
	return p.Hold && p.held.Has(uid)
}

// names reports whether the reservation claims this replica by name.
//
// IT IS ABOUT THE SLOT, NOT THE MEMBERS, and the distinction matters at the rollout. The protocol
// holds individual Pods by UID, so a rollout that condemned a replica would delete only the members
// it did not hold -- turning a per-replica decision into a partial one, and leaving the operation
// retiring a replica whose other members the rollout has already taken. A rollout asked to act on
// a reserved replica is asked to step over it entirely.
func (p modelDeploymentRetirementPlan) names(role string, ordinal int) bool {
	return p.InFlight && p.Reservation != nil && p.Reservation.RoleName == role &&
		int(p.Reservation.ReplicaOrdinal) == ordinal
}

// planModelDeploymentRetirement evaluates the reservation as a level and returns what this pass
// should persist and what it may do.
//
// A NO-RESERVATION PASS IS NOT AN ERROR AND IS NOT A HELD ONE. It returns the zero plan, and every
// caller treats that as "carry on exactly as before".
func (r *ModelDeploymentReconciler) planModelDeploymentRetirement(
	ctx context.Context, md *workercore.ModelDeployment, pods []core.Pod,
) *modelDeploymentRetirementPlan {
	// Implement this function in chapter 19.
	var result0 *modelDeploymentRetirementPlan
	return result0
}

// commitModelDeploymentRetirement issues the operation's delete, and it is the ONLY place in this
// controller that deletes a replica on the protocol's own authority.
//
// EVERY DELETE CARRIES ITS UID AS A PRECONDITION, which is what keeps a same-name replacement from
// being deleted in the target's place. The reservation holds members by UID precisely so that this
// is enforceable, and a delete without the precondition would make the UID set advisory -- a target
// deleted and a replacement created between the plan and the call would see the delete succeed
// against the replacement and report the operation complete while the original is still running.
//
// THE WORKLOAD GOES WITH THE PODS, as one decision, for the same reason it does everywhere else in
// this file: it is what releases the finalizer Kueue holds on every member of the group, and a Pod
// delete without it leaves the replica terminating forever.
func (r *ModelDeploymentReconciler) commitModelDeploymentRetirement(
	ctx context.Context, md *workercore.ModelDeployment, plan *modelDeploymentRetirementPlan,
	pods []core.Pod,
) error {
	logger := ctrllog.FromContext(ctx)
	if !plan.DeletesAllowed || len(plan.Deletes) == 0 {
		return nil
	}

	for i := range plan.Deletes {
		pod := &plan.Deletes[i]
		uid := pod.UID
		if err := r.Client.Delete(ctx, pod,
			ctrlcli.Preconditions{UID: &uid}); err != nil && !kerrors.IsNotFound(err) {
			return fmt.Errorf("delete retired replica %s: %w", pod.Name, err)
		}
		logger.Info("deleting retired replica member", "pod", pod.Name, "state", plan.Reservation.State)
	}

	return r.commitModelDeploymentRetirementWorkload(ctx, md, plan, pods)
}

// commitModelDeploymentRetirementWorkload deletes the Workloads that would otherwise hold the
// retired members' finalizers, and it is the subtle half of the delete.
//
// THE WORKLOAD IS THE KILL SWITCH AND IT IS GROUP-SCOPED. Kueue holds a deleted Pod of a managed
// group until that GROUP's Workload goes, and a group is a role's serving set rather than one
// replica's: four replicas of a role can share one Workload. Deleting it for the sake of one
// retiring replica releases the finalizer on all four, and the retirement takes the whole
// deployment down with it. The Pod delete above is per-member and safe; this one is not, which is
// exactly why it is asked as a question rather than issued.
//
// SO A WORKLOAD IS DELETED ONLY WHEN IT OWNS NOTHING OUTSIDE THE TARGET. A replica that is the
// whole group goes, and its Workload goes with it because it is about to be nothing anyway. A
// replica that is one of several leaves its Workload alone: the group is still serving, the
// remaining members keep their finalizer behind a Workload that still describes them, and the
// retired member is released when the group is next composed without it.
//
// The question is asked about the deployment's LIVE Pods rather than about the Workload's own
// status, so a Workload whose membership could not be read from its own object is not mistaken for
// one that describes nothing else. Being unable to prove the group is the target means not deleting.
func (r *ModelDeploymentReconciler) commitModelDeploymentRetirementWorkload(
	ctx context.Context, md *workercore.ModelDeployment, plan *modelDeploymentRetirementPlan,
	pods []core.Pod,
) error {
	workloads, err := r.findModelDeploymentGroupWorkloads(ctx, md, plan.Deletes)
	if err != nil {
		return err
	}

	target := sets.New[types.UID]()
	for i := range plan.Deletes {
		target.Insert(plan.Deletes[i].UID)
	}
	outsiders := sets.New[types.UID]()
	for i := range pods {
		if !target.Has(pods[i].UID) {
			outsiders.Insert(pods[i].UID)
		}
	}

	for _, workload := range workloads {
		if modelDeploymentWorkloadOwnsAny(workload, outsiders) {
			continue
		}

		uid := workload.UID
		if err = r.Client.Delete(ctx, workload,
			ctrlcli.Preconditions{UID: &uid}); err != nil && !kerrors.IsNotFound(err) {
			return fmt.Errorf("delete retired replica's workload %s: %w", workload.Name, err)
		}
	}

	return nil
}

// persistModelDeploymentRetirement writes the state this pass reached, and writes nothing when the
// pass reached the state that was already there.
//
// IT IS ITS OWN WRITE rather than a field set on the object before the pass's status sync, and the
// reason is that the sync compares the derived status with the observed one to decide whether to
// write at all. A field assigned in place would be equal to itself by the time the comparison ran,
// so the operation's progress would never reach the API server and every pass would re-enter at
// the state the first one reached. A pass that changed nothing writes nothing, which is what keeps
// a controller running on every Pod event from rewriting the object continuously.
//
// A NO-RESERVATION PASS NEVER WRITES: both sides are nil, and the comparison is the whole answer.
func (r *ModelDeploymentReconciler) persistModelDeploymentRetirement(
	ctx context.Context, md *workercore.ModelDeployment,
	previous *workercore.ModelDeploymentRetirementStatus, plan *modelDeploymentRetirementPlan,
) error {
	next := plan.Reservation
	if plan.Clear {
		next = nil
	}

	return r.writeModelDeploymentRetirement(ctx, md, previous, next)
}

// writeModelDeploymentRetirement persists the reservation and nothing else.
//
// The three early retirement writes share one shape, and they share one failure mode if they do
// not share one writer: a full-object status write sends the whole typed status back to the API
// server, including every field the object happens to carry, and the server validates all of it.
// A stored object written by an older version can carry enum values the current schema does not
// accept in fields this operation never touches, and then a write that only means "move the
// operation one phase on" is refused for something it did not say. Patching only the retirement
// field sends only what this operation asserts.
//
// The patch is optimistic: it carries the resourceVersion this pass read, so a stored object that
// moved underneath the write is a Conflict rather than a silent overwrite. A no-op writes nothing
// at all, and clearing a reservation writes an explicit null, because a patch that omits the field
// leaves whatever was there.
//
// The object is updated from the server's response, so a later write in the same pass carries the
// version this one produced rather than the one this pass started with.
func (r *ModelDeploymentReconciler) writeModelDeploymentRetirement(
	ctx context.Context, md *workercore.ModelDeployment,
	previous, next *workercore.ModelDeploymentRetirementStatus,
) error {
	if kubemeta.DeepEqual(previous, next) {
		return nil
	}

	// The base is the object as the server last had it, which is why the caller hands both sides
	// over. A base copied after the caller assigned the new value would already contain it, and the
	// diff between the two would be empty.
	base := md.DeepCopy()
	base.Status.Retirement = previous
	candidate := base.DeepCopy()
	candidate.Status.Retirement = next

	optimistic := ctrlcli.MergeFromWithOptions(base, ctrlcli.MergeFromWithOptimisticLock{})
	if err := r.Client.Status().Patch(ctx, candidate, optimistic); err != nil {
		return err
	}

	// The patch decodes the server's response into the candidate, so the version this write produced
	// is the version the next write in the pass carries. Nothing is read back, because that would
	// ask the client for a Get this writer does not otherwise depend on.
	//
	// The candidate is copied back only once the server has accepted it. A write that fails leaves
	// the caller's object describing what the server still holds, which is what the rest of the pass
	// reads to decide whether a token was consumed and whether a delete is allowed.
	md.ResourceVersion = candidate.ResourceVersion
	md.Status = candidate.Status

	return nil
}

// modelDeploymentNow reads the clock, which is a field rather than a call to time.Now so a test
// can place a budget — a reservation's phases or an elastic retry's backoff — at a moment of its
// choosing instead of waiting for one.
func (r *ModelDeploymentReconciler) modelDeploymentNow() time.Time {
	if r.clock == nil {
		return time.Now()
	}

	return r.clock()
}

func modelDeploymentContainerEnv(container *core.Container) []workercore.ModelDeploymentEnvVar {
	env := make([]workercore.ModelDeploymentEnvVar, 0, len(container.Env))
	for _, entry := range container.Env {
		env = append(env, workercore.ModelDeploymentEnvVar{Name: entry.Name, Value: entry.Value})
	}
	return env
}

// modelDeploymentDrainActivityValue reports whether a gauge value can be read as activity at all.
//
// A count of work in flight is a nonnegative finite number, and anything else is an answer this
// protocol cannot use: NaN compares false against every bound, so it slips past a zero test while
// being no measurement of anything, and an infinity or a negative count is a signed row that says
// more about the scrape than about the queue. The parser and the protocol share this one predicate
// so a value refused at the envelope is refused identically where it is judged.
func modelDeploymentDrainActivityValue(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0) && value >= 0
}

// errModelDeploymentReservationUnnameable is a target that cannot be identified, so no reservation
// can name it. It holds the removal rather than declining it, because the only way to be sure the
// removal is safe is to have an identity to check it against.
var errModelDeploymentReservationUnnameable = errors.New(
	"the target has no uid, so a reservation cannot name it",
)

// refuseModelDeploymentRemoval is the admission leg: it decides, at the moment a path is about to
// delete a replica it no longer wants, whether the retirement protocol may take that deletion over.
//
// IT ALWAYS HOLDS, and that is the whole of it. A removal intent either starts the protocol -- the
// first one, with nothing in flight -- or is refused because an operation already holds a replica,
// and in both cases the delete does not happen on this pass. The one thing it never does is delete.
//
// WHY THE PROTOCOL HAS TO OWN THE DELETION. A replica is removed by deleting its Pod and the
// Workload Kueue holds its finalizer behind. That is an instantaneous cut: the engine is signaled
// and the request it was serving is refused mid-flight, with no observation of what the target was
// actually doing. The protocol exists to spend a withdrawal and a drain before that cut, so a path
// that is about to delete must hand the decision to it first. Without this leg the reservation
// object has no writer, a 2-to-1 scale-down deletes immediately as it always did, and no accepted
// answer about what the target was serving can ever be reached.
//
// ONE OPERATION AT A TIME, AND A SECOND INTENT IS REFUSED RATHER THAN QUEUED. The wire holds one
// reservation per deployment, and two operations withdrawing at once would be two answers to one
// question with no way to merge them. So while a reservation is in flight every other removal holds,
// and it holds on a LATER PASS rather than being recorded: nothing is lost by waiting, because the
// removal intent is still there when the operation ends, and the pass after that admits it. That is
// what makes this level-based -- a pass that holds is not a pass that failed, and a removal blocked
// today is a removal that proceeds once the protocol is finished.
//
// A POD CLAIMING NO ORDINAL IS NOT ADMITTED, because the wire names its target by (role, ordinal)
// and a Pod belonging to no replica has none to name. It is also not held, because a hold with no
// reservation to eventually clear is a replica nothing will ever remove. The rollout is what removes
// it, which is where a Pod with no seat has always been removed from.
func (r *ModelDeploymentReconciler) refuseModelDeploymentRemoval(
	ctx context.Context, md *workercore.ModelDeployment, plan *modelDeploymentRetirementPlan,
	pod *core.Pod, pods, members []core.Pod,
) bool {
	logger := ctrllog.FromContext(ctx)
	role := modelDeploymentPodRole(pod)
	ordinal, seated := modelDeploymentPodOrdinal(pod)

	// A reserved pod is refused before it is asked whether it is seated. The reservation names its
	// members by UID, and the ordinal label is mutable: strip it and the pod is no longer a replica
	// the wire can name, while the operation still holds that very pod. The unseated return below
	// would hand it to a path that deletes on the role. The check is narrow, and only a pod the
	// reservation actually holds is refused here.
	if plan.holds(pod.UID) {
		logger.V(3).Info("holding a removal of a pod the reservation already holds",
			"pod", pod.Name, "role", role, "ordinal", ordinal, "seated", seated,
			"state", plan.Reservation.State)

		return true
	}

	if !seated {
		return false
	}

	if plan.Reservation == nil || plan.Reservation.RoleName != role ||
		int(plan.Reservation.ReplicaOrdinal) != ordinal {
		podPointers := make([]*core.Pod, 0, len(pods))
		for i := range pods {
			podPointers = append(podPointers, &pods[i])
		}
		members := make([]*core.Pod, 0, 1)
		for _, member := range modelDeploymentReplicaMembers(podPointers, ordinal) {
			if modelDeploymentPodRole(member) == role {
				members = append(members, member)
			}
		}
		neverScheduled := len(members) > 0
		for i := range members {
			if !modelDeploymentReplicaNeverScheduled(members[i]) {
				neverScheduled = false
				break
			}
		}
		if neverScheduled {
			workloads, err := r.findModelDeploymentGroupWorkloads(ctx, md, modelDeploymentPodValues(members))
			if err != nil {
				logger.Error(err, "find workloads for never-scheduled retirement candidate", "pod", pod.Name)
			}
			admitted := false
			for _, workload := range workloads {
				if kubeapistatus.ConditionType(kueue.WorkloadAdmitted).IsTrue(workload) {
					admitted = true
					break
				}
			}
			// On transient list error, fall through conservatively to standard retirement admission
			// rather than bypassing retirement protocols on unconfirmed admission status.
			if err == nil && !admitted {
				return false
			}
		}
	}

	// A removal is refused while the protocol runs and while it retains. InFlight covers only the
	// first: an abort clears it while keeping the target, so a second intent would overwrite the
	// retained reservation. Hold is true exactly when something is retained, so both together
	// refuse a conflict without freezing the exits that legitimately replace the reservation.
	if plan.InFlight || plan.Hold {
		logger.V(3).Info("holding a removal while a retirement reservation is in flight or retained",
			"pod", pod.Name, "role", role, "ordinal", ordinal,
			"state", plan.Reservation.State, "retained", plan.Hold)

		return true
	}

	if err := r.admitModelDeploymentRetirement(ctx, md, plan, pod, pods, members); err != nil {
		// A reservation that could not be written is a hold, not a delete. The pass comes back
		// because the requeue that produces the retry is set on the plan either way, and a
		// deletion issued instead would be the cut this protocol exists to prevent.
		logger.Error(err, "admit a retirement reservation", "pod", pod.Name)

		return true
	}

	return true
}

func modelDeploymentReplicaNeverScheduled(pod *core.Pod) bool {
	return pod.Spec.NodeName == "" && pod.Status.PodIP == "" && pod.Status.Phase == core.PodPending &&
		pod.Labels[modelDeploymentLabelKeyEndpointEligible] != modelDeploymentEndpointEligibleValue
}

// admitModelDeploymentRetirement writes the reservation for a replica a removal path wants gone.
//
// THE MEMBER SET IS THE CALLER'S TO NARROW, and that is the difference between a retirement and a
// shed. An ordinal the spec no longer declares is retired whole, so the members are the replica's.
// The surplus rule's decision is finer: it sheds one MEMBER of an ordinal that is still wanted, and
// binding the whole ordinal there would delete the member the current render still describes --
// which is precisely the member the surplus rule exists to keep. A caller that knows a narrower set
// passes it, and the wire's member UIDs are what express it.
//
// IT BINDS THE REPLICA AS IT IS NOW, not as the spec describes it. The member UIDs are what a later
// delete's precondition is checked against, and the Workload UID is what releases the finalizer
// holding each member, so both are read at admission rather than recomputed later: a slot refilled
// between admission and the delete would otherwise be deleted in the reserved member's place.
//
// THE PASS THAT ADMITS DELETES NOTHING, which is the caller's half: this function returns, the
// caller holds, and because the plan now carries a reservation every later removal on the same pass
// is refused by the branch above.
func (r *ModelDeploymentReconciler) admitModelDeploymentRetirement(
	ctx context.Context, md *workercore.ModelDeployment, plan *modelDeploymentRetirementPlan,
	pod *core.Pod, pods, members []core.Pod,
) error {
	role := modelDeploymentPodRole(pod)
	ordinal, _ := modelDeploymentPodOrdinal(pod)
	now := r.modelDeploymentNow()

	if members == nil {
		members = modelDeploymentRetirementReplicaMembers(pods, role, ordinal)
	}
	workloadUID := ""
	// A replica short of its declared total composes no Workload at all, which is the same state
	// every other reader of the group's Workload already tolerates as absence. Binding the empty
	// string rather than inventing one is what keeps admission from failing on a group Kueue has
	// not composed yet.
	if len(members) > 0 {
		workloads, err := r.findModelDeploymentGroupWorkloads(ctx, md, members)
		if err != nil {
			return err
		}
		if len(workloads) > 0 {
			workloadUID = string(workloads[0].UID)
		}
	}

	// A MEMBER WITH NO UID CANNOT BE NAMED, and a reservation that binds the empty string names
	// EVERY POD IN THE DEPLOYMENT: the target is resolved by UID, so an unnamed target resolves to
	// the whole list and the protocol deletes all of it. A cluster always assigns a UID before this
	// controller ever sees a Pod, so this is a fixture and a defensive check rather than a branch
	// production takes -- which is exactly why the branch HOLDS rather than falls through to the
	// deletion. Failing open here would delete a deployment over a missing identity.
	uids := make([]string, 0, len(members))
	for _, member := range members {
		if member.UID == "" {
			return errModelDeploymentReservationUnnameable
		}
		uids = append(uids, string(member.UID))
	}
	slices.Sort(uids)

	reservation := &workercore.ModelDeploymentRetirementStatus{
		RoleName:           role,
		ReplicaOrdinal:     int32(ordinal),
		ObservedGeneration: md.Generation,
		TargetMemberUIDs:   uids,
		TargetWorkloadUID:  workloadUID,
		State:              workercore.ModelDeploymentRetirementStateAdmitted,
		Reason:             "admitted for a replica this pass no longer declares",
		StartedAt:          meta.NewTime(now),
		Deadline:           meta.NewTime(now.Add(modelDeploymentRetirementOverallBudget)),
		PhaseStartedAt:     meta.NewTime(now),
	}

	if err := r.writeModelDeploymentRetirement(ctx, md, md.Status.Retirement, reservation); err != nil {
		return fmt.Errorf("persist an admitted retirement reservation: %w", err)
	}

	plan.Reservation = reservation
	plan.Hold = true
	plan.InFlight = true
	plan.Target = modelDeploymentReplicaView{Role: role, Ordinal: ordinal, Seated: true}
	plan.held = sets.New[types.UID]()
	for _, member := range members {
		plan.held.Insert(member.UID)
		plan.Target.Members = append(plan.Target.Members, &member)
	}

	return nil
}

// modelDeploymentRetirementReplicaMembers are the live Pods seated on one replica, resolved through
// the tree's own grouping so that admission and the protocol's later target resolution cannot
// disagree about what one replica is.
func modelDeploymentRetirementReplicaMembers(
	pods []core.Pod, role string, ordinal int,
) []core.Pod {
	members := make([]core.Pod, 0, len(pods))
	for _, view := range modelDeploymentGroupPodsByReplica(pods) {
		if view.Role != role || !view.Seated || view.Ordinal != ordinal {
			continue
		}
		for _, member := range view.Members {
			members = append(members, *member)
		}
	}

	return members
}

// modelDeploymentShedMembers is a shed set in the value form admission binds. The shed is a list
// of Pod pointers because the surplus rule works in pointers, and a reservation binds values, so
// the conversion is done once here rather than at each call site.
func modelDeploymentShedMembers(shed []*core.Pod) []core.Pod {
	members := make([]core.Pod, 0, len(shed))
	for _, pod := range shed {
		members = append(members, *pod)
	}

	return members
}

// modelDeploymentRetirementShedSeat reports whether a shed set has a replica the protocol can name,
// and returns its first member's ordinal. A shed can be empty even where the count is over -- the
// pick is by slot and by what the current render describes, and a set it cannot separate yields
// nothing -- and an empty set is not a removal to hand over.
func modelDeploymentRetirementShedSeat(shed []*core.Pod) (int, bool) {
	if len(shed) == 0 {
		return 0, false
	}

	return modelDeploymentPodOrdinal(shed[0])
}
