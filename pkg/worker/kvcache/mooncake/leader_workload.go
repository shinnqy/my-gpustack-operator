package mooncake

import (
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

const (
	// leaderResourceNoteRole distinguishes the leader's objects from a member group's, which are
	// noted the same way and land in the same namespace.
	leaderResourceNoteRole = "leader"

	// The identity labels the leader's objects and Pods carry. They are constants because a watch
	// on the leader's Pods has to read back exactly what the renderer wrote, and a Pod is the one
	// object here that carries no resource note: it is made by the Deployment's own controller from
	// a template, and the note sits on the Deployment rather than inside that template.
	labelKeyName      = "app.kubernetes.io/name"
	labelKeyInstance  = "app.kubernetes.io/instance"
	labelKeyComponent = "app.kubernetes.io/component"
	labelValueName    = "kv-cache-backend"
	labelValueLeader  = "leader"
	// Outside the selector, and named here rather than written inline because the HA accounts
	// carry it too and a second spelling is how the two sets of objects stop being findable
	// together.
	labelKeyPartOf   = "app.kubernetes.io/part-of"
	labelValuePartOf = "gpustack-operator-worker"

	// leaderRPCPortName and leaderAdminPortName name the two published ports. They are names
	// rather than numbers on the Service so a consumer can ask for a role, and because the two
	// serve entirely different audiences: one is what an inference engine dials, the other is what
	// this operator scrapes.
	leaderRPCPortName   = "rpc"
	leaderAdminPortName = "admin"

	// leaderReadinessPath is a GATED route: the artifact wraps it in a check on the service plane
	// and answers 503 until that plane is up. That is exactly the readiness question — may traffic
	// go here — so the probe asks it directly instead of inferring it.
	leaderReadinessPath = "/get_all_segments"
	// leaderLivenessPath is an UNGATED route: it answers 200 whenever the process answers at all,
	// whatever state the service plane is in. That is the liveness question, and it is the reason
	// liveness may NOT reuse the readiness path — a leader that is merely slow to activate would be
	// killed for being slow.
	leaderLivenessPath = "/health"

	// The two volumes the quota policy needs, and the container that bridges them. They are separate
	// volumes because they answer opposite requirements: one has to be writable and cannot be a
	// ConfigMap, the other has to carry the operator's desired state and can only be a ConfigMap.
	quotaPolicyVolumeName     = "tenant-quota-policy"
	quotaPolicySeedVolumeName = "tenant-quota-policy-seed"
	quotaPolicyInitName       = "seed-tenant-quota-policy"

	// The environment variables the seed command reads. The command itself is a fixed string and
	// every value it touches arrives this way — a rendered YAML document interpolated into a shell
	// command would be a policy passing through quoting rules.
	quotaPolicySeedEnv  = "POLICY_SEED"
	quotaPolicyFileEnv  = "POLICY_FILE"
	quotaPolicyEmptyEnv = "POLICY_EMPTY"

	// quotaPolicySeedCommand copies the operator's desired policy onto the writable volume, and
	// writes an empty one when there is nothing to copy. The variables it reads are the three
	// constants above; a test pins both sides to the same literal names.
	//
	// The fallback is not defensive: the ConfigMap is rendered by the POOL reconciler, so a backend
	// with multi-tenancy on and no pool bound to it yet has nobody to write one. Its volume is
	// optional and mounts as an empty directory, the copy finds no source, and without a document
	// here the master would refuse to start — an empty file does not satisfy its parser either.
	//
	// Which is why the fallback is gated on the source EXISTING, and not on the copy failing.
	// `cp || fallback` cannot tell the intended case apart from a copy that failed for any other
	// reason — a permission, a truncated mount, a transient IO error — and `2>/dev/null` removed
	// the last trace of it. Laundered into the no-pool case, a real failure seeds an EMPTY ledger,
	// the init container exits 0, the master starts with no tenant quotas, and the pool goes on
	// reporting Ready over grants that are gone. Gated on the test, that same failure fails the
	// init container with cp's own message, which is the only place it can still be seen.
	//
	// This runs on every Pod start, so it OVERWRITES whatever the master last wrote through the
	// admin API. That is the intended direction: the ConfigMap is the desired state and the file is
	// the master's copy of it, so a restart resets the copy to what the operator wants and the next
	// reconcile pass re-converges the ledger to match.
	quotaPolicySeedCommand = `if [ -f "$POLICY_SEED" ]; then ` +
		`cp "$POLICY_SEED" "$POLICY_FILE"; else ` +
		`printf '%s' "$POLICY_EMPTY" > "$POLICY_FILE"; fi`
)

// LeaderObjectNameSuffix is what LeaderObjectName appends. It is exported because one object with
// this name is created by the STORE rather than by this operator -- the Lease the election runs
// through -- so the only way back from it to the backend is to strip this, and a second literal
// spelling it is how that reverse stops agreeing with the forward direction.
const LeaderObjectNameSuffix = "-leader"

// LeaderObjectName is the name of every object rendered for a backend's leader.
//
// The backend is cluster-scoped and its objects are not, so the name has to survive the move into
// one shared namespace: it is the backend's own name with a role suffix, which keeps two backends
// apart and keeps a backend's own objects together.
func LeaderObjectName(kvcb *workercore.KVCacheBackend) string {
	return kvcb.Name + LeaderObjectNameSuffix
}

// LeaderPodBackendName reports which backend a Pod belongs to, and "" for any Pod that is not a
// leader's.
//
// It reads the identity labels rather than a resource note because the leader's Pods carry no note
// — see the label constants. Those labels are the Deployment's own selector, which is immutable, so
// a watch matching on them cannot drift away from what the renderer writes.
func LeaderPodBackendName(labels map[string]string) string {
	if labels[labelKeyName] != labelValueName || labels[labelKeyComponent] != labelValueLeader {
		return ""
	}
	return labels[labelKeyInstance]
}
