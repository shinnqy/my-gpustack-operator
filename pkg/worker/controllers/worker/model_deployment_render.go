package worker

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"

	worker "gpustack.ai/gpustack/api/worker/v1"
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/systemname"
	"gpustack.ai/gpustack/pkg/utils/json"
	"gpustack.ai/gpustack/pkg/utils/slicex"
	"gpustack.ai/gpustack/pkg/utils/stringx"
)

const (
	// ModelDeploymentResourceType is the resource note every object a ModelDeployment renders
	// carries, so a watch can tell them from every other Pod in the namespace.
	ModelDeploymentResourceType = "modeldeployments"
	// ModelDeploymentResourceNoteRole names the role a rendered object belongs to.
	ModelDeploymentResourceNoteRole = "role"

	// modelDeploymentLabelKeyName, modelDeploymentLabelKeyInstance and
	// modelDeploymentLabelKeyComponent are the identity labels a replica's Pod carries.
	//
	// They exist alongside the resource note because a Service selects on LABELS and cannot read an
	// annotation, and because they carry only identity — which deployment, which role — and never
	// anything a spec update can move. A selector that moved would orphan the Pods it used to
	// front. The same three keys, with the same meanings, front the KV cache backend's leader.
	modelDeploymentLabelKeyName      = "app.kubernetes.io/name"
	modelDeploymentLabelKeyInstance  = "app.kubernetes.io/instance"
	modelDeploymentLabelKeyComponent = "app.kubernetes.io/component"
	modelDeploymentLabelValueName    = "model-deployment"

	// modelDeploymentLabelKeyRoleKind carries the role's EFFECTIVE kind, so that something in front
	// of the replicas can tell a prefiller from a decoder by selector. It is the resolved value, not
	// the field: a role that names no kind is a server, and a selector matching the empty string
	// would miss every replica of the default shape.
	//
	// It is rendered for every deployment, including one whose only role is a server. A key that
	// appeared only under disaggregation would make "no prefiller exists" and "this deployment does
	// not label roles" the same query result.
	//
	// It is NOT in the selector labels. A Service's selector cannot change without orphaning the
	// replicas it used to front, and while a role's kind is immutable today, the reason this key
	// stays out is the same one that keeps the entrance label out: the selector carries identity,
	// and the kind is a property of the role rather than a name for it.
	//
	// Adding it rolls every existing deployment once. The Pod spec hash covers the labels -- see
	// modelDeploymentPodSpecHash -- so every replica rendered before this key existed has a stale
	// fingerprint, and the recreate rollout replaces it. The rebuild is the one the recreate policy
	// already describes, and a replica that leaves loses its cached blocks to its siblings.
	modelDeploymentLabelKeyRoleKind = "modeldeployment." + systemname.LabelPrefix + "role-kind"

	// modelDeploymentRouterCandidateLabel marks a member as a routing candidate of its group.
	//
	// IT IS A ROUTING FACT AND NOT A QUALIFICATION: it says the member may be discovered, never
	// that it passed a probe.
	//
	// A MEMBER OF A ROLE THE OPERATOR JUDGES CARRIES IT WHILE ITS GROUP IS ELIGIBLE, and it gains
	// and loses it in the same write as the eligibility label. The judged roles are the ones the
	// operator decides on its own: a protected managed group, an encoder, and an ordinary
	// single-member server. A MANAGED PREFILL OR DECODE PAIR IS ONE OF THEM WHEN THE OPERATOR CAN
	// OBSERVE THE PAIR, and a pair whose probe path is unavailable is not judged at all. THE
	// PROTECTED CLASS AND THE JUDGED CLASS ARE TWO DIFFERENT QUESTIONS: the requirement is in force
	// from the first enable either way, and the native selectors still require eligibility.
	//
	// A MEMBER OF A ROLE THE OPERATOR CANNOT JUDGE CARRIES IT WHILE THE DEPLOYED SHAPE LETS IT
	// ANSWER, which is the membership it always had: a route this operator cannot judge is
	// discovered by its own rules, and the marker says the same thing about it.
	//
	// IT EXISTS FOR THE MIXED UNION, where one deployment-wide selector has to name the candidates
	// of roles whose requirements differ. There the eligibility term cannot be used, because it
	// would withhold the roles that carry no requirement, and the per-kind filters cannot hold the
	// requirements apart either, because they are per kind and two roles of one kind share one.
	modelDeploymentRouterCandidateLabel = "modeldeployment." + systemname.LabelPrefix + "router-candidate"

	// modelDeploymentPodSpecHashAnnotation carries the fingerprint of the explicit spec inputs and
	// external resource bindings a replica member was built from.
	modelDeploymentPodSpecHashAnnotation = "modeldeployment." + systemname.LabelPrefix + "pod-spec-hash"
	// modelDeploymentRenderVersionAnnotation records the operator render version a replica's Pod
	// was synthesized under.
	modelDeploymentRenderVersionAnnotation = "modeldeployment." + systemname.LabelPrefix + "render-version"
	// modelDeploymentMinRenderVersionAnnotation is the per-deployment annotation that raises the
	// minimum acceptable render version for running replicas.
	modelDeploymentMinRenderVersionAnnotation = "modeldeployment." + systemname.LabelPrefix + "min-render-version"
	// modelDeploymentTemplateBindingHashAnnotation carries the template-level external binding digest
	// from renderModelDeploymentPodTemplate into stampModelDeploymentPod and is stripped when stamped.
	modelDeploymentTemplateBindingHashAnnotation = "modeldeployment." + systemname.LabelPrefix + "template-binding-hash"

	// modelDeploymentRenderVersion is the current operator Pod rendering baseline. Bump this
	// constant whenever a change modifies the rendered Pod output of any ModelDeployment shape.
	modelDeploymentRenderVersion = 4
	// modelDeploymentMinSupportedRenderVersion is the lowest render version that remains compatible
	// with the current controller without replacing running Pods.
	modelDeploymentMinSupportedRenderVersion = 1

	// modelDeploymentDefaultPort is the port a replica serves on when the role names
	// none, and it is RENDERED INTO THE ENGINE'S OWN LISTEN ARGUMENT rather than left to the engine
	// to pick, because the supported engines do not agree on a default. vLLM opens 8000 on every
	// interface; SGLang opens 30000 on the loopback address alone, which neither a Service endpoint
	// nor a kubelet probe can reach.
	modelDeploymentDefaultPort int32 = 8000
	// modelDeploymentDefaultPortName names that port on the container and on the Service fronting it.
	modelDeploymentDefaultPortName = "http"
	// A managed decoder's routing proxy owns the serving port, while vLLM listens behind it here.
	modelDeploymentInternalPort int32 = 8200
	// modelDeploymentMainContainerName is the container the role's own image and command run in,
	// and the only one a rank belongs to: every other container a Pod here carries is this
	// operator's, and joins no collective.
	modelDeploymentMainContainerName = "main"

	// modelDeploymentLeaderAddressEnv, modelDeploymentReplicaSizeEnv and
	// modelDeploymentMemberIndexEnv publish the rank layout of an instance that spans several Pods.
	// They appear only above size one, so a single-Pod instance renders exactly as it did before
	// the field existed.
	//
	// THEY ARE OWNED FOR EVERY ENGINE, unlike the connector's keys, because they describe the
	// Kubernetes shape the Pod was rendered in rather than anything one engine understands. The
	// ownership is not a preference: a user entry of the same name would be merged by VALUE onto an
	// entry that carries a fieldRef instead, and an EnvVar holding both is rejected by the API
	// server -- so an unowned key here turns a legal deployment into one that cannot render.
	modelDeploymentLeaderAddressEnv = "GPUSTACK_REPLICA_LEADER_ADDRESS"
	modelDeploymentReplicaSizeEnv   = "GPUSTACK_REPLICA_SIZE"
	modelDeploymentMemberIndexEnv   = "GPUSTACK_MEMBER_INDEX"

	// modelDeploymentEngineHostArg and modelDeploymentEnginePortArg are the two flags that decide
	// where an engine listens. They are FILLED RATHER THAN OWNED: a role that passes either one
	// keeps its own value, which is the rule the non-Kubernetes worker applies to the same two flags.
	modelDeploymentEngineHostArg = "--host"
	modelDeploymentEnginePortArg = "--port"
	// modelDeploymentEngineBindHost is every interface in the Pod's network namespace, which is what
	// a Service endpoint and a kubelet probe both dial. An engine left on its own default may open
	// the loopback address instead, where only a process inside that container can reach it.
	modelDeploymentEngineBindHost = "0.0.0.0"
	// modelDeploymentEngineTLSCertArg and modelDeploymentEngineTLSKeyArg are the ONLY two flags that
	// turn the engine's listener into a TLS one, and the whole --ssl-* family is deliberately not
	// used in their place.
	//
	// Both supported engines pass every ssl argument straight to uvicorn, which enables TLS on
	// `ssl_keyfile or ssl_certfile` and on nothing else. --ssl-ca-certs, --ssl-ciphers,
	// --ssl-keyfile-password and --ssl-cert-reqs passed ALONE all leave an ordinary HTTP server, so
	// treating the prefix as the family would publish an https:// address for a listener serving
	// plaintext.
	//
	// WHERE TO RE-CHECK THIS, since it is engine behavior and nothing here can enforce it: uvicorn's
	// Config.is_ssl, reached from vllm's api_server.py serve_http call and from sglang's
	// http_server.py launch_server. Each engine ALSO logs a line of its own about SSL being enabled,
	// computed differently -- vllm from keyfile AND certfile -- and neither line decides the
	// listener. Read the uvicorn property, not the log statement.
	//
	// THE SCOPE IS COMMAND-LINE FLAGS. An engine that grew an environment variable putting the same
	// listener on TLS would not be seen here, and both the probe and the published address would
	// then say http:// for a TLS listener. No such variable exists in either engine today.
	modelDeploymentEngineTLSCertArg = "--ssl-certfile"
	modelDeploymentEngineTLSKeyArg  = "--ssl-keyfile"
	// modelDeploymentEngineTLSClientAuthArg is the one TLS-related flag a gate cannot always follow:
	// it can demand a CLIENT certificate, and a kubelet probe has none to present. It withdraws the
	// gate only where one of the two above turned TLS on -- uvicorn builds no TLS context for it to
	// apply to otherwise -- and only at the VALUE below.
	modelDeploymentEngineTLSClientAuthArg = "--ssl-cert-reqs"
	// modelDeploymentEngineTLSClientAuthNone and modelDeploymentEngineTLSClientAuthOptional are
	// ssl.CERT_NONE and ssl.CERT_OPTIONAL, the only two values of that flag under which a kubelet
	// probe still completes: the first asks for no certificate, the second verifies one only if the
	// client offers it.
	//
	// THEY ARE AN ALLOW LIST, not a "reject on CERT_REQUIRED" test, and the difference shows on a
	// value outside the enum. ssl defines exactly these plus CERT_REQUIRED, so a 3 or a -1 is a
	// configuration this operator cannot reason about; a test naming only the rejecting value would
	// read it as safe and gate a listener whose behavior is unknown. Everything not on this list
	// withdraws the gates, which matches how an unreadable value is treated below and for the same
	// reason.
	modelDeploymentEngineTLSClientAuthNone     = 0
	modelDeploymentEngineTLSClientAuthOptional = 1

	// modelDeploymentProbePath is the route all three gates read, and it is the ONLY route both supported
	// engines answer usefully -- which they do for OPPOSITE reasons, so neither engine's behavior
	// may be used to reason about the other.
	//
	// On vllm the handler is a LIVENESS check: it answers 200 in every state and 503 only once the
	// engine is dead, and it answers 200 outright when the server carries no engine at all. What
	// makes it a readiness signal there is one layer below the handler: the server socket is bound
	// before the engine is built but never listened on until uvicorn serves, which happens after the
	// engine finishes loading. Until then the probe does not get a response to grade -- the
	// connection is refused.
	//
	// On sglang the handler IS the readiness check: the server listens from the start and answers
	// 503 while its status is still starting.
	//
	// A TCP probe would therefore be correct on vllm and vacuous on sglang, whose port accepts
	// immediately. So would /v1/models. Nothing else is shared: vllm's /ready and /readyz exist only
	// in its data-parallel supervisor, not in the server a role runs.
	modelDeploymentProbePath = "/health"

	// modelDeploymentDirectDecodeProbePath is the route the gates read in the direct-decode shape,
	// where a routing sidecar owns the service port the gates grade. The sidecar answers /health
	// itself, in every state, so that path grades the sidecar and stops saying anything about the
	// engine the moment the Pod starts. Every other route the sidecar forwards to the engine, whose
	// port refuses connections until the engine finishes loading and serves -- a refusal the
	// sidecar answers with a 503. Reading /v1/models through the sidecar therefore keeps the gates
	// on the address the Service fronts while the answer once again turns on the engine.
	//
	// The vacuity recorded above does not follow this route into the branch: the direct-decode
	// shape renders only for vLLM, the engine whose /v1/models is a readiness signal.
	modelDeploymentDirectDecodeProbePath = "/v1/models"

	// modelDeploymentProbePeriodSeconds paces all three gates, which is what makes their failure
	// thresholds comparable to each other.
	modelDeploymentProbePeriodSeconds int32 = 10
	// modelDeploymentProbeTimeoutSeconds bounds one probe request. It is above the kubelet's
	// one-second default because the route is served by the same process that is loading the
	// model, and it is wide enough that a probe which arrives while a large weight shard is being
	// read reports a timeout rather than cutting the read short.
	modelDeploymentProbeTimeoutSeconds int32 = 5
	// modelDeploymentStartupFailureThreshold is how many periods a replica that declared no
	// startup budget may take to load before the kubelet gives up on it, which at the period above
	// and DefaultModelDeploymentStartupTimeoutSeconds is thirty minutes. A role that declares a
	// budget gets its own threshold, derived from that budget rather than from this constant.
	//
	// The budget is generous on purpose: load time grows with model size and with how the weights
	// arrive, and a threshold that fits the smallest model turns a slow start into a restart loop.
	// Overshooting costs only a replica that is already broken staying broken for longer, which the
	// deployment's own status already reports.
	modelDeploymentStartupFailureThreshold int32 = 180
	// modelDeploymentReadinessFailureThreshold is how many consecutive failures take a replica that
	// HAS served out of the Service's endpoints.
	modelDeploymentReadinessFailureThreshold int32 = 3
	// modelDeploymentLivenessFailureThreshold is how many consecutive failures restart a replica
	// whose engine answered once and then stopped answering.
	//
	// It is deliberately wider than the readiness threshold, because the two failures cost different
	// things. Losing readiness withdraws a replica from the Service and is recovered by answering
	// again, so a brief stall should reach it first. A restart throws away a model that took the
	// startup budget above to load, so only a failure that outlives the readiness one should reach
	// it.
	modelDeploymentLivenessFailureThreshold int32 = 6

	// The three values below replace their counterparts above on a prefiller that dials an encoder
	// stage by address. Its health route answers late, or not at all, while the encoder is busy
	// with a large media payload or is restarting. With the defaults, an encoder stall would both
	// withdraw a healthy prefiller and, through the liveness gate, restart it. The wider gates keep
	// the same ratio between readiness and liveness: sixty and one hundred and twenty seconds at
	// the period above.
	modelDeploymentEncoderProbeTimeoutSeconds       int32 = 5
	modelDeploymentEncoderReadinessFailureThreshold int32 = 6
	modelDeploymentEncoderLivenessFailureThreshold  int32 = 12

	// modelDeploymentVLLMEngramConfigArg and modelDeploymentSGLangEngramHostTableEnv are the
	// engine settings that place an Engram lookup table in pinned host DRAM.
	modelDeploymentVLLMEngramConfigArg      = "--engram-config"
	modelDeploymentSGLangEngramHostTableEnv = "SGLANG_ENABLE_DSV41_ENGRAM_HOST_TABLE"
)

// modelDeploymentListenArgs are the listen and TLS flags each engine registers: the set the render
// reads back off a command line, through ModelDeploymentResolveArg, to learn where and how the
// engine listens. It is per engine because the sets differ -- SGLang registers no --ssl-cert-reqs,
// so "--ssl-cert" is its certificate flag, while vLLM calls the same prefix ambiguous.
//
// A PREFIX OF A KEY IS READ AS THAT KEY, and that is the engine's own reading wherever the engine
// starts at all: no flag either engine registers is a strict prefix of one of these, so a prefix
// the engine accepts is one it resolved to the key, and a prefix it calls ambiguous stops it before
// anything listens, which leaves nothing to read back. That premise is engine behavior nothing here
// enforces. Re-check it on an engine bump by listing, for each key, the option strings of the
// engine's parser that are a strict prefix of it (vLLM's make_arg_parser, SGLang's
// ServerArgs.add_cli_args); a non-empty list means that spelling is read as the wrong flag here.
var modelDeploymentListenArgs = map[string][]string{
	workercore.ModelDeploymentEngineVLLM: {
		modelDeploymentEngineHostArg, modelDeploymentEnginePortArg,
		modelDeploymentEngineTLSCertArg, modelDeploymentEngineTLSKeyArg,
		modelDeploymentEngineTLSClientAuthArg,
	},
	workercore.ModelDeploymentEngineSGLang: {
		modelDeploymentEngineHostArg, modelDeploymentEnginePortArg,
		modelDeploymentEngineTLSCertArg, modelDeploymentEngineTLSKeyArg,
	},
}

// ModelDeploymentListenArg reports which listen or TLS flag the engine's own parser reads a
// command-line entry as, and "" for any other entry.
func ModelDeploymentListenArg(engine, arg string) string {
	key, _ := ModelDeploymentResolveArg(engine, arg, modelDeploymentListenArgs[engine])

	return key
}

// ModelDeploymentRenderInput is everything one replica's Pod is rendered from.
//
// The InstanceType and the connector arrive as values rather than being read here, because the
// render must stay a pure function of its inputs: the reconciler converges the same object on every
// pass, so a render that reached for a client could return two different Pods for one spec and roll
// the deployment forever.
type ModelDeploymentRenderInput struct {
	// Deployment is the object being rendered.
	Deployment *workercore.ModelDeployment
	// Role is the entry of Deployment.Spec.Roles this replica belongs to.
	Role *workercore.ModelDeploymentRole
	// InstanceType is the type the role's Pods are admitted against. It supplies how to spell the
	// accelerator keys and the per-card unit resources the host request is derived from.
	InstanceType *worker.InstanceType
	// Connector is what the engine needs to reach the pool. Its zero value renders a replica with
	// no connector at all, which is what a deployment whose Binding has not been resolved yet gets.
	Connector ModelDeploymentConnectorRender
	// ECConnector is the encoder-cache connector the decode sidecar is told to drive, in the
	// SIDECAR's own spelling, or empty when this deployment runs no encoder stage.
	//
	// IT IS THE MATRIX ROW'S ANSWER, RESOLVED BY THE CALLER rather than re-derived from the
	// platform here. The row is admission's whole answer for a router/engine/accelerator
	// combination, and the render already carries a copy of it inside Connector.Args for the engine
	// half; re-deriving the sidecar half from the platform beside it would be a second switch over
	// one platform, and two switches are two answers to one question. Empty renders no flag, which
	// the sidecar reads as "skip the encoder stage" -- so the two reasons an empty value occurs
	// (no Encode role, or an unadmitted combination) render identically rather than differently.
	ECConnector string
	// InterfaceResource is the extended resource selected from the role's effective fabric
	// protocol. It is empty when the role requests no interface.
	InterfaceResource core.ResourceName
	// There is NO ConfigMap name here, and there is no object to name: the client configuration
	// travels in Connector.PodAnnotations and reaches the container as a downwardAPI projection of
	// it. The field this struct used to carry was never filled by anything, so the mount it guarded
	// was dead code; Connector.PodAnnotations is the carrier.
	// RuntimeClassName is the runtime class an accelerated replica needs. The reconciler resolves it,
	// because deciding it requires reading whether the class exists on the cluster.
	RuntimeClassName string
	// GeneralResourcesOvercommit mirrors the Instance path's overcommit setting, which decides
	// whether the derived CPU and memory are requested at full size or scaled down.
	GeneralResourcesOvercommit bool
	// TCPTWReuse mirrors the model-deployment-tcp-tw-reuse setting. It is the cluster's answer, not
	// the role's: which Pods turn it into a sysctl is decided by modelDeploymentTakesTCPTWReuse.
	TCPTWReuse bool
	// NativeSidecar decides which shape a direct decoder's routing proxy is rendered in: a native
	// sidecar -- an init container carrying restartPolicy: Always -- when the cluster keeps that
	// field, or a classic regular container when it does not. The reconciler resolves it from the
	// cluster's version rather than the render guessing, because the field is DROPPED without an
	// error below 1.29, and a Pod rendered with it there never leaves Init.
	NativeSidecar bool
	// Ordinal is the replica this render is for, stamped into the group metadata. The template
	// half never reads it -- only the stamp does -- and the zero value renders the first replica,
	// which is what a caller with no particular replica in mind gets.
	Ordinal int

	// Artifact is how the weights spec.model.artifactRef names reach the engine, resolved by the
	// reconciler. Nil renders a deployment naming no artifact exactly as before the field existed;
	// a deployment that names one is not rendered at all until it resolves.
	Artifact *ModelDeploymentArtifactRender

	// Member is which Pod of that replica this render is for, counting from zero. It is stamped,
	// never templated, for the same reason Ordinal is: the template is what every Pod of the role
	// shares, and a value naming one Pod inside it would make two members of one replica hash
	// differently and read as a permanent rollout.
	//
	// The zero value is the leader, which is what a caller with no particular member in mind gets
	// and is also the only member a replica of size one has.
	Member int
}

// modelDeploymentSelectorLabels is what fronts a role's replicas: the identity of the deployment and
// of the role, and nothing a spec update can move.
func modelDeploymentSelectorLabels(
	md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole,
) map[string]string {
	return map[string]string{
		modelDeploymentLabelKeyName:      modelDeploymentLabelValueName,
		modelDeploymentLabelKeyInstance:  md.Name,
		modelDeploymentLabelKeyComponent: role.Name,
	}
}

// renderModelDeploymentPodTemplate renders the whole of a replica's Pod that its role's spec
// states: the labels, the annotations, the PodSpec and every container in it.
//
// IT PRODUCES NOTHING THAT VARIES FROM ONE REPLICA OF A ROLE TO THE NEXT, and that is the property
// the split into a template and a stamp exists to keep checkable: the Kueue group metadata and the
// spec-hash fingerprint are stamped on afterwards, because a per-replica group name differs between
// the members of one role, and a value naming one member leaking into this function would render
// each replica a different Pod with nothing erroring anywhere. The connector's PodAnnotations are
// written here rather than in the stamp because they carry the pool's endpoint and domain, which
// every replica of the role shares.
func renderModelDeploymentPodTemplate(ctx context.Context, in ModelDeploymentRenderInput) (*core.Pod, error) {
	// Implement this function in chapter 19.
	var result0 *core.Pod
	var result1 error
	return result0, result1
}

// modelDeploymentStampRouterCandidate marks a rendered Pod that is a routing candidate.
//
// IT READS THE COMPLETED POD AND NOT THE DESIRED ROLE. The Pod says which mode it runs: a Pod that
// took its command line over carries no metrics annotation, because the operator did not build that
// command line and cannot claim the container serves the metrics the hook reads. The desired role
// says what this Pod is asked to run, and a Pod is what is actually running.
//
// A ROLE THE OPERATOR CONTROLS IS NOT A CANDIDATE AT BIRTH. It has not been judged, and the marker
// is written to such a member only while its group is eligible, so a managed newborn starts
// unmarked and gains the marker with the eligibility label.
//
// A ROLE THE OPERATOR CANNOT JUDGE IS A CANDIDATE FROM BIRTH, and only the members the shape lets
// answer carry it: the leader of a leader-served replica, and every member of an External-DP one.
func modelDeploymentStampRouterCandidate(
	pod *core.Pod, md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole, member int,
) {
	if md.Spec.Router == nil {
		return
	}
	managed, _ := modelDeploymentMemberExecution(pod)
	effective := *role
	if managed {
		// THE REPLICA RUNS THE OPERATOR'S COMMAND LINE, so the role's own command says nothing
		// about the mode the replica is in.
		effective.Command = nil
	}
	if modelDeploymentGroupForwardActivatable(md, &effective) {
		return
	}
	// A MEMBER THIS OPERATOR DID NOT BUILD CARRIES NO MANAGED LOAD BALANCE, which is an
	// established answer and not a missing one.
	externalDP := managed && modelDeploymentRoleExternalDP(md, &effective)
	switch modelDeploymentAnsweringShapeOf(externalDP, modelDeploymentRoleSize(&effective)) {
	case modelDeploymentAnsweringAll, modelDeploymentAnsweringSole:
	case modelDeploymentAnsweringLeader:
		if member != modelDeploymentLeaderMemberIndex {
			return
		}
	default:
		return
	}
	if pod.Labels == nil {
		pod.Labels = map[string]string{}
	}
	pod.Labels[modelDeploymentRouterCandidateLabel] = modelDeploymentEndpointEligibleValue
}

// modelDeploymentSharedMemoryVolumes adds the role's default mount unless one is explicit.
func modelDeploymentSharedMemoryVolumes(
	role *workercore.ModelDeploymentRole, vols []core.Volume, mounts []core.VolumeMount,
) ([]core.Volume, []core.VolumeMount) {
	if !slices.ContainsFunc(mounts, func(m core.VolumeMount) bool { return m.MountPath == "/dev/shm" }) {
		size := resource.MustParse("16Gi")
		if role.ShmSize != nil {
			size = role.ShmSize.DeepCopy()
		}
		vols = append(vols, core.Volume{
			Name: "model-shm",
			VolumeSource: core.VolumeSource{EmptyDir: &core.EmptyDirVolumeSource{
				Medium: core.StorageMediumMemory, SizeLimit: &size,
			}},
		})
		mounts = append(mounts, core.VolumeMount{Name: "model-shm", MountPath: "/dev/shm"})
	}

	return vols, mounts
}

// stampModelDeploymentPod puts the Kueue group metadata on a rendered replica and then writes the
// explicit spec-hash fingerprint and the operator render-version annotation.
func stampModelDeploymentPod(
	pod *core.Pod, md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole,
	ordinal, member int,
) {
	// THE NAME IS DERIVED RATHER THAN GENERATED, above size one, and that is what the whole
	// multi-Member shape rests on: a member cannot join a collective it cannot address, and a Pod
	// created by GenerateName has no name any sibling can predict. Every reader -- this renderer,
	// the converger, a sibling's entrypoint -- derives the same string from the same four values
	// without reading anything.
	//
	// AT SIZE ONE THE NAME STAYS GENERATED, deliberately. A replica's identity is obtained from its
	// ordinal label rather than from its name, so naming single-Member replicas would buy nothing
	// and would cost the one thing GenerateName gives: a create that cannot collide with a Pod of
	// the same ordinal still draining, which is exactly the state the converger's create gate waits
	// out. Above one the collision is unavoidable -- the address IS the name -- and the gate is
	// what handles it instead.
	if modelDeploymentRoleSize(role) > 1 {
		pod.Name = modelDeploymentMemberName(md, role.Name, ordinal, member)
		pod.GenerateName = ""

		// hostname and subdomain are what turn that name into a DNS record: the headless Service
		// named here publishes <hostname>.<subdomain> for every Pod that names it, which is
		// core/v1 behavior owing nothing to any controller.
		pod.Spec.Hostname = pod.Name
		pod.Spec.Subdomain = modelDeploymentReplicaServiceName(md, role.Name, ordinal)

		stampModelDeploymentRankEnv(pod, md, role, ordinal)
	}

	// The Kueue group metadata, which is what makes a replica's admission unit that replica alone:
	// the group name is derived per (role, ordinal), so each Pod joins its own one-member group.
	//
	// The labels and the annotations are applied together because the group's own type returns them
	// together -- a Pod carrying the membership label without the total count joins a group whose
	// size Kueue cannot learn, and no Workload is composed at all.
	group := ModelDeploymentPodGroup(md, role, ordinal, member)
	for k, v := range group.Labels {
		pod.Labels[k] = v
	}
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string, len(group.Annotations)+2)
	}
	for k, v := range group.Annotations {
		pod.Annotations[k] = v
	}

	bindingHash := pod.Annotations[modelDeploymentTemplateBindingHashAnnotation]
	delete(pod.Annotations, modelDeploymentTemplateBindingHashAnnotation)
	pod.Annotations[modelDeploymentPodSpecHashAnnotation] = modelDeploymentExplicitSpecHash(
		md, role, ordinal, member, bindingHash,
	)
	pod.Annotations[modelDeploymentRenderVersionAnnotation] = strconv.Itoa(modelDeploymentRenderVersion)
	modelDeploymentStampRouterCandidate(pod, md, role, member)
}

// stampModelDeploymentRankEnv publishes the three facts a member needs to join its collective: who
// the leader is, how many members there are, and which one this is.
//
// IT PUBLISHES FACTS AND COMPOSES NO ARGUMENT. Which parallelism an engine turns on, and over how
// many ranks, stays the author's to say: the member count is a product of degrees that cannot be
// decomposed from one number, so a formula here would be a guess that runs instead of an error.
// The declared degrees ARE read elsewhere -- the transfer document's parallel blocks are parsed
// out of the roles' own books, their extra arguments and, for vLLM's data-parallel width, a
// literal VLLM_DP_SIZE entry -- but that read copies the author's numbers into another document
// and changes none of the membership facts published here.
//
// THE INDEX COMES THROUGH THE DOWNWARD API RATHER THAN AS A LITERAL, and that is what keeps one
// template per ReplicaGroup: every member's container declares the same `fieldRef`, so the three
// entries are byte-identical across the members of a replica and only the label the stamp already
// wrote differs. Writing the index as a literal would work -- a live Pod is compared against the
// render of its own member index, not its replica's -- but it would make the container spec a
// per-member document, and the Boundaries invariant that one template describes a whole replica is
// worth more than the two lines it saves.
//
// ONLY THE MAIN CONTAINER IS STAMPED. The routing proxy is this operator's own sidecar and joins no
// collective; giving it ranks would describe a membership nothing acts on.
//
// A ROLE THAT TOOK OVER ITS COMMAND LINE IS STAMPED TOO, which is where the connector's rule does
// NOT carry over. The connector's entries name a config file the operator also wrote, so a take-over
// argv that never reads it has no use for them; these describe the cluster the Pod is running in, and
// an entrypoint composing its own launch is exactly what needs them.
func stampModelDeploymentRankEnv(
	pod *core.Pod, md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole, ordinal int,
) {
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name != modelDeploymentMainContainerName {
			continue
		}
		pod.Spec.Containers[i].Env = append(pod.Spec.Containers[i].Env,
			core.EnvVar{
				Name: modelDeploymentLeaderAddressEnv,
				Value: modelDeploymentMemberName(
					md, role.Name, ordinal, modelDeploymentLeaderMemberIndex,
				) +
					"." + modelDeploymentReplicaServiceName(md, role.Name, ordinal),
			},
			core.EnvVar{
				Name:  modelDeploymentReplicaSizeEnv,
				Value: strconv.Itoa(modelDeploymentRoleSize(role)),
			},
			core.EnvVar{
				Name: modelDeploymentMemberIndexEnv,
				ValueFrom: &core.EnvVarSource{FieldRef: &core.ObjectFieldSelector{
					FieldPath: "metadata.labels['" + modelDeploymentMemberIndexLabel + "']",
				}},
			},
		)

		return
	}
}

func modelDeploymentCommandPort(engine string, command []string) (int32, error) {
	for i := len(command) - 1; i >= 0; i-- {
		if ModelDeploymentListenArg(engine, command[i]) != modelDeploymentEnginePortArg {
			continue
		}
		_, value, inline := strings.Cut(command[i], "=")
		if !inline {
			if i+1 >= len(command) {
				return 0, fmt.Errorf("%s has no value", modelDeploymentEnginePortArg)
			}
			value = command[i+1]
		}
		port, err := strconv.ParseInt(value, 10, 32)
		if err != nil || port < 1 || port > 65535 {
			return 0, fmt.Errorf("%s has invalid value %q", modelDeploymentEnginePortArg, value)
		}
		return int32(port), nil
	}
	return 0, fmt.Errorf("%s is missing", modelDeploymentEnginePortArg)
}

// renderModelDeploymentRoutingSidecar builds the decode Pod's routing proxy. Which handshake it
// speaks follows the ENGINE and the pool's vendor, because the connector is how the sidecar asks
// the prefiller's bootstrap registry for transfer endpoints and each engine serves a different
// one: the value names a connector the sidecar dispatches on, not a flavor of one protocol.
//
// THE BOOTSTRAP PORT IS ONE VALUE WRITTEN AT BOTH ENDS. The prefiller names it in its own launch
// argument, rendered by the inject package from its constant, and the sidecar reads it here. Under
// mooncake the port travels as a flag; under SGLang it cannot, because the sidecar's SGLang port
// is a process-wide constant with no flag at all, overridable only through the
// SGLANG_BOOTSTRAP_PORT environment variable (llm-d/llm-d-router@v0.10.0
// `pkg/sidecar/proxy/connector_sglang.go:38-50`). Both ends read the same constant, so a pair
// cannot disagree about where the registry lives.
//
// ASCEND GETS NO PORT FLAG, because its pair has no registry to point at: the vLLM-Ascend
// connector hands the decoder the prefiller's host and port inside the per-request
// kv_transfer_params, and the sidecar's nixlv2 mode relays exactly that document
// (llm-d/llm-d-router@v0.10.0 `pkg/sidecar/proxy/connector_nixlv2.go:162-170,405`). The connector
// value is the sidecar's own constant vocabulary (`pkg/sidecar/constants/constants.go:21`,
// KVConnectorNIXLV2), spelled here rather than derived from anything the engine names.
// The encoder-cache connectors the decode sidecar pulls embeddings through. These are the
// sidecar's own words, NOT the engine connector names they correspond to: the engine runs
// ECCPUConnector or ECExampleConnector, while the sidecar either pulls the encoder's output over
// NIXL or reads it from the directory both roles mount.
//
//	llm-d/llm-d-router@v0.11.0 `pkg/sidecar/constants/constants.go:35-39`, validated against
//	`pkg/sidecar/proxy/options.go:180-183,671-673`.
//
// The set is closed and a value outside it is fatal rather than degraded: the sidecar refuses to
// start on an unrecognized connector, so a decoder rendered with a third spelling never becomes
// ready and never answers.
const (
	// modelDeploymentECConnectorNIXL is the network path, for the platforms whose copy path
	// NIXL supports.
	modelDeploymentECConnectorNIXL = "ec-nixl"
	// modelDeploymentECConnectorExample is the shared-directory path, which needs no copy path
	// of any kind and is the only one the Ascend example runs.
	modelDeploymentECConnectorExample = "ec-example"
)

// modelDeploymentContainerPorts renders the ports a replica exposes.
//
// A role that names none still gets one, because a replica nothing can reach serves nothing and the
// Service fronting the deployment needs a target. Every supported engine's OpenAI-compatible server
// listens on 8000 by default.
func modelDeploymentContainerPorts(role *workercore.ModelDeploymentRole) []core.ContainerPort {
	if len(role.Ports) == 0 {
		return []core.ContainerPort{{
			Name:          modelDeploymentDefaultPortName,
			Protocol:      core.ProtocolTCP,
			ContainerPort: modelDeploymentDefaultPort,
		}}
	}

	return slicex.Transform(role.Ports, func(p workercore.ModelDeploymentPort) core.ContainerPort {
		return core.ContainerPort{
			Name:          modelDeploymentPortName(p),
			Protocol:      p.Protocol,
			ContainerPort: p.Port,
		}
	})
}

// modelDeploymentPortName derives a container port's name from its protocol and number, because a
// container port name must satisfy Kubernetes' own DNS-label rule and a user-supplied free-form
// name need not. The Instance path derives its own names the same way.
func modelDeploymentPortName(port workercore.ModelDeploymentPort) string {
	return strings.ToLower(fmt.Sprintf("%s-%d", port.Protocol, port.Port))
}

// convertModelDeploymentAdditionalVolumes turns a role's declared volumes into Pod volumes and
// container mounts. An entry with no source is skipped rather than rendered: a volume with an empty
// source would make the API server refuse the whole Pod on every reconcile.
func convertModelDeploymentAdditionalVolumes(
	avs []workercore.ModelDeploymentAdditionalVolume,
) (vols []core.Volume, mounts []core.VolumeMount) {
	if len(avs) == 0 {
		return nil, nil
	}

	vols = make([]core.Volume, 0, len(avs))
	mounts = make([]core.VolumeMount, 0, len(avs))
	for i := range avs {
		av := &avs[i]

		var vs core.VolumeSource
		switch {
		case av.ConfigMap != nil:
			vs.ConfigMap = &core.ConfigMapVolumeSource{
				LocalObjectReference: *av.ConfigMap,
			}
		case av.Secret != nil:
			vs.Secret = &core.SecretVolumeSource{
				SecretName: av.Secret.Name,
			}
		case av.HostPath != nil:
			vs.HostPath = av.HostPath.DeepCopy()
		case av.PersistentVolumeClaim != nil:
			// The claim name and its own read-only are copied as they arrive: a Pod volume
			// reaches a claim in the Pod's namespace only, so there is no namespace to add.
			vs.PersistentVolumeClaim = av.PersistentVolumeClaim.DeepCopy()
		default:
			continue
		}

		name := additionalVolumeName(i)
		vols = append(vols, core.Volume{
			Name:         name,
			VolumeSource: vs,
		})
		mounts = append(mounts, core.VolumeMount{
			Name:      name,
			MountPath: av.MountPath,
			ReadOnly:  av.ReadOnly,
			SubPath:   av.SubPath,
		})
	}

	return vols, mounts
}

// modelDeploymentEngineTransport reports the scheme the engine's listener speaks, and whether the
// operator can grade that listener at all.
//
// TWO FLAGS TURN THE LISTENER INTO A TLS ONE, and the rest of the --ssl-* family does not. Both
// supported engines hand every ssl argument to uvicorn without deciding anything themselves, and
// uvicorn enables TLS on `ssl_keyfile or ssl_certfile` alone. A CA bundle, a cipher list, a key
// password or a client-certificate mode passed BY ITSELF leaves an ordinary HTTP server -- so a
// prefix over the whole family would publish https:// for a listener serving plaintext, and withhold
// a gate that would have worked. Each engine also logs its own idea of "SSL enabled" from a
// different expression; those lines decide nothing and are not what this follows.
//
// THE TWO ANSWERS ARE INDEPENDENT, and deciding either one mid-scan would decide the other. Whether
// a client certificate may be demanded is only meaningful once TLS is on, and the two flags can
// arrive in either order, so both are collected before either answer is formed.
//
// IT READS THE RENDERED COMMAND LINE where one exists, and it must then be called BEFORE the listen
// address is filled in. The fill skips a flag wherever it already appears -- base argv, connector
// arguments, extra arguments -- so a check reading a narrower list would honor a connector-supplied
// port and then grade the operator's own.
//
// WHAT WITHDRAWS THE GATE. --host moves WHERE the engine listens, and the operator then leaves it
// alone, so it no longer knows whether a probe reaches the address and cannot grade it; the scheme
// such a listener speaks is still observable, and is still reported. --port does NOT withdraw it:
// the port the role names is read back and the gates follow it there, and the caller withdraws them
// only where the engine and the Service's target disagree. --ssl-cert-reqs can demand a CLIENT
// certificate, which a kubelet probe has none to present -- but only where TLS is actually on,
// because uvicorn builds no TLS context for it to apply to otherwise.
//
// Everything else about TLS stays gated: the address is still the operator's own, only the transport
// moved, and the kubelet does not verify the server certificate on an HTTPS probe, so a self-signed
// pair is graded as readily as a public one.
func modelDeploymentEngineTransport(engine string, command []string) (core.URIScheme, bool) {
	var tls, clientAuth, moved bool

	for i, arg := range command {
		switch ModelDeploymentListenArg(engine, arg) {
		case modelDeploymentEngineHostArg:
			moved = true
		case modelDeploymentEngineTLSCertArg, modelDeploymentEngineTLSKeyArg:
			tls = true
		case modelDeploymentEngineTLSClientAuthArg:
			// Assigned rather than OR-ed, so a repeated flag settles on its last occurrence, which
			// is the value the engine's own parser keeps.
			clientAuth = modelDeploymentDemandsClientCert(command, i)
		}
	}

	scheme := core.URISchemeHTTP
	if tls {
		scheme = core.URISchemeHTTPS
	}

	return scheme, !moved && (!tls || !clientAuth)
}

// modelDeploymentDemandsClientCert reports whether the --ssl-cert-reqs at index i asks for a client
// certificate the kubelet cannot present.
//
// ANYTHING BUT A KNOWN-PERMISSIVE VALUE MEANS YES, which is the cautious direction for this
// particular question. Withdrawing a gate costs a replica the readiness signal it had before any of
// this existed; fitting one to a listener that then refuses the probe leaves a replica serving every
// real client while the kubelet restarts it every startup budget. The first is a lost improvement,
// the second is an outage -- so an unreadable value, a missing one, and a number outside the enum
// are all treated alike as the refusal.
//
// The value is read as an INTEGER rather than compared as text, because the engine's parser does the
// same and "02" is CERT_REQUIRED to it.
func modelDeploymentDemandsClientCert(command []string, i int) bool {
	_, value, ok := strings.Cut(command[i], "=")
	if !ok {
		if i+1 >= len(command) {
			// The flag ends the command line, so it has no value to read at all.
			return true
		}

		value = command[i+1]
	}

	mode, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		return true
	}

	return mode != modelDeploymentEngineTLSClientAuthNone &&
		mode != modelDeploymentEngineTLSClientAuthOptional
}

// ModelDeploymentRoleArgs is the part of a replica's command line the ROLE contributes, which is
// the only part that can carry a listen or a TLS flag.
//
// It exists so that a caller with nothing but the spec -- the published endpoint is derived from the
// spec alone -- reaches the same answer the rendered command line would give it. It is also the
// stream the parallelism declaration parses: the engine reads exactly these tokens under either
// tier, so the books and the document can never come from different command lines.
//
// IT IS A NARROWER LIST THAN THE RENDER SCANS, and the two agree only because of an invariant this
// function cannot enforce: the engine's base argv and the connector's rendered arguments carry no
// listen or TLS flag. A connector that grew one would put an HTTPS probe on a replica whose
// published address still said http://. That is why the invariant is asserted against what
// inject.Render actually emits, rather than left as a promise here.
//
// A take-over role contributes its whole argv and nothing else: the extra arguments are not appended
// to a command the user replaced, so reading them here would describe a command line that is not
// being run.
func ModelDeploymentRoleArgs(role *workercore.ModelDeploymentRole) []string {
	// Implement this function in chapter 19.
	return nil
}

// mergeModelDeploymentEnv folds the operator's environment and the role's into one list.
//
// The order of the result is the order of authority, most authoritative first, so that reading a
// rendered Pod answers "who set this" without consulting the table: what the operator OWNS, then
// what it merely DEFAULTS, then what the user appended. Owned entries are refused at admission, so
// skipping them here is belt and braces rather than the enforcement — but a renderer that let one
// through would hand the failure to the engine, one layer away from the field that caused it.
//
// A role that takes over the command line gets none of the operator's entries. Its argv never names
// the file they point at, so setting them would describe a configuration nothing reads.
func mergeModelDeploymentEnv(
	engine string, role *workercore.ModelDeploymentRole,
	connector ModelDeploymentConnectorRender, artifact *ModelDeploymentArtifactRender, takeOver bool,
) []core.EnvVar {
	if takeOver {
		return mergeModelDeploymentUserEnv(nil, engine, role.Env, false)
	}

	var artifactEnv, artifactDefaulted []core.EnvVar
	if artifact != nil {
		artifactEnv, artifactDefaulted = artifact.env(takeOver, engine)
	}

	// THE CACHE ROOT IS DEFAULTED, not owned, and the tier is what says so. A user who mounts the
	// cache somewhere else, or runs an image whose engine spells the variable differently, has to
	// be able to say so and have the entry survive: the operator's claim is only about the default,
	// which the volume mounted above is what makes true.
	//
	// THE ENGINE'S OTHER STARTUP CACHES ARE DEFAULTED ALONGSIDE IT, for the same reason and for
	// one more: each of them defaults to the container's writable layer, which the pod's
	// ephemeral-storage limit measures, so a cache that grows there evicts the pod filling it
	// while the same bytes on the mounted cache do not count against the limit at all.
	defaulted := slices.Concat(
		connector.DefaultedEnv, artifactDefaulted,
		modelDeploymentCompileCacheDefaultedEnv(engine),
		modelDeploymentRuntimeCacheDefaultedEnv(engine),
	)

	env := make([]core.EnvVar, 0,
		len(connector.Env)+len(artifactEnv)+len(defaulted)+len(role.Env))
	env = append(env, connector.Env...)
	env = append(env, artifactEnv...)
	for _, e := range defaulted {
		if modelDeploymentUserSetsEnv(role.Env, e.Name) {
			continue
		}
		env = append(env, e)
	}

	return mergeModelDeploymentUserEnv(env, engine, role.Env, artifact != nil)
}

// mergeModelDeploymentUserEnv appends the user's entries to what the operator already rendered,
// letting a later tier replace an earlier one by name and never replacing what the operator owns.
// With an artifact the artifact-owned names are the operator's too; admission refuses them, and
// dropping them here keeps an object stored before that rule from overriding the artifact's.
func mergeModelDeploymentUserEnv(
	env []core.EnvVar, engine string, userEnv []workercore.ModelDeploymentEnvVar, artifact bool,
) []core.EnvVar {
	for i := range userEnv {
		if ModelDeploymentOwnsEnv(engine, userEnv[i].Name) ||
			(artifact && ModelDeploymentArtifactOwnsEnv(engine, userEnv[i].Name)) {
			continue
		}
		replaced := false
		for j := range env {
			if env[j].Name != userEnv[i].Name {
				continue
			}
			env[j].Value = userEnv[i].Value
			replaced = true

			break
		}
		if !replaced {
			env = append(env, core.EnvVar{Name: userEnv[i].Name, Value: userEnv[i].Value})
		}
	}

	return env
}

// modelDeploymentUserSetsEnv reports whether the user supplied the named variable.
func modelDeploymentUserSetsEnv(userEnv []workercore.ModelDeploymentEnvVar, name string) bool {
	for i := range userEnv {
		if userEnv[i].Name == name {
			return true
		}
	}

	return false
}

type modelDeploymentRoleSiblingHashInput struct {
	Name string                             `json:"name"`
	Kind workercore.ModelDeploymentRoleKind `json:"kind,omitempty"`
}

// modelDeploymentExplicitSpecHash fingerprints the explicit user-configured ModelDeployment and
// role spec together with the resolved external binding digest and replica seat coordinates.
func modelDeploymentExplicitSpecHash(
	md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole,
	ordinal, member int, bindingHash string,
) string {
	canonicalRole := *role.DeepCopy()
	canonicalRole.Kind = ModelDeploymentEffectiveRoleKind(&canonicalRole)
	if canonicalRole.Image == "" {
		canonicalRole.Image = md.Spec.Engine.Image
	}
	if len(canonicalRole.Command) == 0 {
		canonicalRole.TerminationGracePeriodSeconds = ptr.To(
			modelDeploymentTerminationGracePeriodSeconds(&canonicalRole),
		)
	} else {
		canonicalRole.ExtraArgs = nil
	}
	canonicalRole.Replicas = 0
	canonicalRole.ElasticEP = nil

	siblings := make([]modelDeploymentRoleSiblingHashInput, 0, len(md.Spec.Roles))
	for i := range md.Spec.Roles {
		siblings = append(siblings, modelDeploymentRoleSiblingHashInput{
			Name: md.Spec.Roles[i].Name,
			Kind: ModelDeploymentEffectiveRoleKind(&md.Spec.Roles[i]),
		})
	}

	var kvCache *workercore.ModelDeploymentKVCache
	if md.Spec.KVCache != nil {
		copied := *md.Spec.KVCache
		copied.Connector = ""
		kvCache = &copied
	}

	subject := struct {
		Name        string                                `json:"name"`
		Namespace   string                                `json:"namespace"`
		Engine      string                                `json:"engine,omitempty"`
		Model       workercore.ModelDeploymentModel       `json:"model"`
		Router      *workercore.ModelDeploymentRouter     `json:"router,omitempty"`
		KVCache     *workercore.ModelDeploymentKVCache    `json:"kvCache,omitempty"`
		KVTransfer  *workercore.ModelDeploymentKVTransfer `json:"kvTransfer,omitempty"`
		Role        workercore.ModelDeploymentRole        `json:"role"`
		Siblings    []modelDeploymentRoleSiblingHashInput `json:"siblings,omitempty"`
		Ordinal     int                                   `json:"ordinal"`
		Member      int                                   `json:"member"`
		BindingHash string                                `json:"bindingHash,omitempty"`
	}{
		Name:        md.Name,
		Namespace:   md.Namespace,
		Engine:      md.Spec.Engine.Name,
		Model:       md.Spec.Model,
		Router:      md.Spec.Router,
		KVCache:     kvCache,
		KVTransfer:  md.Spec.KVTransfer,
		Role:        canonicalRole,
		Siblings:    siblings,
		Ordinal:     ordinal,
		Member:      member,
		BindingHash: bindingHash,
	}

	encoded, err := json.Marshal(subject)
	if err != nil {
		return "unhashable-" + err.Error()
	}

	return stringx.SumBytesBySHA256(encoded)
}

// modelDeploymentEffectiveMinRenderVersion computes the minimum acceptable render version for a
// deployment's running replicas, clamped to [modelDeploymentMinSupportedRenderVersion, modelDeploymentRenderVersion].
func modelDeploymentEffectiveMinRenderVersion(md *workercore.ModelDeployment) int {
	floor := modelDeploymentMinSupportedRenderVersion
	if md == nil {
		return floor
	}
	if raw := md.Annotations[modelDeploymentMinRenderVersionAnnotation]; raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v > floor {
			floor = min(v, modelDeploymentRenderVersion)
		}
	}

	return floor
}

// modelDeploymentPodRenderVersion returns the integer render version stamped on a Pod, or 0 when
// the annotation is absent or invalid.
func modelDeploymentPodRenderVersion(pod *core.Pod) int {
	if pod == nil {
		return 0
	}
	v, err := strconv.Atoi(pod.Annotations[modelDeploymentRenderVersionAnnotation])
	if err != nil || v <= 0 {
		return 0
	}

	return v
}

// modelDeploymentPodMatchesCurrency reports whether a live Pod matches the desired explicit spec
// hash and meets the deployment's minimum render version floor.
func modelDeploymentPodMatchesCurrency(actual, desired *core.Pod, minRenderVersion int) bool {
	if actual == nil || desired == nil {
		return false
	}

	return actual.Annotations[modelDeploymentPodSpecHashAnnotation] ==
		desired.Annotations[modelDeploymentPodSpecHashAnnotation] &&
		modelDeploymentPodRenderVersion(actual) >= minRenderVersion
}

// modelDeploymentReplicaRenderVersionUniform reports whether all members of a replica carry the
// same render version.
func modelDeploymentReplicaRenderVersionUniform(members []*core.Pod) bool {
	if len(members) <= 1 {
		return true
	}
	first := modelDeploymentPodRenderVersion(members[0])
	for _, member := range members[1:] {
		if modelDeploymentPodRenderVersion(member) != first {
			return false
		}
	}

	return true
}
