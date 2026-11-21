package mooncake

import (
	"fmt"

	core "k8s.io/api/core/v1"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/systemname"
)

const (
	// memberResourceNoteRole distinguishes a member group's objects from the leader's.
	memberResourceNoteRole = "member"

	// memberEntrypoint is the image's own console script, measured rather than assumed. It builds
	// the store, mounts a segment and then serves an HTTP API; its main() installs shutdown signal
	// handlers and its stop() closes the store, so it unmounts on SIGTERM by itself.
	memberEntrypoint = "mc_store_rest_server"

	// memberRESTPortBase is where the FIRST group's entrypoint serves its HTTP API, and it is the
	// entrypoint's own default. Later groups offset from it; see memberRESTPort for why.
	memberRESTPortBase int32 = 8080

	// RDMADevicePath is the host's device tree, and NO member mounts it. Each host fabric's device
	// plugin injects the verbs node of every device it grants, at this same path inside the
	// container: this operator's own Device Manager for RDMA, AWS's plugin for EFA.
	//
	// Mounting the tree beside that grant is not merely redundant, it breaks a partial one. The tree
	// carries every OTHER adapter on the node into a container granted some of them, and the verbs
	// library lists a device by finding its node, so the ungranted ones are listed too. Opening one
	// of those is refused by the device cgroup with EPERM, and libfabric's EFA provider abandons its
	// whole device list on the first EPERM it meets: a member granted fewer EFA devices than its node
	// has fails to initialize the fabric at all, while the same member without the mount starts.
	//
	// An allocator that injects nothing is therefore not served by mounting the tree back: the mount
	// makes a device visible, and only the request makes it openable.
	//
	// Exported because admission has to refuse a declared mount that would land on the injected
	// device nodes: both end up in one container, and a collision there is resolved by the container
	// runtime rather than reported by this operator.
	RDMADevicePath = "/dev/infiniband"

	// MemberPodSpecHashAnnotation carries the fingerprint of a member's pod template, minus its node
	// selector. It rides on the template, so every Pod the DaemonSet creates inherits it and the
	// reconciler can tell a Pod built from the current spec from one built before a change.
	MemberPodSpecHashAnnotation = "kvcache." + systemname.LabelPrefix + "pod-spec-hash"

	// memberShutdownSeconds is the time the entrypoint needs for its own shutdown: closing its
	// store and letting the leader see the client go, rather than being cut off mid-shutdown and
	// leaving the leader to time the client out after its client_ttl instead.
	//
	// It is NOT a drain window for the memory segment, and no memory-unmount hook is rendered.
	//
	// THE MEMBER'S OWN API CANNOT UNMOUNT THE SEGMENT THIS RENDERER GIVES IT, which is a stronger
	// reason than the one this comment used to carry: the segment MOONCAKE_GLOBAL_SEGMENT_SIZE asks
	// for is mounted by the client's own setup(), and that files it in neither record set the unmount
	// routes look in. Measured against mooncake 0.3.13:
	//
	//   - /api/unmount answers 500 with "segment_id not found in allocated records"
	//     (real_client.cpp:1791), and /api/unmount_shm answers 500 with "not found in mounted
	//     records" (real_client.cpp:1626).
	//   - Both answer in under three milliseconds, at every grace period tried, for the id the
	//     LEADER itself published for that segment.
	//   - The same /api/unmount returns 200 for a segment mounted through /api/mount, so the refusal
	//     is a record-set boundary and not a wrong id.
	//
	// SO IDENTITY WAS NEVER THE BLOCKER. The earlier reason -- that a host-network member cannot
	// learn its own client id -- describes a problem that is never reached: a member holding exactly
	// the right id for a segment it is certain is its own is refused just the same. Nothing about
	// matching members to segments changes that, so no amount of work on this repository's side
	// reaches a hook that drains. See renderMemberEnv for the segment name correction this
	// measurement also forced.
	//
	// A group with a local disk tier adds its scale-in grace ON TOP of this, rather than sharing
	// it, which is what keeps the kubelet from killing the container in the middle of a wait the
	// spec asked for. See memberTerminationGracePeriodSeconds.
	memberShutdownSeconds int64 = 60

	// memberProtocolRDMA and memberProtocolEFA are the two RESOLVED protocols that reach the host's
	// fabric, in the store's own spelling rather than the API's. They are constants because two
	// places have to agree on the pair -- the host-fabric predicate and the resource derived from
	// it -- and a third spelling of "efa" is how the two would drift.
	memberProtocolRDMA = "rdma"
	memberProtocolEFA  = "efa"

	// efaDeviceResource is the extended resource AWS's EFA device plugin advertises. EFA is AWS's own
	// fabric and its plugin advertises exactly one name, so the protocol selects the resource family.
	// The plugin advertises one unit per EFA interface, so a member asking for more than its node
	// has stays Pending.
	//
	// IT IS WHAT MAKES THE DEVICE OPENABLE. Mounting /dev/infiniband carries the device node into
	// the container's mount namespace and does nothing else: the device cgroup still denies open(),
	// which comes back as EPERM even for uid 0 on a node whose file mode already permits everyone.
	// The store reads that as a host with no fabric -- it discovers zero HCAs and installs a TCP
	// transport instead, reporting no error. A plugin allocation is what adds the cgroup rule, and
	// it adds it without the privileged flag that would otherwise be the only way to get one.
	//
	// Requesting it also decides what a node without EFA looks like. Such a node advertises none of
	// this resource, so the member is never placed there at all, rather than starting and running
	// TCP under a spec that says EFA. That refusal is the wanted one: a cluster whose nodes cannot
	// serve the fabric is a cluster whose backend should say TCP.
	efaDeviceResource core.ResourceName = "vpc.amazonaws.com/efa"

	// memberLocalDiskVolumeName names the host directory holding a group's disk tier.
	memberLocalDiskVolumeName = "local-disk"

	// memberHostPathVolumeNamePrefix prefixes the volume backing one declared host path, which is
	// suffixed with the entry's position.
	//
	// The position and not the path, because a name derived from user input can collide with the
	// name above and a DNS label cannot hold a path anyway. The position already identifies an
	// entry everywhere else, so the mapping needs no state to remember it.
	memberHostPathVolumeNamePrefix = "host-path"

	// MemberLocalDiskSurveyContainerName names the init container that reports what the tier
	// directory already held. The controller finds its reading by this name, so the two are one
	// fact and not two spellings of it.
	MemberLocalDiskSurveyContainerName = "local-disk-survey"

	// memberUnmountLocalDiskPath is the entrypoint's own route for deregistering this store's SSD
	// tier before the process goes away. The master stops naming this store as the owner of its
	// offloaded keys, so a reader gets a clean miss instead of a peer that is about to disappear.
	//
	// The call then holds for the grace, and that wait keeps the PROCESS alive -- it does not keep
	// the tier readable. Deregistration takes effect at once, so it drains nothing; the upstream
	// docstring says otherwise and was refuted by measurement.
	//
	// It needs no segment id, and that is why it is the route that exists here. Memory unmount takes
	// one, and the segment this renderer gives a member cannot be named to it at all -- see
	// memberShutdownSeconds for the measurement.
	memberUnmountLocalDiskPath = "/api/unmount_local_disk"

	// MemberMaxGracePeriodSeconds is the entrypoint's own ceiling on that call. Above it the
	// handler answers 400, so a larger value would render a hook that fails every time it runs.
	//
	// It is exported because admission is what refuses it, and the rule belongs beside the route
	// it protects rather than restated as a number in another package.
	MemberMaxGracePeriodSeconds = 3600

	// memberHookTimeoutMarginSeconds is how much longer than the grace the shutdown hook waits
	// before giving up on its own request.
	//
	// It is deliberately much smaller than memberShutdownSeconds. The Pod's termination budget
	// covers the hook AND the entrypoint's shutdown, in that order, so every second the hook spends
	// past the grace is a second taken from the close that follows it.
	memberHookTimeoutMarginSeconds int64 = 10
)

// The member's environment. Every key the client reads has a real named variable, so the whole
// CONFIGURATION renders as environment: no ConfigMap, and nothing mounted or prepared in order to
// configure the member.
//
// A group declaring a disk tier does carry a volume and an init container, and neither of them
// configures anything - one is the tier itself and the other only reports what was already in it.
// The claim is about where configuration comes from, and it is written that way because the
// unqualified version was true when the member had no tier and stopped being true when it got one.
const (
	// memberEnvMetadataServer carries an underscore inside META_DATA. It is NOT
	// MOONCAKE_TE_METADATA_SERVER, and normalising it to the spelling that reads correctly does not
	// error — it silently degrades the metadata plane. A unit test asserts this byte for byte.
	memberEnvMetadataServer = "MOONCAKE_TE_META_DATA_SERVER"
	// memberMetadataServerValue is the literal the metadata plane takes, unconditionally: this
	// scope ships a peer-to-peer plane, so there is no store to point at.
	memberMetadataServerValue = "P2PHANDSHAKE"

	memberEnvMaster            = "MOONCAKE_MASTER"
	memberEnvProtocol          = "MOONCAKE_PROTOCOL"
	memberEnvLocalHostname     = "MOONCAKE_LOCAL_HOSTNAME"
	memberEnvGlobalSegmentSize = "MOONCAKE_GLOBAL_SEGMENT_SIZE"
	memberEnvLocalBufferSize   = "MOONCAKE_LOCAL_BUFFER_SIZE"

	// The vendor visibility variables, set to "void" on a host-memory group so that no accelerator
	// is injected into it.
	//
	// "void" is not "none": it tells the vendor's container runtime hook to stay out of the
	// container entirely, rather than to inject a device list that happens to be empty, so the
	// driver libraries do not arrive either.
	//
	// EVERY VENDOR AT ONCE, without asking what the node carries. A group's medium is a property of
	// what it declares, not of where it lands, and a node this operator manages can carry any of
	// them; rendering the set that matches the node would make the same spec produce different
	// containers on different hosts.
	//
	// These five are the vendors whose runtimes take a visibility variable at all. The others
	// (Ascend, Hygon, Metax, T-Head) reach their devices through an explicitly mounted device
	// plugin allocation instead, which a container that requested none never receives.
	memberEnvVisibleDevicesAMD       = "AMD_VISIBLE_DEVICES"
	memberEnvVisibleDevicesCambricon = "CAMBRICON_VISIBLE_DEVICES"
	memberEnvVisibleDevicesIluvatar  = "IX_VISIBLE_DEVICES"
	memberEnvVisibleDevicesMThreads  = "MTHREADS_VISIBLE_DEVICES"
	memberEnvVisibleDevicesNVIDIA    = "NVIDIA_VISIBLE_DEVICES"

	// memberVisibleDevicesNone is the value each of the above takes on a host-memory group.
	memberVisibleDevicesNone = "void"

	// memberEnvTransferMetrics turns on the transfer engine's own reporting, which is OFF by
	// default in the artifact and is the only source of the member's throughput and task-latency
	// distribution. The leader's Prometheus surface counts keys and bytes; it does not measure the
	// data plane, so without this the receiving half of every transfer is unmeasurable.
	//
	// The engine side already sets it, and setting it here is what makes the two ends of one
	// transfer symmetric rather than adding a cost the pool did not already carry.
	//
	// The reporting goes to the container log rather than to a port: one line per interval, and
	// none at all in an interval that moved nothing.
	memberEnvTransferMetrics   = "MC_TE_METRIC"
	memberTransferMetricsValue = "1"

	// The local disk tier's keys. The first two are the client's own enable_ssd_offload and
	// ssd_offload_path; without both, the client registers no local disk segment and the leader's
	// offload queue has nowhere to send it.
	memberEnvOffloadEnabled = "MOONCAKE_OFFLOAD_ENABLED"
	memberEnvOffloadPath    = "MOONCAKE_OFFLOAD_FILE_STORAGE_PATH"
	// memberEnvOffloadSizeLimit and memberEnvOffloadKeysLimit cap what the tier stores, by bytes and
	// by key count. Left unrendered, the client's own ceilings apply, so a ceiling that moves
	// upstream shows up as a change to investigate rather than one this renderer silently restated.
	memberEnvOffloadSizeLimit = "MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES"
	memberEnvOffloadKeysLimit = "MOONCAKE_OFFLOAD_TOTAL_KEYS_LIMIT"

	// memberEnvOffloadBucketSizeLimit and memberEnvOffloadBucketKeysLimit are the pair a bucket is
	// closed on. See MemberBucketSizeLimit for why they are rendered at all.
	memberEnvOffloadBucketSizeLimit = "MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES"
	memberEnvOffloadBucketKeysLimit = "MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT"

	// memberEnvOffloadBucketMaxTotalSize is the quota the watermarks below are a fraction OF, and it
	// is a different setting from memberEnvOffloadSizeLimit even though this renderer sends one value
	// into both. Its own default is 0, which the client's watermark eviction reads as "no quota" and
	// returns from without evicting anything -- so a watermark rendered without this reaches a path
	// that is switched off, and the marks would look configured while doing nothing.
	memberEnvOffloadBucketMaxTotalSize = "MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE"

	// memberEnvOffloadEvictionPolicy is the order buckets leave in.
	//
	// The client maps a string it does not recognize onto NO EVICTION -- silently, with no error and
	// no failure to start -- so this renderer sends only values from memberEvictionPolicies, and
	// never a value that arrived as a string from somewhere else.
	memberEnvOffloadEvictionPolicy = "MOONCAKE_OFFLOAD_BUCKET_EVICTION_POLICY"

	// The watermark eviction switch and its two marks. The switch sits above the on-disk format and
	// the policy above sits inside it, which is why turning eviction off renders both.
	memberEnvOffloadWatermarkEnabled = "MOONCAKE_OFFLOAD_ENABLE_DISK_WATERMARK_EVICTION"
	memberEnvOffloadWatermarkHigh    = "MOONCAKE_OFFLOAD_DISK_EVICTION_HIGH_WATERMARK_RATIO"
	memberEnvOffloadWatermarkLow     = "MOONCAKE_OFFLOAD_DISK_EVICTION_LOW_WATERMARK_RATIO"
)

// MemberBucketSizeLimit and MemberBucketKeysLimit are the size and the object count at which the
// client closes a bucket and writes it to the tier. They are RENDERED rather than left at the
// client's own values, which is the exception to this package's "no setting at its own default" rule
// and the one place it would be wrong to follow it.
//
// The client assembles offloaded objects into a bucket and writes NOTHING until one of the two
// thresholds is reached; what is short of it is carried over to the next attempt, indefinitely. Its
// own pair -- 256Mi and 500 objects -- is sized for a saturated production store, and a backend that
// never reaches either has a tier that is mounted, publishes its capacity, runs eviction, and holds
// no data. Nothing reports that: every signal an operator would look at is the declared figure
// rather than an observed one.
//
// So leaving these alone does not preserve a default to investigate later, it ships a feature that
// does not work. The pair below is small enough that a modest backend closes buckets, and the cost is
// more and smaller files rather than anything correctness depends on.
//
// They are EXPORTED because admission bounds three fields against them -- a group's
// capacityPerMember, and its tier's capacity and keyLimit -- and each of those bounds is the same
// fact as this pair rather than a number restated beside it.
const (
	MemberBucketSizeLimit int64 = 16 * 1024 * 1024
	MemberBucketKeysLimit int64 = 64
)

// memberProtocols maps this API's spelling of a transport onto the artifact's.
//
// Auto maps to the artifact's tcp rather than resolving upward against the node, and the two
// reasons are in the API type's own comment: one DaemonSet covers every node a group selects and so
// cannot carry a per-node transport, and promoting to RDMA would grant hostNetwork and two
// capabilities nobody asked for. The artifact has no "auto" of its own either — it looks its
// protocol string up in a transport map, so rendering the literal would find nothing there.
//
// CANN and ROCM are the ONLY two entries whose artifact spelling differs by more than case: the API
// names the toolchain the member image is built against, matching the published image tags, while
// the artifact's transport map knows the vendor's own protocol string (ascend and hip). The rest
// differ only in case, which is enough that comparing an API value against a rendered one as
// strings finds a difference that is not one.
// MemberProtocolAuto is the transport the schema defaults to, named because the renderer falls back
// to it for an object that never reached an API server.
const MemberProtocolAuto = "Auto"

var memberProtocols = map[string]string{
	MemberProtocolAuto: "tcp",
	"TCP":              "tcp",
	"RDMA":             "rdma",
	"EFA":              "efa",
	"CANN":             "ascend",
	"ROCM":             "hip",
	"MUSA":             "musa",
	"MACA":             "maca",
}

// TransportProtocol maps a KVCacheBackend transport value to Mooncake's spelling. An empty
// value resolves to Auto, which renders tcp; an unknown value returns empty.
func TransportProtocol(protocol string) string {
	if protocol == "" {
		protocol = MemberProtocolAuto
	}
	return memberProtocols[protocol]
}

// memberProtocolIsHostFabric reports whether a RESOLVED protocol is one of the two that reach the
// host's fabric: the ones that take hostNetwork, get the two capabilities and are granted a device.
// It takes MemberProtocol's output, not the API's spelling.
//
// rdma and efa are the only two. musa and maca are intra-node IPC transports, not host fabrics, and
// the rest never leave the member's own network namespace, so none of them earns hostNetwork, the
// capabilities or a device.
//
// Both of the two DO get a device request. The effective protocol selects its resource family and
// the member's interface count selects its quantity and RDMA allocation mode, so this predicate
// answers "is a device granted" as well. Neither mounts the device tree; RDMADevicePath says why.
//
// It is UNEXPORTED so that the pair is stated in exactly two places, both in this file and both
// total over it — here and in FabricDeviceResource. A second spelling anywhere else is what would
// let them drift apart the day a third fabric joins. Admission asks the same question through
// MemberGroupIsHostFabric, which delegates here.
func memberProtocolIsHostFabric(protocol string) bool {
	return protocol == memberProtocolRDMA || protocol == memberProtocolEFA
}

// MemberGroupIsHostFabric reports whether one member group's effective protocol is RDMA or EFA,
// which is where the group's fabric interface count selects a device request.
func MemberGroupIsHostFabric(kvcb *workercore.KVCacheBackend, group workercore.KVCacheBackendMember) bool {
	return memberProtocolIsHostFabric(MemberProtocolForGroup(kvcb, group))
}

// MemberObjectName is the name of the objects rendered for one member group.
//
// The group's INDEX is in the name because a group has no name of its own, and because the position
// is what the spec's list already identifies it by. A second group is therefore a second DaemonSet
// rather than a rename of this one.
func MemberObjectName(kvcb *workercore.KVCacheBackend, group int) string {
	return fmt.Sprintf("%s-member-%d", kvcb.Name, group)
}

// MemberProtocol is the transport a group resolves to, in the artifact's own spelling. An
// unrecognized value renders nothing rather than a guess: the schema enumerates this field, so an
// empty result means the object never went through admission.
func MemberProtocol(kvcb *workercore.KVCacheBackend) string {
	return TransportProtocol(kvcb.Spec.Transport.Protocol)
}

// MemberProtocolForGroup is the transport ONE member group resolves to, in the artifact's own
// spelling: the group's own transport.protocol when it declares one, and the backend's value —
// with its Auto and empty handling — when it does not. A group carrying a transport block with no
// protocol inherits too, which is the shape an object that never reached the API server's
// defaulting takes.
func MemberProtocolForGroup(kvcb *workercore.KVCacheBackend, group workercore.KVCacheBackendMember) string {
	if group.Transport != nil && group.Transport.Protocol != "" {
		return TransportProtocol(group.Transport.Protocol)
	}
	return MemberProtocol(kvcb)
}

// MemberProtocols is what a pool's backend offers an engine, in group declaration order: each
// member group's effective protocol, or the backend-wide value alone for a backend that declares
// no groups — an external one, whose store is somebody else's to describe. The slice is never
// empty, so a caller matching against it never has to ask whether the pool offers anything.
func MemberProtocols(kvcb *workercore.KVCacheBackend) []string {
	managed := kvcb.Spec.Connection.Managed
	if managed == nil || len(managed.Members) == 0 {
		return []string{MemberProtocol(kvcb)}
	}

	offers := make([]string, 0, len(managed.Members))
	for i := range managed.Members {
		offers = append(offers, MemberProtocolForGroup(kvcb, managed.Members[i]))
	}
	return offers
}

// FabricDeviceResource is the extended resource one host-fabric protocol asks for, and it NAMES ITS
// PROTOCOLS rather than falling through to one of them.
//
// The fallthrough is the thing to avoid here, not a style preference. Both spellings are correct
// today, because memberProtocolIsHostFabric admits exactly this pair and applyMemberFabric consults
// it first. What separates them is the day a third host fabric joins — which the comments in this
// file expect: a fallthrough hands that fabric's members a seat on an RDMA adapter, silently and
// with the wrong device, while naming the protocols returns the empty resource and the caller
// renders no request at all. A member that asks for nothing is a visible failure on a fabric path;
// a member granted the wrong adapter is not.
//
// Deriving the RDMA name rather than writing it here keeps one spelling in the repository: the
// Device Manager serves whatever GetRDMAResourceName returns, so a member asking for a literal
// would stop matching the day that function's keys moved and nothing would report it. One interface
// asks for the SHARED mode because a member wants a seat on an adapter rather than the whole
// adapter. More than one asks for EXCLUSIVE mode because shared tokens can repeat one endpoint,
// silently satisfying the quantity without granting distinct interfaces.
func FabricDeviceResource(protocol string, interfaceCount int32) core.ResourceName {
	switch protocol {
	case memberProtocolRDMA:
		if interfaceCount > 1 {
			return nodefeature.GetRDMAResourceName(workercore.DeviceAllocationModeExclusive)
		}
		return nodefeature.GetRDMAResourceName(workercore.DeviceAllocationModeShared)
	case memberProtocolEFA:
		return efaDeviceResource
	default:
		return ""
	}
}
