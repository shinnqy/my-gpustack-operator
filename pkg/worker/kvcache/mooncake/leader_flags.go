package mooncake

const (
	// LeaderRPCPort is the port the leader serves its RPC on, and the port an engine client
	// connects to. It is pinned rather than left to the artifact's own default because the
	// Service and the published endpoint have to name a number, and a default that moved would
	// move them silently.
	LeaderRPCPort = 50051
	// LeaderMetricsPort is the port the leader serves BOTH its Prometheus exposition and its HTTP
	// admin API on. One port, two surfaces — that is the artifact's design, not a simplification
	// here, and it is why the published admin endpoint and the scrape target are the same address.
	LeaderMetricsPort = 9003

	// LeaderPodNameEnv and LeaderPodNamespaceEnv are the environment variables the rendered argv
	// refers to under Kubernetes election. The workload that runs this argv has to define both from
	// the downward API, which is what makes the reference resolve.
	//
	// They carry this repository's own names rather than the bare POD_NAME / POD_NAMESPACE the
	// flag documents as its default source. Nothing is lost by that: the flags are rendered
	// explicitly, so the artifact never falls back to reading them itself, and every other
	// component here already spells them this way.
	LeaderPodNameEnv      = "KUBERNETES_POD_NAME"
	LeaderPodNamespaceEnv = "KUBERNETES_POD_NAMESPACE"
	// LeaderPodIPEnv is the third, and like the other two it is defined only under Kubernetes election
	// because only the election reads it. See the -rpc_address flag for what it decides.
	LeaderPodIPEnv = "KUBERNETES_POD_IP"

	// LeaderKVLeaseTTL is how long a cached object is protected from eviction, rendered in place of
	// the artifact's own ten seconds.
	//
	// A lease is granted when an object is READ, not when it is written: completing a write grants a
	// zero-length one, so an object nobody has read yet is evictable immediately. What this value
	// bounds is therefore the RECENTLY READ set, not the write rate, and eviction skips a protected
	// object outright rather than falling back to it when it finds nothing else.
	//
	// Ten seconds is short enough that a block read by one engine replica is evictable again before
	// the next replica asks for it, which is the reuse a shared cache exists to provide. Five
	// minutes covers that, and costs nothing until the set read within it outgrows the store, which
	// is a store sized below its own working set rather than a consequence of this value.
	//
	// It is a duration string because the flag accepts ms, s, m and h suffixes.
	LeaderKVLeaseTTL = "5m"
)
