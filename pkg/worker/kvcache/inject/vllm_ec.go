// This file renders the encoder-cache leg of an encoder/prefill/decode deployment for the vLLM
// family: the arguments, the variables and the port that move a multimodal encoding from the
// encoder stage to the prefiller.
//
// It is an arm of its own beside the KV rendering, because the two caches are unrelated. One moves
// attention blocks from a prefill half to a decode half; the other moves encodings from an encoder
// stage to the prefill half. A Pod therefore renders one, the other, or - the prefiller - both, and
// the encoder renders only this one.
package inject

const (
	// vllmECTransferConfigArg selects the encoder-cache connector and the role. Like its KV
	// sibling it is an EngineArgs field parsed off the command line (`ec_transfer_config`, read
	// at vLLM v0.30.0 `vllm/engine/arg_utils.py:1717`) and has no environment equivalent, so
	// this leg cannot be configured without appending an argument.
	vllmECTransferConfigArg = "--ec-transfer-config"

	// vllmECCPUConnector is the name vLLM PROPER registers for the encoder-cache connector that
	// offloads encodings into a CPU mmap region and hands them to a peer with NIXL (read at v0.30.0
	// `docs/features/ec_cpu_connector.md`).
	//
	// Reach it through the caller-supplied connector name, which the selector below checks, rather
	// than by rendering it unconditionally: which spellings any one engine's registry resolves is
	// upstream state this repository neither controls nor observes.
	vllmECCPUConnector = "ECCPUConnector"

	// vllmECExampleConnector is the name vLLM PROPER registers for the encoder-cache connector
	// that writes encodings to a shared directory as safetensors (`example_connector.py:53-56`,
	// read at v0.30.0), which is what the upstream encoder example ships
	// (`examples/disaggregated/disaggregated_encoder/README.md:69-89`).
	vllmECExampleConnector = "ECExampleConnector"

	// vllmECProducerRole and vllmECConsumerRole are the connector's own spellings of the two ends
	// of the leg (`ECProducer`/`ECConsumer`, read at v0.30.0 `vllm/config/ec_transfer.py:12-14`).
	// Upstream also accepts "ec_both", which this renderer never names: one of these roles is one
	// engine each here, and a Pod that both produced and consumed its own encodings would be a
	// deployment shape nothing in this repository builds.
	vllmECProducerRole = "ec_producer"
	vllmECConsumerRole = "ec_consumer"

	// VLLMECSideChannelPort is where an NIXL producer binds the ZMQ ROUTER socket its consumers
	// pull from. It is the upstream default (v0.30.0 `vllm/envs.py:231`), which is where a
	// measured value beats one chosen here. It is separate from every other port this package
	// renders because every other one carries a different protocol: HTTP, metrics and the KV
	// event publisher each sit on their own, and the KV transfer handshake sits on a fourth.
	VLLMECSideChannelPort int32 = 5601

	// vllmECSideChannelHostEnv is the bind address of that socket. Upstream reads the pair only on
	// a producer with NIXL transfer enabled (v0.30.0 `docs/features/ec_cpu_connector.md:108`), and
	// its default of "localhost" works only when a producer and its consumer share a host
	// (`:105`) - which two Pods never do, so the renderer names the Pod's own address.
	vllmECSideChannelHostEnv = "VLLM_EC_SIDE_CHANNEL_HOST"

	// vllmECSideChannelPortEnv is the port half of the same pair.
	vllmECSideChannelPortEnv = "VLLM_EC_SIDE_CHANNEL_PORT"

	// vllmModelRunnerEnv selects the engine's model runner. ECCPUConnector requires the second one
	// and raises ValueError at construction without it (v0.30.0
	// `docs/features/ec_cpu_connector.md:10`), so on this leg the variable is the connector's
	// precondition rather than a preference, and it is owned rather than defaulted.
	vllmModelRunnerEnv = "VLLM_USE_V2_MODEL_RUNNER"

	// vllmECPodIPFieldPath is the field the kubelet resolves at container start, which is what
	// makes the side channel bind address routable without the renderer knowing the Pod's address.
	vllmECPodIPFieldPath = "status.podIP"

	// The encoder-instance flags below are the four the upstream example lists for an encoder
	// (v0.30.0 `examples/disaggregated/disaggregated_encoder/README.md:38-46`). All four are
	// DEFAULTED rather than owned: upstream marks the first two required, but a workload that
	// declares the same flag has asked for the same thing, so the group is dropped and nothing is
	// lost. They render on the producer alone, because every one of them configures an engine that
	// runs no language model - on a prefiller, --mm-encoder-only would skip the very model that
	// has to read the encodings.
	vllmEnforceEagerArg          = "--enforce-eager"
	vllmNoPrefixCachingArg       = "--no-enable-prefix-caching"
	vllmECMaxNumBatchedTokensArg = "--max-num-batched-tokens"
	vllmMMEncoderOnlyArg         = "--mm-encoder-only"

	// vllmECMaxNumBatchedTokens is the token scheduling budget the encoder example sets. Upstream
	// calls the flag irrelevant to an encoder-only instance and says to set it very high to
	// bypass scheduler limits, leaving the real budget to the encoder cache manager (README:44);
	// the example ships 114688 (README:96). It is a default rather than an answer, so a workload
	// with its own budget names the flag and this one is dropped.
	vllmECMaxNumBatchedTokens = "114688"
)

// EncoderCache is the encoder-cache leg one Pod of a deployment runs.
//
// The zero value asks for no leg, which is every input this renderer handled before encoder-cache
// transfer existed. A caller names the connector rather than receiving one because the two paths
// need different facts: the NIXL path needs a CPU region size and the shared-directory path needs a
// directory, and neither is derivable from the other or from the role.
type EncoderCache struct {
	// Connector names the connector the engine must resolve: ECCPUConnector moves encodings
	// between Pods with NIXL, ECExampleConnector through a directory both ends mount. An
	// unrecognized name is refused, because a name the engine's registry does not resolve stops
	// it at startup with no way to tell that apart from a misconfigured cache.
	Connector string

	// CPUBytes is the total size of the CPU mmap region ECCPUConnector offloads encodings into.
	// It has no default here: upstream raises ValueError when the key is absent
	// (v0.30.0 `docs/features/ec_cpu_connector.md:99`), and the region competes for host memory
	// with everything else on the node, so its size is the caller's capacity decision to make.
	CPUBytes int64

	// SharedStoragePath is the directory ECExampleConnector reads and writes the encodings in.
	// It is the mount path the caller attached, and both ends must name the same one: the
	// connector falls back to "/tmp" when the key is absent
	// (`example_connector.py:53-56`, read at v0.30.0), which in a Pod is a directory neither end
	// shares, so an unset value would look configured and move nothing.
	SharedStoragePath string
}

// EncoderCacheOwnedKeys returns the flags and the variables the encoder-cache leg writes for the
// given request, and therefore resolves itself.
//
// A caller uses this to refuse a workload that also declares one of them. Both would otherwise be
// honored and the engine would read whichever it parsed last, so a workload could turn NIXL
// transfer off, point the side channel at a host no consumer dials, or swap the connector under a
// deployment that is already running - none of which any engine log distinguishes from the leg
// simply not working.
//
// The two lists are exact rather than a fixed set, because what the leg writes depends on both the
// connector and the role: the model runner belongs to the NIXL leg on either role, while the side
// channel belongs to its producer alone. They are returned separately because a flag and a variable
// are refused through different mechanisms, and each is always a list - empty where the leg owns
// nothing of that kind - so a caller can compare them against what a render produced without
// normalizing either side.
//
// A request naming no connector owns nothing because it renders nothing, and the caller in
// model_deployment_connector.go that builds the operator's owned-key set always names a connector,
// so the keys are owned on every deployment.
//
// The role is taken alongside the request because a request alone cannot answer the question: the
// same NIXL leg owns one variable as a consumer and three as a producer.
func EncoderCacheOwnedKeys(role Role, ec EncoderCache) (args, env []string) {
	if ec.Connector == "" {
		return []string{}, []string{}
	}
	args = []string{vllmECTransferConfigArg}
	if ec.Connector != vllmECCPUConnector {
		return args, []string{}
	}
	env = []string{vllmModelRunnerEnv}
	if role == RoleEncode {
		env = append(env, vllmECSideChannelHostEnv, vllmECSideChannelPortEnv)
	}
	return args, env
}
