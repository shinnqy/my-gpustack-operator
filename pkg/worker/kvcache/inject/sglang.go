// This file renders for SGLang. Its vehicle is the environment, and the reason is evaluation time
// rather than key coverage.
//
// Every upstream coordinate in this file is READ AT v0.5.18 and names its symbol, because the file
// it points into has tripled in length across releases: a bare line number silently comes to mean
// "wherever this is today", while the symbol is what relocates the claim after the file moves.
//
// SGLang picks its configuration source in `_load_config` (v0.5.18 `mooncake_store.py:294-314`): the
// launch argument's extra config, then a file, then the environment. The first two are fixed when the
// caller runs - at admission, before a Pod has an IP - and both fall back key-for-key to
// `envs.<NAME>.default` (v0.5.18 `mooncake_store.py:110-168` `from_file` and `:212-261`
// `load_from_extra_config`), which is the literal attribute assigned in `EnvField.__init__`
// (v0.5.18 `environ.py:39-40`) and not the accessor that reads the process environment
// (`EnvField.get` at v0.5.18 `environ.py:57`, defaulting through `_resolve_default` at `:52-55`).
// So on either of them `local_hostname` resolves to the literal "localhost" on every Pod in the pool.
//
// That key is an address: vLLM computes its own from the local IP (`rdma_utils.py:21-25`). Only the
// environment defers evaluation to the kubelet, where a fieldRef to status.podIP resolves at container
// start. Waiting for an upstream release would not help either, because the value is not missing from a
// schema - it does not exist yet at the moment a file would be written.
//
// The environment path is selected by NOT setting SGLANG_HICACHE_MOONCAKE_CONFIG_PATH, which is why
// this renderer emits no such variable. A user who sets it themselves takes over, silently and
// correctly: an explicit configuration should outrank a defaulted one.
package inject

import ()

const (
	// The variables SGLang reads through `.get()` in `load_from_env`
	// (v0.5.18 `mooncake_store.py:170-210`).
	//
	// sglangMetadataServerEnv carries a spelling trap worth stating: META_DATA has an underscore the
	// readable METADATA does not. A wrong spelling here does not error - the key simply falls back to
	// its default - so the tests pin this name byte for byte rather than through this constant.
	sglangMasterEnv         = "MOONCAKE_MASTER"
	sglangMetadataServerEnv = "MOONCAKE_TE_META_DATA_SERVER"
	sglangProtocolEnv       = "MOONCAKE_PROTOCOL"
	sglangDeviceEnv         = "MOONCAKE_DEVICE"
	sglangGlobalSegmentEnv  = "MOONCAKE_GLOBAL_SEGMENT_SIZE"
	sglangLocalHostnameEnv  = "MOONCAKE_LOCAL_HOSTNAME"

	// sglangTenantEnv carries the Binding's reuse domain. Whether the selected image reads it is the
	// image owner's compatibility responsibility.
	sglangTenantEnv = "MOONCAKE_TENANT_ID"

	// sglangBackendArg enables the store. Like vLLM's connector argument it has no environment
	// equivalent, so this engine also cannot be configured without appending an argument.
	sglangBackendArg   = "--hicache-storage-backend"
	sglangBackendValue = "mooncake"

	// sglangBackendExtraConfigArg carries the store's extra configuration as JSON. It renders
	// holding extra_backend_tag alone, which SGLang v0.5.18 puts first in every store key
	// (`mem_cache/storage/mooncake_store/mooncake_store.py:419-430`). An extra configuration naming
	// neither master_server_address nor client_server_address does not select the loader that
	// reads the store's settings from it (`:294-314`), so the settings still come from the
	// environment rendered here.
	sglangBackendExtraConfigArg = "--hicache-storage-backend-extra-config"

	// The disaggregation arguments, which name the prefill/decode split itself. All three are
	// ServerArgs fields: disaggregation_mode (v0.5.18 `server_args.py:3101-3105`, Literal
	// "null"/"prefill"/"decode", "null" meaning no split), disaggregation_transfer_backend
	// (v0.5.18 `server_args.py:3106-3113`, choices DISAGG_TRANSFER_BACKEND_CHOICES, default
	// "mooncake") and disaggregation_bootstrap_port (v0.5.18 `server_args.py:3114-3118`, default
	// 8998, "Bootstrap server port on the prefill server").
	//
	// The backend is written even though its default is already mooncake, because the value is a
	// pair property: the router-side handshake and the engines' transfer engine must agree, and
	// leaving the flag to its default lets a role's own argument change one side of the pair
	// without anything here naming the leg it broke.
	sglangDisaggregationModeArg          = "--disaggregation-mode"
	sglangDisaggregationBackendArg       = "--disaggregation-transfer-backend"
	sglangDisaggregationBackendMooncake  = "mooncake"
	sglangDisaggregationBootstrapPortArg = "--disaggregation-bootstrap-port"

	// sglangDisaggregationBackendMooncakeTCP is the same Mooncake backend pinned to TCP. SGLang's
	// disaggregation hook (v0.5.18 `arg_groups/pd_disaggregation_hook.py`) sets MC_FORCE_TCP when
	// it is unset, rewrites the backend back to mooncake and clears the IB device, so it is the
	// engine's own spelling of what the vLLM renderer does with the variable. The router side of
	// the pair speaks mooncake either way; only the engines' transfer engine is pinned.
	sglangDisaggregationBackendMooncakeTCP = "mooncake_tcp"

	// sglangRetractionBackupArg selects where a decode half keeps the KV cache of a request it
	// retracts (v0.5.18 `server_args.py` ServerArgs.disaggregation_decode_retraction_backup,
	// choices cpu_tensor and host_pool). Left unset, a decode half on an MHA pool infers host_pool
	// and builds a hierarchical host pool (v0.5.18 `mem_cache/kv_cache_builder.py`), which SGLang
	// refuses to build unless the node's available memory, read host-wide, exceeds a fixed 10 GiB
	// reserve plus the pool (v0.5.18 `mem_cache/pool_host/base.py`
	// HICACHE_HOST_MEMORY_RESERVE_BYTES). cpu_tensor keeps per-request CPU tensors and builds no
	// pool, so a decode half starts on a node with less than that to spare.
	sglangRetractionBackupArg      = "--disaggregation-decode-retraction-backup"
	sglangRetractionBackupCPUValue = "cpu_tensor"

	// sglangHierarchicalCacheArg turns on the hierarchical cache the storage backend hangs off.
	// At v0.5.18 the scheduler enables storage prefetch whenever a storage backend is named
	// (`managers/scheduler.py`), while the tree cache that prefetch reads is the hierarchical one
	// only under this flag (`mem_cache/registry.py`); naming the backend alone leaves a plain
	// RadixCache, and the first request raises AttributeError on hicache_storage_pass_prefix_keys.
	sglangHierarchicalCacheArg = "--enable-hierarchical-cache"

	// sglangPageSizeArg sets SGLang's KV cache page granularity (`arg_groups/fields/schedule.py`,
	// default None -> effective 1 token in `model_executor/model_runner.py` and `mooncake_store.py`).
	// With page_size=1, both HiCache Mooncake store objects (`_batch_io_v2`) and P/D transfer
	// descriptors (`disaggregation/mooncake/conn.py`) degenerate into per-token operations
	// (`arg_groups/kv_shard_hook.py`). Over Mooncake TCP, defaulting --page-size to 64 cuts
	// descriptor counts by 64x, matches SGLang's Mooncake store and KV-sharding recommendations,
	// satisfies attention backends that cap or require page_size == 64 (flashmla, trtllm_mla,
	// tokenspeed_mla, cutedsl_mla, dsa, hpc_ops), evenly divides chunked_prefill_size, and keeps
	// prefill and decode halves on the identical --page-size required by `disaggregation/common/conn.py`.
	// Emitted in DefaultedArgs so a workload whose model or attention backend requires another
	// value (such as hybrid/linear-attention models requiring `--page-size 1`) overrides the
	// default by declaring `--page-size` in its own arguments.
	sglangPageSizeArg        = "--page-size"
	defaultSGLangTCPPageSize = "64"

	// THE PAIR'S PORTS, AND WHY ONLY ONE OF THEM IS A NUMBER. The encoder serves its requests on
	// the engine's own --port: an encoder-only server runs its HTTP server on server_args.port
	// (v0.5.18 `disaggregation/encode_server.py:4110`) and advertises itself as
	// NetworkAddress(host, server_args.port) (`:3875`), so the address a language-only half dials
	// is the port the caller already renders for every role and no second number exists for it.
	//
	// The reply travels the other way and has NO FIXED PORT AT v0.5.18, which is measured rather
	// than assumed. The language-only half binds a ZMQ PULL socket on a port the operating system
	// assigns (v0.5.18 `utils/network.py:202-218`, "the randomly assigned TCP port"), once for the
	// scheduler-side receiver (v0.5.18 `disaggregation/encode_receiver.py:1611-1620`) and once per
	// waiting request (`:790-797`), and then hands the encoder its own host and that port inside
	// the encode request; the encoder PUSHes to it (v0.5.18
	// `disaggregation/encode_server.py:3010-3038`, `_push_embedding_to_prefill`). So what this leg
	// needs of an operator is reachability between two Pods rather than a published number, and no
	// constant is invented here to stand in for one. The number the pair does share before any
	// request exists is the bootstrap port below, which both halves know from startup.
	//
	// The encoder-disaggregation arguments, which name the third stage of a deployment that splits
	// an encoder off its own server.
	//
	// sglangEncoderOnlyArg and sglangLanguageOnlyArg are ServerArgs bool fields (v0.5.18
	// `server_args.py:3171-3173` and `:3174-3176`), and a bool field is registered with
	// action="store_true" (v0.5.18 `arg_groups/arg_utils.py:313-319`), so each is a switch that
	// takes no value.
	//
	// sglangEncoderURLsArg is a List[str] (v0.5.18 `server_args.py:3193-3195`) and a list field
	// carrying no explicit nargs takes "+" (v0.5.18 `arg_groups/arg_utils.py:293-297`), so ONE flag
	// takes every address, up to the next entry that starts with a dash. That is why the encoder
	// leg renders this flag last: a value belonging to a flag written after it would be read as
	// another address, and the engine would dial a Service that does not exist.
	sglangEncoderOnlyArg  = "--encoder-only"
	sglangLanguageOnlyArg = "--language-only"
	sglangEncoderURLsArg  = "--encoder-urls"

	// sglangEncoderBootstrapPortArg names the EncoderBootstrapServer's port (v0.5.18
	// `server_args.py:3196-3200`). The server runs in the LANGUAGE-ONLY tokenizer manager process
	// (v0.5.18 `managers/tokenizer_manager.py:673-687`), so this is a prefill port rather than an
	// encoder one: encoders register there, and a language-only receiver fetches the current URL
	// list from there. It is rendered because the same process serves it whenever --language-only
	// is set, whether or not any encoder registers.
	sglangEncoderBootstrapPortArg = "--encoder-bootstrap-port"

	// SGLangEncoderBootstrapPort is that port. Upstream defaults it to 8997 (v0.5.18
	// `server_args.py:3196-3200`), and it is rendered explicitly for the reason
	// sglangBootstrapPortAnnotation gives for the pair above: a default leaves every side to
	// restate the number on its own, and this one is a port a Pod has to have open.
	SGLangEncoderBootstrapPort int32 = 8997

	// sglangEncoderBootstrapPortName names that port on the container. A Kubernetes port name is
	// at most fifteen characters, which is what rules out spelling it "encoder-bootstrap".
	sglangEncoderBootstrapPortName = "enc-bootstrap"

	// SGLangBootstrapPort is where a disaggregated prefiller serves the bootstrap registry the
	// decode side learns its transfer endpoints from. Upstream defaults it to 8998
	// (v0.5.18 `server_args.py:3114-3118`, ServerArgs.disaggregation_bootstrap_port); it is
	// rendered explicitly rather than left to that default because TWO other parties pair with
	// it -- the gateway reads it off the prefill Pod's annotation, and the decode Pod's routing
	// sidecar reads it from its environment -- and a default leaves each of them to restate the
	// number on its own.
	SGLangBootstrapPort int32 = 8998

	// sglangBootstrapPortAnnotation publishes the prefiller's bootstrap port for a router that
	// discovers workers through Kubernetes. The gateway reads it on prefill Pods alone
	// (sgl-project/sglang@gateway-v0.3.1 `sgl-model-gateway/src/service_discovery.rs:54,136-141`,
	// the default of Config.bootstrap_port_annotation). The value is the port as a decimal
	// string, which is how the gateway's own discovery test writes it.
	sglangBootstrapPortAnnotation = "sglang.ai/bootstrap-port"

	// sglangPodIPFieldPath is the field the kubelet resolves at container start, which is the whole
	// reason this engine takes the environment.
	sglangPodIPFieldPath = "status.podIP"
)

// SGLangEncoderOwnedArgs returns the encoder-disaggregation arguments this renderer writes, so a
// caller that refuses a user value for an owned key names them without restating a spelling.
//
// It is the whole list rather than one key because the leg has three: the two switches that place
// a Pod in the encoder or the language-only shape, and the flag carrying the addresses the two
// halves pair through. A user value for any of them lands after the operator's on the same command
// line, so leaving one unowned lets a role declared as the encoder start as the language-only
// server with both ends still advertising the first.
//
// The bootstrap port is in the list for the reason the disaggregation mode is in the model's own:
// it is a pair property, written by the language-only half and dialed by an encoder that
// registers against it.
func SGLangEncoderOwnedArgs() []string {
	return []string{
		sglangEncoderOnlyArg,
		sglangLanguageOnlyArg,
		sglangEncoderURLsArg,
		sglangEncoderBootstrapPortArg,
	}
}

// renderSGLang produces the variables and the argument an SGLang container needs.
//
// It writes no `local_buffer_size` in any spelling, and that omission is measured rather than
// overlooked: SGLang's reader has no key for it, and it hardcodes its own 16 MiB
// `DEFAULT_LOCAL_BUFFER_SIZE` on both of its store-setup paths - `setup_dummy` and `setup`, not two
// calls to the same function - each commented "Zero copy interface does not need local buffer"
// (v0.5.18 `mooncake_store.py:28,464,514`). Emitting it would write something nothing reads.
//
// IT ALSO WRITES NO MODE, AND THAT ONE IS NOT MEASURED. This comment used to claim the reader has
// no key for a mode in any spelling. It has one, under a name a search for "mode" does not find:
// `load_from_env` binds `standalone_storage` from `MOONCAKE_STANDALONE_STORAGE`, and
// `sglang.srt.environ` defaults it to `EnvBool(False)` (v0.5.18). So this renderer leaves SGLang in
// a different store shape from the one it gives vLLM, whose annotation carries
// `"mode":"standalone-store"` beside the same `"global_segment_size":0` - and `client_config.go`
// says of that pair that the two are always written together.
//
// Whether writing it here is the repair is UNRESOLVED, which is why nothing is written. Setting
// `MOONCAKE_STANDALONE_STORAGE=true` on a deployment moved the engine's failure rather than
// clearing it: the store's warmup stopped being reached and `setup_dummy` raised a TypeError on
// its own signature instead. Naming the gap is what this paragraph is for; closing it needs a
// reading from an engine that gets past that call.
//
// `global_segment_size` IS written, because SGLang defaults it to "4gb" when absent (v0.5.18
// `environ.py:704`, `MOONCAKE_GLOBAL_SEGMENT_SIZE`) - the same trap vLLM has, so the same explicit
// zero. Note that SGLang divides the configured value across tensor-parallel ranks before passing it
// on (v0.5.18 `mooncake_store.py:413,415-416`, `tp_scale_factor`); zero divides to zero, but anyone
// rendering a non-zero value here must account for the multiplication.
//
// Each reference above carries its version and its symbol because the bare line numbers this comment
// used to cite had every one of them drifted - that upstream file has since grown past 1300 lines -
// and a stale line number survives review precisely because it looks checked.
func renderSGLang(in Input) (*Result, error) {
	// Implement this function in chapter 20.
	var result0 *Result
	var result1 error
	return result0, result1
}
