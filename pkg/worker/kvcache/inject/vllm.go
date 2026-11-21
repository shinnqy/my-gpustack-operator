// This file renders for the vLLM family. Its vehicle is a file, because that engine has no other:
// `MooncakeStoreConfig.load_from_config()` reads MOONCAKE_CONFIG_PATH and RAISES when it is unset
// (`worker.py:144-151`), and the variable names a file rather than carrying configuration.
package inject

const (
	// vllmConfigPathEnv is the variable vLLM reads the file's location from. It names a path; the
	// configuration itself is never in the environment for this engine.
	vllmConfigPathEnv = "MOONCAKE_CONFIG_PATH"

	// vllmTransferConfigArg selects the connector and the role. There is no environment equivalent -
	// it is an EngineArgs field parsed off the command line - so this engine cannot be configured
	// without appending an argument.
	vllmTransferConfigArg = "--kv-transfer-config"
	vllmKVEventsConfigArg = "--kv-events-config"
	// vllmBlockSizeArg sets vLLM's KV cache block granularity (`vllm/config/cache.py:72-76`,
	// `CacheConfig.block_size: int = Field(default=None, gt=0)` at v0.29.0/v0.30.0, where the V1
	// engine lifted the legacy V0 32-token CUDA cap and V1 CUDA FlashAttention/FlashInfer backends
	// accept multiples of 16 including 128). Over Mooncake TCP, defaulting it to 128 tokens reduces
	// the number of KV transfer and store descriptors per prompt by 8x (for example from 16,448 to
	// 2,056 descriptors on an 8K prompt across 64 layers) and amortizes per-descriptor TCP framing
	// overhead. Emitted in DefaultedArgs so any workload whose model or attention backend requires
	// another granularity overrides the default by declaring `--block-size` in its own arguments.
	vllmBlockSizeArg        = "--block-size"
	defaultVLLMTCPBlockSize = "128"
	// VLLMKVEventsPort is the ZMQ publisher port consumed by managed routers.
	VLLMKVEventsPort int32 = 5557
	// VLLMKVEventsReplayPort is the publisher's replay port.
	VLLMKVEventsReplayPort int32 = 5558
	// VLLMKVEventsTopic is the topic both publisher and subscriber use.
	VLLMKVEventsTopic = "kv@"
	// VLLMMooncakeBootstrapPort is where a prefiller exposes Mooncake's transfer handshake.
	VLLMMooncakeBootstrapPort int32 = 8998

	// vllmStoreConnector is the name vLLM PROPER registers for the Mooncake store
	// (`kv_connector/factory.py:223-226`, read at v0.29.0).
	//
	// `create_connector` looks the name up in a registry and raises `ValueError: Unsupported
	// connector type` on a miss, so a name that engine does not know stops it at startup. That does
	// NOT make this constant engine-independent: it is one project's registry spelling, and which
	// spellings any other engine's factory resolves is upstream state this repository neither
	// controls nor observes. Reach it through vllmConnectorFor, which selects by engine, rather than
	// rendering it directly.
	vllmStoreConnector = "MooncakeStoreConnector"

	// vllmAscendStoreConnector is the name vLLM-Ascend registers for its own store
	// (`vllm_ascend/distributed/kv_transfer/__init__.py:39-43`, read at v0.19.1rc1). It registers
	// `MooncakeConnectorStoreV1` for the same class two entries above; either name resolves, and
	// this one is chosen for saying which project owns the class.
	//
	// The connector name does not imply anything about tenant support; that belongs to the engine
	// image and is not a selection criterion here.
	vllmAscendStoreConnector = "AscendStoreConnector"

	// vllmAscendTransferConnector is the name vLLM-Ascend registers for its point-to-point
	// connector (`vllm_ascend/distributed/kv_transfer/__init__.py:29-31`, read at v0.23.0), loaded
	// through the project's `ascend_kv_connector` entry point rather than named by path.
	//
	// It is NOT the connector vLLM proper registers under the same purpose: the two speak different
	// handshake vocabularies over kv_transfer_params -- proper's MooncakeConnector wants a
	// transfer_id and a bootstrap address, while this one wants the prefiller's host, port, block
	// ids and engine id, which is why a router that proxies between them must match the engine.
	vllmAscendTransferConnector = "MooncakeConnectorV1"

	// vllmAscendDriverPath is the host driver tree the Ascend transfer leg reads each NPU's NIC
	// address through: hccn_tool ships under it, while the engine image carries the driver
	// libraries but not the tool. Host source and container mount point are one path, which is
	// where the image's own driver stubs sit.
	vllmAscendDriverPath = "/usr/local/Ascend/driver"

	// vllmAscendDriverVolumeName names the projection of that tree on a Pod.
	vllmAscendDriverVolumeName = "gpustack-ascend-driver"
)

// renderVLLMWithEncoderCache is renderVLLM with the encoder-cache arm composed onto it. An
// EncoderCache naming no connector leaves every other arm exactly as renderVLLM produces it.
func renderVLLMWithEncoderCache(in Input, ec EncoderCache) (*Result, error) {
	// Implement this function in chapter 20.
	var result0 *Result
	var result1 error
	return result0, result1
}
