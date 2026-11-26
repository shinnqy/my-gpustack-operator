package inject

import (
	"testing"

	"github.com/stretchr/testify/require"

	"gpustack.ai/gpustack/pkg/utils/json"
)

// testConnection 是一个已确定的连接，因为调用者会将其交付给对方。
func testConnection() Connection {
	return Connection{MasterAddress: "kvcache-master.gpustack-system.svc:50051", Protocol: "tcp"}
}

// renderedConfig 会执行 vLLM 系列的生成过程，并返回已解码的投影文件。
func renderedConfig(t *testing.T, in Input) map[string]any {
	t.Helper()

	result, err := Render(in)
	require.NoError(t, err)

	raw, ok := result.PodAnnotations[ClientConfigAnnotationKey]
	require.True(t, ok, "the file vehicle carries its content on the annotation it projects from")

	var decoded map[string]any
	require.NoError(t, json.Unmarshal([]byte(raw), &decoded))
	return decoded
}
