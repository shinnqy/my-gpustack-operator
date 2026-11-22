package json

import (
	stdjson "encoding/json"
)

// RawMessage 是原始编码的 JSON 值。
// 它实现了 json.Marshaler 和 Unmarshaler 接口，
// 可用于延迟 JSON 解码或预计算 JSON 编码。
type RawMessage = stdjson.RawMessage
