//go:build !goccy

package json

import (
	"encoding/json"
	"fmt"
)

var (
	Marshal    = json.Marshal
	Unmarshal  = json.Unmarshal
	NewDecoder = json.NewDecoder

	NewEncoder = json.NewEncoder
)

// MustMarshal 与 `Marshal` 类似，但若存在错误时会触发 panic。
func MustMarshal(v any) []byte {
	bs, err := Marshal(v)
	if err != nil {
		panic(fmt.Errorf("marshal json: %w", err))
	}

	return bs
}

// ShouldMarshal 与 `Marshal` 类似，但不会返回错误。
func ShouldMarshal(v any) []byte {
	bs, _ := Marshal(v)
	return bs
}
