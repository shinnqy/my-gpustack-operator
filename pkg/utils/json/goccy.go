//go:build goccy

package json

import (
	"fmt"

	json "github.com/goccy/go-json"
)

var (
	Marshal       = json.Marshal
	Unmarshal     = json.Unmarshal
	MarshalIndent = json.MarshalIndent
	NewDecoder    = json.NewDecoder
	NewEncoder    = json.NewEncoder
)

// MustMarshal 与 Marshal 类似，
// 但遇到 error 时会 panic。
func MustMarshal(v any) []byte {
	bs, err := Marshal(v)
	if err != nil {
		panic(fmt.Errorf("marshal json: %w", err))
	}

	return bs
}

// MustUnmarshal 与 Unmarshal 类似，
// 但遇到 error 时会 panic。
func MustUnmarshal(data []byte, v any) {
	err := Unmarshal(data, v)
	if err != nil {
		panic(fmt.Errorf("unmarshal json: %w", err))
	}
}

// MustMarshalIndent 与 MarshalIndent 类似，
// 但遇到 error 时会 panic。
func MustMarshalIndent(v any, prefix, indent string) []byte {
	bs, err := MarshalIndent(v, prefix, indent)
	if err != nil {
		panic(fmt.Errorf("marshal indent json: %w", err))
	}

	return bs
}

// ShouldMarshal 与 Marshal 类似，
// 但从不返回 error。
func ShouldMarshal(v any) []byte {
	bs, _ := Marshal(v)
	return bs
}

// ShouldUnmarshal 与 Unmarshal 类似，
// 但从不返回 error。
func ShouldUnmarshal(data []byte, v any) {
	_ = Unmarshal(data, v)
}

// ShouldMarshalIndent 与 MarshalIndent 类似，
// 但从不返回 error。
func ShouldMarshalIndent(v any, prefix, indent string) []byte {
	bs, _ := MarshalIndent(v, prefix, indent)
	return bs
}
