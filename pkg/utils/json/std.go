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
)

// MustMarshal is similar to Marshal,
// but panics if found error.
func MustMarshal(v any) []byte {
	bs, err := Marshal(v)
	if err != nil {
		panic(fmt.Errorf("marshal json: %w", err))
	}

	return bs
}

// ShouldMarshal is similar to Marshal,
// but never return error.
func ShouldMarshal(v any) []byte {
	bs, _ := Marshal(v)
	return bs
}
