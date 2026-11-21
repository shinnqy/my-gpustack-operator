package json

import (
	stdjson "encoding/json"
)

// RawMessage is a raw encoded JSON value.
// It implements json.Marshaler and Unmarshaler and can
// be used to delay JSON decoding or precompute a JSON encoding.
type RawMessage = stdjson.RawMessage
