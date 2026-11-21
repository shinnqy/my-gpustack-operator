// Package algorithm contains pure scaling recommendation algorithms.
package algorithm

import (
	"time"

	"gpustack.ai/gpustack/pkg/utils/datax"
)

// RoleKind identifies the serving role represented by an observation.
type RoleKind string

const (
	RoleKindServer  RoleKind = "Server"
	RoleKindPrefill RoleKind = "Prefill"
	RoleKindDecode  RoleKind = "Decode"
	RoleKindEncode  RoleKind = "Encode"
	// RoleKindUnknown says the role is one the algorithms do not know.
	//
	// IT IS NOT A DEFAULT TO SERVER, because a role whose kind was not named is not a server:
	// treating it as one would scale it on a target it never declared.
	RoleKindUnknown RoleKind = "Unknown"

	ServerRole  = RoleKindServer
	PrefillRole = RoleKindPrefill
	DecodeRole  = RoleKindDecode
	EncodeRole  = RoleKindEncode
)

// SignalState describes the quality of one sampled signal.
type SignalState string

// RoleObservation contains the values sampled for one role at one instant.
type RoleObservation struct {
	Role                      string
	Kind                      RoleKind
	CurrentReplicas           int32
	ReadyReplicas             int32
	PromptTokensPerSecond     float64
	GenerationTokensPerSecond float64
	RunningRequests           float64
	WaitingRequests           float64
	KVCacheUsage              float64
	TTFTMilliseconds          float64
	ITLMilliseconds           float64
	// TTFTUnmeasured says the first token latency was not measured.
	//
	// IT IS SET WHEN THE SAMPLER COULD NOT READ THE LATENCY, because a latency that
	// was not measured is not a latency of no time. The value beside it says
	// nothing, and the algorithms that hold a role by this latency skip it.
	TTFTUnmeasured bool
	// ITLUnmeasured says the inter token latency was not measured.
	//
	// IT IS SET WHEN THE SAMPLER COULD NOT READ THE LATENCY, for the same reason
	// the first token latency carries the same mark.
	ITLUnmeasured    bool
	ThroughputState  SignalState
	LatencyState     SignalState
	GaugeState       SignalState
	CompleteCoverage bool
	ObservedAt       time.Time
}

// DemandSample is a timestamped role demand sample.
type DemandSample struct {
	Timestamp       time.Time
	TokensPerSecond float64
}

// PredictorState is the state carried between recommendations for one role.
type PredictorState struct {
	Level       float64
	Trend       float64
	Covariance  [4]float64
	SampleCount int32
	// History is the demand history of the role.
	//
	// IT IS A RING BUFFER, because the history grows by one sample on every tick and is
	// read as a window: a slice would be reallocated on every tick to hold the same window.
	History     *datax.RingBuffer[DemandSample]
	PanicExitAt time.Time
}
