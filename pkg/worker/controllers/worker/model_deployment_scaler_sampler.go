package worker

import (
	"context"
	"net/http"
	"sync"
	"time"

	core "k8s.io/api/core/v1"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/utils/datax"
	"gpustack.ai/gpustack/pkg/worker/scaling/algorithm"
)

// The bounds of one sampling pass.
const (
	// maxSamplerPodsPerRole is how many ready answering engine Pods one role is asked about.
	//
	// IT IS A BOUND AND NOT A TARGET: a role with more ready Pods than this is reported as
	// incompletely covered, because the numbers gathered from the Pods below it would not
	// describe the role.
	maxSamplerPodsPerRole = 64

	// maxSamplerScrapeConcurrency is how many Pods one pass scrapes at the same time.
	maxSamplerScrapeConcurrency = 32

	// samplerPodTimeout bounds one Pod's scrape.
	samplerPodTimeout = 2 * time.Second

	// samplerPassTimeout bounds one whole pass over every role's Pods.
	samplerPassTimeout = 8 * time.Second

	// samplerMaxBodyBytes bounds the response body one scrape may read.
	samplerMaxBodyBytes = 1 << 20

	// modelDeploymentScalerSamplerDefaultIntervalSeconds is the interval a scaler is read as
	// sampling at when its own spec does not say one.
	modelDeploymentScalerSamplerDefaultIntervalSeconds = 15
)

// modelDeploymentScalerScrapeFunc reads one engine Pod's metrics exposition.
//
// IT RECEIVES THE POD RATHER THAN AN ADDRESS, because what may be read from a Pod is a
// question about the Pod: an address it does not advertise is not one this may read.
type modelDeploymentScalerScrapeFunc func(ctx context.Context, pod *core.Pod) ([]byte, error)

// modelDeploymentScalerSampler reads the engine metrics of a scaler's target and turns them into
// one observation per role.
//
// IT KEEPS ITS OWN SAMPLE RING PER SCALER, which is not the metrics subresource's window: a
// scaler needs the rate of a signal over the interval it samples at, and the subresource
// answers a different question over a different window.
type modelDeploymentScalerSampler struct {
	// HTTPClient fetches an engine Pod's metrics endpoint.
	//
	// IT IS THE CLIENT THE DEFAULT SCRAPE USES, and it is a field so that a deployment can
	// bound the transport and a test can answer a pass without a server.
	HTTPClient *http.Client

	// Scrape reads one engine Pod's metrics exposition.
	//
	// IT REPLACES THE TRANSPORT WHILE IT IS SET, so that a test can drive a pass from fixed
	// expositions. It receives the Pod and returns the exposition the Pod answered with.
	Scrape modelDeploymentScalerScrapeFunc

	// Clock reads the current time.
	//
	// IT IS THE SAMPLER'S OWN CLOCK, and it is used when a caller does not supply the time a
	// pass is evaluated at.
	Clock func() time.Time

	mu sync.Mutex
	// rings holds what the sampler remembers about each scaler, keyed by the scaler's UID.
	rings map[string]*modelDeploymentScalerRing
	// passing holds the targets a pass is running for, keyed by the target's UID.
	passing map[string]bool
	// lastPass holds the time the last pass over a target started, keyed by the target's UID.
	//
	// IT IS THE FLOOR ON HOW OFTEN A TARGET IS SAMPLED, because a scaler asks for its
	// observations as fast as it reconciles and the cost of a pass is the scrape load it puts
	// on the engine Pods.
	lastPass map[string]time.Time
}

// modelDeploymentScalerRing is what the sampler remembers about one scaler.
type modelDeploymentScalerRing struct {
	// pods holds the last sample of every engine Pod, keyed by the Pod's UID.
	pods map[string]modelDeploymentScalerPodSample
	// roles holds what the last pass knew about every role, keyed by the role's name.
	roles map[string]modelDeploymentScalerRoleSample
	// windows holds the sliding window of every role, keyed by the role's name.
	windows map[string]*modelDeploymentScalerRoleWindow
	// quantile is the latency quantile the remembered observations were taken at.
	//
	// IT IS THE QUANTILE THE OBSERVATIONS CARRY, and an observation taken at
	// another quantile describes another distribution, so the remembered
	// observations are dropped when the quantile changes.
	quantile float64
	// targetKey is the key the target of the scaler is remembered under.
	//
	// IT IS THE KEY OF THE TARGET AND NOT OF THE SCALER, because the passes are counted per
	// target: two scalers of one target share the floor on how often it is sampled.
	targetKey string
}

// modelDeploymentScalerRoleWindow is the sliding window of one role.
//
// IT IS THE STATE A SCALER NEEDS BETWEEN TICKS, because the stabilization history and the
// demand history are windows over the last samples of a role: they are read as a window and
// grown by one sample per tick. They are kept in memory so that a steady scaler does not
// have to write them to the apiserver on every tick.
type modelDeploymentScalerRoleWindow struct {
	// stabilization holds the stabilization samples of the role.
	stabilization *datax.RingBuffer[workercore.ModelDeploymentScalerStabilizationSample]
	// predictor is the predictor state of the role.
	predictor *algorithm.PredictorState
	// seeded says whether the window was filled from the status of the scaler.
	seeded bool
}

// modelDeploymentScalerRoleSample is what the sampler knows about one role after a pass.
//
// IT CARRIES WHAT THE OBSERVATION CANNOT, which is the reasoning behind it: why a signal is
// not the reading it would be, how long the window the rates were read over was, and how
// many times the role's engine containers restarted. A scaler that reports why it is waiting
// for metrics is a scaler whose reader can tell a missing signal from a broken one.
type modelDeploymentScalerRoleSample struct {
	// Observation is what the role's signals say.
	Observation algorithm.RoleObservation
	// Reasons says why the signals are not the readings they would be.
	Reasons []string
	// WindowSeconds is the shortest window the role's rates were read over.
	WindowSeconds float64
	// ContainerRestarts is the most times any of the role's engine containers restarted.
	ContainerRestarts int32
	// ReadyPods is how many ready engine Pods answered the pass.
	ReadyPods int
}

// Forget drops everything the sampler remembers about a scaler.
//
// IT IS CALLED WHEN THE SCALER IS DELETED, because the ring is keyed by the scaler's UID: a
// scaler deleted and recreated under the same name carries a different UID and is answered
// from a ring of its own, so what the deleted one left behind is only ever memory.
//
// THE FLOOR ON HOW OFTEN THE TARGET IS SAMPLED STAYS, because the floor is the target's and
// not the scaler's: a second scaler of the same target still samples it under that floor.
func (s *modelDeploymentScalerSampler) Forget(scaler *workercore.ModelDeploymentScaler) {
	if scaler == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := string(scaler.UID)
	ring, ok := s.rings[key]
	if !ok {
		return
	}
	targetKey := ring.targetKey
	delete(s.rings, key)
	if targetKey == "" {
		return
	}
	for _, other := range s.rings {
		if other.targetKey == targetKey {
			return
		}
	}
	delete(s.lastPass, targetKey)
}

// modelDeploymentScalerRoleKind reads the kind of a role in the vocabulary the algorithms use.
func modelDeploymentScalerRoleKind(kind workercore.ModelDeploymentRoleKind) algorithm.RoleKind {
	switch kind {
	case workercore.ModelDeploymentRoleKindPrefill:
		return algorithm.PrefillRole
	case workercore.ModelDeploymentRoleKindDecode:
		return algorithm.DecodeRole
	case workercore.ModelDeploymentRoleKindEncode:
		return algorithm.EncodeRole
	case workercore.ModelDeploymentRoleKindServer:
		return algorithm.ServerRole
	default:
		return algorithm.RoleKindUnknown
	}
}
