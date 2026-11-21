// Package router renders configuration for the upstream routers managed by a ModelDeployment.
package router

import (
	"sigs.k8s.io/yaml"
)

// The router names this package renders for, which are the API enum's values. They are declared
// here rather than imported because the refusal messages interpolate them, and a package that took
// them from the API would make this renderer depend on the type it exists to stay independent of.
const (
	LLMD   = "llm-d-router"
	VLLM   = "vllm-router"
	SGLang = "sglang-gateway"
)

// Metrics names the model-server metrics consumed by a router.
type Metrics struct {
	Engine             string
	Port               int32
	QueuedRequests     string
	RunningRequests    string
	KVCacheUtilization string

	// CacheInfo names an info-style gauge whose block_size and num_gpu_blocks labels carry the KV
	// cache's capacity. CacheBlockSize and CacheNumBlocks name gauges carrying the same two values
	// directly, for an engine that exports them that way. An engine fills one form or neither.
	CacheInfo      string
	CacheBlockSize string
	CacheNumBlocks string
}

// llmdStageFilterType is the picker plugin that filters endpoints by label. Named here so the
// renderer and the reader of its own output agree on the spelling.
const llmdStageFilterType = "label-selector-filter"

// DocumentStageFilterDemands reports whether the rendered picker document holds a label-selector
// filter whose matchLabels require one label to equal one value.
//
// IT IS ASKED OF THE DOCUMENT RATHER THAN OF ITS TEXT because the same string is also where a
// user-supplied model name is rendered. A document that merely CONTAINS the label's key is not a
// requirement, and reading it as one would activate a gate nobody decided. A stage filter's own
// matchLabels is the only place the document states a requirement about a Pod, so it is the only
// place this looks.
//
// A document that does not parse, and one that declares no filter, demand nothing.
func DocumentStageFilterDemands(document, labelKey, value string) bool {
	var parsed struct {
		Plugins []struct {
			Type       string `json:"type"`
			Parameters struct {
				MatchLabels map[string]string `json:"matchLabels"`
			} `json:"parameters"`
		} `json:"plugins"`
	}
	if err := yaml.Unmarshal([]byte(document), &parsed); err != nil {
		return false
	}
	for _, plugin := range parsed.Plugins {
		if plugin.Type != llmdStageFilterType {
			continue
		}
		if plugin.Parameters.MatchLabels[labelKey] == value {
			return true
		}
	}

	return false
}
