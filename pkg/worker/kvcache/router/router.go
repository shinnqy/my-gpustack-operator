// Package router 为 ModelDeployment 管理的上游路由器渲染配置。
package router

import (
	"sigs.k8s.io/yaml"
)

// 本包为其渲染配置的路由器名称，对应 API 枚举的值。这里直接声明而非导入，是因为
// 拒绝消息会插值引用它们；如果从 API 导入，就会使本渲染器依赖它本应保持独立的类型包。
const (
	LLMD   = "llm-d-router"
	VLLM   = "vllm-router"
	SGLang = "sglang-gateway"
)

// Metrics 命名路由器所消费的模型服务指标。
type Metrics struct {
	Engine             string
	Port               int32
	QueuedRequests     string
	RunningRequests    string
	KVCacheUtilization string

	// CacheInfo 命名一个 info 风格的 gauge，其 block_size 与 num_gpu_blocks 标签携带
	// KV cache 容量。CacheBlockSize 和 CacheNumBlocks 则分别命名直接携带这两个值的 gauge，
	// 适用于以该方式导出指标的引擎。每个引擎只填充其中一种形式或均留空。
	CacheInfo      string
	CacheBlockSize string
	CacheNumBlocks string
}

// llmdStageFilterType 是按标签过滤端点的 picker 插件。在此命名以确保渲染器与读取其
// 自身输出的组件对拼写达成一致。
const llmdStageFilterType = "label-selector-filter"

// DocumentStageFilterDemands 报告已渲染的 picker 文档中是否包含一个 label-selector
// 过滤器，且其 matchLabels 要求某个标签等于某个值。
//
// 这里是向解析后的文档而非原始文本发问，因为同一段字符串里也渲染了用户提供的模型名称。
// 文档仅仅包含该标签的键并不构成一项要求，若将其当作要求则会激活无人决策过的关卡。
// stage filter 自身的 matchLabels 是文档中唯一声明对 Pod 要求的地方，因此这里只检查该处。
//
// 无法解析的文档以及未声明过滤器的文档不提出任何要求。
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
