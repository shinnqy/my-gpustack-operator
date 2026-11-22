// Package modelstore 负责合并节点模型缓存各层配置，并校验由此生成的 NodeModelStoreSpec。
// Worker 写入前做一次校验，model-manager 使用前再检查一次，覆盖对象被直接编辑的情况。
package modelstore

import (
	"errors"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/modelartifact"
)

// DriverName 是 model-manager 向 kubelet 注册的 CSI 驱动名，也是对应 CSIDriver 对象的名称。
// 只有找到该对象时，消费方才能选择节点交付。
const DriverName = "model.csi.gpustack.ai"

// 消费端控制器将这些属性写入 CSI 卷，节点插件用相同的键读取模型标识、UID 和清单摘要。
const (
	VolumeAttrArtifact       = "artifact"
	VolumeAttrArtifactUID    = "artifactUID"
	VolumeAttrManifestDigest = "manifestDigest"
)

// 节点模型缓存配置中各项数值的合法边界。
const (
	MaxHighWatermarkPercent = 95
	MinLowWatermarkPercent  = 1
	MinDownloadConcurrency  = 1
	MaxDownloadConcurrency  = 64
)

// Layer 表示一层配置。nil 表示继承较低层的值，因此节点池只覆盖明确设置的字段。
type Layer struct {
	HighWatermarkPercent   *int32
	LowWatermarkPercent    *int32
	DownloadConcurrency    *int32
	DownloadBytesPerSecond *int64
	PeerSyncEnabled        *bool
	HuggingFaceEndpoint    *string
	ModelScopeEndpoint     *string
	HTTPSProxy             *string
	NoProxy                *string
	CABundleConfigMap      *string
}

// Merge 按顺序逐字段叠加各层配置，后面的显式值优先。
func Merge(layers ...Layer) workercore.NodeModelStoreSpec {
	var spec workercore.NodeModelStoreSpec
	for _, l := range layers {
		overlay(&spec.Watermarks.HighPercent, l.HighWatermarkPercent)
		overlay(&spec.Watermarks.LowPercent, l.LowWatermarkPercent)
		overlay(&spec.Download.Concurrency, l.DownloadConcurrency)
		overlay(&spec.Download.BytesPerSecond, l.DownloadBytesPerSecond)
		if l.PeerSyncEnabled != nil {
			spec.PeerSyncEnabled = l.PeerSyncEnabled
		}
		overlay(&spec.Hub.HuggingFaceEndpoint, l.HuggingFaceEndpoint)
		overlay(&spec.Hub.ModelScopeEndpoint, l.ModelScopeEndpoint)
		overlay(&spec.Hub.HTTPSProxy, l.HTTPSProxy)
		overlay(&spec.Hub.NoProxy, l.NoProxy)
		overlay(&spec.Hub.CABundleConfigMap, l.CABundleConfigMap)
	}

	return spec
}

func overlay[T any](dst, src *T) {
	if src != nil {
		*dst = *src
	}
}

// Validate 汇总规格违反的所有规则。校验失败时，Worker 停止写入，插件停止按此配置下载。
func Validate(spec workercore.NodeModelStoreSpec) error {
	var errs []error

	w := spec.Watermarks
	if w.LowPercent < MinLowWatermarkPercent || w.HighPercent > MaxHighWatermarkPercent || w.LowPercent >= w.HighPercent {
		errs = append(errs, fmt.Errorf("watermarks: want %d <= low < high <= %d, got low %d and high %d",
			MinLowWatermarkPercent, MaxHighWatermarkPercent, w.LowPercent, w.HighPercent))
	}

	d := spec.Download
	if d.Concurrency < MinDownloadConcurrency || d.Concurrency > MaxDownloadConcurrency {
		errs = append(errs, fmt.Errorf("download concurrency: want %d to %d, got %d",
			MinDownloadConcurrency, MaxDownloadConcurrency, d.Concurrency))
	}
	if d.BytesPerSecond < 0 {
		errs = append(errs, fmt.Errorf("download bandwidth: want 0 or more bytes per second, got %d", d.BytesPerSecond))
	}

	h := spec.Hub
	if err := ValidateEndpoint(h.HuggingFaceEndpoint); err != nil {
		errs = append(errs, fmt.Errorf("hugging face endpoint: %w", err))
	}
	// 老版本 Worker 写入的规格可能没有 ModelScope 端点。需要 ModelScope 的模型会等待配置。
	// 显式配置后必须通过端点校验。
	if h.ModelScopeEndpoint != "" {
		if err := ValidateEndpoint(h.ModelScopeEndpoint); err != nil {
			errs = append(errs, fmt.Errorf("modelscope endpoint: %w", err))
		}
	}
	if h.HTTPSProxy != "" {
		if err := modelartifact.ValidateProxy(h.HTTPSProxy); err != nil {
			errs = append(errs, fmt.Errorf("https proxy: %w", err))
		}
	}

	if k := spec.Kubelet; k != nil {
		for _, t := range []struct{ name, value string }{
			{"nodefsAvailable", k.NodefsAvailable}, {"imagefsAvailable", k.ImagefsAvailable},
		} {
			if t.value == "" {
				continue
			}
			if _, err := ParseThreshold(t.value); err != nil {
				errs = append(errs, fmt.Errorf("kubelet %s: %w", t.name, err))
			}
		}
		if p := k.ImageGCHighThresholdPercent; p != nil && (*p < 0 || *p > 100) {
			errs = append(errs, fmt.Errorf("kubelet imageGCHighThresholdPercent: want 0 to 100, got %d", *p))
		}
	}

	return errors.Join(errs...)
}

// Threshold 表示 kubelet 的驱逐阈值，可按文件系统容量的百分比或固定字节数表达。
type Threshold struct {
	IsPercent bool
	Percent   float64
	Bytes     int64
}

// ParseThreshold 解析 kubelet 的驱逐阈值，例如 10% 或 20Gi。
func ParseThreshold(raw string) (Threshold, error) {
	if p, ok := strings.CutSuffix(raw, "%"); ok {
		f, err := strconv.ParseFloat(p, 64)
		if err != nil || math.IsNaN(f) || math.IsInf(f, 0) || f < 0 || f > 100 {
			return Threshold{}, fmt.Errorf("%q is not a percentage from 0 to 100", raw)
		}
		return Threshold{IsPercent: true, Percent: f}, nil
	}
	q, err := resource.ParseQuantity(raw)
	if err != nil || q.Sign() < 0 {
		return Threshold{}, fmt.Errorf("%q is neither a percentage nor a non-negative quantity", raw)
	}

	return Threshold{Bytes: q.Value()}, nil
}

// ValidateEndpoint 校验端点是否为带主机名的绝对 HTTP 或 HTTPS 地址，使无效值在写入配置时就能被发现。
func ValidateEndpoint(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("parse %q: %w", raw, err)
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("%q is not an http or https URL with a host", raw)
	}

	return nil
}

// ParseBandwidth 解析每秒下载的字节数，支持 200Mi 等非负整数资源量。零表示不限速。
func ParseBandwidth(raw string) (int64, error) {
	q, err := resource.ParseQuantity(raw)
	if err != nil {
		return 0, fmt.Errorf("parse bandwidth %q: %w", raw, err)
	}
	if q.Sign() < 0 {
		return 0, fmt.Errorf("bandwidth %q is negative", raw)
	}
	v, ok := q.AsInt64()
	if !ok {
		return 0, fmt.Errorf("bandwidth %q is not a whole number of bytes", raw)
	}

	return v, nil
}

// DownloadsPath 是 model-manager 安全端口上报告活动下载的只读路径，Worker 的进度子资源从这里读取现场数据。
const DownloadsPath = "/model/downloads"

// Downloads 是进度接口的响应，仅列下载摘要、大小和来源。
type Downloads struct {
	Downloads []Download `json:"downloads"`
}

// Download 表示节点上正在进行的一次下载。
type Download struct {
	Digest          string `json:"digest"`
	DownloadedBytes int64  `json:"downloadedBytes"`
	SizeBytes       int64  `json:"sizeBytes"`
	Source          string `json:"source,omitempty"`
}
