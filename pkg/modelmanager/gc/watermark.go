package gc

import (
	"fmt"
	"math"
	"strings"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/modelstore"
)

// kubelet 默认驱逐与镜像回收阈值，在 kubelet 未显式配置某阈值时用作上限计算依据。
const (
	defaultNodefsAvailablePercent  = 10
	defaultImagefsAvailablePercent = 15
	defaultImageGCHighPercent      = 85
	// watermarkMargin 是上限在每个 kubelet 阈值之下预留的安全余量。
	watermarkMargin = 5
	// watermarkFloor 是 kubelet 永远无法触发驱逐时缓存所采用的最小上限：
	// 当字节形式的驱逐阈值大于等于文件系统总容量时该信号永不触发，缓存自身回收成为唯一回收手段，因此保留最大余量。
	watermarkFloor = 2
)

// Cap 描述与 kubelet 共享文件系统的模型缓存所允许的最高高水位线及其阈值来源。
type Cap struct {
	Percent int32
	Source  string
	// Ignored 记录哪些阈值未被采纳及原因，全部采纳时为空：即便上限未拉低水位，读者也需要知道，
	// 因为 kubelet 仍会按这些阈值执行驱逐。
	Ignored string
}

// KubeletCap 根据 worker 从节点 configz 读取并写入节点 spec 的 kubelet 生效阈值，计算缓存与 kubelet
// 共享文件系统时可用的最高高水位：取 100-nodefs.available-5、100-imagefs.available-5 与
// imageGCHighThresholdPercent-5 中的最小值。由于插件无法判断镜像存储是否共享同一文件系统，因此保守计入
// imagefs 阈值。未配置的阈值采用 kubelet 默认值；k 为 nil 时全部采用默认值并在 Source 中说明。
// 若某字节阈值大于等于文件系统总容量则永不触发，此时将上限压至保底值并注明原因。
func KubeletCap(k *workercore.NodeModelStoreKubelet, totalBytes uint64) Cap {
	nodefs, imagefs, gcHigh := int64(defaultNodefsAvailablePercent), int64(defaultImagefsAvailablePercent),
		int64(defaultImageGCHighPercent)

	source := "kubelet's default thresholds, since the node's effective kubelet configuration could not be read"
	var ignored string
	unreachable := false
	if k != nil {
		source = "the node's effective kubelet configuration"
		var notes []string
		for _, t := range []struct {
			signal, value string
			dst           *int64
		}{{"nodefs.available", k.NodefsAvailable, &nodefs}, {"imagefs.available", k.ImagefsAvailable, &imagefs}} {
			v, ok, neverFires, note := availablePercent(t.signal, t.value, totalBytes)
			if ok {
				*t.dst = v
			}
			if neverFires {
				unreachable = true
			}
			if note != "" {
				notes = append(notes, note)
			}
		}
		if k.ImageGCHighThresholdPercent != nil {
			gcHigh = int64(*k.ImageGCHighThresholdPercent)
		}
		if len(notes) > 0 {
			ignored = strings.Join(notes, "; ")
			source += " (" + ignored + ")"
		}
	}

	c := min(100-nodefs-watermarkMargin, 100-imagefs-watermarkMargin, gcHigh-watermarkMargin)
	if unreachable {
		c = watermarkFloor
	}

	return Cap{Percent: int32(max(c, watermarkFloor)), Source: source, Ignored: ignored} // nolint: gosec // 取值范围在 2..95 之间。
}

// availablePercent 将百分比或字节形式的驱逐阈值换算为向上取整的文件系统百分比。
// 若字节量大于等于文件系统总容量则永不触发，此时通过 neverFires 通知调用方将上限压至保底值，
// 避免回退到 kubelet 默认值反而放宽上限。
func availablePercent(signal, v string, totalBytes uint64) (int64, bool, bool, string) {
	if v == "" {
		return 0, false, false, ""
	}
	t, err := modelstore.ParseThreshold(v)
	switch {
	case err != nil:
		return 0, false, false, fmt.Sprintf("%s %s cannot be read, so kubelet's default applies", signal, v)
	case t.IsPercent:
		return int64(math.Ceil(t.Percent)), true, false, ""
	case totalBytes == 0:
		return 0, false, false, ""
	case uint64(max(t.Bytes, 0)) >= totalBytes: // nolint: gosec // 下界已限制为 0。
		return 0, false, true, fmt.Sprintf("%s %s is not below the filesystem's %d bytes and can never fire, so the cap is floored",
			signal, v, totalBytes)
	}

	return int64(math.Ceil(float64(t.Bytes) / float64(totalBytes) * 100)), true, false, ""
}

// Effective 将上限 c 应用到配置的高低水位线上：高水位不超过上限，且低水位始终低于高水位。
func Effective(high, low int32, c *Cap) (int32, int32, string) {
	if c == nil || high <= c.Percent {
		return high, low, ""
	}
	h := c.Percent

	return h, min(low, h-1), fmt.Sprintf("the high watermark %d%% is capped at %d%% on the filesystem the cache "+
		"shares with kubelet, from %s", high, h, c.Source)
}
