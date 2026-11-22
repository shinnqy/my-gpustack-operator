package devicemanager

import (
	"net/http"
	"time"

	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/devicemanager/detector"
	"gpustack.ai/gpustack/pkg/utils/httpx"
)

// MonitorSnapshotPath 是读取最新加速器监控快照的 HTTP 路径。
const MonitorSnapshotPath = "/monitor/snapshot"

// MonitorSnapshot 是 MonitorSnapshotPath 端点返回的 JSON，包含最近一次采集的指标和保存时间。
type MonitorSnapshot = detector.MonitorSnapshot

// newMonitorSnapshotHandler 创建只读 GET 端点，以 JSON 返回最新快照。首次采样前也返回 HTTP 200，时间为零、
// 列表为空，调用方始终可以按同一结构解析。
func newMonitorSnapshotHandler(snapshot func() *MonitorSnapshot) http.Handler {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			httpx.Error(w, http.StatusMethodNotAllowed)
			return
		}

		resp := MonitorSnapshot{
			Groups: device.MetricsGroupList{},
		}
		if s := snapshot(); s != nil {
			resp = *s
			if resp.Groups == nil {
				resp.Groups = device.MetricsGroupList{}
			}
		}
		httpx.PureJSON(w, http.StatusOK, resp)
	})
	return http.TimeoutHandler(h, 5*time.Second, "monitor snapshot timed out")
}
