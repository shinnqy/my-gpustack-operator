package modelmanager

import (
	"net/http"
	"sort"
	"time"

	"gpustack.ai/gpustack/pkg/modelmanager/materialize"
	"gpustack.ai/gpustack/pkg/modelstore"
	"gpustack.ai/gpustack/pkg/utils/httpx"
)

// newDownloadsHandler 按摘要排序返回正在进行的下载列表，供 worker 的进度子资源查询：
// 在节点 status 阈值写入间隙提供实时读数。该接口为只读且仅返回摘要、大小与来源，不暴露任何租户名称。
func newDownloadsHandler(progress func() map[string]materialize.DownloadProgress) http.Handler {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			httpx.Error(w, http.StatusMethodNotAllowed)
			return
		}

		resp := modelstore.Downloads{Downloads: []modelstore.Download{}}
		for hex, p := range progress() {
			resp.Downloads = append(resp.Downloads, modelstore.Download{
				Digest: "sha256:" + hex, DownloadedBytes: p.DownloadedBytes, SizeBytes: p.SizeBytes, Source: string(p.Source),
			})
		}
		sort.Slice(resp.Downloads, func(i, j int) bool { return resp.Downloads[i].Digest < resp.Downloads[j].Digest })
		httpx.PureJSON(w, http.StatusOK, resp)
	})

	return http.TimeoutHandler(h, 5*time.Second, "downloads timed out")
}

// ownObjectChanged 仅在节点 NodeModelStore 的 spec 发生变化时放行更新事件：
// 插件自身写入 status 也会以更新事件回传，若不加以过滤将引发无限循环上报。
func ownObjectChanged(oldObj, newObj any) bool {
	o, okOld := oldObj.(interface{ GetGeneration() int64 })
	n, okNew := newObj.(interface{ GetGeneration() int64 })
	return !okOld || !okNew || o.GetGeneration() != n.GetGeneration()
}
