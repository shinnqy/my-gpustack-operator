package httpx

import (
	"net/http"

	"gpustack.ai/gpustack/pkg/utils/bytex"
	"gpustack.ai/gpustack/pkg/utils/json"
)

func PureJSON(w http.ResponseWriter, code int, v any) {
	buf := bytex.GetBuffer()
	defer bytex.Put(buf)

	err := json.NewEncoder(buf).Encode(v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(buf.Bytes())
}

// Error 与 http.Error 类似，但它可以通过给定的代码获取错误信息。
func Error(w http.ResponseWriter, code int) {
	http.Error(w, http.StatusText(code), code)
}
