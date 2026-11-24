package httpx

import (
	"bufio"
	"context"
	"errors"
	"net"
	"net/http"

	klog "k8s.io/klog/v2"
)

type loggingResponseWriter struct {
	http.ResponseWriter
	status int
	cancel context.CancelFunc
}

// WebSocket 接管连接后，net/http 会等处理函数返回才取消请求上下文。
// 在连接实际关闭时通知事件订阅，空闲的 Watch 也能结束等待。
type cancelOnCloseConn struct {
	net.Conn
	cancel context.CancelFunc
}

func (c *cancelOnCloseConn) Close() error {
	c.cancel()
	return c.Conn.Close()
}

func (w *loggingResponseWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *loggingResponseWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(p)
}

func (w *loggingResponseWriter) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (w *loggingResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("response writer does not support hijacking")
	}
	conn, buf, err := hijacker.Hijack()
	if err != nil {
		return nil, nil, err
	}
	return &cancelOnCloseConn{Conn: conn, cancel: w.cancel}, buf, nil
}

// AccessLog 为每次请求记录方法、路径和响应状态。记录查询参数时遮住 token 和 password 的值。
func AccessLog(logger klog.Logger, queryAsValues bool) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			kvs := []any{
				"client", r.RemoteAddr,
				"method", r.Method,
				"path", r.URL.Path,
			}
			if queryAsValues {
				for k, v := range r.URL.Query() {
					if k == "token" || k == "password" {
						v = []string{"***"}
					}
					if len(v) == 1 {
						kvs = append(kvs, k, v[0])
					} else {
						kvs = append(kvs, k, v)
					}
				}
			}
			slg := logger.WithValues(kvs...)
			slg.Info("request")
			ctx, cancel := context.WithCancel(klog.NewContext(r.Context(), slg))
			defer cancel()
			rw := &loggingResponseWriter{ResponseWriter: w, cancel: cancel}
			next.ServeHTTP(rw, r.WithContext(ctx))
			if rw.status == 0 {
				rw.status = http.StatusOK
			}
			if rw.status >= 400 {
				slg.Error(nil, "response", "status", rw.status)
			} else {
				slg.V(2).Info("response", "status", rw.status)
			}
		})
	}
}
