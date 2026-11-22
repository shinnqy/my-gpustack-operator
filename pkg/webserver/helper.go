package webserver

import (
	"net/http"
	"time"
)

// newHttpServer 会创建一个新的 http.Server，并配置给定的处理程序和一些默认设置。
func newHttpServer(handler http.Handler) *http.Server {
	return &http.Server{
		Handler:           handler,
		MaxHeaderBytes:    1 << 20,
		IdleTimeout:       90 * time.Second,
		ReadHeaderTimeout: 32 * time.Second,
	}
}
