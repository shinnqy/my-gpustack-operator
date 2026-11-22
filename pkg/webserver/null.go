package webserver

import (
	"context"
	"errors"
	"net/http"

	"sigs.k8s.io/controller-runtime/pkg/healthz"
)

// Null 创建一个空操作的 Server。
func Null() Server {
	return &null{}
}

type null struct{}

func (null) NeedLeaderElection() bool {
	return false
}

func (null) Register(string, http.Handler) {
}

func (null) Start(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func (null) StartedChecker() healthz.Checker {
	return func(_ *http.Request) error {
		return nil
	}
}

func (null) WebhookMux() *http.ServeMux {
	return http.NewServeMux()
}

func (null) HostPort() (string, int, error) {
	return "", 0, errors.New("no listener")
}

func (null) NotFoundHandler(http.Handler) {
}
