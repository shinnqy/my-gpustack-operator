package webserver

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strconv"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	ctrlwebhook "sigs.k8s.io/controller-runtime/pkg/webhook"

	"gpustack.ai/gpustack/pkg/utils/gox"
	"gpustack.ai/gpustack/pkg/utils/netx"
)

// Server 是一个 Web 服务器，能够处理 Webhook 和其他 HTTP 操作。
type Server interface {
	ctrlwebhook.Server

	// HostPort 返回服务器监听的主机地址和端口。
	HostPort() (string, int, error)

	// NotFoundHandler 用于处理与任何已注册路径不匹配的请求。如果未设置该处理器，则使用默认的
	// http.NotFoundHandler。
	NotFoundHandler(http.Handler)
}

// New 会根据给定配置创建一个服务器。
func New(c *Config) (Server, error) {
	if c.Listener == nil {
		return nil, errors.New("listener is required")
	}

	var (
		host string
		port int
	)
	if tcpAddr, ok := c.Listener.Addr().(*net.TCPAddr); ok {
		host = tcpAddr.IP.String()
		port = tcpAddr.Port
	}

	return &server{
		listener: c.Listener,
		host:     host,
		port:     port,
		runners:  c.Runners,
		mux:      http.NewServeMux(),
	}, nil
}

type server struct {
	listener        net.Listener
	host            string
	port            int
	runners         []manager.Runnable
	mux             *http.ServeMux
	notFoundHandler http.Handler
}

func (*server) NeedLeaderElection() bool {
	return false
}

func (s *server) Register(path string, hook http.Handler) {
	s.mux.Handle(path, hook)
}

func (s *server) Start(ctx context.Context) error {
	srv := s.getServer()

	gp := gox.GroupWithContextIn(ctx)
	for i := range s.runners {
		r := s.runners[i]
		gp.Go(func(ctx context.Context) error {
			return r.Start(ctx)
		})
	}
	gp.Go(func(ctx context.Context) error {
		<-ctx.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		return nil
	})
	gp.Go(func(_ context.Context) error {
		err := srv.Serve(s.listener)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	return gp.Wait()
}

func (s *server) StartedChecker() healthz.Checker {
	addr := net.JoinHostPort(s.host, strconv.Itoa(s.port))
	return func(req *http.Request) error {
		return netx.IsConnected(req.Context(), "tls", addr, 10*time.Second)
	}
}

func (s *server) WebhookMux() *http.ServeMux {
	return s.mux
}

func (s *server) HostPort() (string, int, error) {
	if s.host == "" || s.port == 0 {
		return "", 0, errors.New("server is not listening on a TCP address")
	}
	return s.host, s.port, nil
}

func (s *server) NotFoundHandler(notFoundHandler http.Handler) {
	s.notFoundHandler = notFoundHandler
}

func (s *server) getNotFoundHandler() http.Handler {
	if s.notFoundHandler == nil {
		return http.NotFoundHandler()
	}
	return s.notFoundHandler
}

func (s *server) getServer() *http.Server {
	nf := s.getNotFoundHandler()

	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h, pattern := s.mux.Handler(r)
		if pattern == "" {
			nf.ServeHTTP(w, r)
			return
		}
		h.ServeHTTP(w, r)
	})

	return newHttpServer(h)
}
