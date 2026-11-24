package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr/funcr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/websocket"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/rest"

	worker "gpustack.ai/gpustack/api/worker/v1"
	"gpustack.ai/gpustack/pkg/utils/events/topic"
	"gpustack.ai/gpustack/pkg/workergateway/manager"
	"gpustack.ai/gpustack/pkg/workergateway/service"
)

type disconnectedClient struct {
	http.ResponseWriter
	ready chan struct{}
}

func (c *disconnectedClient) Flush() {
	select {
	case c.ready <- struct{}{}:
	default:
	}
}

func (*disconnectedClient) Write([]byte) (int, error) {
	return 0, errors.New("client disconnected during write")
}

func TestHTTPGatewayReleasesInterruptedEvent(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	svc, err := service.New(ctx, nil)
	require.NoError(t, err)
	// 写客户端和写管道会得到不同的错误。只有编码器的阻塞写返回后，网关才会记录管道错误。
	released := make(chan struct{}, 1)
	svc.Logger = funcr.New(func(_, message string) {
		if strings.Contains(message, io.ErrClosedPipe.Error()) {
			select {
			case released <- struct{}{}:
			default:
			}
		}
	}, funcr.Options{})
	w := &disconnectedClient{ResponseWriter: httptest.NewRecorder(), ready: make(chan struct{}, 1)}
	r := httptest.NewRequestWithContext(ctx, http.MethodGet, "/instances?watch=true", nil)
	done := make(chan struct{})
	go func() {
		defer close(done)
		svc.Index().ServeHTTP(w, r)
	}()
	select {
	case <-w.ready:
	case <-ctx.Done():
		t.Fatal("Watch 尚未开始")
	}
	item := instance("lab", "chat", "1")
	item.Annotations = map[string]string{"payload": strings.Repeat("x", 128<<10)}
	err = topic.Publish(ctx, manager.WorkerEventTopic(worker.SchemeGroupVersionKind("Instance")),
		&manager.WorkerEvent{Type: watch.Modified, Cluster: "edge-a", Object: &item})
	require.NoError(t, err)
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("客户端写入失败后处理函数未退出")
	}
	select {
	case <-released:
	case <-ctx.Done():
		t.Fatal("编码器仍未收到管道关闭的错误")
	}
}

// WebSocket 空闲时关闭连接也应结束 handler，无须等下一条集群事件。
func TestHTTPGatewayClosesIdleWebSocket(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	svc, err := service.New(ctx, nil)
	require.NoError(t, err)
	routes := svc.Index()
	finished := make(chan struct{})
	gw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(finished)
		routes.ServeHTTP(w, r)
	}))
	t.Cleanup(func() { cancel(); gw.Close() })
	ws, err := websocket.Dial("ws"+strings.TrimPrefix(gw.URL, "http")+"/instances?watch=true", "binary.k8s.io", gw.URL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ws.Close() })
	require.NoError(t, ws.SetDeadline(time.Now().Add(3*time.Second)))
	var ping []byte
	require.NoError(t, websocket.Message.Receive(ws, &ping))
	require.Empty(t, ping)
	require.NoError(t, ws.Close())
	select {
	case <-finished:
	case <-time.After(3 * time.Second):
		t.Fatal("客户端已断开，网关仍在等待事件")
	}
}

// 每个测试端点有自己的库存、凭据和 Watch。请求由网关的生产 Kubernetes client 发出。
type workerAPI struct {
	*httptest.Server
	events   chan worker.Instance
	labLists atomic.Int32
	watches  atomic.Int32
}

func instance(namespace, name, rv string) worker.Instance {
	return worker.Instance{
		TypeMeta:   meta.TypeMeta{APIVersion: "worker.gpustack.ai/v1", Kind: "Instance"},
		ObjectMeta: meta.ObjectMeta{Namespace: namespace, Name: name, ResourceVersion: rv},
	}
}

func newWorkerAPI(t *testing.T, token string) *workerAPI {
	t.Helper()
	api := &workerAPI{events: make(chan worker.Instance, 4)}
	api.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			http.Error(w, "unexpected test credential", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		switch r.URL.Path {
		case "/apis/apiregistration.k8s.io/v1/apiservices/v1.worker.gpustack.ai",
			"/apis/apiregistration.k8s.io/v1/apiservices/v1.gpustack.ai":
			_, _ = w.Write([]byte(`{"apiVersion":"apiregistration.k8s.io/v1","kind":"APIService","status":{"conditions":[{"type":"Available","status":"True"}]}}`))
		case "/apis/worker.gpustack.ai/v1/instancetypeflavors":
			_, _ = w.Write([]byte(`{"apiVersion":"worker.gpustack.ai/v1","kind":"InstanceTypeFlavorList","items":[{"metadata":{"name":"same-hardware"},"spec":{"acceleratable":true,"acceleratorGroup":"nvidia-a10g"}}]}`))
		case "/apis/worker.gpustack.ai/v1/instances", "/apis/worker.gpustack.ai/v1/namespaces/lab/instances":
			items := []worker.Instance{instance("lab", "chat", "1")}
			if strings.Contains(r.URL.Path, "/namespaces/lab/") {
				api.labLists.Add(1)
			} else {
				items = append(items, instance("other", "private", "1"))
			}
			if r.URL.Query().Get("watch") != "true" {
				_ = enc.Encode(worker.InstanceList{
					TypeMeta: meta.TypeMeta{APIVersion: "worker.gpustack.ai/v1", Kind: "InstanceList"},
					ListMeta: meta.ListMeta{ResourceVersion: "1"}, Items: items,
				})
				return
			}
			api.watches.Add(1)
			flusher := w.(http.Flusher)
			writeEvent := func(kind watch.EventType, item worker.Instance) {
				_ = enc.Encode(map[string]any{"type": kind, "object": &item})
				flusher.Flush()
			}
			w.WriteHeader(http.StatusOK)
			flusher.Flush()
			// 新版 client-go 也可以用带初始事件的 Watch 建缓存，结束书签标明初始清单已经发送完。
			if r.URL.Query().Get("sendInitialEvents") == "true" {
				for _, item := range items {
					writeEvent(watch.Added, item)
				}
				bookmark := instance("", "", "1")
				bookmark.Annotations = map[string]string{meta.InitialEventsAnnotationKey: "true"}
				writeEvent(watch.Bookmark, bookmark)
			}
			for {
				select {
				case <-r.Context().Done():
					return
				case item := <-api.events:
					writeEvent(watch.Modified, item)
				}
			}
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(api.Close)
	return api
}

func TestHTTPGatewayListsWatchesAndUnsubscribes(t *testing.T) {
	a := newWorkerAPI(t, "token-a")
	b := newWorkerAPI(t, "token-b")
	upstreams := map[string]string{"edge-a": a.URL, "edge-b": b.URL}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	mgr, err := manager.New(ctx, &manager.Config{
		ConstructRestConfig: func(cluster, token string) (*rest.Config, error) {
			host, ok := upstreams[cluster]
			if !ok {
				return nil, fmt.Errorf("unknown test cluster %q", cluster)
			}
			return &rest.Config{Host: host, BearerToken: token, ContentConfig: rest.ContentConfig{ContentType: "application/json"}}, nil
		},
	})
	require.NoError(t, err)
	svc, err := service.New(ctx, mgr)
	require.NoError(t, err)
	gateway := httptest.NewServer(http.StripPrefix("/apis", svc.Index()))
	t.Cleanup(gateway.Close)
	client := gateway.Client()

	workers := func(method, query, body string) {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, method, gateway.URL+"/apis/workers?"+query, strings.NewReader(body))
		require.NoError(t, err)
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := client.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
	}
	list := func(path string, dst any) error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, gateway.URL+"/apis/"+path, nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("GET %s: %s", path, resp.Status)
		}
		return json.NewDecoder(resp.Body).Decode(dst)
	}
	const gvk = "worker.gpustack.ai/v1/Instance"
	workers(http.MethodPost, url.Values{"cluster": {"edge-a"}, "gvk": {gvk, gvk}}.Encode(), `{"token":"token-a"}`)
	workers(http.MethodPost, "", `{"clusters":"edge-b","token":"token-b"}`)

	const both = "instances?cluster=edge-a&cluster=edge-b&namespace=lab"
	require.Eventually(t, func() bool {
		var got service.ClusterInstanceList
		return list(both, &got) == nil && len(got.Items) == 2
	}, 8*time.Second, 20*time.Millisecond)
	var got service.ClusterInstanceList
	require.NoError(t, list(both, &got))
	require.Len(t, got.Items, 2)
	assert.Equal(t, []string{"edge-a", "edge-b"}, []string{got.Items[0].Cluster, got.Items[1].Cluster})
	for _, item := range got.Items {
		assert.Equal(t, "chat", item.Name)
		assert.Equal(t, "lab", item.Namespace)
	}
	assert.Zero(t, a.labLists.Load())
	assert.Positive(t, b.labLists.Load())
	assert.EqualValues(t, 1, a.watches.Load())
	assert.Zero(t, b.watches.Load())

	var flavors service.AggregatedInstanceTypeFlavorList
	require.NoError(t, list("instancetypeflavors?aggregated=true", &flavors))
	require.Len(t, flavors.Items, 1)
	assert.ElementsMatch(t, []string{"edge-a", "edge-b"}, flavors.Items[0].Spec.Clusters)

	// 收到响应头意味着 HTTP 服务已经订阅事件。先送入另一个命名空间的事件，再送入目标事件。
	watchCtx, stopWatch := context.WithTimeout(ctx, 8*time.Second)
	defer stopWatch()
	req, err := http.NewRequestWithContext(watchCtx, http.MethodGet, gateway.URL+"/apis/instances?cluster=edge-a&namespace=lab&watch=true", nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	a.events <- instance("other", "private", "2")
	a.events <- instance("lab", "chat", "3")
	var event struct {
		Type    watch.EventType
		Cluster string
		Object  *service.ClusterInstance
	}
	dec := json.NewDecoder(resp.Body)
	require.NoError(t, dec.Decode(&event))
	require.Equal(t, watch.Modified, event.Type)
	require.Equal(t, "edge-a", event.Cluster)
	require.NotNil(t, event.Object)
	assert.Equal(t, "chat", event.Object.Name)
	assert.Equal(t, "3", event.Object.ResourceVersion)

	workers(http.MethodDelete, "cluster=edge-a", "")
	require.NoError(t, dec.Decode(&event))
	assert.Equal(t, watch.Deleted, event.Type)
	assert.Equal(t, "edge-a", event.Cluster)
	assert.Nil(t, event.Object)
	require.NoError(t, list(both, &got))
	require.Len(t, got.Items, 1)
	assert.Equal(t, "edge-b", got.Items[0].Cluster)
}
