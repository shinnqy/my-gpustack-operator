// Package peer 实现 model-manager 插件的节点间同步：服务端将本节点已发布的目录树提供给其他节点插件，
// 客户端按需从这些 peer 拉取文件并在所有 peer 均不可用时由调用方回退到 Hub。
//
// peer 只以只读、按文件且支持字节范围的方式提供已发布目录树；客户端使用与 Hub 相同的校验流程验证每个字节。
// 插件之间以插件身份相互认证，任何租户凭据都不会经过该链路。
package peer

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/deviceplugin"
	"gpustack.ai/gpustack/pkg/modelstore"
	"gpustack.ai/gpustack/pkg/utils/httpx"
)

// hexPattern 匹配清单摘要的 64 位十六进制部分，也是此处标识目录树的唯一名称。
var hexPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// peerPortName 是 chart 在插件容器上声明的 peer 同步端口名。
const peerPortName = "peer-sync"

// DefaultStreamsPerSource 是未配置更小上限时，本节点对单个 peer 来源允许的最大并发请求数。
const DefaultStreamsPerSource = 4

// Source 表示在拉取方视角下某个已发布目标摘要的邻居节点插件。
type Source struct {
	// NodeName 是运行服务方插件的节点名称。
	NodeName string
	// BaseURL 是 peer 端点的基础地址，形如 "https://<address>:<port>"。
	BaseURL string
	// Client 用于向该来源发送带认证信息的请求。
	Client *http.Client
	// streams 限制发往该来源的并发请求数；为 nil 表示不限并发。
	streams chan struct{}
}

// take 为当前来源申请一个并发请求槽位，并返回请求结束后需调用的释放函数。
func (s *Source) take(ctx context.Context) (func(), error) {
	if s.streams == nil {
		return func() {}, nil
	}
	select {
	case s.streams <- struct{}{}:
		return func() { <-s.streams }, nil
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
}

// DiscoverOptions 定义 peer 发现所需的查询参数。
type DiscoverOptions struct {
	// Namespace 是插件 Pod 所在的 Namespace。
	Namespace string
	// SelfNode 是本插件所在节点名，永远不作为拉取来源。
	SelfNode string
	// Port 是各插件统一监听的 peer 端口。
	Port int
	// StreamsPerSource 限制单个来源的并发请求数。
	StreamsPerSource int
	// Client 用于向所有发现的来源发起请求。
	Client *http.Client
}

// Discover 找出已就绪持有 digest 的其他节点及其插件访问地址，并排除本节点自身。
// NodeModelStore 记录各节点持有的内容，插件 Pod 提供对应端点的 IP 地址与就绪状态。
func Discover(ctx context.Context, reader ctrlcli.Reader, opts DiscoverOptions, digest string) ([]*Source, error) {
	if _, ok := HexOf(digest); !ok {
		return nil, fmt.Errorf("digest %q is not a manifest digest", digest)
	}
	stores := new(workercore.NodeModelStoreList)
	if err := reader.List(ctx, stores); err != nil {
		return nil, fmt.Errorf("list node model stores: %w", err)
	}
	ready := map[string]bool{}
	for i := range stores.Items {
		if stores.Items[i].Name == opts.SelfNode {
			continue
		}
		for _, m := range stores.Items[i].Status.Models {
			if m.Digest == digest && m.State == workercore.NodeModelStoreModelStateReady {
				ready[stores.Items[i].Name] = true
			}
		}
	}
	if len(ready) == 0 {
		return nil, nil
	}

	pods := new(core.PodList)
	if err := reader.List(ctx, pods,
		ctrlcli.InNamespace(opts.Namespace),
		ctrlcli.MatchingLabels{deviceplugin.ComponentLabelKey: modelstore.PluginComponent},
	); err != nil {
		if kerrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("list the model-manager plugins: %w", err)
	}
	var sources []*Source
	for i := range pods.Items {
		p := &pods.Items[i]
		switch {
		case p.DeletionTimestamp != nil, p.Status.PodIP == "", p.Spec.NodeName == "", !isPodReady(p):
			continue
		case !ready[p.Spec.NodeName], !hasPeerPort(p):
			// 未声明 peer 端口的 Pod 属于不提供 peer 监听的旧版插件，不纳入候选来源以免无效拨号。
			continue
		}
		s := &Source{
			NodeName: p.Spec.NodeName,
			BaseURL:  (&url.URL{Scheme: "https", Host: p.Status.PodIP + ":" + fmt.Sprint(opts.Port)}).String(),
			Client:   opts.Client,
		}
		if opts.StreamsPerSource > 0 {
			s.streams = make(chan struct{}, opts.StreamsPerSource)
		}
		sources = append(sources, s)
	}
	ctrllog.FromContext(ctx).V(1).Info("discovered peer sources", "digest", digest, "sources", len(sources))

	return sources, nil
}

// HexOf 校验 digest 格式并返回其十六进制部分。
func HexOf(digest string) (string, bool) {
	h, ok := strings.CutPrefix(digest, "sha256:")

	return h, ok && hexPattern.MatchString(h)
}

// isPodReady 报告插件 Pod 当前是否处于 Ready 状态。
func isPodReady(p *core.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == core.PodReady && c.Status == core.ConditionTrue {
			return true
		}
	}

	return false
}

// hasPeerPort 报告插件 Pod 是否声明了 peer 端口：未声明该端口的旧版插件没有启动对应监听。
func hasPeerPort(p *core.Pod) bool {
	for _, c := range p.Spec.Containers {
		for _, port := range c.Ports {
			if port.Name == peerPortName {
				return true
			}
		}
	}

	return false
}

// requestTimeout 限制单次 peer 请求耗时：超时未响应的 peer 视为失败来源，转由下一个 peer 或 Hub 处理。
const requestTimeout = 2 * time.Minute

// NewClient 构建所有 peer 来源共用的 HTTP 客户端并挂载拉取方认证：
// 禁用代理（Pod 间流量不得经由外部代理），并接受自签名服务端证书。
func NewClient(auth AuthOptions) (*http.Client, error) {
	tlsCfg, err := auth.ClientTLSConfig()
	if err != nil {
		return nil, err
	}
	transport := httpx.Transport(httpx.TransportOptions().WithoutProxy().WithTLSClientConfig(tlsCfg))

	return &http.Client{Transport: auth.RoundTripper(transport), Timeout: requestTimeout}, nil
}

// Sync 是单个节点运行 peer 同步的配置：既控制对外提供服务，也控制向其他节点拉取。
type Sync struct {
	// Port 是 peer 端点监听的 TCP 端口；0 表示关闭。
	Port int
	// Auth 是 peer 间的认证配置。
	Auth AuthOptions
	// MaxServingStreams 是本节点同时对外服务的文件响应上限。
	MaxServingStreams int
	// StreamsPerSource 是本节点向单个 peer 来源同时发起的请求上限。
	StreamsPerSource int
}
