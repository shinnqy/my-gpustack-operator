package modelmanager

import (
	"context"
	"errors"
	"path/filepath"

	"github.com/spf13/pflag"
	"k8s.io/apimachinery/pkg/runtime"

	"gpustack.ai/gpustack/pkg/manager"
	"gpustack.ai/gpustack/pkg/modelmanager/peer"
	"gpustack.ai/gpustack/pkg/modelstore"
	"gpustack.ai/gpustack/pkg/systemname"
	"gpustack.ai/gpustack/pkg/webserver"
)

// Options 定义 model-manager 命令的命令行参数。
type Options struct {
	// 服务端配置。
	ServerOptions *webserver.Options

	// 管理器配置。
	ManagerOptions *manager.Options

	// 节点本地路径与 CSI 套接字配置。
	NodeName   string
	KubeletDir string
	CacheRoot  string
	CSISocket  string

	// peer 节点间同步配置。
	PeerSyncPort              int
	PeerSyncAuth              string
	PeerSyncServiceAccount    string
	PeerSyncMaxServingStreams int
	PeerSyncStreamsPerSource  int
}

// NewOptions 返回带有默认值的 Options：使用标准节点的 kubelet 目录、缓存根目录和插件安全端口。
func NewOptions() *Options {
	opts := &Options{
		ServerOptions:  webserver.NewOptions(),
		ManagerOptions: manager.NewOptions(),
		KubeletDir:     "/var/lib/kubelet",
		CacheRoot:      "/var/lib/gpustack/models",

		PeerSyncAuth:              string(peer.AuthToken),
		PeerSyncMaxServingStreams: 8,
		PeerSyncStreamsPerSource:  peer.DefaultStreamsPerSource,
	}
	opts.ServerOptions.BindPort = 32444
	opts.ManagerOptions.KubeContentType = runtime.ContentTypeJSON

	return opts
}

// AddFlags 将选项注册到标志集 fs。
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	// 服务端。
	o.ServerOptions.AddFlags(fs, webserver.WithoutBindUnixPath())

	// 管理器。
	o.ManagerOptions.AddFlags(fs, manager.WithoutKubeElectionOptions())

	// 节点参数。
	fs.StringVar(&o.NodeName, "node-name", o.NodeName,
		"the name of the node this plugin runs on, also its NodeModelStore's name.")
	fs.StringVar(&o.KubeletDir, "kubelet-dir", o.KubeletDir,
		"the kubelet's root directory, whose pods directory holds the volume targets.")
	fs.StringVar(&o.CacheRoot, "cache-root", o.CacheRoot,
		"the node's model cache directory.")
	fs.StringVar(&o.CSISocket, "csi-socket", o.CSISocket,
		"the Unix socket the CSI services listen on; empty is <kubelet-dir>/plugins/"+modelstore.DriverName+"/csi.sock.")

	// peer 同步参数。
	fs.IntVar(&o.PeerSyncPort, "peer-sync-port", o.PeerSyncPort,
		"the TCP port the node's published trees are served to the other nodes' plugins on; 0 is off.")
	fs.StringVar(&o.PeerSyncAuth, "peer-sync-auth", o.PeerSyncAuth,
		"how peers authenticate each other: token, the plugins' projected ServiceAccount tokens reviewed")
	fs.StringVar(&o.PeerSyncServiceAccount, "peer-sync-service-account", o.PeerSyncServiceAccount,
		"the plugins' ServiceAccount name, which the token authentication admits.")
	fs.IntVar(&o.PeerSyncMaxServingStreams, "peer-sync-max-serving-streams", o.PeerSyncMaxServingStreams,
		"how many peer file answers this node serves at once.")
	fs.IntVar(&o.PeerSyncStreamsPerSource, "peer-sync-streams-per-source", o.PeerSyncStreamsPerSource,
		"how many requests this node opens to one peer at once.")
}

// Validate 校验命令行参数：必须提供节点名，且所有目录与套接字路径都必须为绝对路径。
func (o *Options) Validate(ctx context.Context) error {
	if err := o.ServerOptions.Validate(ctx); err != nil {
		return err
	}
	if err := o.ManagerOptions.Validate(ctx); err != nil {
		return err
	}
	switch {
	case o.NodeName == "":
		return errors.New("--node-name: required")
	case !filepath.IsAbs(o.KubeletDir):
		return errors.New("--kubelet-dir: must be absolute")
	case !filepath.IsAbs(o.CacheRoot):
		return errors.New("--cache-root: must be absolute")
	case o.CSISocket != "" && !filepath.IsAbs(o.CSISocket):
		return errors.New("--csi-socket: must be absolute")
	}

	_, err := o.PeerSync()

	return err
}

// PeerSync 返回 peer 同步配置；端口为 0 关闭时返回 nil。
func (o *Options) PeerSync() (*peer.Sync, error) {
	if o.PeerSyncPort == 0 {
		return nil, nil
	}
	auth := peer.AuthOptions{
		Mode:           peer.AuthMode(o.PeerSyncAuth),
		Audience:       peer.PeerAudience,
		Namespace:      systemname.NamespaceName,
		ServiceAccount: o.PeerSyncServiceAccount,
	}
	switch auth.Mode {
	case peer.AuthToken:
		if auth.ServiceAccount == "" {
			return nil, errors.New("--peer-sync-service-account: required by --peer-sync-auth=token")
		}
	case peer.AuthNone:
	default:
		return nil, errors.New("--peer-sync-auth: token is the only mode this release serves")
	}
	switch {
	case o.PeerSyncPort < 1 || o.PeerSyncPort > 65535:
		return nil, errors.New("--peer-sync-port: out of range")
	case o.PeerSyncMaxServingStreams < 1 || o.PeerSyncMaxServingStreams > 1024:
		return nil, errors.New("--peer-sync-max-serving-streams: want 1 to 1024")
	case o.PeerSyncStreamsPerSource < 1 || o.PeerSyncStreamsPerSource > 64:
		return nil, errors.New("--peer-sync-streams-per-source: want 1 to 64")
	}

	return &peer.Sync{
		Port:              o.PeerSyncPort,
		Auth:              auth,
		MaxServingStreams: o.PeerSyncMaxServingStreams,
		StreamsPerSource:  o.PeerSyncStreamsPerSource,
	}, nil
}

// Complete 将选项补全为插件运行所需的 Config。
func (o *Options) Complete(ctx context.Context) (*Config, error) {
	srvConfig, err := o.ServerOptions.Complete(ctx)
	if err != nil {
		return nil, err
	}
	mgrConfig, err := o.ManagerOptions.Complete(ctx)
	if err != nil {
		return nil, err
	}
	socket := o.CSISocket
	if socket == "" {
		socket = filepath.Join(o.KubeletDir, "plugins", modelstore.DriverName, "csi.sock")
	}
	peerSync, err := o.PeerSync()
	if err != nil {
		return nil, err
	}

	return &Config{
		ServerConfig:  srvConfig,
		ManagerConfig: mgrConfig,
		NodeName:      o.NodeName,
		KubeletDir:    filepath.Clean(o.KubeletDir),
		CacheRoot:     filepath.Clean(o.CacheRoot),
		CSISocket:     socket,
		PeerSync:      peerSync,
	}, nil
}
