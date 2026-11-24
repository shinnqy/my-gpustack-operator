// Package driver 实现 model-manager 的 CSI Identity 与 Node 服务：对照 API 校验挂载授权，
// 将已发布内容以只读方式挂载给容器，并将缺失内容交给物化器处理。
package driver

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	klog "k8s.io/klog/v2"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/modelmanager/metrics"
	"gpustack.ai/gpustack/pkg/modelmanager/store"
)

// secretTokenKey 是制品 Secret 中存放 Hub 令牌的键名，kubelet 通过 nodePublishSecretRef 传入。
const secretTokenKey = "token"

// Request 表示一次物化请求，包含单次挂载调用所掌握的全部信息。
type Request struct {
	// Hex 是清单摘要的十六进制部分。
	Hex string
	// Artifact 是通过授权校验的已解析 ModelArtifact。
	Artifact *workercore.ModelArtifact
	// Token 是 kubelet 随本次调用传入的凭据，无凭据时为空字符串。
	Token string
}

// Progress 描述挂载调用启动或加入物化任务后的当前状态。
type Progress struct {
	// Published 表示内容已发布，可以立即挂载。
	Published bool
	// Code 和 Message 是内容尚未发布时挂载调用返回的 gRPC 状态码和说明：
	// 物化进行中返回 Aborted，失败退避中返回 Unavailable，磁盘空间不足返回 ResourceExhausted。
	Code    codes.Code
	Message string
}

// Materializer 启动或加入指定摘要的物化任务。
type Materializer interface {
	Ensure(ctx context.Context, req Request) Progress
}

// Events 在摘要新增或移除挂载引用时收到通知，供状态上报与垃圾回收使用。
type Events interface {
	Mounted(hex string)
	Unmounted(hex string)
}

// Mounter 执行 bind mount 与卸载操作。抽象为接口以便无需特权即可测试驱动逻辑；
// 仅 Linux 实现会执行真实内核挂载。
type Mounter interface {
	IsMountPoint(path string) (bool, error)
	// IsReadOnly 报告 path 处的挂载是否为只读且禁用了 set-user-ID 和设备文件。
	IsReadOnly(path string) (bool, error)
	BindReadOnly(source, target string) error
	Unmount(target string) error
}

// Driver 提供 CSI Identity 与 Node 服务。
type Driver struct {
	csi.UnimplementedIdentityServer
	csi.UnimplementedNodeServer

	Name       string
	Version    string
	NodeID     string
	KubeletDir string

	Store *store.Store
	// Artifacts 从 informer 缓存读取 ModelArtifact，绝不在每次挂载调用时直接请求 API Server。
	Artifacts ctrlcli.Reader
	// Ready 报告缓存是否已完成同步且挂载引用已重建；就绪前所有挂载均返回 Unavailable，
	// 避免因缓存尚未同步而误拒合法挂载。
	Ready        func() bool
	Mounter      Mounter
	Materializer Materializer
	Events       Events

	targetsMu sync.Mutex
	targets   map[string]*targetLock // 目标路径到其锁的映射，仅在有调用持有或等待时保留
}

var (
	_ csi.IdentityServer = (*Driver)(nil)
	_ csi.NodeServer     = (*Driver)(nil)
)

// GetPluginInfo 实现 csi.IdentityServer。
func (d *Driver) GetPluginInfo(context.Context, *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{Name: d.Name, VendorVersion: d.Version}, nil
}

// GetPluginCapabilities 实现 csi.IdentityServer。仅运行在节点侧的插件不声明额外能力。
func (d *Driver) GetPluginCapabilities(context.Context, *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	return &csi.GetPluginCapabilitiesResponse{}, nil
}

// Probe 实现 csi.IdentityServer。
func (d *Driver) Probe(context.Context, *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return &csi.ProbeResponse{}, nil
}

// NodeGetInfo 实现 csi.NodeServer。
func (d *Driver) NodeGetInfo(context.Context, *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{NodeId: d.NodeID}, nil
}

// NodeGetCapabilities 实现 csi.NodeServer。内联临时卷不需要 stage 阶段。
func (d *Driver) NodeGetCapabilities(context.Context, *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{}, nil
}

// NodePublishVolume 实现 csi.NodeServer。
//
// kubelet 可能针对同一 target 并发且反复调用并持续重试直到成功，因此本方法必须幂等：
// 已按只读挂载的 target 直接返回成功。节点尚未持有的摘要会交给物化器处理并立即返回可重试状态码，
// 避免在单次 RPC 超时内长时间阻塞。
func (d *Driver) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	target := req.GetTargetPath()
	vc := req.GetVolumeContext()
	switch {
	case req.GetVolumeId() == "" || !isUnder(target, filepath.Join(d.KubeletDir, "pods")):
		return nil, status.Errorf(codes.InvalidArgument, "a volume ID and a target under %s/pods are required", d.KubeletDir)
	case req.GetVolumeCapability().GetMount() == nil:
		return nil, status.Error(codes.InvalidArgument, "only a mount volume is supported")
	case vc[attrEphemeral] != "true":
		return nil, status.Error(codes.InvalidArgument, "only a CSI inline ephemeral volume is supported")
	case d.Ready == nil || !d.Ready():
		return nil, status.Error(codes.Unavailable, "the plugin is starting: its caches have not synced yet")
	}

	unlock := d.lockTarget(target)
	defer unlock()

	namespace, attrs := vc[attrPodNamespace], attributesOf(vc)
	ma, err := d.artifact(ctx, namespace, attrs.Artifact)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "read ModelArtifact %s/%s: %v", namespace, attrs.Artifact, err)
	}
	if rule, msg := authorizeMount(namespace, attrs, ma); rule != "" {
		metrics.Mounts.WithLabelValues(metrics.MountDenied).Inc()
		klog.V(2).InfoS("mount refused", "rule", rule, "pod", namespace+"/"+vc[attrPodName], "volumeID", req.GetVolumeId())
		return nil, status.Error(codes.PermissionDenied, msg)
	}

	mounted, err := d.Mounter.IsMountPoint(target)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "check the target: %v", err)
	}
	if mounted {
		// 只有符合本方法预期的只读挂载才能作为重复调用的成功响应：若残留了因 remount 回滚失败
		// 留下的可写挂载，先卸载并拒绝本次调用，让 kubelet 下次重试重新以只读方式挂载。
		readOnly, err := d.Mounter.IsReadOnly(target)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "check the target's mount: %v", err)
		}
		if readOnly {
			return &csi.NodePublishVolumeResponse{}, nil
		}
		if err := d.Mounter.Unmount(target); err != nil {
			return nil, status.Errorf(codes.Internal, "the target is mounted writable and cannot be unmounted: %v", err)
		}
		return nil, status.Error(codes.Unavailable, "the target was mounted writable and is unmounted; the next call mounts it read-only")
	}

	hex := store.HexOf(attrs.Digest)
	ref := store.Ref{
		VolumeID: req.GetVolumeId(), TargetPath: target, Hex: hex,
		PodNamespace: namespace, PodName: vc[attrPodName], PodUID: vc[attrPodUID],
	}
	// 在 bind mount 之前、在与回收器删除目录树相同的锁保护下记录引用：
	// 目录树在仍处于已发布状态时即被声明持有；若引用无法写入则直接失败，
	// 避免把回收器不知情的目录树提供给容器。
	result := metrics.MountHit
	published, err := d.Store.Claim(ref)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "record the reference: %v", err)
	}
	if !published {
		p := d.Materializer.Ensure(ctx, Request{Hex: hex, Artifact: ma, Token: req.GetSecrets()[secretTokenKey]})
		if !p.Published {
			metrics.Mounts.WithLabelValues(metrics.MountPending).Inc()
			return nil, status.Error(p.Code, p.Message)
		}
		result = metrics.MountMaterialized
		if published, err = d.Store.Claim(ref); err != nil {
			return nil, status.Errorf(codes.Internal, "record the reference: %v", err)
		}
		if !published {
			metrics.Mounts.WithLabelValues(metrics.MountPending).Inc()
			return nil, status.Error(codes.Unavailable, "the published tree was removed before the mount; the next call materializes it again")
		}
	}

	if err := os.MkdirAll(target, 0o750); err != nil {
		d.dropRef(ref)
		return nil, status.Errorf(codes.Internal, "create the target: %v", err)
	}
	if err := d.Mounter.BindReadOnly(d.Store.PublishedTree(hex), target); err != nil {
		d.dropRef(ref)
		return nil, status.Errorf(codes.Internal, "mount: %v", err)
	}
	metrics.Mounts.WithLabelValues(result).Inc()
	if d.Events != nil {
		d.Events.Mounted(hex)
	}

	return &csi.NodePublishVolumeResponse{}, nil
}

// dropRef 撤销未能完成挂载的引用记录，避免目录树因此被长期保留。
func (d *Driver) dropRef(ref store.Ref) {
	if err := d.Store.RemoveRef(ref.VolumeID); err != nil {
		klog.ErrorS(err, "remove the reference of a failed mount", "volumeID", ref.VolumeID)
	}
}

// NodeUnpublishVolume 实现 csi.NodeServer。
//
// 仅凭 target 即可工作，且对从未挂载成功的 target 也返回成功：kubelet 会为所有曾返回可重试错误
// 而被标记为不确定状态的卷在 Pod 退出时调用本方法。
func (d *Driver) NodeUnpublishVolume(_ context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	target := req.GetTargetPath()
	if req.GetVolumeId() == "" || !isUnder(target, filepath.Join(d.KubeletDir, "pods")) {
		return nil, status.Errorf(codes.InvalidArgument, "a volume ID and a target under %s/pods are required", d.KubeletDir)
	}

	unlock := d.lockTarget(target)
	defer unlock()

	mounted, err := d.Mounter.IsMountPoint(target)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, status.Errorf(codes.Internal, "check the target: %v", err)
	}
	if mounted {
		if err := d.Mounter.Unmount(target); err != nil {
			return nil, status.Errorf(codes.Internal, "unmount: %v", err)
		}
	}
	if err := os.Remove(target); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, status.Errorf(codes.Internal, "remove the target: %v", err)
	}

	ref, found, err := d.Store.ReadRef(req.GetVolumeId())
	if err != nil {
		klog.ErrorS(err, "read the reference", "volumeID", req.GetVolumeId())
	}
	if err := d.Store.RemoveRef(req.GetVolumeId()); err != nil {
		klog.ErrorS(err, "remove the reference", "volumeID", req.GetVolumeId())
	}
	// 无论 target 当时是否仍处于挂载状态，移除一条已存在引用就意味着一次使用结束：
	// 因节点重启丢失或被人工清理的挂载在此时之前仍视为在用。
	if found && d.Events != nil {
		d.Events.Unmounted(ref.Hex)
	}

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (d *Driver) artifact(ctx context.Context, namespace, name string) (*workercore.ModelArtifact, error) {
	if namespace == "" || name == "" {
		return nil, nil
	}
	ma := new(workercore.ModelArtifact)
	if err := d.Artifacts.Get(ctx, ctrlcli.ObjectKey{Namespace: namespace, Name: name}, ma); err != nil {
		if kerrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	return ma, nil
}

// targetLock 串行化针对同一目标路径的调用，并统计持有或等待它的调用数。
type targetLock struct {
	mu    sync.Mutex
	users int
}

// lockTarget 串行化针对同一 target 的调用，不同 target 仍并行执行。
// 一旦没有调用持有或等待该 target，立即释放锁项，因为每个 Pod 卷都有独立的 target 路径。
func (d *Driver) lockTarget(target string) func() {
	d.targetsMu.Lock()
	if d.targets == nil {
		d.targets = map[string]*targetLock{}
	}
	l := d.targets[target]
	if l == nil {
		l = &targetLock{}
		d.targets[target] = l
	}
	l.users++
	d.targetsMu.Unlock()

	l.mu.Lock()

	return func() {
		l.mu.Unlock()
		d.targetsMu.Lock()
		defer d.targetsMu.Unlock()
		if l.users--; l.users == 0 {
			delete(d.targets, target)
		}
	}
}

// isUnder 报告路径规范化后 target 是否严格位于 dir 目录之内。
func isUnder(target, dir string) bool {
	if target == "" {
		return false
	}
	rel, err := filepath.Rel(dir, filepath.Clean(target))

	return err == nil && rel != "." && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}
