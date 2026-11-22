package kuberess

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"

	kerrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	"gpustack.ai/gpustack/pkg/kubeapp"
	"gpustack.ai/gpustack/pkg/kubeclients/kubernetes"
	"gpustack.ai/gpustack/pkg/system"
	"gpustack.ai/gpustack/pkg/utils/osx"
	"gpustack.ai/gpustack/pkg/worker/settings"
)

// applicationWildcard 用于一次性禁用所有应用。它由 kubeapp.ExecuteInstall 使用，
// 当该通配符存在时将完全跳过安装。
const applicationWildcard = "*"

// applicationValuesKeys 将 --disable-applications 接受的每个名称映射到用于关闭对应组件的
// chart values 键。它是权威的名称集合：worker 会对照该集合校验参数，
// overlay 也会为每个条目渲染一个开关。
//
// gpustack-cpu-info NodeFeatureRule 没有对应条目，因为它没有开关：
// 调度链从该规则开始，没有它的 release 将无法对任何节点进行分类。
var applicationValuesKeys = map[string]string{
	"kueue":                  "kueue",
	"node-feature-discovery": "node-feature-discovery",
	"csi-driver-nfs":         "csi-driver-nfs",
	"csi-driver-s3":          "csi-driver-s3",
	"device-manager":         "deviceManager",
	"model-manager":          "modelManager",
}

// ApplicationNames 返回 --disable-applications 接受的所有名称（已排序）：
// 包含通配符以及每个可安装组件对应的名称。
func ApplicationNames() []string {
	names := make([]string, 0, len(applicationValuesKeys)+1)
	names = append(names, applicationWildcard)
	for name := range applicationValuesKeys {
		names = append(names, name)
	}
	slices.Sort(names)

	return names
}

// componentSwitches 将每个组件的 chart values 键映射到在给定的禁用集合下
// 该组件是否保持启用。
func componentSwitches(disable sets.Set[string]) map[string]bool {
	switches := make(map[string]bool, len(applicationValuesKeys))
	for name, key := range applicationValuesKeys {
		switches[key] = !disable.Has(name)
	}

	return switches
}

// installs 是应用安装器列表。
var installs = []kubeapp.Install{
	installGPUStackOperator,
}

// applicationInstallLockName 是所有 worker 副本用来串行化应用安装的 Lease 名称。
//
// 安装在 Prepare 阶段执行，早于 leader 选举的任何控制，因此每个副本都会执行到这里，
// 即使单副本下的滚动更新也会有两个副本并发重叠。Helm 自身的 release 存储只是单次
// 比较并创建（compare-and-create），而非互斥锁：竞争失败方会根据执行阶段落入多种错误之一，
// 且两个 Helm 操作同时应用同一批对象可能会导致 release 卡在后续无法恢复的状态。
const applicationInstallLockName = "applications.worker.gpustack.ai"

// InstallApplications 安装应用，每次仅允许一个副本执行。
func InstallApplications(ctx context.Context, manufacturers []string) error {
	gvc := map[string]any{
		"ContainerRegistry":  settings.ContainerRegistry.ShouldValueFromRemote(ctx),
		"ContainerNamespace": settings.ContainerNamespace.ShouldValueFromRemote(ctx),
		"ImagePullSecrets": func() []string {
			v := settings.ImagePullSecrets.ShouldValueFromRemote(ctx)
			if v == "" {
				return nil
			}
			return strings.Split(v, ",")
		}(),
		"ImagePullPolicy": settings.ImagePullPolicy.ShouldValueFromRemote(ctx),
		"Manufacturers":   manufacturers,
	}

	disable := system.DisableApplications.Get()
	install := func(ctx context.Context, exclusive bool) error {
		return kubeapp.ExecuteInstall(
			ctx,
			system.LoopbackKubeRestConfig.Get(),
			disable,
			SystemNamespaceName,
			installs,
			gvc,
			exclusive,
		)
	}

	// 当通过 chart 部署 worker 时无需串行化：遇到通配符时安装流程会直接返回而不做任何操作，
	// 若此时获取锁反而会遗留一个多余的 Lease。
	if disable.Has(applicationWildcard) {
		return install(ctx, false)
	}

	lock := kubeapp.Lock{
		Leases: system.LoopbackKubeClient.Get().CoordinationV1().Leases(SystemNamespaceName),
		Name:   applicationInstallLockName,
		Holder: applicationInstallLockHolder(),
	}

	return lock.Do(ctx, func(ctx context.Context, predecessor string) error {
		return install(ctx, predecessorHasStopped(ctx, system.LoopbackKubeClient.Get(), predecessor))
	})
}

// podHolderPrefix 将锁持有者标识标记为系统 Namespace 中的 Pod 名称，
// 从而允许其他副本查询持有者状态。另一种形式标识集群外部的进程，其他副本无法对其进行检查。
const podHolderPrefix = "pod/"

// applicationInstallLockHolder 用于向其他副本标识当前副本。
//
// 在 Pod 中它是 Pod 名称，每个副本唯一，并且作为可被其他副本读取的对象，
// 能让已消失的持有者被识别出来。在 Pod 之外则是主机名加上当前进程 ID：
// 仅主机名会被运行在同一主机上的所有 worker 共享，导致各副本都将该声明误认为自身所有而各自拿锁。
func applicationInstallLockHolder() string {
	if name := osx.Getenv("KUBERNETES_POD_NAME"); name != "" {
		return podHolderPrefix + name
	}
	host, _ := os.Hostname()

	return fmt.Sprintf("process/%s-%d", host, os.Getpid())
}

// predecessorHasStopped 返回上次持有安装锁的进程是否已知停止，
// 这是判断是否可以安全修复其操作中途遗留的 release 的依据。
//
// 空的 predecessor 表示锁原本空闲而非被接管，且锁仅在所保护的调用自行结束时才会释放，
// 因此其持有者已完成执行。相反，具名的 predecessor 只是失去了响应，但无响应不等于已停止：
// 它可能只是与 API 连接停滞但仍在继续执行 Helm apply 的副本。其指定的 Pod 可以给出答案：
// 已消失或容器不再运行的 Pod 不会留下存活进程。当前副本无法查询到的持有者按仍在运行处理，
// 因为一旦误判就会导致两个 Helm 操作同时作用于同一个 release。
func predecessorHasStopped(ctx context.Context, cli kubernetes.Interface, predecessor string) bool {
	if predecessor == "" {
		return true
	}

	name, ok := strings.CutPrefix(predecessor, podHolderPrefix)
	if !ok {
		return false
	}

	// 直接透传读取 API server 而非 watch 缓存：缓存中的 Pod 可能滞后数秒，
	// 而“未在运行”是授权执行修复的判定依据。
	pod, err := cli.CoreV1().Pods(SystemNamespaceName).Get(ctx, name, meta.GetOptions{})
	if kerrors.IsNotFound(err) {
		return true
	}
	if err != nil {
		return false
	}

	// 容器状态尚未上报的 Pod 无法确定是否停止，若将空切片视作“已停止”
	// 就会在没有任何依据的情况下触发修复。
	if len(pod.Status.ContainerStatuses) == 0 {
		return false
	}

	for i := range pod.Status.ContainerStatuses {
		if pod.Status.ContainerStatuses[i].State.Running != nil {
			return false
		}
	}

	return true
}
