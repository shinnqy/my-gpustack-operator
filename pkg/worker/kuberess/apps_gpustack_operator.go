package kuberess

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	conregname "github.com/google/go-containerregistry/pkg/name"
	helmdriver "helm.sh/helm/v3/pkg/storage/driver"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	klog "k8s.io/klog/v2"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubeapp/helm"
	"gpustack.ai/gpustack/pkg/kubeclients/kubernetes"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/system"
	"gpustack.ai/gpustack/pkg/utils/osx"
	"gpustack.ai/gpustack/pkg/utils/version"
)

const (
	// gpustackOperatorChartName 是 operator 自身 chart 的名称。
	gpustackOperatorChartName = "gpustack-operator"
	// gpustackOperatorReleaseName 是 worker 安装内置 chart 时使用的 release 名称。
	// 尽管该 release 现已涵盖所有内置应用，但它沿用了早期版本为仅包含 device-manager 的
	// release 所起的名称，从而使已有集群无需进行 release 迁移。
	gpustackOperatorReleaseName = "gpustack-operator-device-manager"
)

// legacyApplicationReleases 是早期版本在应用成为 operator 自身 chart 的 subchart 之前
// 按应用分别安装的 Helm release 列表。它们的存在是判定需要迁移的明确依据。
var legacyApplicationReleases = []string{
	"gpustack-kueue",
	"gpustack-node-feature-discovery",
	"gpustack-csi-driver-nfs",
	"gpustack-csi-driver-s3",
}

// installGPUStackOperator 安装 operator 自身的 chart，其中除了 device-manager 外，
// 还将 Kueue、Node Feature Discovery 和 CSI 驱动作为 subchart 打包在内。
//
// 这是 worker 的自包含安装路径：由于没有外部 chart 驱动 worker，它会使用基于自身配置和参数
// 计算出的 overlay 来安装打包在自身镜像中的 chart（参见 pack/gpustack-operator/Dockerfile）。
// 当已有 chart 部署 worker 时，--disable-applications 会携带通配符，此路径不会运行。
func installGPUStackOperator(
	ctx context.Context,
	helmCli *helm.Client,
	globalValuesContext map[string]any,
	disable sets.Set[string],
	exclusive bool,
) error {
	chartVersion := gpustackOperatorChartVersion()
	path, err := gpustackOperatorChartPath(chartVersion)
	if err != nil {
		return err
	}

	// 复用正在运行的 worker 所使用的精确镜像，以确保 device-manager 始终与当前使用的 operator 一致；
	// 无法确定时回退到由 chart 拼接的镜像。
	imageRepository, imageTag := splitImageReference(extractWorkerImage(ctx, helmCli.KubeClientSet()))

	// 重申 worker 的 --manufacturer 列表中每个厂商的标识信息，使 chart 基于当前生效的值
	// 而非自身默认值来渲染 device-manager 和 Kueue 的 credits 映射。
	manufacturers, _ := globalValuesContext["Manufacturers"].([]string)
	manufacturerIdentities := make(map[string]map[string]string, len(manufacturers))
	for _, m := range manufacturers {
		manufacturerIdentities[m] = manufacturerIdentity(m)
	}

	valuesContext := globalValuesContext
	valuesContext["Release"] = gpustackOperatorReleaseName
	valuesContext["Namespace"] = helmCli.DefaultNamespace()
	valuesContext["ImageRepository"] = imageRepository
	valuesContext["ImageTag"] = imageTag
	valuesContext["ManufacturerIdentities"] = manufacturerIdentities
	valuesContext["ComponentSwitches"] = componentSwitches(disable)

	values := getGPUStackOperatorChartTemplateValues(gpustackOperatorChartName, valuesContext)

	takeOwnership, err := hasLegacyApplicationRelease(ctx, helmCli.KubeClientSet(), helmCli.DefaultNamespace())
	if err != nil {
		return fmt.Errorf("detect legacy application releases: %w", err)
	}
	if takeOwnership {
		klog.Info("adopting the objects of the legacy per-application releases")
	}

	chart := &helm.Chart{
		Name:    gpustackOperatorChartName,
		Version: chartVersion,
		Release: gpustackOperatorReleaseName,
		Path:    path,
		Values:  values,
		// 需要安装内置 subchart（即 Node Feature Discovery）的 CRD：
		// 没有其他地方提供它们，且父 chart 的 NodeFeatureRule 依赖它们。Helm 会跳过已存在的 CRD，
		// 因此自带 NFD 的集群不会受影响。Kueue 通过模板渲染其 CRD，因此无论如何都会随 release 一同下发。
		//
		// 修复失败的 release 时仅使用 upgrade，绝不采用卸载后重装（uninstall+install）路径：
		// 该 release 现在拥有 Kueue，其 CRD 是受 Helm 管理的模板且自定义资源带有控制器 finalizer，
		// 卸载会在 finalizer 仍锁定 CR 的情况下删掉控制器，从而导致 CRD 被滞留卡死。
		RepairViaUpgradeOnly: true,
		TakeOwnership:        takeOwnership,
		// 仅在 InstallApplications 获取了前任已释放的 Lease 时才设置，因此此处发现的
		// pending release 记录属于已结束或已终止的副本，而非仍在运行的副本。
		// 修复此类记录可以解除后续任何尝试都无法自行恢复的卡死状态。
		ExclusiveAccess: exclusive,
	}

	return installConvergedChart(ctx, helmCli, chart)
}

// manufacturerIdentity 以 chart 的 global.manufacturers 映射中一行的形式返回厂商标识，
// 携带当前进程中生效的值——每个值均源自可能已被 GPUSTACK_<MANUFACTURER>_* 环境变量
// 覆盖的 pkg/nodefeature 默认值。
//
// operator 没有对应值的字段会被省略，runtimeName 亦然：pkg/nodefeature 为比 chart
// 创建的 RuntimeClass 多出两个的厂商定义了运行时名称，而 Helm 会按行将这些值合并进
// chart 的默认值中，若在此处显式给出，就会创建出 chart 模式安装从不创建的 RuntimeClass。
func manufacturerIdentity(manufacturer string) map[string]string {
	identity := map[string]string{
		"pciVendorID": nodefeature.GetPciVendorID(manufacturer),
		"resourceName": string(nodefeature.GetAcceleratableResourceName(
			manufacturer, workercore.DeviceAllocationModeExclusive,
		)),
	}
	if partitionKind := nodefeature.GetPartitionKind(manufacturer); partitionKind != "" {
		identity["partitionKind"] = partitionKind
	}

	return identity
}

const (
	// heldReleaseRetries 限制因竞争输给并发启动的副本而触发的安装重试次数上限，
	// 超过该次数则启动失败。
	heldReleaseRetries = 5
	// heldReleaseRetryInterval 是上述重试之间的等待间隔。该间隔特意采用固定值：
	// 等待的是对等副本自身的安装完成，其耗时与当前进程检查了多少次无关。
	heldReleaseRetryInterval = 10 * time.Second
)

// installConvergedChart 安装该 chart，并容忍并发启动的副本在当前进程查询 release 与
// 执行自身安装之间的时间窗口内创建 release：只有一个副本能创建成功，竞争失败方不应导致启动失败。
//
// 每次重试都会重新查询，此时会发现胜出方的 release，进而要么将其视为已按当前 values 收敛并接受，
// 要么等待其 pending 中的安装完成。
func installConvergedChart(ctx context.Context, helmCli *helm.Client, chart *helm.Chart) error {
	for attempt := 0; ; attempt++ {
		_, err := helmCli.Install(ctx, chart)
		if err == nil || !isReleaseHeldByPeer(err) || attempt == heldReleaseRetries {
			return err
		}

		klog.InfoS("release is held by a peer, converging on it",
			"release", chart.Release, "attempt", attempt+1, "err", err)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(heldReleaseRetryInterval):
		}
	}
}

// isReleaseHeldByPeer 返回错误是否表示 Helm 因另一个进程正在操作同一 release 而拒绝执行。
// Helm 通过三种方式发出该信号，分别对应竞争失败方可能触发的三个时机：
//
//   - 写入任何内容之前的名称查重
//     (helm.sh/helm/v3/pkg/action.(*Install).availableName)；
//   - 创建 revision 记录时，这是整个流程中唯一真正的 compare-and-create 操作
//     (helm.sh/helm/v3/pkg/storage/driver.ErrReleaseExists)；
//   - upgrade 时发现 release 已处于 pending 状态 (helm.sh/helm/v3/pkg/action.errPending)。
//
// 只有第二种携带哨兵错误（sentinel error）；其余两种均为普通错误，因此错误信息是唯一判断依据。
func isReleaseHeldByPeer(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, helmdriver.ErrReleaseExists) {
		return true
	}

	msg := err.Error()

	return strings.Contains(msg, "cannot re-use a name that is still in use") ||
		strings.Contains(msg, "another operation (install/upgrade/rollback) is in progress")
}

// hasLegacyApplicationRelease 返回 Namespace 中是否仍记录有采用 subchart 布局之前的
// 按应用安装的旧版 Helm release。
//
// 它用于控制 TakeOwnership，后者会按名称、Namespace 和类型接管渲染出的所有现存对象——
// 它无法区分旧版 release 创建的对象与用户手动创建的对象。因此，仅当集群中明确存在
// 待迁移的 release 时才启用接管，绝不无条件启用。
func hasLegacyApplicationRelease(ctx context.Context, cli kubernetes.Interface, namespace string) (bool, error) {
	// Helm 将 release 记录为带有 owner 和 release 名称标签的 Secret。
	sel := fmt.Sprintf("owner=helm,name in (%s)", strings.Join(legacyApplicationReleases, ","))
	list, err := cli.CoreV1().Secrets(namespace).List(ctx, meta.ListOptions{
		LabelSelector: sel,
		Limit:         1,
	})
	if err != nil {
		return false, fmt.Errorf("list helm release secrets: %w", err)
	}

	return len(list.Items) > 0, nil
}

// gpustackOperatorChartTemplate 为 worker 自身的安装流程渲染 operator chart 的 values：
// worker 本身已经在运行，因此只部署各应用，并由 --disable-applications 分别控制开关。
// fullnameOverride 会固定资源名称，使其不受 release 名称影响。
//
// 拉取策略设置在 global.imagePullPolicy 上，这样也能传递给 subchart；
// 父 chart 自身的 image.pullPolicy 不会传递到 subchart。
const gpustackOperatorChartTemplate = `
{{- if or $.ContainerRegistry (not $.ImageRepository) $.ImagePullPolicy $.ImagePullSecrets $.ManufacturerIdentities }}
global:
  {{- /*
    The registry goes out whatever the worker's own image turns out to be, because it is the
    subcharts' only route to a mirror: image mode has no user-values channel, so a registry
    withheld here means every subchart pulling from wherever its reference already points.
    Rewriting the pinned operator image's registry with it is a no-op in any deployment that
    has a mirror at all — that image came from it.

    The namespace cannot travel the same way. It replaces the namespace segment of every
    reference, the operator image this overlay just pinned to the running worker's included,
    and the two need not agree: a packaged build puts the operator in its own namespace while
    the mirrored dependencies stay in the default one. Rewriting it there points the
    device-managers and the hook Jobs at an image that does not exist, which fails the install
    on a pull it can never satisfy. So it is emitted only where no worker image was resolved
    and nothing is at risk of being rewritten.
  */}}
  {{- with $.ContainerRegistry }}
  imageRegistry: {{ . | quote }}
  {{- end }}
  {{- if not $.ImageRepository }}
  imageNamespace: {{ default "" $.ContainerNamespace | quote }}
  {{- end }}
  {{- if $.ImagePullPolicy }}
  imagePullPolicy: {{ $.ImagePullPolicy | quote }}
  {{- end }}
  {{- if $.ImagePullSecrets }}
  imagePullSecrets:
  {{- range $.ImagePullSecrets }}
    - name: {{ . }}
  {{- end }}
  {{- end }}
  {{- if $.ManufacturerIdentities }}
  manufacturers:
  {{- range $manu, $identity := $.ManufacturerIdentities }}
    {{ $manu }}:
    {{- range $field, $value := $identity }}
      {{ $field }}: {{ $value | quote }}
    {{- end }}
  {{- end }}
  {{- end }}
{{- end }}

fullnameOverride: gpustack-operator
{{- if $.ImageRepository }}

# The chart's own image knob, not deviceManager's: every component that runs this image reads it
# through the same helper, which merges a per-component override over this one. Setting it here
# reaches the device-managers AND the migration hook Jobs, which run this image too and would
# otherwise resolve the chart's default tag — a tag that need not exist wherever this build came
# from, leaving the hook to fail the install on a pull it can never satisfy.
image:
  repository: {{ $.ImageRepository | quote }}
  {{- if $.ImageTag }}
  tag: {{ $.ImageTag | quote }}
  {{- end }}
{{- end }}

worker:
  enabled: false

deviceManager:
  enabled: {{ index $.ComponentSwitches "deviceManager" }}

modelManager:
  enabled: {{ index $.ComponentSwitches "modelManager" }}

kueue:
  enabled: {{ index $.ComponentSwitches "kueue" }}

node-feature-discovery:
  enabled: {{ index $.ComponentSwitches "node-feature-discovery" }}

csi-driver-nfs:
  enabled: {{ index $.ComponentSwitches "csi-driver-nfs" }}

csi-driver-s3:
  enabled: {{ index $.ComponentSwitches "csi-driver-s3" }}
`

func getGPUStackOperatorChartTemplateValues(name string, data map[string]any) helm.TemplateValues {
	return helm.TemplateValues{
		Application: name,
		Template:    gpustackOperatorChartTemplate,
		Context:     data,
	}
}

// gpustackOperatorChartPath 查找打包在 operator 镜像中的 chart 路径，
// 若不存在则回退到源码树路径，以便 worker 也能直接从代码仓库运行。
func gpustackOperatorChartPath(chartVersion string) (string, error) {
	path := filepath.Join(system.SubConfDir("charts"),
		fmt.Sprintf("%s-%s.tgz", gpustackOperatorChartName, chartVersion))
	if osx.ExistsFile(path) {
		return path, nil
	}

	dir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("get working directory: %w", err)
	}
	path = filepath.Join(dir, "deploy", gpustackOperatorChartName, "chart")
	if !osx.ExistsDir(path) {
		return "", fmt.Errorf("chart not found at %s", path)
	}

	return path, nil
}

// extractWorkerImage 返回正在运行的 worker 的 "main" 容器所使用的镜像；
// 当无法确定时（例如在集群外运行）返回空字符串。
func extractWorkerImage(ctx context.Context, cli kubernetes.Interface) string {
	podName := osx.Getenv("KUBERNETES_POD_NAME")
	if podName == "" {
		return ""
	}

	pod, err := cli.CoreV1().
		Pods(SystemNamespaceName).
		Get(ctx, podName, meta.GetOptions{ResourceVersion: "0"})
	if err != nil {
		return ""
	}

	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == "main" {
			return pod.Spec.Containers[i].Image
		}
	}
	if len(pod.Spec.Containers) > 0 {
		return pod.Spec.Containers[0].Image
	}
	return ""
}

// splitImageReference 将带 tag 的镜像引用拆分为 chart 的 "image" values 所识别的 repository 和 tag。
// 对于无法表示为 "<repository>:<tag>" 的引用——例如通过 digest 固定的引用（"<repo>@sha256:..."）
// 或其他无法解析的引用——返回空字段，让 chart 自行拼接默认镜像，而不是生成无效的 "<repo>@<digest>:<tag>"。
func splitImageReference(ref string) (repository, tag string) {
	if ref == "" {
		return "", ""
	}
	// 使用 registry 库对镜像引用进行分类；只有基于 tag 的引用才能准确映射到 chart 的
	// repository/tag 字段。digest 引用会解析为 conregname.Digest 而非 Tag。
	parsed, err := conregname.ParseReference(ref)
	if err != nil {
		return "", ""
	}
	if _, ok := parsed.(conregname.Tag); !ok {
		return "", ""
	}
	lastSlash := strings.LastIndex(ref, "/")
	if lastColon := strings.LastIndex(ref, ":"); lastColon > lastSlash {
		return ref[:lastColon], ref[lastColon+1:]
	}
	return ref, ""
}

// gpustackOperatorChartVersion 解析打包在镜像中的 operator chart 版本。
// 它跟随 operator 二进制文件版本（例如去掉前缀 "v" 的发布标签 "v0.6.2"），
// 开发构建时回退为 "0.0.0"。镜像构建打包 chart 时会推导出相同的值（参见 pack/gpustack-operator/Dockerfile）。
func gpustackOperatorChartVersion() string {
	if version.IsValid() {
		return strings.TrimPrefix(version.Version, "v")
	}
	return "0.0.0"
}
