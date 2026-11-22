package kuberess

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"
	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/pkg/kubeappyaml"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/system"
	"gpustack.ai/gpustack/pkg/utils/waitx"
)

const (
	// cpuInfoNodeFeatureRuleName 是调度链起点的规则名称。
	// 它是集群作用域且名称固定的，因此每种安装模式都会收敛到同一个对象。
	cpuInfoNodeFeatureRuleName = "gpustack-cpu-info"
	// cpuInfoNodeFeatureRuleTimeout 限制等待 Node Feature Discovery 的 NodeFeatureRule CRD 就绪的超时时间。
	// 通过 chart 安装时由于 NFD 将 CRD 放在 crds/ 目录下，会在 release 之前建立它们；
	// 但自带 NFD 的集群会按自己的节奏安装 CRD，并行启动的 worker 可能会先执行到此步骤。
	cpuInfoNodeFeatureRuleTimeout = 5 * time.Minute
	// cpuInfoNodeFeatureRuleInterval 是上述超时等待期间的重试间隔。
	cpuInfoNodeFeatureRuleInterval = 5 * time.Second
)

// cpuInfoNodeFeatureRuleTemplate 是将原始节点硬件转换为调度链后续步骤所需标签和注解的规则：
// 包括 CPU 型号注解，以及节点是否携带可加速设备。
//
// 它的两个匹配器列表决定了节点是否被分类。vendor ID 列表对应当前 worker 管理的厂商，
// 因此在 chart 的 `manufacturers`（worker 的 --manufacturer 列表即源于此）中添加或移除厂商时，
// 会立即被检测到或忽略。class 前缀则是 NFD 配置为打标的设备类别前缀，
// 因为规则只能匹配 NFD 被指示发布的设备。
const cpuInfoNodeFeatureRuleTemplate = `
apiVersion: nfd.k8s-sigs.io/v1alpha1
kind: NodeFeatureRule
metadata:
  name: ` + cpuInfoNodeFeatureRuleName + `
  labels:
    "app.kubernetes.io/part-of": "gpustack-operator"
spec:
  rules:
    - name: "attached cpu info to annotations"
      annotations:
        feature.gpustack.ai/cpu-name: "@cpu.model.name"
        feature.gpustack.ai/cpu-family: "@cpu.model.family"
        feature.gpustack.ai/cpu-physical-cores: "@cpu.model.physical_cores"
        feature.gpustack.ai/cpu-threads-per-core: "@cpu.model.threads_per_core"
        feature.gpustack.ai/cpu-logical-cores: "@cpu.model.logical_cores"
        feature.gpustack.ai/cpu-stepping: "@cpu.model.stepping"
        feature.gpustack.ai/cpu-cache-line: "@cpu.model.cache_line"
        feature.gpustack.ai/cpu-hz: "@cpu.model.hz"
        feature.gpustack.ai/cpu-boost-freq: "@cpu.model.boost_freq"
        feature.gpustack.ai/cpu-cache-l1i: "@cpu.model.cache_l1i"
        feature.gpustack.ai/cpu-cache-l1d: "@cpu.model.cache_l1d"
        feature.gpustack.ai/cpu-cache-l2: "@cpu.model.cache_l2"
        feature.gpustack.ai/cpu-cache-l3: "@cpu.model.cache_l3"
      matchFeatures:
        - feature: cpu.model
          matchExpressions:
            vendor_id:
              op: Exists
    - name: "detect whether the node has acceleratable devices"
      vars:
        has-acceleratable-devices: "true"
      matchFeatures:
        - feature: pci.device
          matchExpressions:
            class:
              op: InRegexp
              value:
              {{- range $.PciClassPrefixes }}
                - {{ printf "^%s" . | quote }}
              {{- end }}
            vendor:
              op: In
              value:
              {{- range $.PciVendorIDs }}
                - {{ . | quote }}
              {{- end }}
    - name: "identify the node without any acceleratable device"
      labels:
        feature.gpustack.ai/acceleratable: "false"
      matchFeatures:
        - feature: rule.matched
          matchExpressions:
            has-acceleratable-devices:
              op: DoesNotExist
`

// InstallCPUInfoNodeFeatureRule 应用 gpustack-cpu-info NodeFeatureRule，并持续重试直至
// Node Feature Discovery 的 CRD 建立完成。
//
// chart 无法直接下发该对象：其 CRD 属于 NFD，且即使在 chart 完全不部署 NFD 的场景
// （即集群运行自带的 NFD）下也需要该规则——而这正是 Helm 清单无法适配的安装模式。
// Apply 仅设置 spec，因此该步骤可安全重复执行。
func InstallCPUInfoNodeFeatureRule(ctx context.Context, manufacturers []string) error {
	content, err := cpuInfoNodeFeatureRule(manufacturers)
	if err != nil {
		return fmt.Errorf("render the cpu-info NodeFeatureRule: %w", err)
	}

	restCfg := system.LoopbackKubeRestConfig.Get()

	err = waitx.RetryUntilSuccessWithTimeout(ctx,
		cpuInfoNodeFeatureRuleInterval, cpuInfoNodeFeatureRuleTimeout, true,
		func(ctx context.Context) error {
			err := kubeappyaml.Apply(ctx, content, restCfg)
			if err != nil {
				// 记录每次尝试。该等待在服务响应探针之前执行，容器可能会在此期间被终止，
				// 而下方的错误仅在轮询放弃时才会打印——没有此日志，等待过程将与卡死无异。
				klog.InfoS("waiting for node feature discovery's NodeFeatureRule CRD",
					"err", err)
			}

			return err
		})
	if err != nil {
		return fmt.Errorf("waiting for node feature discovery's NodeFeatureRule CRD: %w", err)
	}

	return nil
}

// cpuInfoNodeFeatureRule 为给定的厂商列表渲染规则。无对应厂商的名称不会提供任何 vendor ID，
// 因此未知名称会收窄匹配范围，而非渲染出空的匹配器。
func cpuInfoNodeFeatureRule(manufacturers []string) (string, error) {
	vendorIDs := sets.New[string]()
	for _, manufacturer := range manufacturers {
		if id := nodefeature.GetPciVendorID(manufacturer); id != "" {
			vendorIDs.Insert(id)
		}
	}

	return kubeappyaml.Template(cpuInfoNodeFeatureRuleTemplate).Render(map[string]any{
		"PciClassPrefixes": nodefeature.GetAcceleratablePciClassPrefixes(),
		"PciVendorIDs":     sets.List(vendorIDs),
	}, nil)
}
