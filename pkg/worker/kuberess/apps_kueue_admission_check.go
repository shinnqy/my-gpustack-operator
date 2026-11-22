package kuberess

import (
	"context"
	"fmt"
	"time"

	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/pkg/kubeappyaml"
	"gpustack.ai/gpustack/pkg/system"
	"gpustack.ai/gpustack/pkg/utils/waitx"
)

const (
	// nodeDevicesAdmissionCheckTimeout 限制等待 Kueue 的 AdmissionCheck CRD 就绪的超时时间。
	// Kueue 将其 CRD 作为 chart 模板发布，因此它们会随 release 的其余部分一同安装而非提前就绪，
	// 与该发布并行启动的 worker 可能会在 CRD 提供服务之前就执行到此步骤。
	nodeDevicesAdmissionCheckTimeout = 5 * time.Minute
	// nodeDevicesAdmissionCheckInterval 是上述超时等待期间的重试间隔。
	nodeDevicesAdmissionCheckInterval = 5 * time.Second
)

// nodeDevicesAdmissionCheckYAML 是第三道准入关（gate-3）的 AdmissionCheck 对象。
// 其 name 和 controllerName 是与 worker 的 NodeDevicesAdmission 控制器共享的契约
// （_NodeDevicesAdmissionCheckName / _NodeDevicesControllerName）。
const nodeDevicesAdmissionCheckYAML = `
apiVersion: kueue.x-k8s.io/v1beta2
kind: AdmissionCheck
metadata:
  name: gpustack-node-devices
  labels:
    "app.kubernetes.io/part-of": "gpustack-operator"
spec:
  controllerName: worker.gpustack.ai/node-devices
`

// InstallNodeDevicesAdmissionCheck 应用 node-devices AdmissionCheck，并持续重试直至 Kueue 的 CRD 建立完成。
//
// chart 无法直接下发该对象：其 CRD 属于 Kueue，且 Kueue 通过模板渲染 CRD，因此同一次渲染中
// 无法保证 CRD 排在自定义资源之前。Apply 仅设置 spec，因此不会覆盖由控制器维护的 Active 状态条件，
// 且该步骤可安全重复执行。worker 的 NodeDevicesAdmissionCheckReconciler 会将该检查标记为 Active，
// 而 NodeQueueReconciler 会在带加速器的 ClusterQueue 中引用它，因此缺少该对象时调度链无法成形。
func InstallNodeDevicesAdmissionCheck(ctx context.Context) error {
	restCfg := system.LoopbackKubeRestConfig.Get()

	err := waitx.RetryUntilSuccessWithTimeout(ctx,
		nodeDevicesAdmissionCheckInterval, nodeDevicesAdmissionCheckTimeout, true,
		func(ctx context.Context) error {
			err := kubeappyaml.Apply(ctx, nodeDevicesAdmissionCheckYAML, restCfg)
			if err != nil {
				// 记录每次尝试。该等待在服务响应探针之前执行，容器可能会在此期间被终止，
				// 而下方的错误仅在轮询放弃时才会打印——没有此日志，等待过程将与卡死无异。
				klog.InfoS("waiting for kueue's AdmissionCheck CRD", "err", err)
			}

			return err
		})
	if err != nil {
		return fmt.Errorf("waiting for kueue's AdmissionCheck CRD: %w", err)
	}

	return nil
}

const (
	// ModelDeploymentJointAdmissionCheckName 是每个由 operator 管理的 ClusterQueue 所引用的
	// AdmissionCheck 对象名称，ModelDeploymentJointAdmissionControllerName 则是认领它的控制器名称。
	//
	// 它们定义在此处是因为本包负责安装该对象且 controllers 包已导入本包，
	// 从而使这一契约成为编译器强制校验的单一定义，而非目前恰好一致的两个字符串。
	// 这样可避免在两处重复拼写带来的故障：若仅修改其中一处，会导致 Workload 失去准入屏障
	// 或该检查永久处于非激活状态，且没有任何组件会报错——队列会继续放行，只有需要该屏障的
	// 部署才会受到影响。
	ModelDeploymentJointAdmissionCheckName = "gpustack-model-deployment-joint"

	// ModelDeploymentJointAdmissionControllerName 将该检查路由至声明它的控制器。
	ModelDeploymentJointAdmissionControllerName = "worker.gpustack.ai/model-deployment-joint"
)

// modelDeploymentJointAdmissionCheckYAML 是用于控制单个 ModelDeployment 中所有分组
// 联合准入的 AdmissionCheck。
var modelDeploymentJointAdmissionCheckYAML = fmt.Sprintf(`
apiVersion: kueue.x-k8s.io/v1beta2
kind: AdmissionCheck
metadata:
  name: %s
  labels:
    "app.kubernetes.io/part-of": "gpustack-operator"
spec:
  controllerName: %s
`, ModelDeploymentJointAdmissionCheckName, ModelDeploymentJointAdmissionControllerName)

// InstallModelDeploymentJointAdmissionCheck 应用联合准入 AdmissionCheck，并持续重试直至 Kueue 的 CRD 建立完成。
//
// 它与上方的 node-devices 检查出于相同原因、以相同的超时上限等待同一个 CRD：
// Kueue 通过模板渲染其 CRD，因此同一次渲染中无法保证 CRD 排在自定义资源之前。
// Apply 仅设置 spec，因此不会覆盖由控制器维护的 Active 状态条件。
func InstallModelDeploymentJointAdmissionCheck(ctx context.Context) error {
	restCfg := system.LoopbackKubeRestConfig.Get()

	err := waitx.RetryUntilSuccessWithTimeout(ctx,
		nodeDevicesAdmissionCheckInterval, nodeDevicesAdmissionCheckTimeout, true,
		func(ctx context.Context) error {
			err := kubeappyaml.Apply(ctx, modelDeploymentJointAdmissionCheckYAML, restCfg)
			if err != nil {
				klog.InfoS("waiting for kueue's AdmissionCheck CRD", "err", err)
			}

			return err
		})
	if err != nil {
		return fmt.Errorf("waiting for kueue's AdmissionCheck CRD: %w", err)
	}

	return nil
}
