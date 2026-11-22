package worker

import (
	"fmt"
	"strconv"

	corev1 "k8s.io/api/core/v1"
)

// 弹性 master 在重组其集合通信组期间保持存活，引擎会在整个扩缩容期间对所有端点返回 503 以示该状态。
// kubelet 自带的 HTTP 探针无法理解这一点：它把 200-399 之外的所有状态都视为失败，
// 因此只要扩缩容耗时达到存活阈值，就会被判定为引擎死亡，kubelet 进而重启正在扩缩容的进程。
// 在扩缩容期间摘除流量由就绪探针负责，readiness 探针仍会正常履行该职责。
//
// 因此存活探针通过 exec 调用镜像自带的 python3 实现，由其自行读取状态码，
// 并仅在两种响应下通过：200 表示引擎正常提供服务，503 表示引擎存活且正在重组。
// 连接被拒、超时或任何其他状态码均视为失败，因为已退出或损坏的引擎也会给出这些响应，必须予以重启。
const (
	// modelDeploymentElasticLivenessURL 是 master 容器上引擎自身的健康检查地址，
	// 通过回环接口访问以确保探针流量不离开 Pod。
	modelDeploymentElasticLivenessURL = "http://127.0.0.1:8000/health"

	// modelDeploymentElasticLivenessRequestSeconds 限制脚本内部单次读取的超时时间。
	// 该值低于 kubelet 探针的超时设置，以便程序先返回结果并让 kubelet 读取其退出码，
	// 而不是在返回退出码之前被提前终止。
	modelDeploymentElasticLivenessRequestSeconds = 1
)

// modelDeploymentElasticLivenessProbe 是 master 容器运行的存活探针。
// 它是三类探针中唯一改变的探针：startup 仍等待引擎加载完成，
// readiness 仍会在整个扩缩容窗口期内拦截流量。
//
// 它直接读取引擎状态且不从 spec 接收配置项，从而防止部署通过放宽可接受状态码集合
// 使探针变成永不重启异常引擎的无效检查。
func modelDeploymentElasticLivenessProbe() *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			Exec: &corev1.ExecAction{
				Command: []string{
					"python3", "-c",
					modelDeploymentElasticLivenessScript(modelDeploymentElasticLivenessURL),
				},
			},
		},
		PeriodSeconds:    modelDeploymentProbePeriodSeconds,
		TimeoutSeconds:   modelDeploymentProbeTimeoutSeconds,
		FailureThreshold: modelDeploymentLivenessFailureThreshold,
	}
}

// modelDeploymentElasticLivenessScript 生成检测脚本。它接收地址作为参数，
// 以便测试将同一脚本指向测试服务器；生产探针则传入实际地址。
//
// HTTPError 在通用异常之前捕获，因为它是 URLError 的子类，且本文件需要接受的 503
// 会作为异常而非正常响应抛出。脚本会从解释器响应对象所携带的任意属性中读取状态码，
// 从而兼容仅暴露 code 属性的旧版本镜像。该异常对象本身也是响应对象，
// 因此会与成功路径一样被关闭，确保每条打开连接的路径都释放资源。
//
// 无法运行的脚本代表探针失败而非通过：任何未获取到状态码的路径均以非零状态退出，
// 且可接受的状态码集合采用显式列举而非隐式区间判断。
func modelDeploymentElasticLivenessScript(url string) string {
	return fmt.Sprintf(`import sys, urllib.error, urllib.request
try:
    response = urllib.request.urlopen(%[1]s, timeout=%[2]d)
    code = getattr(response, "status", None)
    if code is None:
        code = response.getcode()
    response.close()
except urllib.error.HTTPError as err:
    code = err.code
    err.close()
except Exception:
    sys.exit(1)
sys.exit(0 if code in (200, 503) else 1)
`, strconv.Quote(url), modelDeploymentElasticLivenessRequestSeconds)
}
