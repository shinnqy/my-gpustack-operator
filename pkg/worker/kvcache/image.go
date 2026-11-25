// 本文件保存除镜像名之外、各组件渲染镜像时共用的规则。目前仅包含镜像拉取策略解析，放在这里
// 而非具体渲染器旁边，以便路由器与后端等所有需要收敛容器规格的渲染器都遵循同一套规则。
package kvcache

import (
	dockerref "github.com/distribution/reference"
	core "k8s.io/api/core/v1"
)

// 本文件负责：按 K8s 同款规则提前算出 ImagePullPolicy

// ResolvePullPolicy 解析渲染后的容器应携带的具体拉取策略。
//
// 这里直接解析出具体值而不留空交由 API server 填充默认值，这是本函数存在的根本原因：空策略
// 无法收敛——写入时 API server 会自动补全默认值，导致对齐器在每轮调谐都看到期望值与线上值
// 不一致并持续触发滚动更新；若对齐器为了回避这一点而跳过该字段比较，就永远无法纠正过期策略。
// 只要每轮都渲染出明确的值，就不会与服务端默认值冲突，也无需跳过比较。
func ResolvePullPolicy(declared core.PullPolicy, image string) core.PullPolicy {
	if declared != "" {
		return declared
	}
	if imageTag(image) == "latest" {
		return core.PullAlways
	}
	return core.PullIfNotPresent
}

// imageTag 返回 API server 自身默认化逻辑会读取到的镜像标签，从而让未声明策略的解析结果与留空
// 时服务端填充的默认值完全一致。
//
// 其逻辑与 k8s.io/kubernetes/pkg/util/parsers.ParseImageName 保持一致并使用相同的解析库：既无
// 标签也无摘要的引用按 ":latest" 解析；仅带摘要的引用没有标签；同时携带标签和摘要的引用保留其
// 标签。读取该标签的规则对应 SetDefaults_Container，在本项目构建所依赖的各个版本中完全一致。
//
// 无法解析的引用返回空标签（不等于 "latest"），这与 API server 的结果一致：服务端忽略解析错误，
// 直接读取传入的空标签。准入控制本就会拒绝非法镜像；这里处理的是绕过准入时的渲染兜底行为。
func imageTag(image string) string {
	named, err := dockerref.ParseNormalizedNamed(image)
	if err != nil {
		return ""
	}

	var tag, digest string
	if tagged, ok := named.(dockerref.Tagged); ok {
		tag = tagged.Tag()
	}
	if digested, ok := named.(dockerref.Digested); ok {
		digest = digested.Digest().String()
	}
	if tag == "" && digest == "" {
		tag = "latest"
	}

	return tag
}
