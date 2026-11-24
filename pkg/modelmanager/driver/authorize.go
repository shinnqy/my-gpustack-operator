package driver

import (
	"fmt"

	gpustack "gpustack.ai/gpustack/api/v1"
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/modelstore"
)

// 卷属性键名。由于 CSIDriver 开启了 podInfoOnMount，kubelet 会写入 csi.storage.k8s.io/* 前缀的键
// 并覆盖 Pod 自行填写的同名键；其余键来自 Pod 的 volumeAttributes，仅作为提示信息。
const (
	attrPodNamespace = "csi.storage.k8s.io/pod.namespace"
	attrPodName      = "csi.storage.k8s.io/pod.name"
	attrPodUID       = "csi.storage.k8s.io/pod.uid"
	attrEphemeral    = "csi.storage.k8s.io/ephemeral"

	// AttrArtifact、AttrArtifactUID 和 AttrManifestDigest 是消费方在卷属性中声明的键名。
	AttrArtifact       = modelstore.VolumeAttrArtifact
	AttrArtifactUID    = modelstore.VolumeAttrArtifactUID
	AttrManifestDigest = modelstore.VolumeAttrManifestDigest
)

// 拒绝挂载时使用的授权校验规则描述。
const (
	ruleArtifactMissing  = "the Pod's namespace has no such ModelArtifact"
	ruleUIDMismatch      = "the ModelArtifact is not the one the volume names"
	ruleNotHub           = "the ModelArtifact is not on a model hub"
	ruleNotResolved      = "the ModelArtifact is not resolved"
	ruleDigestMismatch   = "the ModelArtifact resolves to another digest"
	resolvedCondition    = "Resolved"
	conditionStatusTrue  = "True"
	unauthorizedTemplate = "%s: ModelArtifact %s/%s: %s"
)

// mountAttributes 是卷请求携带的提示属性。
type mountAttributes struct {
	Artifact    string
	ArtifactUID string
	Digest      string
}

func attributesOf(vc map[string]string) mountAttributes {
	return mountAttributes{Artifact: vc[AttrArtifact], ArtifactUID: vc[AttrArtifactUID], Digest: vc[AttrManifestDigest]}
}

// authorizeMount 根据 API 返回的制品对象判断 podNamespace 下的 Pod 是否有权挂载该卷；
// 当该 Namespace 下不存在同名制品时 ma 为 nil。
//
// 卷属性只是提示信息。租户完全可以手写 Pod 填入任意制品名和任意摘要，因此每项属性都必须与 Pod 自身
// Namespace 下已解析的 ModelArtifact 一致，且该 Namespace 以 kubelet 覆盖写入的值为准。
// 摘要本身只是内容地址，不能单独证明访问权限：任何 Namespace 都只能通过自己用自身凭据解析的制品挂载内容。
func authorizeMount(podNamespace string, attrs mountAttributes, ma *workercore.ModelArtifact) (rule, message string) {
	deny := func(rule, detail string) (string, string) {
		return rule, fmt.Sprintf(unauthorizedTemplate, rule, podNamespace, attrs.Artifact, detail)
	}
	switch {
	case ma == nil:
		return deny(ruleArtifactMissing, "does not exist")
	case string(ma.UID) != attrs.ArtifactUID:
		return deny(ruleUIDMismatch, fmt.Sprintf("has UID %q, the volume names %q", ma.UID, attrs.ArtifactUID))
	case ma.Spec.Source.HuggingFace == nil && ma.Spec.Source.ModelScope == nil:
		return deny(ruleNotHub, "only a hub artifact is delivered by the node")
	case !conditionTrue(ma.Status.Conditions, resolvedCondition) || ma.Status.Resolved == nil:
		return deny(ruleNotResolved, "is not Resolved; new mounts wait until it is")
	case ma.Status.Resolved.ManifestDigest != attrs.Digest:
		return deny(ruleDigestMismatch, fmt.Sprintf("resolves to %s, the volume asks for %s",
			ma.Status.Resolved.ManifestDigest, attrs.Digest))
	}

	return "", ""
}

func conditionTrue(conds []gpustack.Condition, typ string) bool {
	for _, c := range conds {
		if c.Type == typ {
			return string(c.Status) == conditionStatusTrue
		}
	}

	return false
}
