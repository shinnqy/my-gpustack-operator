package worker

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"unicode"

	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrladmission "sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubediscovery"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/system"
	"gpustack.ai/gpustack/pkg/webhook"
)

// ModelArtifactWebhook 对 ModelArtifact 执行默认值填充与校验。
//
// 所有规则均仅依据该对象本身作出判定。此处不读取 artifact 引用的 Secret 和声明：它们可能在
// artifact 之后才创建，而依赖创建顺序的拒绝策略无法满足声明式 apply 的要求。它们的缺失会
// 反映在 artifact 的 status 中。
//
// nolint: lll
// +k8s:webhook-gen:validating:group="worker.gpustack.ai",version="v1alpha1",resource="modelartifacts",scope="Namespaced"
// +k8s:webhook-gen:validating:operations=["CREATE","UPDATE"],failurePolicy="Fail",sideEffects="None",matchPolicy="Equivalent",timeoutSeconds=10
// +k8s:webhook-gen:validating:subResources=["status"]
// +k8s:webhook-gen:mutating:group="worker.gpustack.ai",version="v1alpha1",resource="modelartifacts",scope="Namespaced"
// +k8s:webhook-gen:mutating:operations=["CREATE","UPDATE"],failurePolicy="Fail",sideEffects="None",matchPolicy="Equivalent",timeoutSeconds=10
type ModelArtifactWebhook struct {
	// worker 是 worker 自身请求所携带的用户名，也是 status 的唯一写入者。
	worker string
}

func (r *ModelArtifactWebhook) SetupWebhook(ctx context.Context, _ webhook.SetupOptions) (runtime.Object, error) {
	worker, err := selfUsername(ctx)
	if err != nil {
		return nil, fmt.Errorf("learn the worker's identity for the ModelArtifact status rule: %w", err)
	}
	r.worker = worker

	return &workercore.ModelArtifact{}, nil
}

var (
	_ ctrladmission.Validator[runtime.Object] = (*ModelArtifactWebhook)(nil)
	_ ctrladmission.Defaulter[runtime.Object] = (*ModelArtifactWebhook)(nil)
	_ webhook.ReceiveDeletionUpdate           = (*ModelArtifactWebhook)(nil)
)

// ReceiveDeletionUpdate 使规则在处于 Terminating 状态的 artifact 上依然生效。status 校验规则
// 在此状态下也必须成立，因为被引用的 artifact 在最后一个使用方离开前会一直保持 Terminating 且
// 仍可挂载；更新路径触达的其他所有规则仅比较传入的新旧两个对象，默认值填充也不读取其他任何资源。
func (*ModelArtifactWebhook) ReceiveDeletionUpdate() {}

// ModelArtifactDefaultRevision 是 Hugging Face 来源未指定版本时使用的 revision，即 Hugging Face 仓库的默认分支。
const ModelArtifactDefaultRevision = "main"

// ModelArtifactDefaultModelScopeRevision 是 ModelScope 来源未指定版本时使用的 revision，即 ModelScope 仓库的默认分支。
const ModelArtifactDefaultModelScopeRevision = "master"

// Default 在创建和更新时均用对应 hub 的默认分支填充未设置的 revision：否则，省略 revision 的全量替换将被视为将其修改为空值，并作为 spec 编辑而被拒绝。
func (*ModelArtifactWebhook) Default(_ context.Context, obj runtime.Object) error {
	ma := obj.(*workercore.ModelArtifact)
	if hub := ma.Spec.Source.HuggingFace; hub != nil && hub.Revision == "" {
		hub.Revision = ModelArtifactDefaultRevision
	}
	if hub := ma.Spec.Source.ModelScope; hub != nil && hub.Revision == "" {
		hub.Revision = ModelArtifactDefaultModelScopeRevision
	}

	return nil
}

func (*ModelArtifactWebhook) ValidateCreate(_ context.Context, obj runtime.Object) (ctrladmission.Warnings, error) {
	ma := obj.(*workercore.ModelArtifact)
	if errs := validateModelArtifact(ma, nil); len(errs) > 0 {
		return nil, kerrors.NewInvalid(workercore.SchemeGroupVersionKind("ModelArtifact").GroupKind(), ma.Name, errs)
	}

	return nil, nil
}

func (r *ModelArtifactWebhook) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) (ctrladmission.Warnings, error) {
	ma, old := newObj.(*workercore.ModelArtifact), oldObj.(*workercore.ModelArtifact)
	if isStatusRequest(ctx) {
		return nil, r.validateModelArtifactStatus(ctx, ma)
	}
	if errs := validateModelArtifact(ma, old); len(errs) > 0 {
		return nil, kerrors.NewInvalid(workercore.SchemeGroupVersionKind("ModelArtifact").GroupKind(), ma.Name, errs)
	}

	return nil, nil
}

func (*ModelArtifactWebhook) ValidateDelete(_ context.Context, _ runtime.Object) (ctrladmission.Warnings, error) {
	return nil, nil
}

// modelArtifactIdentityMessage 说明为何整个 spec 均不可变更。
const modelArtifactIdentityMessage = "a ModelArtifact is an identity, so its spec cannot change after " +
	"creation: a different source or revision is a different artifact, created rather than edited"

// huggingFaceRepositoryPartPattern 匹配 Hugging Face 仓库 ID 中的单个组成部分（遵循 Hub 命名规则）：
// 仅含字母、数字、"-"、"_" 和 "."，不以 "-" 或 "." 开头或结尾，长度不超过 96 个字符。
var huggingFaceRepositoryPartPattern = regexp.MustCompile(`^[A-Za-z0-9_]([A-Za-z0-9_.-]{0,94}[A-Za-z0-9_])?$`)

func validateModelArtifact(ma, old *workercore.ModelArtifact) field.ErrorList {
	specPath := field.NewPath("spec")
	if old != nil {
		if !kubemeta.DeepEqual(ma.Spec, old.Spec) {
			return field.ErrorList{field.Invalid(specPath, ma.Spec, modelArtifactIdentityMessage)}
		}
		// 未变更的 spec 在存储时已经过校验。若再次校验，在后续新规则生效前已存储的对象将被锁死：永远无法为其写入 finalizer 或 label。
		return nil
	}

	sourcePath := specPath.Child("source")
	source := ma.Spec.Source
	var members []string
	if source.HuggingFace != nil {
		members = append(members, "huggingFace")
	}
	if source.ModelScope != nil {
		members = append(members, "modelScope")
	}
	if source.PersistentVolumeClaim != nil {
		members = append(members, "persistentVolumeClaim")
	}
	if source.Image != nil {
		members = append(members, "image")
	}
	if len(members) != 1 {
		return field.ErrorList{field.Invalid(sourcePath, members,
			"exactly one of huggingFace, modelScope, persistentVolumeClaim or image is required")}
	}

	var errs field.ErrorList
	switch {
	case source.HuggingFace != nil:
		errs = validateModelArtifactHub(source.HuggingFace, sourcePath.Child("huggingFace"))
	case source.ModelScope != nil:
		errs = validateModelArtifactHub(source.ModelScope, sourcePath.Child("modelScope"))
	case source.Image != nil:
		errs = validateModelArtifactImage(source.Image, sourcePath.Child("image"))
		errs = append(errs, validateModelArtifactImageVolume(sourcePath.Child("image"))...)
	default:
		errs = validateModelArtifactClaim(source.PersistentVolumeClaim, sourcePath.Child("persistentVolumeClaim"))
	}

	hub := source.HuggingFace != nil || source.ModelScope != nil
	errs = append(errs, validateModelArtifactExpectedDigest(ma.Spec.ExpectedDigest, &source,
		specPath.Child("expectedDigest"))...)

	return append(errs, validateModelArtifactPatterns(&ma.Spec, specPath, hub, source.Image != nil)...)
}

// validateModelArtifactExpectedDigest 仅在 hub 来源上接受采用 manifest 摘要格式的摘要锚点。
// claim 的内容取决于挂载时卷中的实际数据（动态制备，每次可能不同），其身份由用户确认的 claim 本身决定；
// 镜像的身份则由其引用的 digest 决定。两者均无法锚定到 manifest 摘要，而永远无法匹配的锚点只会导致制品长期处于未解析状态。
func validateModelArtifactExpectedDigest(digest string, source *workercore.ModelArtifactSource, path *field.Path) field.ErrorList {
	switch {
	case digest == "":
		return nil
	case source.HuggingFace != nil || source.ModelScope != nil:
		if !modelArtifactDigestPattern.MatchString(digest) {
			return field.ErrorList{field.Invalid(path, digest, `must be "sha256:" and 64 lowercase hex`)}
		}
		return nil
	case source.Image != nil:
		return field.ErrorList{field.Forbidden(path,
			"expectedDigest asserts the manifest digest a hub source resolves to; an image's identity is "+
				"its reference's digest")}
	default:
		return field.ErrorList{field.Forbidden(path,
			"expectedDigest asserts the manifest digest a hub source resolves to; a claim's content is "+
				"whatever it holds at mount time and its identity is the claim itself")}
	}
}

// modelArtifactMaxPatterns 和 modelArtifactMaxPatternLength 分别限制模式列表的数量及每条模式的长度；
// schema 也会限制列表数量，但无法限制单个条目长度。
const (
	modelArtifactMaxPatterns      = 32
	modelArtifactMaxPatternLength = 256
)

// validateModelArtifactPatterns 仅在 hub 来源上接受 allow 和 ignore 模式：其他所有来源的内容均由用户管理，并会整体挂载。
func validateModelArtifactPatterns(spec *workercore.ModelArtifactSpec, specPath *field.Path, hub, image bool) field.ErrorList {
	// whole 是拒绝错误信息中用于说明“将整体挂载而非按模式筛选”的提示文本。
	whole := "a claim's directory is mounted whole"
	if image {
		whole = "an image is mounted whole"
	}
	var errs field.ErrorList
	for _, list := range []struct {
		name     string
		patterns []string
	}{
		{"allowPatterns", spec.AllowPatterns},
		{"ignorePatterns", spec.IgnorePatterns},
	} {
		path := specPath.Child(list.name)
		switch {
		case len(list.patterns) == 0:
			continue
		case !hub:
			errs = append(errs, field.Forbidden(path,
				"patterns select files of a hub source; "+whole))
			continue
		case len(list.patterns) > modelArtifactMaxPatterns:
			errs = append(errs, field.TooMany(path, len(list.patterns), modelArtifactMaxPatterns))
			continue
		}
		for i, p := range list.patterns {
			if p == "" || len(p) > modelArtifactMaxPatternLength ||
				strings.ContainsFunc(p, unicode.IsControl) {
				errs = append(errs, field.Invalid(path.Index(i), p, fmt.Sprintf(
					"must be 1 to %d characters without control characters", modelArtifactMaxPatternLength,
				)))
			}
		}
	}

	return errs
}

// validateModelArtifactHub 校验 hub 来源的结构，即两个平台共用的规则：仓库 ID 与 revision 的字符约束。
// ModelScope ID 与 Hugging Face ID 的写法相同，因此共用同一个校验器；解析 revision 的差异逻辑位于客户端中，而非此处。
func validateModelArtifactHub(hub *workercore.ModelArtifactHubSource, path *field.Path) field.ErrorList {
	var errs field.ErrorList

	parts := strings.Split(hub.Repository, "/")
	if len(parts) > 2 || slices.ContainsFunc(parts, func(p string) bool {
		return !huggingFaceRepositoryPartPattern.MatchString(p) || strings.Contains(p, "--") || strings.Contains(p, "..")
	}) {
		errs = append(errs, field.Invalid(path.Child("repository"), hub.Repository,
			`must be "owner/name" or a bare name, each made of letters, digits, "-", "_" and ".", `+
				`at most 96 characters, not starting or ending with "-" or ".", and without "--" or ".."`))
	}

	// Default 会在创建时填充空的 revision；若走到校验时仍为空，说明请求通过其他路径绕过了默认值填充，而解析 "" 将无法向 Hub 请求任何有效版本。
	revision := hub.Revision
	switch {
	case revision == "":
		errs = append(errs, field.Required(path.Child("revision"), "a branch, a tag or a commit"))
	case strings.ContainsFunc(revision, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }):
		errs = append(errs, field.Invalid(path.Child("revision"), revision,
			"must not contain whitespace or control characters"))
	}

	return errs
}

func validateModelArtifactClaim(claim *workercore.ModelArtifactPersistentVolumeClaimSource, path *field.Path) field.ErrorList {
	var errs field.ErrorList
	if claim.ClaimName == "" {
		errs = append(errs, field.Required(path.Child("claimName"), "the claim in this namespace holding the weights"))
	}
	// schema 会拒绝绝对路径；由于检测 ".." 路径段需要正则负向先行断言（schema 模式不支持），因此该部分校验放在此处。
	if strings.HasPrefix(claim.Path, "/") || slices.Contains(strings.Split(claim.Path, "/"), "..") {
		errs = append(errs, field.Invalid(path.Child("path"), claim.Path,
			`must be relative to the volume's root and must not contain a ".." element`))
	}

	return errs
}

// modelArtifactClusterVersion 是能力校验规则读取的 Kubernetes 版本。将其定义为变量以便测试在无集群环境下指定版本：
// 快照的 Configure 会忽略后续调用，因此测试无法直接重定向快照本身。
var modelArtifactClusterVersion = func() kubediscovery.Version {
	return system.LoopbackKubeVersion.Get()
}

// validateModelArtifactImageVolume 在集群不支持镜像卷时拒绝镜像来源。ImageVolume 特性自 Kubernetes 1.35 起默认开启；
// 不识别 volumes[].image 字段的 apiserver 会静默丢弃该字段而不报错，从而导致模型权重悄然缺失。
// 该函数读取启动时的版本快照，若无法读取版本号则直接拒绝：不可读的版本绝不能放行该能力。
func validateModelArtifactImageVolume(path *field.Path) field.ErrorList {
	version := modelArtifactClusterVersion()
	if kubediscovery.SupportsFeature(&version, kubediscovery.FeatureImageVolume) {
		return nil
	}

	return field.ErrorList{field.Forbidden(path, fmt.Sprintf(
		"this cluster's Kubernetes version %q does not serve image volumes: the ImageVolume feature is "+
			"on by default from 1.35, and 1.33 or 1.34 would need the gate opened on the apiserver, which "+
			"this version does not offer; deliver the weights from a hub or a PersistentVolumeClaim, or "+
			"upgrade the cluster", version.GitVersion,
	))}
}

// modelArtifactDigestPattern 匹配固定镜像引用中的摘要部分，即 "sha256:" 后跟 64 位小写十六进制字符。
var modelArtifactDigestPattern = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)

// validateModelArtifactImage 拒绝未固定摘要的镜像引用。tag 是可变的，同一制品在不同次拉取时可能获取不同的权重，从而使不可变引用所锚定的身份失去意义。
func validateModelArtifactImage(image *workercore.ModelArtifactImageSource, path *field.Path) field.ErrorList {
	var errs field.ErrorList
	reference := image.Reference
	ref, digest, pinned := strings.Cut(reference, "@")
	switch {
	case !pinned:
		errs = append(errs, field.Invalid(path.Child("reference"), reference,
			"must pin the image to its digest: registry/repository@sha256:<64 hex>; a tag is mutable, "+
				"so one artifact could deliver different weights on different pulls"))
	case !modelArtifactDigestPattern.MatchString(digest):
		errs = append(errs, field.Invalid(path.Child("reference"), reference,
			`must pin the digest as "sha256:" and 64 lowercase hex`))
	case ref == "" || strings.ContainsAny(ref, "@ \t\n\r"):
		errs = append(errs, field.Invalid(path.Child("reference"), reference,
			"must be one image reference without whitespace, followed by @ and the digest"))
	}

	return errs
}
