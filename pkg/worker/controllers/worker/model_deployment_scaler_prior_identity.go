package worker

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"

	workerapi "gpustack.ai/gpustack/api/worker/v1"
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/worker/scaling/priors"
)

// modelDeploymentScalerPriorDeclaration 表示部署声明的内容。
//
// 该信息从部署中读取而非从 profile 中读取，因为目录是根据部署声明的内容来选择的，而不是相反。
type modelDeploymentScalerPriorDeclaration struct {
	// identity 是部署声明的标识。
	identity priors.Identity

	// topology 是部署声明的单个扩缩容副本形态。
	topology priors.Topology

	// flags 是部署传入的引擎参数。
	flags map[string]string

	// engineFamily 是部署运行的引擎。
	engineFamily string

	// instance 是部署运行的 InstanceType。
	instance *workerapi.InstanceType
}

// shape 返回部署声明的形态。
//
// 这里显式列出六个要素，因为目录是根据形态而非名称来选择的。
func (d modelDeploymentScalerPriorDeclaration) shape(role priors.RoleKind) priors.Shape {
	return priors.Shape{
		Role:         role,
		Model:        priors.CanonicalModelName(d.identity.Model.Name),
		Quantization: priors.CanonicalQuantization(d.identity.Model.Quantization),
		Engine:       d.engineFamily,
		Hardware:     priors.CanonicalAcceleratorFamily(d.identity.Hardware.Accelerator),
		Topology:     d.topology.Normalized(),
	}
}

// modelDeploymentScalerPriorDeclarationOf 读取部署声明的内容。
//
// 它会读取模型、引擎、硬件以及副本的形态，因为目录正是根据这四项来选择的。
func modelDeploymentScalerPriorDeclarationOf(
	ctx context.Context, reader ctrlcli.Reader, target *workercore.ModelDeployment, role *workercore.ModelDeploymentRole,
) (declaration modelDeploymentScalerPriorDeclaration, err error) {
	// 此处对拒绝信息进行脱敏，因为并行度读取器会报告其读取到的值，而用户填写的值不应泄露给其他方。
	defer func() {
		err = modelDeploymentScalerRedactPriorRefusal(err, role, nil)
	}()
	declaration = modelDeploymentScalerPriorDeclaration{}
	flags, err := modelDeploymentScalerPriorFlags(target.Spec.Engine.Name, role.ExtraArgs)
	if err != nil {
		return declaration, modelDeploymentScalerPriorRefusal(role.Name, "%v", err)
	}
	declaration.flags = flags
	// 在读取形态之前先拦截此类副本，因为 operator 无法托管的部署没有可供测量结果适用的形态，且形态读取器会报告其无法读取并行度。
	if len(role.Command) != 0 || role.ReplicaSize > 1 || role.ElasticEP != nil {
		return declaration, modelDeploymentScalerPriorRefusal(role.Name, "Catalog requires a managed single-serving-Pod replica")
	}
	topology, err := modelDeploymentScalerPriorTopology(target.Spec.Engine.Name, role)
	if err != nil {
		return declaration, err
	}
	declaration.topology = topology
	model, err := modelDeploymentScalerPriorModelIdentity(ctx, reader, target, role.Name, flags)
	if err != nil {
		return declaration, err
	}
	declaration.identity.Model = model
	declaration.identity.Engine = priors.EngineIdentity{Image: modelDeploymentScalerPriorEngineImage(target, role)}
	declaration.engineFamily = modelDeploymentScalerEngineFamily(target.Spec.Engine.Name)
	if declaration.engineFamily == "" {
		return declaration, modelDeploymentScalerPriorRefusal(role.Name, "the deployment names no known engine")
	}
	instance, hardware, err := modelDeploymentScalerPriorHardwareIdentity(ctx, reader, role)
	if err != nil {
		return declaration, err
	}
	declaration.instance = instance
	declaration.identity.Hardware = hardware
	return declaration, nil
}

// modelDeploymentScalerPriorEngineImage 返回部署运行的镜像。
//
// 当未指定镜像时回退到引擎名称，因为引擎由 operator 命名，而测量记录使用引擎族，且引擎名称所属的引擎族就是该引擎的引擎族。
func modelDeploymentScalerPriorEngineImage(target *workercore.ModelDeployment, role *workercore.ModelDeploymentRole) string {
	if role.Image != "" {
		return role.Image
	}
	if target.Spec.Engine.Image != "" {
		return target.Spec.Engine.Image
	}
	return target.Spec.Engine.Name
}

// modelDeploymentScalerPriorRequest 读取用于解析容量先验的请求。
//
// 它会对返回的每条拒绝信息中的环境变量值进行脱敏，因为拒绝信息会进入扩缩容器的条件状态中，而用户设置的环境变量值不应泄露给其他方。
func modelDeploymentScalerPriorRequest(
	target *workercore.ModelDeployment, role *workercore.ModelDeploymentRole, profile *priors.Profile,
	declaration modelDeploymentScalerPriorDeclaration,
) (priors.Request, error) {
	request, err := modelDeploymentScalerPriorRequestMatched(target, role, profile, declaration)
	if err == nil {
		return request, nil
	}
	return request, modelDeploymentScalerRedactPriorRefusal(err, role, profile)
}

// modelDeploymentScalerPriorRequestMatched 在对拒绝信息脱敏之前，读取用于解析容量先验的请求。
//
// 它将部署与 profile 进行比对校验，因为一条测量结果仅描述测量时所用的部署，而非同形态下的所有部署。
func modelDeploymentScalerPriorRequestMatched(
	target *workercore.ModelDeployment, role *workercore.ModelDeploymentRole, profile *priors.Profile,
	declaration modelDeploymentScalerPriorDeclaration,
) (priors.Request, error) {
	request := priors.Request{
		Role:     modelDeploymentScalerRoleKind(ModelDeploymentEffectiveRoleKind(role)),
		Identity: declaration.identity,
		Topology: declaration.topology,
	}
	// 允许挂载卷，因为部署需要为其自身角色挂载权重、缓存和日志；同时引擎按引擎族进行匹配，而非按所运行镜像的 digest 匹配。
	if target.Spec.KVCache != nil || target.Spec.KVTransfer != nil || target.Spec.Router != nil {
		return request, modelDeploymentScalerPriorRefusal(role.Name, "generated cache, transfer and router configuration is not supported by Catalog")
	}
	for _, other := range target.Spec.Roles {
		if ModelDeploymentEffectiveRoleKind(&other) == workercore.ModelDeploymentRoleKindEncode {
			return request, modelDeploymentScalerPriorRefusal(role.Name, "generated encoder connector configuration is not supported by Catalog")
		}
	}
	deploymentEnv, envErr := modelDeploymentScalerDeploymentEnv(role)
	if envErr != nil {
		return request, modelDeploymentScalerPriorRefusal(role.Name, "%v", envErr)
	}
	if err := modelDeploymentScalerMatchEngineEnv(
		profile.ServingConfig.EngineEnv, sortedEnvironmentEntries(deploymentEnv),
		profile.ServingConfig.EngineEnvComplete,
	); err != nil {
		return request, modelDeploymentScalerPriorRefusal(role.Name, "%v", err)
	}
	if err := modelDeploymentScalerPriorModelAnnotation(target, role, profile.Identity); err != nil {
		return request, err
	}
	if err := modelDeploymentScalerPriorHardwareAnnotation(target, role, profile.Identity); err != nil {
		return request, err
	}
	if err := modelDeploymentScalerPriorEngine(target, role, profile.Identity); err != nil {
		return request, err
	}
	if err := modelDeploymentScalerPriorModel(role, profile.Identity.Model, declaration); err != nil {
		return request, err
	}
	if err := modelDeploymentScalerPriorHardware(role, profile.Identity.Hardware, declaration.instance); err != nil {
		return request, err
	}
	serving, err := modelDeploymentScalerPriorServingConfig(target.Spec.Engine.Name, role, declaration.flags, profile.ServingConfig)
	if err != nil {
		return request, err
	}
	if err := modelDeploymentScalerPriorOmittedDefaultsGate(target.Spec.Engine.Name, profile.ServingConfig, declaration.flags); err != nil {
		return request, modelDeploymentScalerPriorRefusal(role.Name, "%v", err)
	}
	if err := modelDeploymentScalerOperatorEnvGate(target.Spec.Engine.Name, target, role); err != nil {
		return request, modelDeploymentScalerPriorRefusal(role.Name, "%v", err)
	}
	request.ServingConfig = serving
	return request, nil
}

func modelDeploymentScalerPriorTopology(engine string, role *workercore.ModelDeploymentRole) (priors.Topology, error) {
	declared, err := ParseModelDeploymentDeclaredParallelism(engine, role.ExtraArgs, role.Env)
	if err != nil {
		return priors.Topology{}, modelDeploymentScalerPriorRefusal(role.Name, "unreadable parallelism: %v", err)
	}
	reading := ReadModelDeploymentRoleParallelism(engine, role)
	if !reading.Source.Complete || reading.LoadBalance != workercore.ModelDeploymentLoadBalanceInternal ||
		declared.DataParallel != 1 || declared.DataParallelLocal > 1 || declared.ElasticEPMaxDataParallel != 0 ||
		reading.Declared.DataParallelLocal != nil && *reading.Declared.DataParallelLocal == 0 ||
		len(declared.Wiring) != 0 || len(declared.Modes) != 0 || declared.AttentionDataParallel != 1 ||
		declared.AttentionContextParallel != 1 || declared.MoEDataParallel != 1 || declared.DWDPSize != 1 ||
		declared.PrefillContextParallel != 1 && declared.PrefillContextParallel != declared.DecodeContextParallel {
		return priors.Topology{}, modelDeploymentScalerPriorRefusal(role.Name, "parallelism has no proven single-Pod metric denominator")
	}
	resources := role.Resources
	if resources == nil || resources.Accelerator == nil || resources.Accelerator.Sign() <= 0 ||
		resources.AcceleratorSlicedMemoryPercentage != 0 || resources.AcceleratorSlicedCoresPercentage != 0 ||
		resources.AcceleratorPartitionedProfile != "" {
		return priors.Topology{}, modelDeploymentScalerPriorRefusal(role.Name, "Catalog requires an explicit whole-accelerator count")
	}
	count, exact := resources.Accelerator.AsInt64()
	if !exact || count > 1<<31-1 {
		return priors.Topology{}, modelDeploymentScalerPriorRefusal(role.Name, "accelerator count must be a bounded integer")
	}
	return priors.Topology{
		GPUsPerReplica: int(count), TensorParallel: declared.TensorParallel, PipelineParallel: declared.PipelineParallel,
		DataParallel: 1, LocalDataParallel: 1, ContextParallel: declared.DecodeContextParallel, ExpertParallel: declared.ExpertParallel,
	}, nil
}

// modelDeploymentScalerPriorModelIdentity 读取部署所服务的模型。
//
// 当部署指定了制品（artifact）时从该制品读取，因为制品解析了部署所运行的仓库及权重提交版本。
//
// 当部署未指定版本（revision）时也不填写修订版本，因为未固定权重提交版本的部署并未说明其实际运行的权重。
func modelDeploymentScalerPriorModelIdentity(
	ctx context.Context, reader ctrlcli.Reader, target *workercore.ModelDeployment, role string,
	flags map[string]string,
) (priors.ModelIdentity, error) {
	name, revision := target.Spec.Model.Name, flags["--revision"]
	if ref := target.Spec.Model.ArtifactRef; ref != nil {
		artifact := new(workercore.ModelArtifact)
		if err := reader.Get(ctx, ctrlcli.ObjectKey{Namespace: target.Namespace, Name: ref.Name}, artifact); err != nil {
			if apierrors.IsNotFound(err) {
				return priors.ModelIdentity{}, modelDeploymentScalerPriorRefusal(role, "ModelArtifact %q does not exist", ref.Name)
			}
			return priors.ModelIdentity{}, err
		}
		if artifact.Status.Resolved == nil || artifact.Status.Resolved.Revision == "" {
			return priors.ModelIdentity{}, modelDeploymentScalerPriorRefusal(role, "ModelArtifact %q has no resolved weight revision", ref.Name)
		}
		source := artifact.Spec.Source.HuggingFace
		if source == nil {
			source = artifact.Spec.Source.ModelScope
		}
		if source == nil {
			return priors.ModelIdentity{}, modelDeploymentScalerPriorRefusal(role, "artifact source has no verifiable model repository identity")
		}
		name = source.Repository
		resolved := artifact.Status.Resolved.Revision
		if revision != "" && revision != resolved {
			return priors.ModelIdentity{}, modelDeploymentScalerPriorRefusal(role, "launch revision conflicts with resolved artifact")
		}
		revision = resolved
	}
	tokenizer, specified := flags["--tokenizer"]
	tokenizerRevision := flags["--tokenizer-revision"]
	if tokenizerRevision != "" {
		if !specified {
			tokenizer = name
		}
		tokenizer += "@" + tokenizerRevision
		specified = true
	}
	if !specified {
		tokenizer = ""
	}
	// 量化方式来自引擎参数或注解，因为部署会在二者之一中声明其量化方案；若二者均未声明，则表示未选择任何量化方案。
	quantization := flags["--quantization"]
	if quantization == "" {
		quantization = target.Annotations[workercore.ModelDeploymentScalerPrecisionAnnotationKey]
	}
	return priors.ModelIdentity{
		Name:         name,
		Revision:     revision,
		Tokenizer:    tokenizer,
		Quantization: quantization,
	}, nil
}

// modelDeploymentScalerPriorModel 拒绝声明了 profile 未曾测量过的模型的部署。
//
// 模型名称在比较时忽略组织前缀和大小写，因为部署声明其服务的模型，而数据源声明其测量的模型，两者对同一模型的拼写方式可能不同。
//
// 仅当双方都固定了版本（revision）时才比较修订版本，因为公开数据源不固定权重提交版本，部署通常也不固定。
func modelDeploymentScalerPriorModel(
	role *workercore.ModelDeploymentRole, identity priors.ModelIdentity,
	declaration modelDeploymentScalerPriorDeclaration,
) error {
	if !priors.ModelNameMatches(identity.Name, declaration.identity.Model.Name) {
		return modelDeploymentScalerPriorRefusal(role.Name, "model name conflicts with the selected model identity")
	}
	if !priors.RevisionMatches(identity.Revision, declaration.identity.Model.Revision) {
		return modelDeploymentScalerPriorRefusal(role.Name, "model revision conflicts with the selected model identity")
	}
	if quantization, specified := declaration.flags["--quantization"]; specified &&
		priors.CanonicalQuantization(quantization) != priors.CanonicalQuantization(identity.Quantization) {
		return modelDeploymentScalerPriorRefusal(role.Name, "--quantization conflicts with declared model identity")
	}
	if declaration.identity.Model.Tokenizer != "" && !priors.TokenizerMatches(identity.Tokenizer, declaration.identity.Model.Tokenizer) {
		return modelDeploymentScalerPriorRefusal(role.Name, "--tokenizer conflicts with declared tokenizer identity and revision")
	}
	return nil
}

// modelDeploymentScalerPriorHardwareIdentity 读取部署运行的硬件。
//
// 它会读取 InstanceType，因为 InstanceType 指明了部署所运行的加速器。
func modelDeploymentScalerPriorHardwareIdentity(
	ctx context.Context, reader ctrlcli.Reader, role *workercore.ModelDeploymentRole,
) (*workerapi.InstanceType, priors.HardwareIdentity, error) {
	instance := new(workerapi.InstanceType)
	if err := reader.Get(ctx, ctrlcli.ObjectKey{Name: role.InstanceType}, instance); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, priors.HardwareIdentity{}, modelDeploymentScalerPriorRefusal(role.Name, "InstanceType %q does not exist", role.InstanceType)
		}
		return nil, priors.HardwareIdentity{}, err
	}
	if !instance.Spec.Acceleratable {
		return nil, priors.HardwareIdentity{}, modelDeploymentScalerPriorRefusal(role.Name, "InstanceType has no accelerators")
	}
	identity := priors.NormalizeHardwareIdentity(priors.HardwareIdentity{Accelerator: instance.Status.Detail.Product})
	return instance, identity, nil
}

// modelDeploymentScalerPriorHardware 拒绝其 InstanceType 与 profile 所测硬件相矛盾的部署。
//
// 产品型号按其规范名称（canonical name）进行比较，因为厂商可能同时以产品线名称和部件号来发布同一款加速器。
func modelDeploymentScalerPriorHardware(
	role *workercore.ModelDeploymentRole, identity priors.HardwareIdentity, instance *workerapi.InstanceType,
) error {
	normalized := priors.NormalizeHardwareIdentity(identity)
	if product := instance.Status.Detail.Product; product != "" &&
		priors.NormalizeHardwareIdentity(priors.HardwareIdentity{Accelerator: product}).Accelerator != normalized.Accelerator {
		return modelDeploymentScalerPriorRefusal(role.Name, "InstanceType product conflicts with the declared hardware identity")
	}
	if memory := instance.Status.Detail.Memory; memory != "" {
		quantity, err := resource.ParseQuantity(memory)
		if err != nil || quantity.Cmp(*resource.NewQuantity(int64(normalized.MemoryGB)*(1<<30), resource.BinarySI)) != 0 {
			return modelDeploymentScalerPriorRefusal(role.Name, "InstanceType memory conflicts with the declared hardware identity")
		}
	}
	return nil
}

// modelDeploymentScalerPriorModelAnnotation 检查部署声明的模型标识。
//
// 该注解是可选的，因为部署已指定其服务的模型名称，该名称即构成模型标识。部署也会在传入的引擎参数中指定模型，而准入控制会确保服务模型名与声明的模型名保持一致。
//
// 当存在该注解时，它声明的要么是名称，要么是 digest，因为 digest 本身就是标识，而名称则是该标识所指代的模型。
//
// 精度（precision）是可选的且会做大小写折叠归一化，因为同一种量化方案可能有多种拼写，而拼写差异本身不影响权重。
func modelDeploymentScalerPriorModelAnnotation(
	target *workercore.ModelDeployment, role *workercore.ModelDeploymentRole, declared priors.Identity,
) error {
	annotation := target.Annotations[workercore.ModelDeploymentScalerModelIDAnnotationKey]
	if annotation != "" {
		if digest, isDigest := strings.CutPrefix(annotation, "model:"); isDigest {
			modelID, err := priors.Digest(declared.Model)
			if err != nil {
				return err
			}
			if digest != modelID {
				return modelDeploymentScalerPriorRefusal(role.Name, "model-id does not match the selected model identity")
			}
		} else if !priors.ModelNameMatches(declared.Model.Name, annotation) {
			return modelDeploymentScalerPriorRefusal(role.Name, "model-id does not name the selected model")
		}
	}
	precision := target.Annotations[workercore.ModelDeploymentScalerPrecisionAnnotationKey]
	if precision != "" &&
		priors.CanonicalQuantization(precision) != priors.CanonicalQuantization(declared.Model.Quantization) {
		return modelDeploymentScalerPriorRefusal(role.Name, "precision does not match the selected model identity")
	}
	return nil
}

// modelDeploymentScalerPriorHardwareAnnotation 检查部署声明的硬件标识。
//
// 该注解是可选的，因为部署选择其所需的 profile，而 profile 本身已指明了测量所使用的硬件。
//
// 当存在该注解时，它声明的要么是名称，要么是 digest，因为 digest 本身就是标识，而名称则是该标识所指代的加速器。
func modelDeploymentScalerPriorHardwareAnnotation(
	target *workercore.ModelDeployment, role *workercore.ModelDeploymentRole, declared priors.Identity,
) error {
	hardwareKey := workercore.ModelDeploymentScalerHardwareIDAnnotationKey
	switch ModelDeploymentEffectiveRoleKind(role) {
	case workercore.ModelDeploymentRoleKindPrefill:
		if target.Annotations[workercore.ModelDeploymentScalerPrefillHardwareIDAnnotationKey] != "" {
			hardwareKey = workercore.ModelDeploymentScalerPrefillHardwareIDAnnotationKey
		}
	case workercore.ModelDeploymentRoleKindDecode:
		if target.Annotations[workercore.ModelDeploymentScalerDecodeHardwareIDAnnotationKey] != "" {
			hardwareKey = workercore.ModelDeploymentScalerDecodeHardwareIDAnnotationKey
		}
	}
	annotation := target.Annotations[hardwareKey]
	if annotation == "" {
		return nil
	}
	if digest, isDigest := strings.CutPrefix(annotation, "hardware:"); isDigest {
		hardwareID, err := priors.Digest(declared.Hardware)
		if err != nil {
			return err
		}
		if digest != hardwareID {
			return modelDeploymentScalerPriorRefusal(role.Name, "hardware-id does not match the selected hardware identity")
		}
		return nil
	}
	named := priors.NormalizeHardwareIdentity(priors.HardwareIdentity{Accelerator: annotation})
	if named.Accelerator != priors.NormalizeHardwareIdentity(declared.Hardware).Accelerator {
		return modelDeploymentScalerPriorRefusal(role.Name, "hardware-id does not name the selected hardware")
	}
	return nil
}

// modelDeploymentScalerPriorEngine 检查部署运行的引擎。
//
// 引擎族必须匹配，因为基于某一种引擎的测量结果不能说明另一种引擎的表现。
//
// 不要求镜像 digest 一致，因为部署指定其运行的镜像，而数据源记录其测量的镜像，两者在 tag 或 digest 上可能不同，但底层引擎是相同的。
func modelDeploymentScalerPriorEngine(
	target *workercore.ModelDeployment, role *workercore.ModelDeploymentRole, declared priors.Identity,
) error {
	family := modelDeploymentScalerEngineFamily(target.Spec.Engine.Name)
	if family == "" {
		return modelDeploymentScalerPriorRefusal(role.Name, "the deployment names no known engine")
	}
	image := role.Image
	if image == "" {
		image = target.Spec.Engine.Image
	}
	// 当角色镜像标明了引擎族时，该镜像必须运行部署所指定的引擎。
	if imageFamily := priors.EngineFamily(image); imageFamily != "" && imageFamily != family {
		return modelDeploymentScalerPriorRefusal(role.Name, "the engine image of the role does not match the deployment engine")
	}
	// profile 必须是在部署所运行的同一引擎下测得的。当 profile 镜像未标明任何引擎族时，仅在部署运行完全相同的镜像时才予接受，因为比较两个均未标明引擎族的镜像无法判断它们背后的引擎是否相同。
	profileFamily := priors.EngineFamily(declared.Engine.Image)
	if profileFamily == "" {
		if declared.Engine.Image != image {
			return modelDeploymentScalerPriorRefusal(role.Name, "the selected profile names no engine family and the deployment runs another image")
		}
		return nil
	}
	if profileFamily != family {
		return modelDeploymentScalerPriorRefusal(role.Name, "the engine family of the selected profile does not match the deployment engine")
	}
	return nil
}

// modelDeploymentScalerEngineFamily 返回部署运行的引擎族。
func modelDeploymentScalerEngineFamily(engine string) string {
	switch engine {
	case workercore.ModelDeploymentEngineVLLM:
		return "vllm"
	case workercore.ModelDeploymentEngineSGLang:
		return "sglang"
	default:
		return ""
	}
}

// modelDeploymentScalerPriorServingConfig 读取部署运行所采用的服务配置。
//
// 对于部署未显式声明的每一项设置，采用测量运行时的对应设置，因为部署省略某个参数并不代表对引擎的声明，且 operator 负责渲染其自身控制的参数。
//
// 保留部署显式声明的每一项设置，这样当部署声明的设置与测量运行时的设置不同时，与 profile 的比对便会拒绝该部署。
//
// 保留部署的 router 配置，因为带有 router 的部署在此关卡之前已被拒绝，而在 router 之后测得的结果也不能用来描述不带 router 的部署。
func modelDeploymentScalerPriorServingConfig(
	engine string, role *workercore.ModelDeploymentRole, flags map[string]string, profile priors.ServingConfig,
) (priors.ServingConfig, error) {
	deploymentEnv, err := modelDeploymentScalerDeploymentEnv(role)
	if err != nil {
		return priors.ServingConfig{}, err
	}
	result := priors.ServingConfig{
		Router:              "none",
		KVCacheDType:        profile.KVCacheDType,
		Offload:             profile.Offload,
		PrefixCaching:       profile.PrefixCaching,
		SpeculativeDecoding: profile.SpeculativeDecoding,
		LaunchParameters:    slices.Clone(role.ExtraArgs),
		EngineEnv:           sortedEnvironmentEntries(deploymentEnv),
	}
	if dtype, stated := flags["--kv-cache-dtype"]; stated {
		result.KVCacheDType = dtype
	}
	if offload, stated := flags["--cpu-offload-gb"]; stated && offload != "0" && profile.Offload == "none" {
		result.Offload = "cpu"
	}
	switch {
	case engine == workercore.ModelDeploymentEngineVLLM && flags["--enable-prefix-caching"] != "":
		result.PrefixCaching = "enabled"
	case engine == workercore.ModelDeploymentEngineVLLM && flags["--no-enable-prefix-caching"] != "":
		result.PrefixCaching = "disabled"
	case engine == workercore.ModelDeploymentEngineSGLang && flags["--disable-radix-cache"] != "":
		result.PrefixCaching = "disabled"
	}
	return result, nil
}

// sortedEnvironmentEntries 将有效环境变量渲染为目录 schema 所存储的、已排序的 KEY=value 列表。
func sortedEnvironmentEntries(env map[string]string) []string {
	entries := make([]string, 0, len(env))
	for name, value := range env {
		entries = append(entries, name+"="+value)
	}
	slices.Sort(entries)
	return entries
}

// modelDeploymentScalerDeploymentEnv 返回角色的有效字面量环境变量，同名变量以最后一次出现的为准。
//
// 从 API 结构上看，每一项都是字面量 NAME=VALUE 对——此 ModelDeploymentEnvVar 不包含间接引用，因此此处没有需要拒绝的值来源。
func modelDeploymentScalerDeploymentEnv(role *workercore.ModelDeploymentRole) (map[string]string, error) {
	env := map[string]string{}
	for i := range role.Env {
		entry := &role.Env[i]
		if entry.Name == "" {
			return nil, fmt.Errorf("environment variable has an empty name")
		}
		env[entry.Name] = entry.Value
	}
	return env, nil
}

// modelDeploymentScalerMatchEngineEnv 在部署的有效环境变量与先验测量时的环境变量不一致时予以拒绝。
//
// 对于记录中包含的变量，此校验是双向且精确的：测量记录中有而部署缺少的变量，以及任何值上的差异，都会导致拒绝，因为其中每一项都会改变测得容量时所依赖的引擎配置。
//
// 当记录不完整时，不会依据记录未包含的变量来限制部署，因为无法观测全部环境的来源无法说明其未观测到的变量，未被观测到的环境也不等同于空环境。
//
// 拒绝信息中不列出任何变量值，因为拒绝信息会进入扩缩容器的条件状态中，而用户填写的值不应泄露给其他方。
func modelDeploymentScalerMatchEngineEnv(recorded, deployed []string, complete bool) error {
	measured := map[string]string{}
	for _, entry := range recorded {
		name, value, found := strings.Cut(entry, "=")
		if !found || name == "" {
			return fmt.Errorf("the catalog records a malformed engine environment entry")
		}
		if previous, seen := measured[name]; seen && previous != value {
			return fmt.Errorf("the catalog records environment variable %q twice with different values", name)
		}
		measured[name] = value
	}
	deployedEnv := map[string]string{}
	for _, entry := range deployed {
		name, value, _ := strings.Cut(entry, "=")
		if strings.Contains(value, "$") {
			return fmt.Errorf("environment variable %q uses runtime expansion the catalog cannot evaluate", name)
		}
		deployedEnv[name] = value
	}
	var unrecorded, differing []string
	for name, value := range deployedEnv {
		prior, ok := measured[name]
		if !ok {
			if complete {
				unrecorded = append(unrecorded, name)
			}
			continue
		}
		if value != prior {
			differing = append(differing, name)
		}
	}
	var missing []string
	for name := range measured {
		if _, carried := deployedEnv[name]; !carried {
			missing = append(missing, name)
		}
	}
	slices.Sort(unrecorded)
	slices.Sort(differing)
	slices.Sort(missing)
	if len(unrecorded) > 0 {
		return fmt.Errorf("environment variables the catalog does not record: %v", unrecorded)
	}
	if len(differing) > 0 {
		return fmt.Errorf("environment variables with values differing from the recorded measurement: %v", differing)
	}
	if len(missing) > 0 {
		return fmt.Errorf("environment variables the prior was measured with are missing: %v", missing)
	}
	return nil
}

// modelDeploymentScalerPriorOmittedDefaultsGate 拒绝省略了某引擎参数、但目录为该参数记录了相互矛盾的有效默认值的部署。
//
// 省略参数本身不会触发拒绝，因为部署将参数保留为引擎默认值并不构成对引擎的声明，且目录对未记录的默认值也不作约束。此时会采用目录中记录的设置。
//
// 若默认值与服务配置相矛盾则会拒绝，例如测量时将 KV cache 卸载到主机内存，而目录记录卸载策略为 none，这描述的是不同的引擎行为。
func modelDeploymentScalerPriorOmittedDefaultsGate(
	engine string, profile priors.ServingConfig, flags map[string]string,
) error {
	omitted := []string{"--dtype", "--cpu-offload-gb"}
	if engine == workercore.ModelDeploymentEngineVLLM {
		omitted = append(omitted, "--swap-space")
	}
	for _, flag := range omitted {
		if flags[flag] != "" {
			continue
		}
		recorded, found := profile.EffectiveDefaults[flag]
		if !found || recorded == "" {
			continue
		}
		if flag == "--dtype" {
			if !slices.Contains(modelDeploymentScalerDtypeSpellings, recorded) {
				return fmt.Errorf("the catalog records an effective default the engine does not accept for the omitted %s", flag)
			}
			continue
		}
		value, err := strconv.ParseUint(recorded, 10, 32)
		if err != nil {
			return fmt.Errorf("the catalog records an effective default that is not a whole number for the omitted %s", flag)
		}
		if profile.Offload == "none" && value != 0 {
			return fmt.Errorf(
				"the catalog records an effective default that offloads the KV cache while the serving configuration says the offload was none",
			)
		}
	}
	return nil
}

// modelDeploymentScalerDtypeSpellings 是引擎接受的 dtype 拼写列表。
//
// 这是一个封闭列表而非形态校验，因为不在该列表中的拼写是本 operator 无法保证的拼写，而未经确认的默认值不能视为引擎实际运行所依据的默认值。
var modelDeploymentScalerDtypeSpellings = []string{
	"auto", "half", "bfloat16", "bf16", "float16", "fp16", "float32", "fp32",
}

// modelDeploymentScalerOperatorEnvGate 拒绝其引擎会携带目录无法解释的环境变量的部署。
//
// operator 会向引擎容器写入环境变量；凡是它写入而测量运行时并未包含的变量，都会造成两套引擎配置之间的差异。operator 清楚自己写入的每一个变量名，因此下面将这些写入的名称声明为容量中立（capacity-neutral）；如果它开始写入未在声明列表中的新名称，在 operator 确认该变量为中立之前，该关卡将拒绝使用此先验。
//
// 该关卡同时检查声明表和实际渲染结果，因为最终进入容器的是渲染结果。仅靠手写声明表无法约束实际执行的渲染逻辑，任何由渲染器写入但声明表未包含的变量名，都是测量运行时未曾出现的变量。
//
// 此处已考虑默认值注入的变量名和制品（artifact）相关变量名，因为连接器在此关卡之前已被拒绝，而 rank 相关变量名属于跨多个 Pod 的实例，同样已被拒绝。
//
// 拒绝信息中不列出任何变量值，因为变量值属于私密信息，而变量名不是。
func modelDeploymentScalerOperatorEnvGate(
	engine string, target *workercore.ModelDeployment, role *workercore.ModelDeploymentRole,
) error {
	declared := slices.Clone(modelDeploymentDefaultedEnvNames)
	if target.Spec.Model.ArtifactRef != nil {
		declared = append(declared, modelDeploymentArtifactOwnedKeys[engine].Env...)
	}
	rendered := modelDeploymentScalerRenderedOperatorEnv(engine, role)
	return modelDeploymentScalerAccountForInjectedEnv(append(declared, rendered...))
}

// modelDeploymentScalerRenderedOperatorEnv 返回由渲染器实际写入引擎容器的 operator 环境变量名称。
//
// 它直接读取渲染结果，因为渲染结果才是引擎实际运行所依据的配置。只读取声明表的关卡无法发现渲染器新开始写入的变量名。
//
// 它会排除角色自身设置的变量名，因为那些属于部署自身的变量。测量记录的环境已覆盖这些变量，而此关卡关注的是 operator 额外注入的变量。
//
// 渲染时不传入连接器和制品（artifact），因为带有连接器和制品的情况在此关卡之前已被拒绝，其涉及的变量名已在被拒绝的位置得到处理。
func modelDeploymentScalerRenderedOperatorEnv(engine string, role *workercore.ModelDeploymentRole) []string {
	env := mergeModelDeploymentEnv(engine, role, ModelDeploymentConnectorRender{}, nil, false)
	names := make([]string, 0, len(env))
	for _, entry := range env {
		if modelDeploymentUserSetsEnv(role.Env, entry.Name) {
			continue
		}
		names = append(names, entry.Name)
	}
	return names
}

// modelDeploymentScalerAccountForInjectedEnv 拒绝尚未被 operator 声明为容量中立（capacity-neutral）的注入环境变量。
//
// 将其拆分为独立函数，是为了能够使用 operator 尚未实际写入的变量名来测试该关卡，因为一个无法针对其设计目标场景进行测试的关卡是不可信的。
func modelDeploymentScalerAccountForInjectedEnv(names []string) error {
	for _, name := range names {
		if !slices.Contains(modelDeploymentScalerCapacityNeutralEnvNames, name) {
			return fmt.Errorf(
				"the operator injects the environment variable %q and the catalog does not account for it", name,
			)
		}
	}
	return nil
}

// modelDeploymentScalerCapacityNeutralEnvNames 是 operator 写入引擎容器、且声明
// 不会影响引擎实测容量的环境变量名称列表。
//
// 这里仅记录变量名而非变量值，因为值可能包含敏感凭据，而变量名不会。
//
// 该列表采用显式列举而非从渲染结果推导，因为若从渲染结果提取，列表会随渲染
// 内容自动增长，无法察觉新增变量的写入。上方的校验门会比对两者，差异本身就是
// 信号：编写新变量的开发者必须在此明确声明该变量不会改变容量。
//
// 缓存目录指明引擎存放编译产物的位置，凭据与端点指明权重来源，两者均不决定
// 引擎的服务方式。
var modelDeploymentScalerCapacityNeutralEnvNames = []string{
	"HF_HUB_CACHE", "HF_XET_CACHE", "PIP_CACHE_DIR", "TORCH_HOME",
	"FLASHINFER_WORKSPACE_BASE", "TORCHINDUCTOR_CACHE_DIR", "TRITON_CACHE_DIR",
	"VLLM_CACHE_ROOT", "SGLANG_CACHE_DIR",
	"MC_TE_METRIC", "MC_FORCE_TCP", "MC_TCP_MAX_QUEUED_TRANSFERS_PER_PEER",
	"MC_TCP_MAX_PENDING_ADMISSIONS_PER_PEER", "MC_TCP_LANES_PER_PEER",
	"MC_TCP_ADMISSION_TIMEOUT_MS",
	"HF_TOKEN", "HF_ENDPOINT", "HF_HOME",
	"MODELSCOPE_API_TOKEN", "MODELSCOPE_DOMAIN", "MODELSCOPE_CACHE",
	"VLLM_USE_MODELSCOPE", "SGLANG_USE_MODELSCOPE",
}

// modelDeploymentScalerRedactPriorRefusal 从拒绝错误中移除环境变量的值。
//
// 它会对整条错误链进行脱敏，因为变量值可能经由解析它的读取器进入错误消息，
// 只要在一处完成清理，值可能渗入的所有位置都会被一并清理。
//
// 它会保留变量名称，因为名称仅用于指明拒绝错误涉及的变量，本身并非敏感值。
//
// 对于非拒绝类的 error，它会保持原样，因为读取器故障需要保留自身类型以供
// 准入控制和重试逻辑使用，且其自身不携带环境变量值。
func modelDeploymentScalerRedactPriorRefusal(
	err error, role *workercore.ModelDeploymentRole, profile *priors.Profile,
) error {
	var refusal *priors.RefusalError
	if !errors.As(err, &refusal) {
		return err
	}
	values := modelDeploymentScalerEnvValues(role, profile)
	redacted := *refusal
	redacted.Message = modelDeploymentScalerRedactEnvValues(refusal.Message, values)
	if len(refusal.Detail) > 0 {
		detail := make(map[string]string, len(refusal.Detail))
		for name, value := range refusal.Detail {
			detail[name] = modelDeploymentScalerRedactEnvValues(value, values)
		}
		redacted.Detail = detail
	}
	return &redacted
}

// modelDeploymentScalerEnvValues 返回角色环境变量及已记录环境变量中所携带的值。
//
// 它接受尚未确定的 profile（可为 nil），因为声明在检索目录前就已读取，且并行度
// 读取器会报告部署中配置的变量值。
func modelDeploymentScalerEnvValues(role *workercore.ModelDeploymentRole, profile *priors.Profile) []string {
	values := make([]string, 0, len(role.Env))
	for i := range role.Env {
		if role.Env[i].Value != "" {
			values = append(values, role.Env[i].Value)
		}
	}
	if profile == nil {
		return values
	}
	for _, entry := range profile.ServingConfig.EngineEnv {
		_, value, found := strings.Cut(entry, "=")
		if found && value != "" {
			values = append(values, value)
		}
	}
	return values
}

// modelDeploymentScalerRedactEnvValues 从消息中移除指定的变量值。
//
// 它仅移除带引号的形式及完整独立的 token，因为单字符的值可能出现在任意数字
// 和标识符内部，若把所有数字都删掉会导致消息丢失过多有效信息。
//
// 它通过相邻字符而非单词边界来定位 token，因为单词边界仅存在于单词字符旁，
// 而以负号或小数点开头的值前端并不存在单词边界。负数与普通值一样，也会以
// 相同方式进入消息。
func modelDeploymentScalerRedactEnvValues(message string, values []string) string {
	for _, value := range values {
		if value == "" {
			continue
		}
		message = strings.ReplaceAll(
			message, strconv.Quote(value), strconv.Quote(modelDeploymentScalerRedactedValue),
		)
		message = modelDeploymentScalerRedactWholeToken(message, value)
		if len(value) >= 4 && !strings.Contains(modelDeploymentScalerRedactedValue, value) {
			message = strings.ReplaceAll(message, value, modelDeploymentScalerRedactedValue)
		}
	}
	return message
}

// modelDeploymentScalerRedactWholeToken 将作为独立 token 出现的变量值替换为脱敏标记。
//
// 它不会改动作为更长单词一部分出现的值，因为单字符的值可能出现在各类标识符
// 和数字内部。
func modelDeploymentScalerRedactWholeToken(message, value string) string {
	var b strings.Builder
	b.Grow(len(message))
	rest := message
	offset := 0
	for {
		index := strings.Index(rest, value)
		if index < 0 {
			b.WriteString(rest)
			return b.String()
		}
		start := offset + index
		end := start + len(value)
		if modelDeploymentScalerTokenStandsAlone(message, start, end, value) {
			b.WriteString(rest[:index])
			b.WriteString(modelDeploymentScalerRedactedValue)
			rest = rest[index+len(value):]
			offset = end
			continue
		}
		b.WriteString(rest[:index+1])
		rest = rest[index+1:]
		offset = start + 1
	}
}

// modelDeploymentScalerTokenStandsAlone 判断给定值两侧的字符是否使其保持独立，
// 而不会与相邻字符共同组成更长的单词。
func modelDeploymentScalerTokenStandsAlone(message string, start, end int, value string) bool {
	before := byte(0)
	if start > 0 {
		before = message[start-1]
	}
	after := byte(0)
	if end < len(message) {
		after = message[end]
	}
	return modelDeploymentScalerTokenEdge(value[0], before) &&
		modelDeploymentScalerTokenEdge(value[len(value)-1], after)
}

// modelDeploymentScalerTokenEdge 判断值边缘侧的相邻字符是否仍使其构成独立 token。
//
// 单词字符旁必须是非单词字符，因为两个单词字符相邻会组合成更长的单词。
// 非单词字符则可与任意字符相邻，例如负号属于其后紧跟的数字。
func modelDeploymentScalerTokenEdge(edge, neighbor byte) bool {
	if modelDeploymentScalerWordByte(edge) {
		return !modelDeploymentScalerWordByte(neighbor)
	}
	return true
}

// modelDeploymentScalerWordByte 判断字节是否为字母、数字或下划线。
func modelDeploymentScalerWordByte(b byte) bool {
	return b == '_' || (b >= '0' && b <= '9') || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}

// modelDeploymentScalerRedactedValue 是脱敏值被替换后的占位文本。
const modelDeploymentScalerRedactedValue = "[redacted]"

// modelDeploymentScalerPriorFlags 按引擎的参数拼写规则读取身份敏感的标志。
// 完整的有序 argv（包括未识别的性能参数）仍保留在目录匹配中。
func modelDeploymentScalerPriorFlags(engine string, args []string) (map[string]string, error) {
	if engine != workercore.ModelDeploymentEngineVLLM && engine != workercore.ModelDeploymentEngineSGLang {
		return nil, fmt.Errorf("unsupported engine %q", engine)
	}
	// 划定“身份敏感参数”白名单
	values := map[string]string{}
	// --dtype / --quantization：模型权重计算精度（如 fp16 / bf16 / fp8 / fp4），直接决定算力速度和显存占用，对吞吐影响翻倍。
	// --kv-cache-dtype：KV Cache 的存储精度（比如 fp8 KV Cache 可以塞下更大 batch，显著改变吞吐）。
	// --revision / --tokenizer / --tokenizer-revision：权重和分词器的版本——换了分词器，同样的句子切出的 token 数都不一样，tokens/s 指标就不可比了。
	// --cpu-offload-gb / --swap-space：把模型权重或 KV Cache 卸载/交换到宿主机内存（CPU RAM），一旦走 PCIe 内存交换，延迟会暴增，跟纯显存压测完全是两个世界。
	// --enable-prefix-caching / --no-enable-prefix-caching（vLLM）与 --disable-radix-cache（SGLang）：前缀缓存（Prefix Caching / RadixAttention）开关——复用相同系统提示词的 KV Cache 会大幅拉低 TTFT，所以必须记录用户有没有改过它！
	valueFlags := []string{
		"--dtype", "--kv-cache-dtype", "--revision", "--tokenizer", "--tokenizer-revision",
		"--quantization", "--cpu-offload-gb", "--swap-space",
	}
	boolFlags := []string{"--enable-prefix-caching", "--no-enable-prefix-caching", "--disable-radix-cache"}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			return nil, fmt.Errorf("catalog cannot interpret arguments after --")
		}
		key, value, inline := strings.Cut(arg, "=")
		if engine == workercore.ModelDeploymentEngineVLLM {
			key = strings.ReplaceAll(key, "_", "-")
			if strings.HasPrefix(key, "-q") && len(key) > 2 && !inline {
				value, key, inline = key[2:], "--quantization", true
			}
		}
		if key == "-q" && engine == workercore.ModelDeploymentEngineVLLM {
			key = "--quantization"
		}
		if key == "--tokenizer-path" && engine == workercore.ModelDeploymentEngineSGLang {
			key = "--tokenizer"
		}
		if key == "--model" || key == "--model-path" || strings.Contains(key, "speculative") ||
			strings.Contains(key, "hierarchical-cache") || strings.Contains(key, "kv-transfer") ||
			strings.Contains(key, "config") || strings.Contains(key, "lora") {
			return nil, fmt.Errorf("catalog cannot establish settings from %s", key)
		}
		if slices.Contains(boolFlags, key) {
			if inline {
				return nil, fmt.Errorf("%s requires its bare boolean spelling", key)
			}
			if engine == workercore.ModelDeploymentEngineSGLang && key != "--disable-radix-cache" ||
				engine == workercore.ModelDeploymentEngineVLLM && key == "--disable-radix-cache" {
				return nil, fmt.Errorf("%s is not an engine-native cache flag", key)
			}
			values[key] = "true"
			switch key {
			case "--enable-prefix-caching":
				delete(values, "--no-enable-prefix-caching")
			case "--no-enable-prefix-caching":
				delete(values, "--enable-prefix-caching")
			}
			continue
		}
		if !slices.Contains(valueFlags, key) {
			for _, known := range append(slices.Clone(valueFlags), boolFlags...) {
				if strings.HasPrefix(key, "--") && strings.HasPrefix(known, key) {
					return nil, fmt.Errorf("catalog requires the full identity flag %s", known)
				}
			}
			continue
		}
		if !inline {
			if i+1 >= len(args) || strings.HasPrefix(args[i+1], "-") {
				return nil, fmt.Errorf("%s requires a value", key)
			}
			i++
			value = args[i]
		}
		if value == "" {
			return nil, fmt.Errorf("%s requires a nonempty value", key)
		}
		values[key] = value
	}
	return values, nil
}
