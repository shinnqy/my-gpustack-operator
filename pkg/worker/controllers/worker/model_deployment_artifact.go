package worker

import (
	"net/url"

	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/modelstore"
)

const (
	// modelDeploymentModelVolumeName 和 modelDeploymentModelCacheVolumeName 分别标识模型权重和缓存卷，
	// 名称与角色的 additional-<n> 卷及连接器创建的卷分开。
	modelDeploymentModelVolumeName      = "gpustack-model"
	modelDeploymentModelCacheVolumeName = "gpustack-model-cache"

	// modelDeploymentRevisionArg 将权重和分词器固定在同一仓库版本。两种受支持的引擎默认使用同一个 revision。
	modelDeploymentRevisionArg = "--revision"

	modelDeploymentHFHomeEnv     = "HF_HOME"
	modelDeploymentHFEndpointEnv = "HF_ENDPOINT"
	modelDeploymentHFTokenEnv    = "HF_TOKEN"
	modelDeploymentHTTPSProxyEnv = "HTTPS_PROXY"
	modelDeploymentNoProxyEnv    = "NO_PROXY"

	modelDeploymentModelScopeCacheEnv  = "MODELSCOPE_CACHE"
	modelDeploymentModelScopeDomainEnv = "MODELSCOPE_DOMAIN"
	modelDeploymentModelScopeTokenEnv  = "MODELSCOPE_API_TOKEN"
	// 各引擎各自读取对应的开关，选择通过内置的 ModelScope SDK 下载。
	modelDeploymentVLLMModelScopeSwitch   = "VLLM_USE_MODELSCOPE"
	modelDeploymentSGLangModelScopeSwitch = "SGLANG_USE_MODELSCOPE"
)

// 模型仓库类型决定下载时需要的环境变量。每次只从一个仓库下载。
const (
	modelDeploymentHubHuggingFace = "huggingFace"
	modelDeploymentHubModelScope  = "modelscope"
)

// modelDeploymentCacheHeadroomMin 是引擎下载缓存在清单大小之上保留的最小余量，
// modelDeploymentCacheHeadroomDivisor 则是其他情况下按比例保留余量的除数。
// 这部分余量用于覆盖下载过程中在权重文件之外写入的临时文件以及 hub 客户端自身的元数据。
const (
	modelDeploymentCacheHeadroomMin     = 1 << 30
	modelDeploymentCacheHeadroomDivisor = 10
)

// ModelDeploymentArtifactRender 保存协调器从 ModelArtifact 解析出的权重交付参数，渲染阶段只消费这些值。
// 为 nil 表示部署没有指定模型制品，沿用其原有配置。
type ModelDeploymentArtifactRender struct {
	// Delivery 指定 PVC、Engine、Node 或 Image 交付方式。
	Delivery workercore.ModelDeploymentModelDelivery

	// ArtifactName、ArtifactUID 和 ManifestDigest 写入节点交付卷。插件在挂载前用 API 对象核实这些信息。
	ArtifactName   string
	ArtifactUID    string
	ManifestDigest string

	// ClaimName 和 Path 分别表示权重所在的 PVC 名称和卷内目录。
	ClaimName string
	Path      string

	// ImageReference 用摘要固定镜像，kubelet 拉取后作为只读镜像卷挂载。
	ImageReference string

	// Repository 和 Revision 标识仓库及解析后的提交。SecretName 保存令牌所在 Secret 的名称，可为空。
	// SizeBytes 是清单的总字节数。
	Repository string
	Revision   string
	SecretName string
	SizeBytes  int64

	// Hub 标识引擎下载时要使用的模型仓库，选择对应的环境变量。其他交付方式无需仓库变量。
	Hub string

	// Endpoint、HTTPSProxy 和 NoProxy 来自管理员配置，由协调器读取。
	Endpoint   string
	HTTPSProxy string
	NoProxy    string
}

// model 返回引擎启动命令中指定的模型标识：对于 PVC 或节点交付制品返回固定挂载路径，
// 对于引擎自行下载返回仓库名。
func (a *ModelDeploymentArtifactRender) model() string {
	if a.Delivery == workercore.ModelDeploymentModelDeliveryEngine {
		return a.Repository
	}

	return ModelDeploymentModelMountPath
}

// args 返回紧跟在引擎基础命令之后的受管参数。
func (a *ModelDeploymentArtifactRender) args() []string {
	if a.Delivery != workercore.ModelDeploymentModelDeliveryEngine {
		return nil
	}

	return []string{modelDeploymentRevisionArg, a.Revision}
}

// volumes 返回权重所需的 Volume 列表及主容器对应的 VolumeMount 列表。
// 接管了命令行的角色仍会挂载声明的卷，但不会获得引擎下载缓存卷：由接管命令的作者自行下载权重。
func (a *ModelDeploymentArtifactRender) volumes(takeOver bool) ([]core.Volume, []core.VolumeMount) {
	switch {
	case a.Delivery == workercore.ModelDeploymentModelDeliveryNode:
		return []core.Volume{{
			Name: modelDeploymentModelVolumeName, VolumeSource: a.nodeVolumeSource(),
		}}, []core.VolumeMount{{
			Name: modelDeploymentModelVolumeName, MountPath: ModelDeploymentModelMountPath, ReadOnly: true,
		}}
	case a.Delivery == workercore.ModelDeploymentModelDeliveryPVC:
		return []core.Volume{{
			Name: modelDeploymentModelVolumeName,
			VolumeSource: core.VolumeSource{PersistentVolumeClaim: &core.PersistentVolumeClaimVolumeSource{
				ClaimName: a.ClaimName, ReadOnly: true,
			}},
		}}, []core.VolumeMount{{
			Name: modelDeploymentModelVolumeName, MountPath: ModelDeploymentModelMountPath,
			SubPath: a.Path, ReadOnly: true,
		}}
	case a.Delivery == workercore.ModelDeploymentModelDeliveryImage:
		// 拉取策略保留默认值：按摘要固定的镜像默认使用 IfNotPresent，已缓存镜像的节点不会重复拉取。
		// 无需额外缓存卷或下载环境变量：kubelet 自身的镜像存储承担了全部交付工作。
		return []core.Volume{{
			Name: modelDeploymentModelVolumeName,
			VolumeSource: core.VolumeSource{Image: &core.ImageVolumeSource{
				Reference: a.ImageReference,
			}},
		}}, []core.VolumeMount{{
			Name: modelDeploymentModelVolumeName, MountPath: ModelDeploymentModelMountPath, ReadOnly: true,
		}}
	case takeOver:
		return nil, nil
	default:
		limit := a.cacheSize()
		return []core.Volume{{
			Name: modelDeploymentModelCacheVolumeName,
			VolumeSource: core.VolumeSource{EmptyDir: &core.EmptyDirVolumeSource{
				SizeLimit: &limit,
			}},
		}}, []core.VolumeMount{{
			Name: modelDeploymentModelCacheVolumeName, MountPath: ModelDeploymentModelCachePath,
		}}
	}
}

// nodeVolumeSource 生成节点 CSI 插件使用的内联卷。插件在 Pod 所在命名空间核对制品和摘要后才挂载。
// 认证 Secret 通过 kubelet 传给插件，令牌不会进入 Pod 环境变量。
func (a *ModelDeploymentArtifactRender) nodeVolumeSource() core.VolumeSource {
	csi := &core.CSIVolumeSource{
		Driver:   modelstore.DriverName,
		ReadOnly: ptr.To(true),
		VolumeAttributes: map[string]string{
			modelArtifactVolumeAttrArtifact:    a.ArtifactName,
			modelArtifactVolumeAttrArtifactUID: a.ArtifactUID,
			modelArtifactVolumeAttrDigest:      a.ManifestDigest,
		},
	}
	if a.SecretName != "" {
		csi.NodePublishSecretRef = &core.LocalObjectReference{Name: a.SecretName}
	}

	return core.VolumeSource{CSI: csi}
}

// 控制器写入和节点插件读取的模型卷属性键。
const (
	modelArtifactVolumeAttrArtifact    = modelstore.VolumeAttrArtifact
	modelArtifactVolumeAttrArtifactUID = modelstore.VolumeAttrArtifactUID
	modelArtifactVolumeAttrDigest      = modelstore.VolumeAttrManifestDigest
)

// cacheSize 计算引擎下载缓存的上限：在清单总大小的基础上增加十分之一的余量，且不低于
// modelDeploymentCacheHeadroomMin。清单覆盖整个提交，而引擎只下载其中子集，因此该值能提供上界保护。
func (a *ModelDeploymentArtifactRender) cacheSize() resource.Quantity {
	headroom := max(a.SizeBytes/modelDeploymentCacheHeadroomDivisor, modelDeploymentCacheHeadroomMin)

	return *resource.NewQuantity(a.SizeBytes+headroom, resource.BinarySI)
}

// env 返回引擎下载需要的环境变量，区分由控制器指定的值和允许角色覆盖的默认值。
// 通过 PVC 交付或由角色接管命令时无需注入。
func (a *ModelDeploymentArtifactRender) env(takeOver bool, engine string) (owned, defaulted []core.EnvVar) {
	if takeOver || a.Delivery != workercore.ModelDeploymentModelDeliveryEngine {
		return nil, nil
	}

	switch a.Hub {
	case modelDeploymentHubModelScope:
		owned = []core.EnvVar{
			{Name: modelDeploymentModelScopeCacheEnv, Value: ModelDeploymentModelCachePath},
			// ModelScope SDK 自行补充协议头，只接受主机名。集群设置仍采用与 Hugging Face 相同的完整 URL 形式。
			{Name: modelDeploymentModelScopeDomainEnv, Value: endpointHost(a.Endpoint)},
		}
		if sw := modelScopeSwitch(engine); sw != "" {
			owned = append(owned, core.EnvVar{Name: sw, Value: "true"})
		}
		if a.SecretName != "" {
			owned = append(owned, core.EnvVar{Name: modelDeploymentModelScopeTokenEnv, ValueFrom: &core.EnvVarSource{
				SecretKeyRef: &core.SecretKeySelector{
					LocalObjectReference: core.LocalObjectReference{Name: a.SecretName},
					Key:                  modelArtifactTokenKey,
				},
			}})
		}
	default:
		owned = []core.EnvVar{
			{Name: modelDeploymentHFHomeEnv, Value: ModelDeploymentModelCachePath},
			{Name: modelDeploymentHFEndpointEnv, Value: a.Endpoint},
		}
		if a.SecretName != "" {
			owned = append(owned, core.EnvVar{Name: modelDeploymentHFTokenEnv, ValueFrom: &core.EnvVarSource{
				SecretKeyRef: &core.SecretKeySelector{
					LocalObjectReference: core.LocalObjectReference{Name: a.SecretName},
					Key:                  modelArtifactTokenKey,
				},
			}})
		}
	}
	if a.HTTPSProxy != "" {
		defaulted = append(defaulted, core.EnvVar{Name: modelDeploymentHTTPSProxyEnv, Value: a.HTTPSProxy})
	}
	if a.NoProxy != "" {
		defaulted = append(defaulted, core.EnvVar{Name: modelDeploymentNoProxyEnv, Value: a.NoProxy})
	}

	return owned, defaulted
}

// modelScopeSwitch 返回当前引擎选择 ModelScope SDK 所需的环境变量名。
// 引擎各自读取不同的变量，没有对应开关时返回空字符串。
func modelScopeSwitch(engine string) string {
	switch engine {
	case workercore.ModelDeploymentEngineVLLM:
		return modelDeploymentVLLMModelScopeSwitch
	case workercore.ModelDeploymentEngineSGLang:
		return modelDeploymentSGLangModelScopeSwitch
	default:
		return ""
	}
}

// endpointHost 从完整地址中取出 SDK 要求的主机部分。配置已经过 URL 校验，异常输入仍有回退处理。
func endpointHost(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return raw
	}

	return u.Host
}

// raiseEphemeralStorageLimit 将引擎下载缓存的上限叠加到主容器的 ephemeral-storage limit 上，
// 同时保持 request 不变。
//
// kubelet 不仅按 emptyDir 自身的 sizeLimit 计费，还会将 emptyDir 的用量计入 Pod 的
// ephemeral-storage limit（即各容器 limit 之和）。容器默认 limit 来自 InstanceType 的本地存储，
// 若模型大于该值，即使卷的 sizeLimit 足够大，Pod 仍会在下载中途被驱逐。request 由 Kueue 和调度器
// 记账，因此维持 InstanceType 的声明值。
func (a *ModelDeploymentArtifactRender) raiseEphemeralStorageLimit(c *core.Container, takeOver bool) {
	if takeOver || a.Delivery != workercore.ModelDeploymentModelDeliveryEngine {
		return
	}
	// 未设置 limit 的容器本来就没有上限可提高，直接新增反而会凭空施加限制。
	limit, ok := c.Resources.Limits[core.ResourceEphemeralStorage]
	if !ok {
		return
	}
	limit.Add(a.cacheSize())
	c.Resources.Limits[core.ResourceEphemeralStorage] = limit
}
