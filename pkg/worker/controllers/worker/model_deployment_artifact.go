package worker

import (
	"net/url"

	core "k8s.io/api/core/v1"
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
