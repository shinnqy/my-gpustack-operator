package worker

import (
	"net/url"

	core "k8s.io/api/core/v1"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

const (
	// modelDeploymentModelVolumeName and modelDeploymentModelCacheVolumeName name the weights'
	// volumes. They share no prefix with the role's own "additional-<n>" volumes or the connector's.
	modelDeploymentModelVolumeName      = "gpustack-model"
	modelDeploymentModelCacheVolumeName = "gpustack-model-cache"

	// modelDeploymentRevisionArg pins a hub download, weights and tokenizer alike: both supported
	// engines load the tokenizer at the same revision unless told otherwise, which is owned.
	modelDeploymentRevisionArg = "--revision"

	modelDeploymentHFHomeEnv     = "HF_HOME"
	modelDeploymentHFEndpointEnv = "HF_ENDPOINT"
	modelDeploymentHFTokenEnv    = "HF_TOKEN"
	modelDeploymentHTTPSProxyEnv = "HTTPS_PROXY"
	modelDeploymentNoProxyEnv    = "NO_PROXY"

	modelDeploymentModelScopeCacheEnv  = "MODELSCOPE_CACHE"
	modelDeploymentModelScopeDomainEnv = "MODELSCOPE_DOMAIN"
	modelDeploymentModelScopeTokenEnv  = "MODELSCOPE_API_TOKEN"
	// The engines' switches that route their own download through their bundled ModelScope SDK
	// (measured, PoC-C): each engine reads only its own.
	modelDeploymentVLLMModelScopeSwitch   = "VLLM_USE_MODELSCOPE"
	modelDeploymentSGLangModelScopeSwitch = "SGLANG_USE_MODELSCOPE"
)

// The hub kinds a hub artifact's download environment is rendered for. An engine downloads from
// one hub, and the names it reads differ by hub.
const (
	modelDeploymentHubHuggingFace = "huggingFace"
	modelDeploymentHubModelScope  = "modelscope"
)

// ModelDeploymentArtifactRender is how a deployment's weights reach its engine, resolved from the
// ModelArtifact by the reconciler and handed to the render as values, so the render stays pure.
//
// A nil one is a deployment that names no artifact, which renders exactly what it rendered before
// the field existed.
type ModelDeploymentArtifactRender struct {
	// Delivery is PVC, Engine, Node or Image.
	Delivery workercore.ModelDeploymentModelDelivery

	// ArtifactName, ArtifactUID and ManifestDigest are what a node-delivered volume names, as hints
	// the node's plugin checks against the API before it mounts anything.
	ArtifactName   string
	ArtifactUID    string
	ManifestDigest string

	// ClaimName and Path are a claim artifact's claim and the directory inside it.
	ClaimName string
	Path      string

	// ImageReference is an image artifact's digest-pinned reference, rendered as a Kubernetes image
	// volume that kubelet pulls and mounts read-only.
	ImageReference string

	// Repository and Revision are a hub artifact's repository and resolved commit, SecretName the
	// Secret holding its token or "", and SizeBytes its manifest's total size.
	Repository string
	Revision   string
	SecretName string
	SizeBytes  int64

	// Hub is which hub a hub artifact downloads from, one of the modelDeploymentHub kinds. The
	// download environment's names differ by hub; a non-hub delivery renders none.
	Hub string

	// Endpoint, HTTPSProxy and NoProxy are the administrator's Settings, read by the reconciler.
	Endpoint   string
	HTTPSProxy string
	NoProxy    string
}

// env returns the environment an engine download needs: the owned entries, then the defaulted
// ones a role's own value replaces. A claim needs none, and a take-over role gets none.
func (a *ModelDeploymentArtifactRender) env(takeOver bool, engine string) (owned, defaulted []core.EnvVar) {
	if takeOver || a.Delivery != workercore.ModelDeploymentModelDeliveryEngine {
		return nil, nil
	}

	switch a.Hub {
	case modelDeploymentHubModelScope:
		owned = []core.EnvVar{
			{Name: modelDeploymentModelScopeCacheEnv, Value: ModelDeploymentModelCachePath},
			// The SDK takes a bare host and prefixes the scheme itself (measured in the runner's
			// own SDK); the Setting keeps the full-URL shape the Hugging Face endpoint uses.
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

// modelScopeSwitch is the environment name that routes an engine's own download through its
// bundled ModelScope SDK. An engine the table does not know reads neither switch, so none is
// rendered for it.
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

// endpointHost is the URL's bare host, for the one consumer that takes a host rather than a URL.
// A value that does not parse rides as it is: the admission that passed it has already demanded a
// schema and a host.
func endpointHost(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return raw
	}

	return u.Host
}
