{{/*
chart 名称可由 nameOverride 覆盖。
*/}}
{{- define "gpustack-operator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
完整资源名前缀。若 release 名已包含 chart 名，直接使用它，避免重复拼接。
*/}}
{{- define "gpustack-operator.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := include "gpustack-operator.name" . -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
返回本次部署使用的命名空间。
*/}}
{{- define "gpustack-operator.namespace" -}}
{{- default .Release.Namespace .Values.namespaceOverride -}}
{{- end -}}

{{/*
生成 chart 名称和版本的标签，例如 gpustack-operator-0.5.0。
*/}}
{{- define "gpustack-operator.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Worker 控制平面的名称采用完整前缀加 -worker。
*/}}
{{- define "gpustack-operator.worker.fullname" -}}
{{- printf "%s-worker" (include "gpustack-operator.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
设备管理器的名称采用完整前缀加 -device-manager。
*/}}
{{- define "gpustack-operator.deviceManager.fullname" -}}
{{- printf "%s-device-manager" (include "gpustack-operator.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
模型管理器的名称采用完整前缀加 -model-manager。
*/}}
{{- define "gpustack-operator.modelManager.fullname" -}}
{{- printf "%s-model-manager" (include "gpustack-operator.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
合并组件与父 chart 的镜像设置，得到完整引用。全局 registry 和 namespace 可分别替换镜像仓库地址和组织名。
*/}}
{{- define "gpustack-operator.image" -}}
{{- $root := .root -}}
{{- $image := merge (deepCopy .overrides) $root.Values.image -}}
{{- $repository := $image.repository -}}
{{- $registry := "" -}}
{{- $segments := splitList "/" $repository -}}
{{- if and (gt (len $segments) 1) (or (contains "." (first $segments)) (contains ":" (first $segments))) -}}
{{- $registry = first $segments -}}
{{- $repository = join "/" (rest $segments) -}}
{{- end -}}
{{- with $root.Values.global.imageNamespace -}}
{{- $repository = printf "%s/%s" . (last (splitList "/" $repository)) -}}
{{- end -}}
{{- /*
    global.hub 是 global.imageRegistry 的别名，便于包含 Istio 子 chart 的父 chart 共用镜像仓库配置。
  */ -}}
{{- with (coalesce $root.Values.global.imageRegistry $root.Values.global.hub) -}}
{{- $registry = trimSuffix "/" . -}}
{{- end -}}
{{- with $registry -}}
{{- $repository = printf "%s/%s" . $repository -}}
{{- end -}}
{{- $tag := default (printf "v%s" (trimPrefix "v" $root.Chart.AppVersion)) $image.tag -}}
{{- printf "%s:%s" $repository $tag -}}
{{- end -}}

{{/*
合并组件和父 chart 的拉取策略。global.imagePullPolicy 非空时优先使用。
*/}}
{{- define "gpustack-operator.imagePullPolicy" -}}
{{- $image := merge (deepCopy .overrides) .root.Values.image -}}
{{- default $image.pullPolicy .root.Values.global.imagePullPolicy -}}
{{- end -}}

{{/*
所有资源共用的元数据标签。
*/}}
{{- define "gpustack-operator.commonLabels" -}}
helm.sh/chart: {{ include "gpustack-operator.chart" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/part-of: gpustack-operator
{{- with .Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{/*
Worker 的元数据标签。
*/}}
{{- define "gpustack-operator.worker.labels" -}}
{{ include "gpustack-operator.commonLabels" . }}
app.kubernetes.io/component: operator
app.kubernetes.io/name: {{ include "gpustack-operator.worker.fullname" . }}
{{- end -}}

{{/*
Worker 的选择器标签保持稳定，支持跨版本升级。
*/}}
{{- define "gpustack-operator.worker.selectorLabels" -}}
app.kubernetes.io/part-of: gpustack-operator
app.kubernetes.io/component: operator
app.kubernetes.io/name: {{ include "gpustack-operator.worker.fullname" . }}
{{- end -}}

{{/*
设备管理器的元数据标签。
*/}}
{{- define "gpustack-operator.deviceManager.labels" -}}
{{ include "gpustack-operator.commonLabels" . }}
app.kubernetes.io/component: device-manager
app.kubernetes.io/name: {{ include "gpustack-operator.deviceManager.fullname" . }}
{{- end -}}

{{/*
设备管理器稳定的选择器标签。
*/}}
{{- define "gpustack-operator.deviceManager.selectorLabels" -}}
app.kubernetes.io/part-of: gpustack-operator
app.kubernetes.io/component: device-manager
app.kubernetes.io/name: {{ include "gpustack-operator.deviceManager.fullname" . }}
{{- end -}}

{{/*
模型管理器的元数据标签。
*/}}
{{- define "gpustack-operator.modelManager.labels" -}}
{{ include "gpustack-operator.commonLabels" . }}
app.kubernetes.io/component: model-manager
app.kubernetes.io/name: {{ include "gpustack-operator.modelManager.fullname" . }}
{{- end -}}

{{/*
模型管理器稳定的选择器标签。
*/}}
{{- define "gpustack-operator.modelManager.selectorLabels" -}}
app.kubernetes.io/part-of: gpustack-operator
app.kubernetes.io/component: model-manager
app.kubernetes.io/name: {{ include "gpustack-operator.modelManager.fullname" . }}
{{- end -}}

{{/*
生成 Kueue 的资源转换规则，将同一厂商的各类加速器资源折算到其 credits 资源，使队列能以单一预算进行准入。换算应与 pkg/nodefeature 一致。
*/}}
{{- define "gpustack-operator.kueue.transformations" -}}
{{- $creditsPerCard := 1600000 -}}
{{- $sharedMaxSize := 10 -}}
{{- $resourceMaxUnits := 1600000 -}}
{{- range $manu := (keys .Values.global.manufacturers | sortAlpha) }}
{{- $row := index $.Values.global.manufacturers $manu }}
{{- $credits := printf "credits.gpustack.ai/%s" $manu }}
- input: {{ $row.resourceName }}
  strategy: Replace
  outputs:
    {{ $credits }}: {{ $creditsPerCard | quote }}
- input: {{ $row.resourceName }}.shared
  strategy: Replace
  outputs:
    {{ $credits }}: {{ div $creditsPerCard $sharedMaxSize | quote }}
- input: {{ $row.resourceName }}.sliced.units
  strategy: Replace
  multiplyBy: {{ $row.resourceName }}.sliced
  outputs:
    {{ $credits }}: {{ div $creditsPerCard $resourceMaxUnits | quote }}
{{- if $row.partitionKind }}
{{- /*
  硬件分区只占一张卡，其 units 已表示完整需求。逻辑切片则须根据 .sliced 资源的卡数折算。
*/}}
- input: {{ $row.resourceName }}.partitioned.units
  strategy: Replace
  outputs:
    {{ $credits }}: {{ div $creditsPerCard $resourceMaxUnits | quote }}
{{- end }}
{{- end }}
{{- end -}}

{{/*
默认将 kube-system 和本 release 的命名空间排除在 Kueue 管理范围外，避免 Kueue 管理自己的 Pod。显式配置的 managedJobsNamespaceSelector 优先。
*/}}
{{- define "gpustack-operator.kueue.managedJobsNamespaceSelector" -}}
matchExpressions:
  - key: kubernetes.io/metadata.name
    operator: NotIn
    values: [ kube-system, {{ .Release.Namespace }} ]
{{- end -}}

{{/*
将 global.manufacturers 中已配置的厂商属性转换成 pkg/nodefeature 使用的环境变量，未指定的字段使用程序默认值。
*/}}
{{- define "gpustack-operator.manufacturer.env" -}}
{{- $prefix := printf "GPUSTACK_%s" (upper .name) -}}
{{- with .identity.pciVendorID }}
- name: {{ $prefix }}_PCI_VENDOR_ID
  value: {{ . | quote }}
{{- end }}
{{- with .identity.resourceName }}
- name: {{ $prefix }}_ACCELERATABLE_RESOURCE_NAME
  value: {{ . | quote }}
{{- end }}
{{- with .identity.runtimeName }}
- name: {{ $prefix }}_ACCELERATABLE_RUNTIME_NAME
  value: {{ . | quote }}
{{- end }}
{{- with .identity.partitionKind }}
- name: {{ $prefix }}_PARTITION_KIND
  value: {{ . | quote }}
{{- end }}
{{- end -}}

{{/*
统一决定 Worker 和 Kueue 的服务证书来源。auto 根据集群中的 cert-manager CRD 判定，显式配置优先。
*/}}
{{- define "gpustack-operator.certmanager.enabled" -}}
{{- $mode := .Values.global.certmanager.enabled | toString -}}
{{- if eq $mode "true" -}}
true
{{- else if eq $mode "false" -}}
{{- else if lookup "apiextensions.k8s.io/v1" "CustomResourceDefinition" "" "certificates.cert-manager.io" -}}
true
{{- end -}}
{{- end -}}
