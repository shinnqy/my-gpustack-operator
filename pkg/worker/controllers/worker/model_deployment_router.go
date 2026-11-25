package worker

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	app "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	rbac "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/systemmeta"
	"gpustack.ai/gpustack/pkg/utils/json"
	"gpustack.ai/gpustack/pkg/utils/strconvx"
	"gpustack.ai/gpustack/pkg/utils/stringx"
	"gpustack.ai/gpustack/pkg/worker/kvcache"
	"gpustack.ai/gpustack/pkg/worker/kvcache/inject"
	"gpustack.ai/gpustack/pkg/worker/kvcache/router"
	"gpustack.ai/gpustack/pkg/worker/settings"
)

const (
	modelDeploymentRouterConfigKey            = "epp.yaml"
	modelDeploymentRouterEnvoyConfigKey       = "envoy.yaml"
	modelDeploymentRouterSelectorKey          = "endpoint-selector"
	modelDeploymentRouterTargetPortsKey       = "endpoint-target-ports"
	modelDeploymentRouterExtraArgsKey         = "extra-args.json"
	modelDeploymentRouterConfigHash           = "modeldeployment.gpustack.ai/router-config-hash"
	modelDeploymentRouterLabelKey             = "modeldeployment.gpustack.ai/router"
	modelDeploymentRouterHTTPPort       int32 = 8000
	modelDeploymentRouterEPPPort        int32 = 9002
	// modelDeploymentRouterMetricsPort 是所有路由器暴露 Prometheus 指标的端口，以便使用同一套
	// 抓取配置即可覆盖全部路由器。llm-d picker 默认监听此端口；通过 argv 配置的路由器则会
	// 显式指定监听该端口，因为它们自身的默认端口不同。
	modelDeploymentRouterMetricsPort  int32 = 9090
	modelDeploymentResourceNoteRouter       = "router"
)

// ModelDeploymentRouterObjects 包含单个托管路由器所需的完整命名空间级对象集合，以及
// 随之渲染出的类型化契约。
type ModelDeploymentRouterObjects struct {
	Deployment     *app.Deployment
	ConfigMap      *core.ConfigMap
	Service        *core.Service
	ServiceAccount *core.ServiceAccount
	Role           *rbac.Role
	RoleBinding    *rbac.RoleBinding

	// Contract 是上述对象渲染所用相同参数的类型化映射。
	Contract workercore.ModelDeploymentRouterStatus
}

// modelDeploymentRouterProtectedRole 报告部署中是否存在任意一个受保护组角色。
//
// 路由器与 Service 必须保持一致，否则请求可能抵达 Service 并未代理的端点。受保护组从首次启用
// 起就需要匹配合格端点（不论是否已完成首轮裁决），因为缺少该条件的暴露面根本不会读取资格标签。
func modelDeploymentRouterProtectedRole(md *workercore.ModelDeployment) bool {
	for i := range md.Spec.Roles {
		if modelDeploymentProtectedRole(md, &md.Spec.Roles[i]) {
			return true
		}
	}

	return false
}

// IMPORTANT: 渲染 Router 需要的全部 K8s 对象与服务发现契约
func renderModelDeploymentRouterObjects(
	ctx context.Context, md *workercore.ModelDeployment, manufacturers map[string]string,
	retainedEligibility bool,
) (ModelDeploymentRouterObjects, error) {
	name := md.Name + "-router"
	labels := map[string]string{
		modelDeploymentLabelKeyName:     modelDeploymentLabelValueName,
		modelDeploymentLabelKeyInstance: md.Name,
		modelDeploymentRouterLabelKey:   md.Spec.Router.Name,
	}
	note := func(object ctrlcli.Object) {
		systemmeta.NoteResource(object, ModelDeploymentResourceType,
			map[string]string{modelDeploymentResourceNoteRouter: md.Spec.Router.Name})
		kubemeta.ControlOnWithoutBlock(object, md, workercore.SchemeGroupVersionKind("ModelDeployment"))
	}

	roles := make([]router.Role, 0, len(md.Spec.Roles))
	ports := make([]int32, 0, len(md.Spec.Roles))
	// 接收静态编码器列表的路由器拿到的是角色级 Service URL，而非单个副本地址：Service 代理所有
	// 副本，因此扩缩容不会改变路由器配置，也不会触发路由器重启。
	var encoderURLs []string
	for i := range md.Spec.Roles {
		roleKind := ModelDeploymentEffectiveRoleKind(&md.Spec.Roles[i])
		kind, ok := ModelDeploymentRoleUpstreamKind(roleKind)
		if !ok {
			return ModelDeploymentRouterObjects{}, fmt.Errorf("unsupported role kind %q", md.Spec.Roles[i].Kind)
		}
		roles = append(roles, router.Role{
			Kind:         kind,
			RoleLabelKey: modelDeploymentLabelKeyRoleKind,
		})
		ports = append(ports, ModelDeploymentRoleServingPort(md, &md.Spec.Roles[i]))
		if roleKind == workercore.ModelDeploymentRoleKindEncode {
			encoderURLs = append(
				encoderURLs,
				// 拼出 http://<md>-<role>.<ns>.svc:<port>
				modelDeploymentRoleEndpoint(md, &md.Spec.Roles[i]),
			)
		}
	}
	slices.Sort(ports)
	ports = slices.Compact(ports)
	if len(ports) != 1 {
		return ModelDeploymentRouterObjects{}, fmt.Errorf(
			"router requires every role to use the same serving port; got %v", ports,
		)
	}

	contract := workercore.ModelDeploymentRouterStatus{
		Name:  md.Spec.Router.Name,
		Roles: make([]workercore.ModelDeploymentRouterRoleStatus, 0, len(md.Spec.Roles)),
	}
	// 引擎指标表仅是 picker（llm-d-router）的前置条件，原因与准入阶段一致：其拒绝消息引用的是
	// 渲染包里的 picker 常量，为其他路由器调用会以第三款路由器的名义报错。另外两款路由器依据自身
	// 观测打分，不读取这些指标，因此对外发布的契约里也不为它们填写 Metrics，避免上报根本未被抓取
	// 的指标契约。
	var metrics router.Metrics
	if md.Spec.Router.Name == workercore.ModelDeploymentRouterLLMD {
		var err error
		engine, ok := ModelDeploymentEngineUpstreamName(md.Spec.Engine.Name)
		if !ok {
			return ModelDeploymentRouterObjects{}, fmt.Errorf("unsupported engine %q", md.Spec.Engine.Name)
		}
		if metrics, err = router.MetricsForEngine(engine); err != nil {
			return ModelDeploymentRouterObjects{}, err
		}
		metrics.Port = ports[0]
		contract.Metrics = &workercore.ModelDeploymentRouterMetrics{
			Port: metrics.Port, QueuedRequests: metrics.QueuedRequests,
			RunningRequests: metrics.RunningRequests, KVCacheUtilization: metrics.KVCacheUtilization,
		}
	}
	for i := range md.Spec.Roles {
		role := &md.Spec.Roles[i]
		kind := ModelDeploymentEffectiveRoleKind(role)
		// 在资格门控激活前保持仅路由 leader 成员；激活后 External-DP 所有成员均提供 HTTP 服务，
		// 因此必须去掉成员索引限制以保持全员可选。
		selector := modelDeploymentSelectorLabels(md, role)
		selector[modelDeploymentLabelKeyRoleKind] = roles[i].Kind
		selector[modelDeploymentMemberIndexLabel] = strconvx.Itoa(modelDeploymentLeaderMemberIndex)
		// 不受支持的形态不会获得任何轮次都无法满足的资格选择器。
		if modelDeploymentEligibilitySelectorActive(md, role, nil, retainedEligibility) {
			selector[modelDeploymentLabelKeyEndpointEligible] = modelDeploymentEndpointEligibleValue
			if modelDeploymentRoleExternalDP(md, role) {
				delete(selector, modelDeploymentMemberIndexLabel)
			}
		}
		// 直接把对外发布的同一份 map 交给渲染器，而不是在旁边另拼一份：通过 argv 配置的路由器
		// 直接用这些标签发现各个角色，若分两处推导，用户看到的契约与路由器实际匹配的标签就可能漂移。
		roles[i].Selector = maps.Clone(selector)
		roleStatus := workercore.ModelDeploymentRouterRoleStatus{
			Name:     role.Name,
			Kind:     kind,
			Selector: selector,
			// 拼出 http://<md>-<role>.<ns>.svc:<port>
			Endpoint: modelDeploymentRoleEndpoint(md, role),
		}
		if modelDeploymentPublishesKVEvents(md, role, manufacturers[role.Name]) {
			events := inject.VLLMKVEvents(md.Name + "-" + role.Name + "." + md.Namespace + ".svc")
			roleStatus.KVEvents = &workercore.ModelDeploymentRouterKVEvents{
				Endpoint: events.Endpoint, ReplayEndpoint: events.ReplayEndpoint, Topic: events.Topic,
			}
		}
		contract.Roles = append(contract.Roles, roleStatus)
	}
	// 区分“资格由 operator 裁决的角色”与“operator 无法判定的角色”。此处看的是稳定的有效策略而非
	// 某轮瞬时结论：无法判定的角色永远不携带资格标签，而可判定的角色可能合格也可能被撤回。部署级
	// 选择器是所有条件的单个逻辑与，无法在同一部署里同时表达“要求角色 A 但不要求角色 B”，此时
	// 需要由候选标记把两者统一纳入。
	controllable, unjudgeable := 0, 0
	for i := range md.Spec.Roles {
		if modelDeploymentRoleJudged(md, &md.Spec.Roles[i]) {
			controllable++
		} else {
			unjudgeable++
		}
	}
	// 初始为仅发现 leader 的旧选择器；激活资格门控的 External-DP 角色改用资格标签，同时也能在混合
	// 角色部署中排除内部 follower。所有路由器的选择器都从这组等式推导，保证发现集合完全一致。
	endpointLabels := map[string]string{
		modelDeploymentLabelKeyName:     modelDeploymentLabelValueName,
		modelDeploymentLabelKeyInstance: md.Name,
		modelDeploymentMemberIndexLabel: strconvx.Itoa(modelDeploymentLeaderMemberIndex),
	}
	// 分别统计部署中“是否有角色可激活门控”以及“是否全部角色都可激活门控”。单靠部署级逻辑与只能回答
	// 第一个问题，而不可激活的角色不能被施加它永远无法满足的条件。
	endpointActivatable, everyActivatable := false, true
	for i := range md.Spec.Roles {
		if modelDeploymentGroupForwardActivatable(md, &md.Spec.Roles[i]) {
			endpointActivatable = true

			continue
		}
		everyActivatable = false
	}
	// picker 在各自的阶段过滤器中按角色声明过滤条件，因此当含 encoder 的部署中各角色可激活状态
	// 不一致时，资格要求下沉到阶段过滤器而非部署级选择器：避免把永远不带资格标签的角色排除在外，
	// 也不会因为 encoder 故障拖垮纯文本服务，同时保持部署级选择器在整个生命周期内稳定不触发滚动。
	stageFiltered := endpointActivatable && !everyActivatable &&
		md.Spec.Router.Name == workercore.ModelDeploymentRouterLLMD &&
		modelDeploymentDeclaresEncoder(md)
	// 当部署同时包含可控与不可判定角色时改用候选标记；当所有可激活角色达成一致时，将资格等式并入
	// 部署级选择器（若包含 External-DP 角色则去掉 leader 索引限制）。
	markerBacked := false
	switch {
	case controllable > 0 && unjudgeable > 0:
		// 混合集合：同时包含 operator 裁决的角色与无法判定的角色，用候选标记覆盖受保护角色中的合格
		// 成员以及兼容角色的所有成员，并移除成员索引限制以便全部应答成员都被发现。
		endpointLabels[modelDeploymentRouterCandidateLabel] = modelDeploymentEndpointEligibleValue
		delete(endpointLabels, modelDeploymentMemberIndexLabel)
		markerBacked = true
	case endpointActivatable && !stageFiltered &&
		(modelDeploymentEligibilityDecided(md) || retainedEligibility ||
			(everyActivatable && modelDeploymentRouterProtectedRole(md))):
		endpointLabels[modelDeploymentLabelKeyEndpointEligible] = modelDeploymentEndpointEligibleValue
		for i := range md.Spec.Roles {
			if modelDeploymentRoleExternalDP(md, &md.Spec.Roles[i]) {
				// 资格标签会选中所有可应答的 External-DP 成员，同时仍只保留内部形态角色的 leader。
				delete(endpointLabels, modelDeploymentMemberIndexLabel)
				break
			}
		}
	}
	// 路由器 Pod 既无成员索引也无端点资格标签，但仍在选择器表达式中显式将其排除。
	//
	// 各等式项按固定顺序手工列出而不从 map 排序：该字符串会写入 ConfigMap 并参与 Pod 配置哈希计算，
	// 变更顺序会无故触发集群中所有路由器滚动更新。
	endpointSelectorTerms := []string{
		modelDeploymentLabelKeyName + "=" + endpointLabels[modelDeploymentLabelKeyName],
		modelDeploymentLabelKeyInstance + "=" + endpointLabels[modelDeploymentLabelKeyInstance],
	}
	if member, present := endpointLabels[modelDeploymentMemberIndexLabel]; present {
		endpointSelectorTerms = append(endpointSelectorTerms, modelDeploymentMemberIndexLabel+"="+member)
	}
	if len(endpointLabels[modelDeploymentLabelKeyEndpointEligible]) > 0 {
		endpointSelectorTerms = append(endpointSelectorTerms,
			modelDeploymentLabelKeyEndpointEligible+"="+endpointLabels[modelDeploymentLabelKeyEndpointEligible])
	}
	if len(endpointLabels[modelDeploymentRouterCandidateLabel]) > 0 {
		endpointSelectorTerms = append(endpointSelectorTerms,
			modelDeploymentRouterCandidateLabel+"="+endpointLabels[modelDeploymentRouterCandidateLabel])
	}
	endpointSelectorTerms = append(endpointSelectorTerms, "!"+modelDeploymentRouterLabelKey)
	// 按地址访问 encoder 的引擎（如 SGLang）由 prefiller 自行拨号 encoder，路由器不会向 encoder
	// 转发请求。此处把 encoder 排除在候选集合之外，且仅在末尾追加以保持其余部署的选择器完全不变。
	if md.Spec.Router.Name == workercore.ModelDeploymentRouterLLMD &&
		modelDeploymentEPDSplitsEncoderByAddress(md.Spec.Engine.Name) &&
		modelDeploymentDeclaresEncoder(md) {
		if encodeKind, ok := ModelDeploymentRoleUpstreamKind(workercore.ModelDeploymentRoleKindEncode); ok {
			endpointSelectorTerms = append(endpointSelectorTerms,
				modelDeploymentLabelKeyRoleKind+"!="+encodeKind)
		}
	}
	endpointSelector := strings.Join(endpointSelectorTerms, ",")
	// IMPORTANT: 路由需求单
	routerInput := router.Input{
		Roles:     roles,
		Metrics:   metrics,
		ModelName: md.Spec.Model.Name,
		// 拼出 http://<md>-<role>.<ns>.svc:<port>
		TokenizerEndpoint: modelDeploymentRoleEndpoint(md, modelDeploymentTokenizerRole(md)),
		Namespace:         md.Namespace, EndpointSelector: endpointSelector,
		EndpointLabels:          endpointLabels,
		EncoderURLs:             encoderURLs,
		ListenPort:              modelDeploymentRouterHTTPPort,
		MetricsPort:             modelDeploymentRouterMetricsPort,
		DiscoveryPort:           ports[0],
		RequestTimeoutSeconds:   md.Spec.Router.RequestTimeoutSeconds,
		DisaggregationThreshold: md.Spec.Router.DisaggregationThresholdTokens,
	}
	// SGLang 的 External DP 形态需要把注意力数据并行宽度告知 gateway，以便其启用 --dp-aware 按
	// 每个成员注册 worker；宽度小于 2 或其他引擎不输出该参数。读取声明中的首个 External DP 角色。
	if md.Spec.Engine.Name == workercore.ModelDeploymentEngineSGLang {
		for i := range md.Spec.Roles {
			if !modelDeploymentRoleExternalDP(md, &md.Spec.Roles[i]) {
				continue
			}
			reading, err := scanModelDeploymentParallelism(
				md.Spec.Engine.Name, md.Spec.Roles[i].ExtraArgs, md.Spec.Roles[i].Env,
			)
			if err != nil {
				return ModelDeploymentRouterObjects{}, fmt.Errorf(
					"read the external-dp width the router renders: %w", err,
				)
			}
			routerInput.AttentionDataParallelSize = int32(reading.declared.AttentionDataParallel)
			break
		}
	}
	if stageFiltered {
		routerInput.EligibleLabelKey = modelDeploymentLabelKeyEndpointEligible
	}
	if markerBacked {
		// 候选标记模式下，各阶段过滤器按候选标记接纳该类型的安全并集，避免单个角色的资格标签把
		// 同类型的兼容候选者误排除。
		routerInput.CandidateLabelKey = modelDeploymentRouterCandidateLabel
		routerInput.CandidateLabelValue = modelDeploymentEndpointEligibleValue
	}
	if md.Spec.Engine.Name == workercore.ModelDeploymentEngineVLLM {
		engine, _ := ModelDeploymentEngineUpstreamName(md.Spec.Engine.Name)
		routerInput.KVEvents = router.KVEvents{
			Engine: engine, Port: inject.VLLMKVEventsPort,
			ReplayPort: inject.VLLMKVEventsReplayPort, Topic: inject.VLLMKVEventsTopic,
		}
	}
	rendered, err := router.Render(md.Spec.Router.Name, routerInput)
	if err != nil {
		return ModelDeploymentRouterObjects{}, err
	}
	extraArgs, err := json.Marshal(md.Spec.Router.ExtraArgs)
	if err != nil {
		return ModelDeploymentRouterObjects{}, fmt.Errorf("render router extra arguments: %w", err)
	}
	workload, err := renderModelDeploymentRouterWorkload(ctx, md, modelDeploymentRouterDerived{
		Name:             name,
		Rendered:         rendered,
		EndpointSelector: endpointSelector,
		TargetPorts:      joinInt32s(ports),
		ExtraArgs:        string(extraArgs),
	})
	if err != nil {
		return ModelDeploymentRouterObjects{}, err
	}

	// 无配置即不建 ConfigMap。完全通过 argv 配置的路由器不读取 ConfigMap，若强行创建空对象，
	// 会让人无法区分“不存在”与“为空”，还会对空数据计算无意义的配置哈希。
	var configMap *core.ConfigMap
	annotations := modelDeploymentMetricsAnnotations(modelDeploymentRouterMetricsPort, core.URISchemeHTTP)
	if len(workload.Config) > 0 {
		configMap = &core.ConfigMap{
			ObjectMeta: meta.ObjectMeta{Name: name, Namespace: md.Namespace, Labels: maps.Clone(labels)},
			Data:       workload.Config,
		}
		note(configMap)
		annotations[modelDeploymentRouterConfigHash] = hashRouterConfig(configMap.Data)
	}

	replicas := int32(1)
	if md.Spec.Router.Replicas != nil {
		replicas = *md.Spec.Router.Replicas
	}
	deployment := &app.Deployment{
		ObjectMeta: meta.ObjectMeta{Name: name, Namespace: md.Namespace, Labels: maps.Clone(labels)},
		Spec: app.DeploymentSpec{
			Replicas: &replicas,
			Selector: &meta.LabelSelector{MatchLabels: maps.Clone(labels)},
			Template: core.PodTemplateSpec{
				ObjectMeta: meta.ObjectMeta{Labels: maps.Clone(labels), Annotations: annotations},
				Spec: core.PodSpec{
					ServiceAccountName: name,
					ImagePullSecrets:   md.Spec.Router.ImagePullSecrets,
					Containers:         workload.Containers,
					Volumes:            workload.Volumes,
				},
			},
		},
	}
	note(deployment)

	service := &core.Service{
		ObjectMeta: meta.ObjectMeta{Name: name, Namespace: md.Namespace, Labels: maps.Clone(labels)},
		Spec: core.ServiceSpec{Selector: maps.Clone(labels), Ports: []core.ServicePort{{
			Name: "http", Port: modelDeploymentRouterHTTPPort,
			TargetPort: intstr.FromInt32(modelDeploymentRouterHTTPPort),
		}}},
	}
	note(service)
	contract.Endpoint = modelDeploymentRouterEndpoint(service)

	serviceAccount := &core.ServiceAccount{ObjectMeta: meta.ObjectMeta{Name: name, Namespace: md.Namespace, Labels: maps.Clone(labels)}}
	note(serviceAccount)
	role := &rbac.Role{
		ObjectMeta: meta.ObjectMeta{Name: name, Namespace: md.Namespace, Labels: maps.Clone(labels)},
		Rules:      []rbac.PolicyRule{{APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"get", "list", "watch"}}},
	}
	note(role)
	roleBinding := &rbac.RoleBinding{
		ObjectMeta: meta.ObjectMeta{Name: name, Namespace: md.Namespace, Labels: maps.Clone(labels)},
		Subjects:   []rbac.Subject{{Kind: rbac.ServiceAccountKind, Name: name, Namespace: md.Namespace}},
		RoleRef:    rbac.RoleRef{APIGroup: rbac.GroupName, Kind: "Role", Name: name},
	}
	note(roleBinding)

	return ModelDeploymentRouterObjects{
		//（<md>-router）：运行路由器容器，负责保活与滚动更新。
		Deployment: deployment,
		//（<md>-router，仅 llm-d-router 创建）：存放路由规则 epp.yaml 和 Envoy 代理配置 envoy.yaml
		ConfigMap: configMap,
		//（<md>-router，监听 8000 端口）：用户发推理请求的统一入口。
		Service: service,
		// RBAC 权限三件套
		ServiceAccount: serviceAccount,
		// RBAC 权限三件套
		Role: role,
		// RBAC 权限三件套
		RoleBinding: roleBinding,
		Contract:    contract,
	}, nil
}

// modelDeploymentRouterWorkload 表示单个路由器向对象集合贡献的差异化部分：仅由该路由器读取的
// 配置条目、它运行的容器列表以及这些容器挂载的卷。
//
// 托管路由器所需的其余部分（对象名与标签、归属备注、Service/ServiceAccount/Role/RoleBinding、
// 服务发现选择器与契约）对所有路由器完全相同，因此由调用方统一渲染一次。
type modelDeploymentRouterWorkload struct {
	// Config 写入共享 ConfigMap。此处的键不得与公共键冲突。
	Config map[string]string

	// Containers 是按启动顺序排列的 Pod 容器列表。
	Containers []core.Container

	// Volumes 仅包含上述容器实际挂载的卷。
	Volumes []core.Volume
}

// modelDeploymentRouterDerived 汇集公共阶段推导出的信息，交给具体路由器分支按需消费。
type modelDeploymentRouterDerived struct {
	// Name 是整套路由器对象共用的名称。
	Name string

	// Rendered 是路由器自身的配置产物（配置文件文档或命令行参数）。
	Rendered router.Output

	// EndpointSelector、TargetPorts 与 ExtraArgs 是与参数等价的 ConfigMap 拼写形式。
	// 完全通过 argv 读取配置的路由器不使用这些字段。
	EndpointSelector string
	TargetPorts      string
	ExtraArgs        string
}

// renderModelDeploymentRouterWorkload 渲染因路由器类型而异的工作负载部分。
//
// Envoy 代理、挂载配置文件及其访问日志仅属于 llm-d 分支：该路由器本质上是 ext_proc 扩展而非
// HTTP 代理，需要前置 Envoy 承接 Service 流量并从文件读取调度配置；单进程路由器不附带无用 sidecar
// 和配置文档。末尾 default 分支用于防范未来新增第四款路由器时误返回空容器列表。
func renderModelDeploymentRouterWorkload(
	ctx context.Context, md *workercore.ModelDeployment, in modelDeploymentRouterDerived,
) (modelDeploymentRouterWorkload, error) {
	switch md.Spec.Router.Name {
	case workercore.ModelDeploymentRouterLLMD:
		return renderModelDeploymentLLMDRouterWorkload(ctx, md, in), nil
	case workercore.ModelDeploymentRouterVLLM, workercore.ModelDeploymentRouterSGLang:
		return renderModelDeploymentArgvRouterWorkload(ctx, md, in.Rendered), nil
	default:
		return modelDeploymentRouterWorkload{},
			fmt.Errorf("router %q renders no workload", md.Spec.Router.Name)
	}
}

// renderModelDeploymentArgvRouterWorkload 渲染完全通过命令行参数配置的单进程路由器。
//
// 该分支不产出任何 ConfigMap 条目，因此调用方不会为其创建 ConfigMap。选择器与目标端口已通过
// 命令行参数直接交给进程，避免在同一对象外再留一份无人读取的副本。
func renderModelDeploymentArgvRouterWorkload(
	ctx context.Context, md *workercore.ModelDeployment, rendered router.Output,
) modelDeploymentRouterWorkload {
	image := md.Spec.Router.Image
	if image == "" {
		image = redirectedImage(ctx, settings.ModelDeploymentRouterImage.ShouldValue(ctx))
	}

	// 用户自定义参数放在推导参数之后，与准入阶段对托管键的拒绝规则保持一致：受管键已在准入阶段
	// 被拦截，能到达这里的参数均属于 operator 未设置的项。
	args := slices.Concat(rendered.Arguments, md.Spec.Router.ExtraArgs)

	return modelDeploymentRouterWorkload{
		Containers: []core.Container{{
			// 与参数一同传入 Command，因为单一镜像内含三个可执行程序且未声明 entrypoint。
			Name: "router", Image: image, Command: rendered.Command, Args: args,
			ImagePullPolicy: kvcache.ResolvePullPolicy(md.Spec.Router.ImagePullPolicy, image),
			Ports: []core.ContainerPort{
				{Name: "http", ContainerPort: modelDeploymentRouterHTTPPort},
				{Name: "metrics", ContainerPort: modelDeploymentRouterMetricsPort},
			},
			// 路由器直接在 Service 指向的端口上响应，因此对该端口的 TCP 探针即可准确反映就绪状态。
			ReadinessProbe: &core.Probe{ProbeHandler: core.ProbeHandler{
				TCPSocket: &core.TCPSocketAction{Port: intstr.FromInt32(modelDeploymentRouterHTTPPort)},
			}},
		}},
	}
}

func renderModelDeploymentLLMDRouterWorkload(
	ctx context.Context, md *workercore.ModelDeployment, in modelDeploymentRouterDerived,
) modelDeploymentRouterWorkload {
	name := in.Name
	// 用户在对象上显式指定的镜像按原样使用；仅 operator 提供的默认镜像经过集群镜像源重定向。
	image := md.Spec.Router.Image
	if image == "" {
		image = redirectedImage(ctx,
			settings.ModelDeploymentRouterImage.ShouldValue(ctx))
	}
	proxyImage := redirectedImage(ctx,
		settings.ModelDeploymentRouterProxyImage.ShouldValue(ctx))

	args := make([]string, 0, 5+len(md.Spec.Router.ExtraArgs))
	// 以下显式关闭上游默认开启的 --secure-serving 与 --metrics-endpoint-auth，这是刻意设计而非
	// 遗漏：路由器仅处理集群内部东西向流量，南北向 TLS 与认证由集群入口网关统一负责；ext_proc
	// gRPC 仅在同 Pod 的 127.0.0.1 回环上由 Envoy 拨号，/metrics 仅在集群内抓取，因此两项均关闭
	// 且在 spec.router.extraArgs 中禁止覆盖。
	args = append(args,
		"--endpoint-selector=$(ENDPOINT_SELECTOR)",
		"--endpoint-target-ports=$(ENDPOINT_TARGET_PORTS)",
		"--config-file=/config/"+modelDeploymentRouterConfigKey,
		"--secure-serving=false",
		"--grpc-health-port=9003",
		"--metrics-endpoint-auth=false",
	)
	args = append(args, md.Spec.Router.ExtraArgs...)

	return modelDeploymentRouterWorkload{
		// 前两项以挂载文件形式读取，中间两项通过环境变量注入，最后一项用于确保用户修改 extraArgs
		// 时配置哈希随之变化并触发 Pod 滚动更新。
		Config: map[string]string{
			// 前面拼出来的 epp.yaml
			modelDeploymentRouterConfigKey: in.Rendered.Document,
			// envoy.yaml 文件
			modelDeploymentRouterEnvoyConfigKey: renderModelDeploymentRouterEnvoyConfig(
				md.Spec.Router.RequestTimeoutSeconds,
			),
			modelDeploymentRouterSelectorKey:    in.EndpointSelector,
			modelDeploymentRouterTargetPortsKey: in.TargetPorts,
			// extra-args.json
			modelDeploymentRouterExtraArgsKey: in.ExtraArgs,
		},
		Containers: []core.Container{
			// IMPORTANT: 容器 1 envoy
			{
				Name: "envoy", Image: proxyImage,
				// 显式解析出具体的拉取策略而非留空：空值会被 API server 默认化，导致后续每轮对齐器
				// 都判定存在差异并持续更新 Deployment。
				ImagePullPolicy: kvcache.ResolvePullPolicy(md.Spec.Router.ImagePullPolicy, proxyImage),
				Args:            []string{"-c", "/config/" + modelDeploymentRouterEnvoyConfigKey},
				Ports:           []core.ContainerPort{{Name: "http", ContainerPort: modelDeploymentRouterHTTPPort}},
				ReadinessProbe: &core.Probe{ProbeHandler: core.ProbeHandler{
					TCPSocket: &core.TCPSocketAction{Port: intstr.FromInt32(modelDeploymentRouterHTTPPort)},
				}},
				VolumeMounts: []core.VolumeMount{{Name: "config", MountPath: "/config", ReadOnly: true}},
			},
			// IMPORTANT: 容器 2 epp
			{
				// 与上文一致：镜像包含三个程序且未设默认 entrypoint，需显式给出 Command。
				Name: "epp", Image: image, Command: in.Rendered.Command, Args: args,
				ImagePullPolicy: kvcache.ResolvePullPolicy(md.Spec.Router.ImagePullPolicy, image),
				Env: []core.EnvVar{
					configMapEnv(modelDeploymentRouterSelectorKey, "ENDPOINT_SELECTOR", name),
					configMapEnv(modelDeploymentRouterTargetPortsKey, "ENDPOINT_TARGET_PORTS", name),
					{Name: "NAMESPACE", ValueFrom: &core.EnvVarSource{FieldRef: &core.ObjectFieldSelector{FieldPath: "metadata.namespace"}}},
					{Name: "POD_NAME", ValueFrom: &core.EnvVarSource{FieldRef: &core.ObjectFieldSelector{FieldPath: "metadata.name"}}},
				},
				Ports: []core.ContainerPort{
					{Name: "grpc", ContainerPort: modelDeploymentRouterEPPPort},
					{Name: "grpc-health", ContainerPort: 9003},
					{Name: "metrics", ContainerPort: 9090},
				},
				ReadinessProbe: &core.Probe{ProbeHandler: core.ProbeHandler{
					GRPC: &core.GRPCAction{Port: 9003},
				}},
				VolumeMounts: []core.VolumeMount{{Name: "config", MountPath: "/config", ReadOnly: true}},
			},
		},
		Volumes: []core.Volume{{Name: "config", VolumeSource: core.VolumeSource{
			ConfigMap: &core.ConfigMapVolumeSource{LocalObjectReference: core.LocalObjectReference{Name: name}},
		}}},
	}
}

func modelDeploymentTokenizerRole(md *workercore.ModelDeployment) *workercore.ModelDeploymentRole {
	roles := slices.Clone(md.Spec.Roles)
	slices.SortFunc(roles, func(a, b workercore.ModelDeploymentRole) int {
		return strings.Compare(a.Name, b.Name)
	})
	for i := range roles {
		if ModelDeploymentEffectiveRoleKind(&roles[i]) == workercore.ModelDeploymentRoleKindPrefill {
			return &roles[i]
		}
	}

	// 无 prefill 角色时优先选择 server 角色，因其同时处理整条请求；由于准入不强制路由器部署必须
	// 包含这两种角色之一，最后按名称排序取首项作为确定性兜底。
	for i := range roles {
		if ModelDeploymentEffectiveRoleKind(&roles[i]) == workercore.ModelDeploymentRoleKindServer {
			return &roles[i]
		}
	}

	return &roles[0]
}

func modelDeploymentRouterEndpoint(service *core.Service) string {
	return "http://" + service.Name + "." + service.Namespace + ".svc:" +
		fmt.Sprintf("%d", service.Spec.Ports[0].Port)
}

func (r *ModelDeploymentReconciler) syncModelDeploymentRouter(
	ctx context.Context, md *workercore.ModelDeployment, eligibilityDecided bool,
) error {
	var rendered ModelDeploymentRouterObjects
	if md.Spec.Router != nil {
		// 保留机制：服务发现等式条件一旦发布，在所有内置 profile 的静默轮次中都会继续保留。
		// llm-d profile 会发布 ConfigMap；而 vllm-router 和 sglang-gateway profile 通过 argv 配置，
		// 不渲染 ConfigMap，因此缺少 ConfigMap 并不能证明路由面从未被收窄——还需要同时读取
		// 线上路由器 Deployment 的 args 和 env。静态可激活条件部分仍然起决定作用：若部署形态
		// 不再具备可激活条件，本轮会移除该过滤项。
		//
		// 除了 selector 键外，还会从配置文档中读取该条件，因为同一要求可能存放在这两处之一。
		// 当部署中各角色的可激活状态不一致时，该要求位于阶段过滤器而非 selector 中；若某轮
		// 只读取 selector，就会查无所获，误判门控从未激活过，进而恢复因故障而撤回的端点。
		// 对文档采用解析而非字符串搜索：用户指定的模型名称恰好可能与该标签字符串相同，若直接
		// 搜索字符串，可能会误将未启用门控的部署当成已激活处理。
		needle := modelDeploymentLabelKeyEndpointEligible + "=" + modelDeploymentEndpointEligibleValue
		// 该标记是这一要求所使用的另一种名称，当部署中各角色状态不一致时，携带的是该标记而非
		// 资格条件。静默轮次必须在任何发布位置都能识别出该要求，否则会误判门控从未触发，
		// 进而恢复因故障而撤回的端点。
		candidateNeedle := modelDeploymentRouterCandidateLabel + "=" + modelDeploymentEndpointEligibleValue
		retained := false
		liveConfig := new(core.ConfigMap)
		err := r.Client.Get(ctx, ctrlcli.ObjectKey{Namespace: md.Namespace, Name: md.Name + "-router"}, liveConfig)
		switch {
		case err == nil:
			retained = strings.Contains(liveConfig.Data[modelDeploymentRouterSelectorKey], needle) ||
				strings.Contains(liveConfig.Data[modelDeploymentRouterSelectorKey], candidateNeedle) ||
				router.DocumentStageFilterDemands(
					liveConfig.Data[modelDeploymentRouterConfigKey],
					modelDeploymentLabelKeyEndpointEligible, modelDeploymentEndpointEligibleValue,
				) ||
				router.DocumentStageFilterDemands(
					liveConfig.Data[modelDeploymentRouterConfigKey],
					modelDeploymentRouterCandidateLabel, modelDeploymentEndpointEligibleValue,
				)
		case apierrors.IsNotFound(err):
		default:
			return err
		}
		if !retained {
			liveDeployment := new(app.Deployment)
			err = r.Client.Get(ctx, ctrlcli.ObjectKey{Namespace: md.Namespace, Name: md.Name + "-router"}, liveDeployment)
			switch {
			case err == nil:
				for _, container := range liveDeployment.Spec.Template.Spec.Containers {
					for _, arg := range container.Args {
						if strings.Contains(arg, needle) || strings.Contains(arg, candidateNeedle) {
							retained = true
						}
					}
					for _, env := range container.Env {
						if strings.Contains(env.Value, needle) || strings.Contains(env.Value, candidateNeedle) {
							retained = true
						}
					}
				}
			case apierrors.IsNotFound(err):
			default:
				return err
			}
		}

		rendered, err = renderModelDeploymentRouterObjects(
			ctx, md, r.modelDeploymentRoleManufacturers(ctx, md), retained || eligibilityDecided,
		)
		if err != nil {
			return err
		}
	}

	wanted := md.Spec.Router != nil
	if err := r.syncModelDeploymentOwnedChildren(ctx, md, objectIfPresent(rendered.Deployment, wanted),
		func() ctrlcli.Object { return new(app.Deployment) }, new(app.DeploymentList),
		func(actual, expected ctrlcli.Object) bool {
			return alignModelDeploymentRouterDeployment(actual.(*app.Deployment), expected.(*app.Deployment))
		}, modelDeploymentResourceNoteRouter, "deployment"); err != nil {
		return err
	}
	// 完全通过 argv 配置的路由器不渲染 ConfigMap，因此其期望列表为空，清理路径会移除此前
	// 形态遗留的 ConfigMap。此处检查的是带类型的 nil 指针而非接口本身，因为持有类型化 nil 的
	// 接口值本身不为 nil。
	if err := r.syncModelDeploymentOwnedChildren(ctx, md,
		objectIfPresent(rendered.ConfigMap, wanted && rendered.ConfigMap != nil),
		func() ctrlcli.Object { return new(core.ConfigMap) }, new(core.ConfigMapList),
		func(actual, expected ctrlcli.Object) bool {
			return alignModelDeploymentRouterConfigMap(actual.(*core.ConfigMap), expected.(*core.ConfigMap))
		}, modelDeploymentResourceNoteRouter, "configmap"); err != nil {
		return err
	}
	if err := r.syncModelDeploymentOwnedChildren(ctx, md, objectIfPresent(rendered.Service, wanted),
		func() ctrlcli.Object { return new(core.Service) }, new(core.ServiceList),
		func(actual, expected ctrlcli.Object) bool {
			changed := alignModelDeploymentService(actual.(*core.Service), expected.(*core.Service))
			return alignModelDeploymentChildMetadata(actual, expected) || changed
		}, modelDeploymentResourceNoteRouter, "service"); err != nil {
		return err
	}
	if err := r.syncModelDeploymentOwnedChildren(ctx, md, objectIfPresent(rendered.ServiceAccount, wanted),
		func() ctrlcli.Object { return new(core.ServiceAccount) }, new(core.ServiceAccountList),
		alignModelDeploymentChildMetadata, modelDeploymentResourceNoteRouter, "serviceaccount"); err != nil {
		return err
	}
	if err := r.syncModelDeploymentOwnedChildren(ctx, md, objectIfPresent(rendered.Role, wanted),
		func() ctrlcli.Object { return new(rbac.Role) }, new(rbac.RoleList),
		func(actual, expected ctrlcli.Object) bool {
			role, want := actual.(*rbac.Role), expected.(*rbac.Role)
			changed := false
			if !kubemeta.DeepEqual(role.Rules, want.Rules) {
				role.Rules = want.Rules
				changed = true
			}
			return alignModelDeploymentChildMetadata(actual, expected) || changed
		}, modelDeploymentResourceNoteRouter, "role"); err != nil {
		return err
	}
	return r.syncModelDeploymentOwnedChildren(ctx, md, objectIfPresent(rendered.RoleBinding, wanted),
		func() ctrlcli.Object { return new(rbac.RoleBinding) }, new(rbac.RoleBindingList),
		func(actual, expected ctrlcli.Object) bool {
			binding, want := actual.(*rbac.RoleBinding), expected.(*rbac.RoleBinding)
			changed := false
			if !kubemeta.DeepEqual(binding.Subjects, want.Subjects) {
				binding.Subjects = want.Subjects
				changed = true
			}
			return alignModelDeploymentChildMetadata(actual, expected) || changed
		}, modelDeploymentResourceNoteRouter, "rolebinding")
}

func objectIfPresent(object ctrlcli.Object, present bool) []ctrlcli.Object {
	if !present {
		return nil
	}
	return []ctrlcli.Object{object}
}

// alignModelDeploymentRouterDeployment 使线上路由器 Deployment 与渲染结果保持一致。
//
// 该函数刻意不比较 selector，因为 selector 在 apps/v1 中是不可变的，一旦存在差异就会
// 导致后续所有 Update 失败。渲染出的 selector 嵌入了 `spec.router.name`，因此改变它的
// 唯一途径就是修改该字段——而准入控制会在创建后冻结该字段。不同部署之间该值可以不同，
// 但同一部署内部绝不会改变，因此忽略 selector 是安全可靠的，而非未经验证的妥协。
//
// 如果未来解除了该字段的冻结限制，问题将暴露在此函数中，且比较 selector 并非正确的修复
// 方式：届时必须删除并重建该对象，因为 API server 不会接受在已有对象上修改 selector。
func alignModelDeploymentRouterDeployment(actual, expected *app.Deployment) (changed bool) {
	if !kubemeta.DeepEqual(actual.Spec.Replicas, expected.Spec.Replicas) {
		actual.Spec.Replicas = expected.Spec.Replicas
		changed = true
	}
	actualPod, expectedPod := &actual.Spec.Template, &expected.Spec.Template
	if !kubemeta.DeepEqual(actualPod.Labels, expectedPod.Labels) {
		actualPod.Labels = expectedPod.Labels
		changed = true
	}
	if !kubemeta.DeepEqual(actualPod.Annotations, expectedPod.Annotations) {
		actualPod.Annotations = expectedPod.Annotations
		changed = true
	}
	if actualPod.Spec.ServiceAccountName != expectedPod.Spec.ServiceAccountName {
		actualPod.Spec.ServiceAccountName = expectedPod.Spec.ServiceAccountName
		changed = true
	}
	// 尽管下方的拉取策略位于容器定义内部，此处仍会比较该字段：在此添加或移除 Secret 会改变
	// 镜像拉取时的认证凭据，而一个渲染了却从不进行对齐的字段，在修改后只能通过手动重建
	// Deployment 才能生效。
	if !kubemeta.DeepEqual(actualPod.Spec.ImagePullSecrets, expectedPod.Spec.ImagePullSecrets) {
		actualPod.Spec.ImagePullSecrets = expectedPod.Spec.ImagePullSecrets
		changed = true
	}
	if !kubemeta.DeepEqual(actualPod.Spec.Volumes, expectedPod.Spec.Volumes) {
		actualPod.Spec.Volumes = expectedPod.Spec.Volumes
		changed = true
	}
	if len(actualPod.Spec.Containers) != len(expectedPod.Spec.Containers) {
		actualPod.Spec.Containers = expectedPod.Spec.Containers
		changed = true
	} else {
		for i := range expectedPod.Spec.Containers {
			if alignRenderedContainer(&actualPod.Spec.Containers[i], expectedPod.Spec.Containers[i]) {
				changed = true
			}
		}
	}

	return alignModelDeploymentChildMetadata(actual, expected) || changed
}

func alignModelDeploymentRouterConfigMap(actual, expected *core.ConfigMap) (changed bool) {
	if !maps.Equal(actual.Data, expected.Data) {
		actual.Data = expected.Data
		changed = true
	}
	return alignModelDeploymentChildMetadata(actual, expected) || changed
}

// alignModelDeploymentChildMetadata 将子对象的标签与资源备注收敛至渲染后的值。其中
// 备注部分并非表面装饰，而是关键依赖：它仅在 Create 时写入，而清理门控会跳过
// 备注为空的任何子对象——因此一旦备注被剥离，该对象将永久免于清理，这一机制
// 正是为了保证该门控的可靠性。
func alignModelDeploymentChildMetadata(actual, expected ctrlcli.Object) (changed bool) {
	if !maps.Equal(actual.GetLabels(), expected.GetLabels()) {
		actual.SetLabels(maps.Clone(expected.GetLabels()))
		changed = true
	}
	if !systemmeta.EqualResourceTypeAndNotes(expected, actual) {
		systemmeta.SyncResourceTypeAndNotes(expected, actual)
		changed = true
	}
	return changed
}

func configMapEnv(key, name, configMap string) core.EnvVar {
	optional := false
	return core.EnvVar{Name: name, ValueFrom: &core.EnvVarSource{ConfigMapKeyRef: &core.ConfigMapKeySelector{
		LocalObjectReference: core.LocalObjectReference{Name: configMap}, Key: key, Optional: &optional,
	}}}
}

func joinInt32s(values []int32) string {
	parts := make([]string, len(values))
	for i := range values {
		parts[i] = fmt.Sprintf("%d", values[i])
	}
	return strings.Join(parts, ",")
}

func hashRouterConfig(data map[string]string) string {
	encoded, err := json.Marshal(data)
	if err != nil {
		return "unhashable-" + err.Error()
	}
	return stringx.SumBytesBySHA256(encoded)
}

const (
	// modelDeploymentRouterEnvoyTimeoutPlaceholder 使用字符串替换而非 fmt 格式化，因为模板中包含
	// 大量 Envoy 自身的百分号格式化占位符，经 fmt 处理要么被破坏要么需全部转义。
	modelDeploymentRouterEnvoyTimeoutPlaceholder = "<ROUTE-TIMEOUT>"

	// modelDeploymentRouterDefaultRouteTimeout 是请求超时未设置时代理沿用的默认值（一天）；两款
	// 通过 argv 配置的路由器默认超时为半小时，显式设置该字段可让三者行为统一。
	modelDeploymentRouterDefaultRouteTimeout int32 = 86400
)

// renderModelDeploymentRouterEnvoyConfig 将路由超时填入代理配置模板。
func renderModelDeploymentRouterEnvoyConfig(timeoutSeconds *int32) string {
	timeout := modelDeploymentRouterDefaultRouteTimeout
	if timeoutSeconds != nil {
		timeout = *timeoutSeconds
	}

	return strings.ReplaceAll(modelDeploymentRouterEnvoyConfig,
		modelDeploymentRouterEnvoyTimeoutPlaceholder, strconvx.Itoa(timeout))
}

const modelDeploymentRouterEnvoyConfig = `static_resources:
  listeners:
  - name: http
    address:
      socket_address: {address: 0.0.0.0, port_value: 8000}
    filter_chains:
    - filters:
      - name: envoy.filters.network.http_connection_manager
        typed_config:
          "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
          stat_prefix: router
          route_config:
            name: router
            virtual_hosts:
            - name: model
              domains: ["*"]
              routes:
              - match: {prefix: "/"}
                route: {cluster: original_destination_cluster, timeout: <ROUTE-TIMEOUT>s}
          # 固定输出一条访问日志，而非开放为配置项。此前这里完全没有日志输出，无需让用户自行选值；
          # 排障者真正需要的是 picker 选中的上游端点以及对应的响应结果，这组信息在该 Pod 的其他
          # 日志中都看不到。upstream_host 记录目标端点，response_flags 与 response_code_details
          # 用于区分“模型返回拒绝”还是“代理根本未能连通上游”。
          access_log:
          - name: envoy.access_loggers.stdout
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.access_loggers.stream.v3.StdoutAccessLog
              log_format:
                json_format:
                  start_time: "%START_TIME%"
                  method: "%REQ(:METHOD)%"
                  path: "%REQ(X-ENVOY-ORIGINAL-PATH?:PATH)%"
                  protocol: "%PROTOCOL%"
                  response_code: "%RESPONSE_CODE%"
                  response_flags: "%RESPONSE_FLAGS%"
                  response_code_details: "%RESPONSE_CODE_DETAILS%"
                  duration: "%DURATION%"
                  upstream_host: "%UPSTREAM_HOST%"
                  request_id: "%REQ(X-REQUEST-ID)%"
                  bytes_received: "%BYTES_RECEIVED%"
                  bytes_sent: "%BYTES_SENT%"
          http_filters:
          # original_destination_cluster 依据 x-gateway-destination-endpoint 路由请求，而
          # failure_mode_allow 会在唯一合法写入该请求头的 EPP 不可用时放行请求。客户端自行
          # 携带的同名请求头绝不能到达该 cluster，否则调用方就可以绕过端点选择器指定上游。
          #
          # 移除动作必须作为独立 filter 放在 ext_proc 之前，而不能写在 connection manager 或
          # route 层，这两处各有原因：任何 Envoy 版本的 HttpConnectionManager 都没有
          # request_headers_to_remove 字段，写在这里会直接导致解析失败并阻塞代理启动；route
          # 层虽然支持该字段，但执行时机在 ext_proc 之后，会把 EPP 刚写入的请求头一并删掉。
          # HTTP filter 按声明顺序执行，放在此处可以在调用 EPP 之前清除客户端伪造的头，同时
          # 完整保留 EPP 后续写入的值。
          - name: envoy.filters.http.header_mutation
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.http.header_mutation.v3.HeaderMutation
              mutations:
                request_mutations:
                - remove: x-gateway-destination-endpoint
          - name: envoy.filters.http.ext_proc
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.http.ext_proc.v3.ExternalProcessor
              failure_mode_allow: true
              grpc_service:
                envoy_grpc: {cluster_name: ext_proc, authority: "localhost:9002"}
                timeout: 10s
              processing_mode:
                request_header_mode: SEND
                response_header_mode: SEND
                request_body_mode: FULL_DUPLEX_STREAMED
                response_body_mode: FULL_DUPLEX_STREAMED
                request_trailer_mode: SEND
                response_trailer_mode: SEND
          - name: envoy.filters.http.router
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router
  clusters:
  - name: original_destination_cluster
    type: ORIGINAL_DST
    connect_timeout: 1s
    lb_policy: CLUSTER_PROVIDED
    original_dst_lb_config:
      use_http_header: true
      http_header_name: x-gateway-destination-endpoint
  - name: ext_proc
    type: STATIC
    connect_timeout: 1s
    typed_extension_protocol_options:
      envoy.extensions.upstreams.http.v3.HttpProtocolOptions:
        "@type": type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions
        explicit_http_config:
          http2_protocol_options: {}
    load_assignment:
      cluster_name: ext_proc
      endpoints:
      - lb_endpoints:
        - endpoint:
            address:
              socket_address: {address: 127.0.0.1, port_value: 9002}
`
