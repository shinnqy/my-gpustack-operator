package worker

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"

	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/systemmeta"
)

// ModelDeploymentElasticTensorParallelSize 从 extraArgs 中读取固定的 TP 度。
// 非弹性角色保留默认的单加速器配置（值为 1）。
func ModelDeploymentElasticTensorParallelSize(role *workercore.ModelDeploymentRole) (int, error) {
	if role == nil || role.ElasticEP == nil {
		return 1, nil
	}
	declared, err := ParseModelDeploymentDeclaredParallelism(workercore.ModelDeploymentEngineVLLM, role.ExtraArgs, role.Env)
	if err != nil {
		return 0, fmt.Errorf("elastic member parallelism is unreadable: %w", err)
	}
	return declared.TensorParallel, nil
}

func modelDeploymentElasticHeadName(md *workercore.ModelDeployment) string {
	return md.Name + "-elastic-head"
}

func modelDeploymentElasticMasters(md *workercore.ModelDeployment, pods []core.Pod) []core.Pod {
	role := ModelDeploymentElasticRole(md)
	var masters []core.Pod
	for i := range pods {
		if modelDeploymentPodRole(&pods[i]) == role.Name && modelDeploymentOrdinalOrFloor(&pods[i]) == 0 {
			masters = append(masters, pods[i])
		}
	}
	return masters
}

// modelDeploymentElasticCollectiveMembers 表示弹性部署的逻辑成员组：即弹性角色当前的所有 GPU worker（包含 master）。
//
// 正在退出的 worker 不属于成员，因为它即将离开集合通信组；辅助的 Ray head 也根本不是 worker：
// 其组件标签使用 head 自身名称，因此角色的 worker 集合在构造上自然将其排除。
//
// 该列表直接复用本轮调谐已获取并校验的结果，无需二次拉取或探测。
func modelDeploymentElasticCollectiveMembers(
	md *workercore.ModelDeployment, pods []core.Pod,
) []core.Pod {
	role := ModelDeploymentElasticRole(md)
	if role == nil {
		return nil
	}
	members := make([]core.Pod, 0, len(pods))
	for i := range pods {
		pod := &pods[i]
		if pod.DeletionTimestamp != nil {
			continue
		}
		if modelDeploymentPodRole(pod) != role.Name {
			continue
		}
		members = append(members, *pod)
	}

	return members
}

func (r *ModelDeploymentReconciler) renderModelDeploymentElasticPods(ctx context.Context, md *workercore.ModelDeployment,
	connection *ModelDeploymentConnectorInput, protocols []string, weights *modelArtifactWeights,
) (map[string]map[int][]*core.Pod, error) {
	role := ModelDeploymentElasticRole(md)
	_, err := ModelDeploymentElasticTensorParallelSize(role)
	if err != nil {
		return nil, err
	}
	boot := role.ElasticEP.Width
	pods, err := r.listModelDeploymentPods(ctx, md)
	if err != nil {
		return nil, err
	}
	masters := modelDeploymentElasticMasters(md, pods)
	if len(masters) > 1 {
		return nil, fmt.Errorf("elastic deployment has multiple master identities")
	}
	if len(masters) == 1 {
		if masters[0].UID == "" {
			return map[string]map[int][]*core.Pod{}, nil
		}
		var valid bool
		boot, valid = ModelDeploymentElasticBootstrapFromMaster(
			string(masters[0].UID), masters[0].Annotations[ModelDeploymentElasticMasterBootWidthAnnotation], md,
		)
		if !valid {
			return map[string]map[int][]*core.Pod{}, nil
		}
	}
	copyMD := md.DeepCopy()
	gpu := copyMD.Spec.Roles[0].DeepCopy()
	gpu.Replicas = role.ElasticEP.Width
	for _, env := range []workercore.ModelDeploymentEnvVar{
		{Name: "RAY_LOG_TO_STDERR", Value: "1"},
		{Name: "RAY_LOGGER_LEVEL", Value: "info"},
		{Name: "RAY_BACKEND_LOG_LEVEL", Value: "info"},
	} {
		if !slices.ContainsFunc(gpu.Env, func(value workercore.ModelDeploymentEnvVar) bool { return value.Name == env.Name }) {
			gpu.Env = append(gpu.Env, env)
		}
	}
	copyMD.Spec.Roles = []workercore.ModelDeploymentRole{*gpu}
	desired, err := r.renderModelDeploymentRolePods(ctx, copyMD, connection, protocols, weights)
	if err != nil {
		return nil, err
	}
	gpu.ElasticEP = nil
	address := modelDeploymentElasticHeadName(md) + "." + md.Namespace + ".svc:6379"
	for ordinal, members := range desired[role.Name] {
		pod := members[0]
		c := &pod.Spec.Containers[0]
		c.Env = append(c.Env,
			core.EnvVar{Name: "GPUSTACK_ELASTIC_POD_UID", ValueFrom: &core.EnvVarSource{
				FieldRef: &core.ObjectFieldSelector{FieldPath: "metadata.uid"},
			}},
			core.EnvVar{Name: "RAY_ADDRESS", Value: address},
			core.EnvVar{Name: "GPUSTACK_ELASTIC_POD_IP", ValueFrom: &core.EnvVarSource{
				FieldRef: &core.ObjectFieldSelector{FieldPath: "status.podIP"},
			}},
			core.EnvVar{Name: "VLLM_RAY_DP_PACK_STRATEGY", Value: "strict"},
		)
		join := "ray start --node-ip-address=\"$GPUSTACK_ELASTIC_POD_IP\" --address=" + address +
			" --labels=" + ModelDeploymentElasticRayNodeIdentityLabel + "=\"$GPUSTACK_ELASTIC_POD_UID\""

		if ordinal == 0 {
			argv := slices.Clone(c.Command)
			argv = append(argv, "--data-parallel-backend", "ray", "--data-parallel-size", strconv.Itoa(int(boot)),
				"--data-parallel-size-local", "1",
				"--enable-expert-parallel", "--enable-eplb", "--enable-elastic-ep")
			c.Command = append([]string{"/bin/sh", "-c", "set -e; " + join +
				"; exec \"$@\" --data-parallel-address \"$GPUSTACK_ELASTIC_POD_IP\"", "elastic-serve"}, argv...)
			pod.Annotations[ModelDeploymentElasticMasterBootWidthAnnotation] = strconv.Itoa(int(boot))
			c.LivenessProbe = modelDeploymentElasticLivenessProbe()
			port := pod.Annotations["prometheus.io/port"]
			if port != "" {
				c.LivenessProbe.Exec.Command[2] = modelDeploymentElasticLivenessScript("http://127.0.0.1:" + port + modelDeploymentProbePath)
			}
		} else {
			c.Command = []string{"/bin/sh", "-ec", join + " --block"}
			c.StartupProbe = nil
			c.ReadinessProbe = nil
			c.LivenessProbe = nil
			c.Lifecycle = nil
		}
	}
	headPod := renderModelDeploymentElasticHeadPod(md, gpu, desired[role.Name][0][0].Spec.Containers[0])
	desired[modelDeploymentElasticHeadName(md)] = map[int][]*core.Pod{0: {headPod}}
	return desired, nil
}

// renderModelDeploymentElasticHeadPod 为辅助 head 绕过 GPU 与 InstanceType 准入校验。
func renderModelDeploymentElasticHeadPod(
	md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole, main core.Container,
) *core.Pod {
	name := modelDeploymentElasticHeadName(md)
	labels := modelDeploymentSelectorLabels(md, &workercore.ModelDeploymentRole{Name: name})
	labels["app.kubernetes.io/part-of"] = "gpustack-operator-worker"
	labels[modelDeploymentReplicaOrdinalLabel] = "0"
	labels[modelDeploymentMemberIndexLabel] = "0"
	env := slices.DeleteFunc(slices.Clone(main.Env), func(env core.EnvVar) bool {
		switch env.Name {
		case "RAY_LOG_TO_STDERR", "RAY_LOGGER_LEVEL", "RAY_BACKEND_LOG_LEVEL", "RAY_DEDUP_LOGS":
			return false
		default:
			return true
		}
	})
	env = append(env, core.EnvVar{Name: "GPUSTACK_ELASTIC_POD_UID", ValueFrom: &core.EnvVarSource{
		FieldRef: &core.ObjectFieldSelector{FieldPath: "metadata.uid"},
	}})
	vols, mounts := convertModelDeploymentAdditionalVolumes(role.AdditionalVolumes)
	vols, mounts = modelDeploymentSharedMemoryVolumes(role, vols, mounts)
	pod := &core.Pod{
		ObjectMeta: meta.ObjectMeta{GenerateName: md.Name + "-" + name + "-", Namespace: md.Namespace, Labels: labels},
		Spec: core.PodSpec{
			AutomountServiceAccountToken: ptr.To(false),
			EnableServiceLinks:           ptr.To(false),
			RestartPolicy:                core.RestartPolicyAlways,
			ImagePullSecrets:             role.ImagePullSecrets,
			Volumes:                      vols,
			Containers: []core.Container{{
				Name: modelDeploymentMainContainerName, Image: main.Image, ImagePullPolicy: role.ImagePullPolicy,
				SecurityContext: main.SecurityContext.DeepCopy(),
				Command: []string{"/bin/sh", "-ec", ModelDeploymentElasticHeadCommand +
					" --num-gpus=0 --port=6379 --dashboard-host=0.0.0.0 --labels=" +
					ModelDeploymentElasticRayNodeIdentityLabel + "=\"$GPUSTACK_ELASTIC_POD_UID\" --block"},
				Env: env, VolumeMounts: mounts,
				Ports: []core.ContainerPort{{Name: "gcs", ContainerPort: 6379}, {Name: "dashboard", ContainerPort: 8265}},
			}},
		},
	}
	if role.TerminationGracePeriodSeconds != nil {
		pod.Spec.TerminationGracePeriodSeconds = ptr.To(*role.TerminationGracePeriodSeconds)
	}
	systemmeta.NoteResource(pod, ModelDeploymentResourceType, map[string]string{ModelDeploymentResourceNoteRole: name})
	kubemeta.ControlOnWithoutBlock(pod, md, workercore.SchemeGroupVersionKind("ModelDeployment"))
	headRole := *role.DeepCopy()
	headRole.Name = name
	pod.Annotations[modelDeploymentPodSpecHashAnnotation] = modelDeploymentExplicitSpecHash(md, &headRole, 0, 0, "")
	pod.Annotations[modelDeploymentRenderVersionAnnotation] = strconv.Itoa(modelDeploymentRenderVersion)
	return pod
}

func renderModelDeploymentElasticHeadService(md *workercore.ModelDeployment) *core.Service {
	name := modelDeploymentElasticHeadName(md)
	svc := &core.Service{
		ObjectMeta: meta.ObjectMeta{
			Name: name, Namespace: md.Namespace,
			Labels: map[string]string{modelDeploymentLabelKeyName: modelDeploymentLabelValueName, modelDeploymentLabelKeyInstance: md.Name},
		},
		Spec: core.ServiceSpec{
			Selector: map[string]string{modelDeploymentLabelKeyInstance: md.Name, modelDeploymentLabelKeyComponent: name},
			Ports: []core.ServicePort{
				{Name: "gcs", Port: 6379, TargetPort: intstr.FromInt32(6379)},
				{Name: "dashboard", Port: 8265, TargetPort: intstr.FromInt32(8265)},
			},
		},
	}
	systemmeta.NoteResource(svc, ModelDeploymentResourceType, map[string]string{ModelDeploymentResourceNoteRole: name})
	kubemeta.ControlOnWithoutBlock(svc, md, workercore.SchemeGroupVersionKind("ModelDeployment"))
	return svc
}

// modelDeploymentTerminalPods 返回已退出且尚无删除操作的成员：即不带删除时间戳的 Succeeded 或 Failed Pod。
// 失败的 Pod 不会再次运行且 Kubernetes 也不会自动垃圾回收，因此非主动发起的退出会残留为静态对象，
// 导致其 Workload 及所属组的配额份额仍被占用。已在删除过程中的 Pod 不会返回，
// 因为其退出正在进行中，对应 Workload 会由发起删除的协议负责释放。
func modelDeploymentTerminalPods(pods []core.Pod) []core.Pod {
	terminal := make([]core.Pod, 0, len(pods))
	for i := range pods {
		pod := &pods[i]
		if pod.Status.Phase != core.PodSucceeded && pod.Status.Phase != core.PodFailed {
			continue
		}
		if pod.DeletionTimestamp != nil {
			continue
		}
		terminal = append(terminal, *pod)
	}

	return terminal
}

// deleteModelDeploymentTerminalPods 移除已退出的成员并释放持有它们的 Workload。
// 两次写操作缺一不可：删除 Pod 释放创建门控读取的序号，删除 Workload 才能移除 Kueue
// 在成员上的 finalizer 并释放其组占用的配额。两者若不存在均视为成功，
// 因为前一轮调谐可能已经发起过删除。
func (r *ModelDeploymentReconciler) deleteModelDeploymentTerminalPods(
	ctx context.Context, md *workercore.ModelDeployment, pods []core.Pod,
) error {
	terminal := modelDeploymentTerminalPods(pods)
	if len(terminal) == 0 {
		return nil
	}
	for i := range terminal {
		logger := ctrllog.FromContext(ctx).WithValues("pod", terminal[i].Name,
			"phase", terminal[i].Status.Phase, "reason", terminal[i].Status.Reason)
		logger.Info("removing elastic member in a terminal phase")
		if err := r.Client.Delete(ctx, &terminal[i]); err != nil && !kerrors.IsNotFound(err) {
			return fmt.Errorf("delete terminal elastic member %s: %w", terminal[i].Name, err)
		}
	}

	return r.deleteModelDeploymentGroupWorkload(ctx, md, terminal)
}

// convergeModelDeploymentElastic 实现弹性部署的收敛并返回本轮调谐的观测结果。
//
// 之所以返回观测结果，是因为状态发布与资格写入都需要基于同一份结果：
// 二次采集会得到另一时刻的不同答案，二次资格判定也会对同一批 Pod 给出另一分组下的不同裁决。
func (r *ModelDeploymentReconciler) convergeModelDeploymentElastic(ctx context.Context, md *workercore.ModelDeployment,
	actual []core.Pod, desired map[string]map[int][]*core.Pod, weights *modelArtifactWeights,
) (bool, modelDeploymentPassObservation, error) {
	role := ModelDeploymentElasticRole(md)
	if role == nil {
		return false, modelDeploymentPassObservation{}, nil
	}
	// 终态成员属于非本 operator 主动发起的退出，且只有弹性路径可能遗留此类成员：
	// 下文用于删除终态副本及其 Workload 的固定期望集合比对逻辑位于本调用之后。Kueue 绝不会自动回收
	// Succeeded 或 Failed 的 Pod，而下方的创建循环刻意将终态成员视为缺失以便重新填充其序号。
	// 若放任不管，它会作为已准入成员继续与自己的替代者共存于同一组中，导致 Kueue 将其判定为多余成员
	// 并反复删除，使该槽位永远无法恢复。因此，此处改用与固定路径相同的条件，主动执行 Pod 删除及释放组 finalizer 与配额的 Workload 删除。
	if err := r.deleteModelDeploymentTerminalPods(ctx, md, actual); err != nil {
		return true, modelDeploymentPassObservation{}, err
	}
	// 已处于删除中的成员是另一类非此处发起的退出，而固定路径的滞留清理永远不会执行到此分支，
	// 因此在此处运行清理。它的 Workload 持有 Pod 上 Kueue 的 finalizer 及组配额，
	// 清理沿用固定路径的条件：如果一个 Workload 同时还拥有仍在运行的成员，则保持不动，
	// 因为 Kueue 对被删除 Workload 的响应是停止整个组。
	if err := r.releaseModelDeploymentStrandedWorkloads(ctx, md, actual); err != nil {
		return true, modelDeploymentPassObservation{}, err
	}
	if len(desired) == 0 {
		return true, modelDeploymentPassObservation{}, r.reconcileModelDeploymentElasticResize(ctx, md)
	}
	for name, ordinals := range desired {
		taken, err := r.modelDeploymentTakenGroups(ctx, md, name, false)
		if err != nil {
			return true, modelDeploymentPassObservation{}, err
		}
		gpuCount := 0
		for i := range actual {
			if actual[i].Status.Phase == core.PodSucceeded || actual[i].Status.Phase == core.PodFailed {
				continue
			}
			if modelDeploymentPodRole(&actual[i]) == role.Name {
				gpuCount++
			}
		}
		for ordinal, members := range ordinals {
			if name == role.Name && gpuCount >= int(role.ElasticEP.Width) {
				continue
			}
			present := false
			for i := range actual {
				if actual[i].Status.Phase == core.PodSucceeded || actual[i].Status.Phase == core.PodFailed {
					continue
				}
				if modelDeploymentPodRole(&actual[i]) == name && modelDeploymentOrdinalOrFloor(&actual[i]) == ordinal {
					present = true
					break
				}
			}
			if present || taken.Has(modelDeploymentReplicaGroupName(md, name, ordinal)) {
				continue
			}
			pod := members[0].DeepCopy()
			if weights != nil && name == role.Name {
				injectModelArtifactAffinity(pod, weights.Affinity)
			}
			if err := r.Client.Create(ctx, pod); err != nil {
				return true, modelDeploymentPassObservation{}, err
			}
			if name == role.Name {
				gpuCount++
			}
		}
	}
	// [IMPORTANT: elastic EP] 总编排入口
	if err := r.reconcileModelDeploymentElasticResize(ctx, md); err != nil {
		return true, modelDeploymentPassObservation{}, err
	}
	if r.APIReader != nil {
		live, err := r.elasticLiveMembers(ctx, md)
		switch {
		case err == nil:
			actual = live
		case errors.Is(err, errElasticDeploymentChanged):
			// 部署在本轮调谐期间发生了变更。下方的写操作会基于本轮已无法担保一致性的成员列表
			// 去给 master 打标签、修补 Service 并重写路由器状态，因此本轮调谐在此终止。
			// spec watch 会重新将部署入队。
			return true, modelDeploymentPassObservation{
				Serving: modelDeploymentServingUnobserved(md, "the deployment moved under this pass"),
			}, nil
		default:
			// 读取失败不能等同于有效成员列表。拒绝执行写操作，而非基于调用方传入的列表强行收敛，
			// 否则会误确认从未观测到的状态。
			return true, modelDeploymentPassObservation{}, err
		}
	}
	fetch := r.groupForwardFetch
	if fetch == nil {
		fetch = defaultGroupForwardFetch
	}
	masters := modelDeploymentElasticMasters(md, actual)
	qualifications := qualifyModelDeploymentInstances(ctx, md, masters, modelDeploymentPendingReplacement{}, fetch)
	byMember := modelDeploymentQualificationsByMember(qualifications)

	// 本轮调谐的观测在此处采集，位于资格判定之后、标签写入之前。
	// 资格判定可能耗时一整次探针周期，若在其之前采集，到恢复决策读取时数据就已经过时。
	serving := r.observeModelDeploymentServing(ctx, md)
	observation := modelDeploymentPassObservation{
		Serving:        serving,
		Qualifications: qualifications,
		// 集合通信组的成员列表直接采用本轮已读取的结果，且在构造上已排除 head。
		// status 会据此发布集合通信组自身的条目。
		CollectiveMembers: modelDeploymentElasticCollectiveMembers(md, actual),
	}
	if err := r.convergeModelDeploymentEndpointEligibility(
		ctx, md, actual, byMember, nil, observation,
	); err != nil {
		return true, observation, err
	}
	decided := modelDeploymentQualificationsDecided(qualifications)
	if err := r.syncModelDeploymentService(ctx, md, decided, actual); err != nil {
		return true, observation, err
	}
	return true, observation, r.syncModelDeploymentRouter(ctx, md, decided)
}
