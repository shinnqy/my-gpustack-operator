package worker

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	kueuepodconst "sigs.k8s.io/kueue/pkg/controller/jobs/pod/constants"
	kueueworkload "sigs.k8s.io/kueue/pkg/workload"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubeclients/kubernetes/scheme"
)

// jointCheckObject 构造本控制器负责的 AdmissionCheck。FilterForController 读取它来确定应响应的条目，
// 若缺少这个对象，测试可能根本没有执行要验证的准入分支。
func jointCheckObject() *kueue.AdmissionCheck {
	ac := new(kueue.AdmissionCheck)
	ac.Name = _JointAdmissionCheckName
	ac.Spec.ControllerName = _JointAdmissionControllerName

	return ac
}

// jointGroupPod 构造带组标识的 Pod。属于部署时还要设置渲染器实际写入的实例标签和所有者引用，
// 这样按部署标签列出副本时才能找到它，并据此确认归属。
func jointGroupPod(name, group, mdName string) *core.Pod {
	pod := &core.Pod{ObjectMeta: meta.ObjectMeta{
		Name:      name,
		Namespace: "team-a",
		UID:       types.UID("uid-" + name),
		Labels:    map[string]string{kueuepodconst.GroupNameLabel: group},
	}}
	if mdName != "" {
		pod.Labels[modelDeploymentLabelKeyInstance] = mdName
		pod.OwnerReferences = []meta.OwnerReference{{
			APIVersion: workercore.SchemeGroupVersion.String(),
			Kind:       "ModelDeployment",
			Name:       mdName,
			UID:        types.UID("uid-" + mdName),
			Controller: ptr.To(true),
		}}
	}

	return pod
}

// jointWorkload 构造一个由指定 Pod 持有、携带本控制器检查的 Workload。
func jointWorkload(name string, reserved bool, pods ...*core.Pod) *kueue.Workload {
	wl := new(kueue.Workload)
	wl.Name, wl.Namespace = name, "team-a"
	for _, pod := range pods {
		wl.OwnerReferences = append(wl.OwnerReferences, meta.OwnerReference{
			APIVersion: "v1", Kind: "Pod", Name: pod.Name, UID: pod.UID,
		})
	}

	status := meta.ConditionFalse
	if reserved {
		status = meta.ConditionTrue
	}
	wl.Status.Conditions = []meta.Condition{{
		Type:               kueue.WorkloadQuotaReserved,
		Status:             status,
		Reason:             "Test",
		LastTransitionTime: meta.Now(),
	}}
	wl.Status.AdmissionChecks = []kueue.AdmissionCheckState{{
		Name:  kueue.AdmissionCheckReference(_JointAdmissionCheckName),
		State: kueue.CheckStatePending,
	}}

	return wl
}

// jointDeployment 按给定规格为部署创建角色，每个角色一个副本。
func jointDeployment(name string, instanceTypes ...string) *workercore.ModelDeployment {
	md := &workercore.ModelDeployment{
		ObjectMeta: meta.ObjectMeta{
			Name: name, Namespace: "team-a", UID: types.UID("uid-" + name),
		},
	}
	for i, it := range instanceTypes {
		md.Spec.Roles = append(md.Spec.Roles, workercore.ModelDeploymentRole{
			Name: []string{"prefill", "decode", "third"}[i], Replicas: 1, InstanceType: it,
		})
	}

	return md
}

func reconcileJoint(t *testing.T, cli ctrlcli.Client, wlName string) *kueue.Workload {
	t.Helper()

	r := &ModelDeploymentJointAdmissionReconciler{Client: cli}
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "team-a", Name: wlName},
	})
	require.NoError(t, err)

	got := new(kueue.Workload)
	require.NoError(t, cli.Get(context.Background(),
		ctrlcli.ObjectKey{Namespace: "team-a", Name: wlName}, got))

	return got
}

// jointCheckState 通过生产代码读取的同一条检查取得状态，条目缺失时返回空字符串。
func jointCheckState(wl *kueue.Workload) kueue.CheckState {
	if acs := jointCheckEntry(wl); acs != nil {
		return acs.State
	}

	return ""
}

func newJointClient(objs ...ctrlcli.Object) ctrlcli.Client {
	return ctrlfake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(objs...).
		WithStatusSubresource(&kueue.Workload{}, &kueue.AdmissionCheck{}).
		Build()
}

// jointReplica 按渲染器使用的角色和序号构造一个副本，并让 Workload 引用其 Pod 的 UID。
func jointReplica(md *workercore.ModelDeployment, role string, ordinal int, reserved bool) (*core.Pod, *kueue.Workload) {
	name := role + "-" + strconv.Itoa(ordinal)
	pod := jointGroupPod(name, modelDeploymentReplicaGroupName(md, role, ordinal), md.Name)
	pod.Labels[modelDeploymentMemberIndexLabel] = "0"
	pod.Annotations = map[string]string{kueuepodconst.GroupTotalCountAnnotation: "1"}
	return pod, jointWorkload("wl-"+name, reserved, pod)
}

// epdJointDeployment 让三个角色使用同一规格，仍各自持有独立的配额预留。
func epdJointDeployment() *workercore.ModelDeployment {
	md := jointDeployment("qwen", "gpu", "gpu")
	md.Spec.Roles[0].Kind = workercore.ModelDeploymentRoleKindPrefill
	md.Spec.Roles[1].Kind = workercore.ModelDeploymentRoleKindDecode
	md.Spec.Roles = append(md.Spec.Roles, workercore.ModelDeploymentRole{
		Name: "encode", Kind: workercore.ModelDeploymentRoleKindEncode, InstanceType: "gpu", Replicas: 1,
	})
	return md
}

func TestModelDeploymentJointAdmission_TheWholeSetOrNone(t *testing.T) {
	ctx := t.Context()
	md := jointDeployment("qwen", "gpu", "gpu")
	prefill, pWorkload := jointReplica(md, "prefill", 0, true)
	decode, dWorkload := jointReplica(md, "decode", 0, true)
	cli := newJointClient(jointCheckObject(), md, prefill, pWorkload)

	got := reconcileJoint(t, cli, pWorkload.Name)
	require.Equal(t, kueue.CheckStatePending, jointCheckState(got))
	assert.Contains(t, jointCheckEntry(got).Message, "still assembling")
	assert.True(t, kueueworkload.HasQuotaReservation(got))

	// Decode 已组齐，尚无配额。此时消息要指出阻塞的角色，并保留 Prefill 的预留。
	require.NoError(t, cli.Create(ctx, decode))
	got = reconcileJoint(t, cli, pWorkload.Name)
	require.Equal(t, kueue.CheckStatePending, jointCheckState(got))
	assert.Contains(t, jointCheckEntry(got).Message, "1 of 2 replicas have reserved quota")
	assert.Contains(t, jointCheckEntry(got).Message, "roles decode")
	assert.True(t, kueueworkload.HasQuotaReservation(got))

	require.NoError(t, cli.Create(ctx, dWorkload))
	for _, wl := range []*kueue.Workload{pWorkload, dWorkload} {
		assert.Equal(t, kueue.CheckStateReady, jointCheckState(reconcileJoint(t, cli, wl.Name)))
	}
}

func TestModelDeploymentJointAdmission_EveryGroupOfAnEncoderDeployment(t *testing.T) {
	for _, waiting := range []string{"", "encode", "prefill", "decode"} {
		t.Run("waiting="+waiting, func(t *testing.T) {
			md := epdJointDeployment()
			objects := []ctrlcli.Object{jointCheckObject(), md}
			for _, role := range md.Spec.Roles {
				pod, wl := jointReplica(md, role.Name, 0, role.Name != waiting)
				objects = append(objects, pod, wl)
			}
			cli := newJointClient(objects...)
			for _, role := range md.Spec.Roles {
				got := reconcileJoint(t, cli, "wl-"+role.Name+"-0")
				entry := jointCheckEntry(got)
				require.NotNil(t, entry)
				switch {
				case waiting == "":
					assert.Equal(t, kueue.CheckStateReady, entry.State)
				case waiting == role.Name:
					assert.Empty(t, entry.Message)
					assert.Equal(t, kueue.CheckStatePending, entry.State)
				default:
					assert.Equal(t, kueue.CheckStatePending, entry.State)
					assert.Contains(t, entry.Message, "roles "+waiting)
					assert.True(t, kueueworkload.HasQuotaReservation(got))
				}
			}
		})
	}
}

func TestModelDeploymentJointAdmission_AScaleUpDoesNotReopenTheAdmittedGroups(t *testing.T) {
	md := epdJointDeployment()
	md.Spec.Roles[0].Replicas = 2
	objects := []ctrlcli.Object{jointCheckObject(), md}
	for _, role := range md.Spec.Roles {
		pod, wl := jointReplica(md, role.Name, 0, true)
		jointCheckEntry(wl).State = kueue.CheckStateReady
		jointCheckEntry(wl).Message = "already serving"
		wl.Status.Conditions = append(wl.Status.Conditions, meta.Condition{
			Type: kueue.WorkloadAdmitted, Status: meta.ConditionTrue,
		})
		objects = append(objects, pod, wl)
	}
	joining, newWorkload := jointReplica(md, "prefill", 1, false)
	cli := newJointClient(append(objects, joining, newWorkload)...)

	got := reconcileJoint(t, cli, newWorkload.Name)
	assert.Equal(t, kueue.CheckStatePending, jointCheckState(got))
	for _, role := range md.Spec.Roles {
		got = reconcileJoint(t, cli, "wl-"+role.Name+"-0")
		assert.True(t, kueueworkload.IsAdmitted(got))
		require.Equal(t, kueue.CheckStateReady, jointCheckState(got))
		assert.Equal(t, "already serving", jointCheckEntry(got).Message)
	}
}

func TestModelDeploymentJointAdmission_AnAbsentReplicaIsReadByItsDeparture(t *testing.T) {
	for _, tc := range []struct {
		name         string
		keepWorkload bool
		want         kueue.CheckState
	}{
		{name: "operator_replacement", want: kueue.CheckStateReady},
		{name: "kueue_preemption", keepWorkload: true, want: kueue.CheckStatePending},
	} {
		t.Run(tc.name, func(t *testing.T) {
			md := jointDeployment("qwen", "gpu", "gpu")
			prefill, pWorkload := jointReplica(md, "prefill", 0, true)
			decode, dWorkload := jointReplica(md, "decode", 0, false)
			now := meta.Now()
			decode.DeletionTimestamp = &now
			decode.Finalizers = []string{"test.gpustack.ai/hold"}
			objects := []ctrlcli.Object{jointCheckObject(), md, prefill, pWorkload, decode}
			if tc.keepWorkload {
				objects = append(objects, dWorkload)
			}
			got := reconcileJoint(t, newJointClient(objects...), pWorkload.Name)
			assert.Equal(t, tc.want, jointCheckState(got))
		})
	}
}

// TestModelDeploymentJointAdmission_ReadyAtOnceForEverythingElse 验证普通工作负载和单角色部署都能立即获得 Ready。
// Operator 管理的所有队列都可能引用联合检查，漏掉这些情况会使请求无限期停在 Pending。
func TestModelDeploymentJointAdmission_ReadyAtOnceForEverythingElse(t *testing.T) {
	cases := []struct {
		name string
		objs func() []ctrlcli.Object
	}{
		{
			// 从 Workload 查到的 Pod 没有 ModelDeployment 所有者，应直接放行。
			name: "not_this_operators_workload",
			objs: func() []ctrlcli.Object {
				pod := jointGroupPod("someone-elses", "their-group", "")

				return []ctrlcli.Object{jointCheckObject(), pod, jointWorkload("wl", true, pod)}
			},
		},
		{
			// 单角色部署无需等其他角色，即使声明多个副本，每个副本仍可由 Kueue 独立准入。
			name: "single_role_deployment",
			objs: func() []ctrlcli.Object {
				md := jointDeployment("qwen", "h20-8x")
				pod := jointGroupPod("qwen-prefill-0",
					modelDeploymentReplicaGroupName(md, "prefill", 0), "qwen")

				return []ctrlcli.Object{jointCheckObject(), md, pod, jointWorkload("wl", true, pod)}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := reconcileJoint(t, newJointClient(tc.objs()...), "wl")

			assert.Equal(t, kueue.CheckStateReady, jointCheckState(got),
				"anything that is not a multi-group model deployment is Ready at once")
		})
	}
}
