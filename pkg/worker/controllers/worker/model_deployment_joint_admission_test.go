package worker

import (
	"context"
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
