package api

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"go.uber.org/multierr"
	apiext "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	klog "k8s.io/klog/v2"
	apireg "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	"k8s.io/utils/ptr"

	"gpustack.ai/gpustack/pkg/kubeclients/kubernetes"
	"gpustack.ai/gpustack/pkg/kubeclientset"
	"gpustack.ai/gpustack/pkg/kubeclientset/review"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/utils/waitx"
)

// ensureInterval 定义 EnsureCRDs 和 EnsureServices 检查缺失对象的轮询间隔。
const ensureInterval = 30 * time.Second

type (
	// CRDGetter 是用于获取 CRD 的函数类型。
	CRDGetter func() map[string]*apiext.CustomResourceDefinition

	// ServiceGetter 是用于获取 APIService 的函数类型。
	ServiceGetter func(apireg.ServiceReference, []byte) *apireg.APIService
)

// MergeCRDs 从多个 getter 中合并 CRD 并以单个列表返回。
func MergeCRDs(crdGetters []CRDGetter) []*apiext.CustomResourceDefinition {
	// 合并来自所有 getter 的 CRD。
	var (
		ret = make([]map[string]*apiext.CustomResourceDefinition, len(crdGetters))
		csc int
	)
	for i, get := range crdGetters {
		ret[i] = get()
		csc += len(ret[i])
	}

	crds := make([]*apiext.CustomResourceDefinition, 0, csc)
	for i := range ret {
		if ret[i] == nil {
			continue
		}
		for _, n := range sets.List(sets.KeySet(ret[i])) {
			crds = append(crds, ret[i][n])
		}
	}

	return crds
}

// InstallCRDs 安装 CRD。
func InstallCRDs(ctx context.Context, cli kubernetes.Interface, crdGetters []CRDGetter) error {
	err := review.CanDoUpdate(ctx,
		cli.AuthorizationV1().SelfSubjectAccessReviews(),
		review.Simples{
			{
				Group:    apiext.SchemeGroupVersion.Group,
				Version:  apiext.SchemeGroupVersion.Version,
				Resource: "customresourcedefinitions",
			},
		},
		review.WithCreateIfNotExisted(),
	)
	if err != nil {
		return err
	}

	crdCli := cli.ApiextensionsV1().CustomResourceDefinitions()

	crds := MergeCRDs(crdGetters)
	for i := range crds {
		eCRD := crds[i]
		// 对齐函数使竞争失败的写入方得以重试而非直接返回冲突：
		// 每个副本在选主前都会安装定义，若无此机制，并发启动会导致其中一方失败。
		var terminating bool
		crdAlignFn := func(aCRD *apiext.CustomResourceDefinition) (_ *apiext.CustomResourceDefinition, skip bool, err error) {
			skip = true
			// 处于终止中的定义仍可提供读服务，但 API Server 会在其自定义资源排空后立即将其删除，
			// 因此向其写入无异于对齐一个即将消失的对象。此处保持原样并报告为未安装。
			if aCRD.GetDeletionTimestamp() != nil {
				terminating = true
				return aCRD, skip, err
			}
			// 对齐 spec。
			if !kubemeta.DeepEqual(aCRD.Spec, eCRD.Spec) {
				aCRD.Spec = *eCRD.Spec.DeepCopy()
				skip = false
			}
			return aCRD, skip, err
		}
		_, err = kubeclientset.Update(ctx, crdCli, eCRD,
			kubeclientset.WithCreateIfNotExisted[*apiext.CustomResourceDefinition](),
			kubeclientset.WithUpdateAlign(crdAlignFn))
		if err != nil {
			return fmt.Errorf("install custom resource definition %q: %w",
				eCRD.GetName(), err)
		}
		if terminating {
			// 这并非错误：阻塞删除的 finalizer 需要等本函数返回后启动的控制器来释放，
			// 若在此处失败，反而会使我们正在等待删除的定义永远无法清除。
			klog.InfoS("terminating custom resource definition, leaving it to drain",
				"crd", eCRD.GetName())
		}
	}

	return nil
}

// EnsureCRDs 持续恢复缺失的 CRD，直至给定的 ctx 结束。每次失败尝试都会记录日志，
// 在 ctx 结束时返回该取消错误，或者若某次尝试恰好与之竞态，则返回记录的最后一次失败——
// 因此调用方应先检查 ctx 状态，再将返回的 error 视为失败。
//
// 仅在启动时安装一次是不够的。之后被删除的定义，或启动时已处于终止状态并在随后排空的定义，
// 在进程生命周期内都会一直处于缺失状态，导致所有 watch 该资源的控制器持续失败。
// 请将本函数与控制器并行运行，而非在控制器之前运行：阻塞终止中定义的 finalizer 正是由这些控制器释放的。
//
// EnsureCRDs 本身不检查权限，且必须仅在 ctx 结束时才返回：调用方将其与维持进程存活的任务并列运行，
// 任何任务提前退出都会导致修复循环静默消失。权限检查由 InstallCRDs 负责，若缺乏权限则启动直接失败。
func EnsureCRDs(ctx context.Context, cli kubernetes.Interface, crdGetters []CRDGetter) error {
	crds := MergeCRDs(crdGetters)

	return waitx.RepeatUntilContextCancel(ctx, ensureInterval, false,
		func(ctx context.Context) error {
			err := restoreCRDs(ctx, cli, crds)
			if err != nil {
				// 记录每次尝试。轮询本身会在 ctx 结束前一直保留错误，
				// 而对于与进程同生命周期的循环而言，ctx 结束即意味着进程关闭。
				klog.InfoS("retrying to restore the custom resource definitions", "err", err)
			}
			return err
		})
}

// restoreCRDs 在给定 CRD 缺失时创建它们，对已存在的则保持原样（无论是否处于终止状态）。
//
// 它仅修复缺失问题。即使副本数为 1，滚动更新期间新旧两个副本也会短暂重叠，
// 若恢复逻辑去对齐 spec，退出的旧副本在存活期间就会每隔一个周期把旧版本定义覆盖回新版本。
// 对齐 spec 应由 InstallCRDs 在携带对应版本的新副本启动时完成。
func restoreCRDs(ctx context.Context, cli kubernetes.Interface, crds []*apiext.CustomResourceDefinition) error {
	crdCli := cli.ApiextensionsV1().CustomResourceDefinitions()

	keepFn := func(aCRD *apiext.CustomResourceDefinition) (*apiext.CustomResourceDefinition, bool, error) {
		return aCRD, true, nil
	}
	// 即使某个定义失败也会继续尝试其余定义：恢复按名称顺序执行，
	// 若在首个失败处放弃，一个持续失败的定义就会使排在其后的所有定义都无法得到处理。
	var errs []error
	for i := range crds {
		_, err := kubeclientset.Update(ctx, crdCli, crds[i],
			kubeclientset.WithCreateIfNotExisted[*apiext.CustomResourceDefinition](),
			kubeclientset.WithUpdateAlign(keepFn))
		if err != nil {
			errs = append(errs, fmt.Errorf("restore custom resource definition %q: %w",
				crds[i].GetName(), err))
		}
	}

	return multierr.Combine(errs...)
}

// MergeServices 从多个 getter 中合并 APIService 并以单个列表返回。
//
// 给定的 Service 引用和 CA 数据会传递给各 getter，用于构造 APIService。
func MergeServices(svc apireg.ServiceReference, ca []byte, getters []ServiceGetter) []*apireg.APIService {
	ret := make([]*apireg.APIService, 0, len(getters))
	for i := range getters {
		r := getters[i](svc, ca)
		if r != nil {
			ret = append(ret, r)
		}
	}
	return ret
}

// InstallServices 安装 APIService。
//
// 给定的 Service 引用和 CA 数据会传递给各 getter，用于构造 APIService。
func InstallServices(ctx context.Context, cli kubernetes.Interface, svc apireg.ServiceReference, ca []byte, getters []ServiceGetter) error {
	err := review.CanDoCreate(ctx,
		cli.AuthorizationV1().SelfSubjectAccessReviews(),
		review.Simples{
			{
				Group:    apireg.SchemeGroupVersion.Group,
				Version:  apireg.SchemeGroupVersion.Version,
				Resource: "apiservices",
			},
		},
		review.WithUpdateIfExisted(),
	)
	if err != nil {
		return err
	}

	svcCli := cli.ApiregistrationV1().APIServices()

	svcs := MergeServices(svc, ca, getters)
	for i := range svcs {
		eSvc := svcs[i]
		svcAlignFn := func(aSvc *apireg.APIService) (_ *apireg.APIService, skip bool, err error) {
			skip = true
			// 若 Service 引用不同则无需更新。
			if aSvc.Spec.Service.Namespace != eSvc.Spec.Service.Namespace ||
				aSvc.Spec.Service.Name != eSvc.Spec.Service.Name {
				return aSvc, skip, err
			}
			// 对齐 Service 端口。
			if !ptr.Equal(aSvc.Spec.Service.Port, eSvc.Spec.Service.Port) {
				aSvc.Spec.Service.Port = eSvc.Spec.Service.Port
				skip = false
			}
			// 对齐 CA 证书包。
			if !bytes.Equal(aSvc.Spec.CABundle, eSvc.Spec.CABundle) {
				aSvc.Spec.CABundle = eSvc.Spec.CABundle
				skip = false
			}
			// 对齐 InsecureSkipTLSVerify。
			if aSvc.Spec.InsecureSkipTLSVerify != eSvc.Spec.InsecureSkipTLSVerify {
				aSvc.Spec.InsecureSkipTLSVerify = eSvc.Spec.InsecureSkipTLSVerify
				skip = false
			}
			return aSvc, skip, err
		}
		_, err = kubeclientset.Create(ctx, svcCli, eSvc,
			kubeclientset.WithUpdateIfExisted(svcAlignFn))
		if err != nil {
			return fmt.Errorf("install api service %q: %w",
				svcs[i].Name, err)
		}
	}

	return nil
}

// EnsureServices 持续恢复缺失的 APIService，直至给定的 ctx 结束。每次失败尝试都会记录日志，
// 在 ctx 结束时的返回值语义与 EnsureCRDs 一致：返回取消错误，或若某次尝试恰好与之竞态，则返回记录的最后一次失败。
//
// 启动后被删除的 APIService 会使其前置的聚合 API 在进程生命周期内一直无响应，正如 EnsureCRDs 为 CRD 所防范的情况。
// 两者遵循相同的约束：自身不检查权限、仅在 ctx 结束时返回、且仅修复缺失问题。
func EnsureServices(ctx context.Context, cli kubernetes.Interface, svc apireg.ServiceReference, ca []byte, getters []ServiceGetter) error {
	svcs := MergeServices(svc, ca, getters)

	return waitx.RepeatUntilContextCancel(ctx, ensureInterval, false,
		func(ctx context.Context) error {
			err := restoreServices(ctx, cli, svcs)
			if err != nil {
				klog.InfoS("retrying to restore the api services", "err", err)
			}
			return err
		})
}

// restoreServices 在给定 APIService 缺失时创建它们，对已存在的则保持原样。
//
// 与 restoreCRDs 相同，它仅修复缺失问题：滚动更新中退出的旧副本也会运行此循环，
// 而它持有的 Service 引用和 CA 证书包属于旧副本自身。对齐这些配置应由 InstallServices 在所属副本启动时完成。
func restoreServices(ctx context.Context, cli kubernetes.Interface, svcs []*apireg.APIService) error {
	svcCli := cli.ApiregistrationV1().APIServices()

	keepFn := func(aSvc *apireg.APIService) (*apireg.APIService, bool, error) {
		return aSvc, true, nil
	}
	// 出于与 restoreCRDs 相同的原因，即使某个服务失败也会继续尝试其余服务。
	var errs []error
	for i := range svcs {
		_, err := kubeclientset.Update(ctx, svcCli, svcs[i],
			kubeclientset.WithCreateIfNotExisted[*apireg.APIService](),
			kubeclientset.WithUpdateAlign(keepFn))
		if err != nil {
			errs = append(errs, fmt.Errorf("restore api service %q: %w",
				svcs[i].GetName(), err))
		}
	}

	return multierr.Combine(errs...)
}

// DeleteServicesBackedBy 删除后端 Service 位于指定 Namespace 的 APIService。
// 这涵盖了非本进程创建的注册项：通过 chart 安装的聚合 API（例如眼前的 Kueue visibility 对）同样是集群作用域的，
// 若跳过 chart 卸载而直接删除 Namespace，它会因无法继续提供 discovery 服务而卡住 Namespace 删除，
// 与本进程注册的 APIService 表现一致。缺失的服务视为已删除，且出于与 restoreCRDs 相同的原因，即使某个服务失败也会继续尝试其余服务。
func DeleteServicesBackedBy(ctx context.Context, cli kubernetes.Interface, namespace string) error {
	svcCli := cli.ApiregistrationV1().APIServices()

	svcs, err := svcCli.List(ctx, meta.ListOptions{})
	if err != nil {
		return fmt.Errorf("list api services: %w", err)
	}

	var errs []error
	for i := range svcs.Items {
		svc := svcs.Items[i].Spec.Service
		if svc == nil || svc.Namespace != namespace {
			continue
		}
		err = svcCli.Delete(ctx, svcs.Items[i].GetName(), meta.DeleteOptions{})
		if err != nil && !kerrors.IsNotFound(err) {
			errs = append(errs, fmt.Errorf("delete api service %q: %w",
				svcs.Items[i].GetName(), err))
		}
	}

	return multierr.Combine(errs...)
}

// IsServicesReady 报告 APIService 是否就绪，对每个服务检查一次。返回的 error 会指明首个不可用的服务。
//
// 当调用方自身已有调度重试机制时，应优先使用此函数而非 WaitForServicesReady，
// 以避免两层重试叠加，从而确保按调用方的调度频率轮询服务。
func IsServicesReady(ctx context.Context, cli kubernetes.Interface, getters []ServiceGetter) error {
	svcCli := cli.ApiregistrationV1().APIServices()
	svcs := MergeServices(apireg.ServiceReference{}, nil, getters)

	for i := range svcs {
		svc, err := svcCli.Get(ctx, svcs[i].Name,
			meta.GetOptions{
				ResourceVersion: "0",
			})
		if err != nil {
			return err
		}

		ready := false
		for j := range svc.Status.Conditions {
			if svc.Status.Conditions[j].Type != apireg.Available {
				continue
			}
			ready = svc.Status.Conditions[j].Status == apireg.ConditionTrue
			break
		}
		if !ready {
			return fmt.Errorf("api service %q is not ready", svc.Name)
		}
	}

	return nil
}

// WaitForServicesReady 等待 APIService 就绪，每 2 秒轮询一次，30 秒后超时放弃，超时时返回该轮执行的最后一次失败。
func WaitForServicesReady(ctx context.Context, cli kubernetes.Interface, getters []ServiceGetter) error {
	return waitx.RetryUntilSuccessWithTimeout(ctx, 2*time.Second, 30*time.Second, false,
		func(ctx context.Context) error {
			return IsServicesReady(ctx, cli, getters)
		})
}
