package systemkuberess

import (
	"context"
	"fmt"
	"slices"

	authz "k8s.io/api/authorization/v1"
	core "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	"gpustack.ai/gpustack/pkg/kubeclients/kubernetes"
	"gpustack.ai/gpustack/pkg/kubeclientset"
	"gpustack.ai/gpustack/pkg/kubeclientset/review"
	"gpustack.ai/gpustack/pkg/kubereviewself"
	"gpustack.ai/gpustack/pkg/system"
)

const (
	// routingLabel 用于标记承载系统路由 Service 的资源，以便将其与集群自身的资源区分开来。
	routingLabel = "gpustack.ai/fake-routing"

	// routingLabelValue 是 routingLabel 的值。
	routingLabelValue = "true"

	// routingPortName 是路由端口的名称。Service、传统 Endpoints 与 EndpointSlice
	// 均使用相同的端口名称，因为聚合器会根据其所服务 Service 的端口名来匹配 Slice 的端口。
	routingPortName = "https"
)

// InstallFakeSystemRoutingService 为系统创建虚拟路由 Service/Endpoint。
//
// 该 Service 指向系统的 SelfIP。
func InstallFakeSystemRoutingService(ctx context.Context, cli kubernetes.Interface, nsName, svcName string, port int32) error {
	return installFakeSystemRoutingService(ctx, cli, nsName, svcName, system.PrimaryIP.Get(), port)
}

// installFakeSystemRoutingService 为系统创建虚拟路由 Service/Endpoint。
//
// address 是该 Service 路由指向的系统地址。将其作为参数传入而非直接从 system 包读取，
// 以便在不依赖全局进程状态的情况下构建并校验期望的资源。
func installFakeSystemRoutingService(ctx context.Context, cli kubernetes.Interface, nsName, svcName, address string, port int32) error {
	err := checkRoutingAccess(ctx, cli, nsName)
	if err != nil {
		return err
	}

	// 该地址决定了 EndpointSlice 的地址族，因此在执行任何写入操作前先进行检查。
	desiredSlice, err := newRoutingEndpointSlice(nsName, svcName, address, port)
	if err != nil {
		return err
	}

	svc, err := installRoutingService(ctx, cli, nsName, svcName, port, desiredSlice.AddressType)
	if err != nil {
		return err
	}

	ep, err := installRoutingEndpoints(ctx, cli, nsName, svcName, address, port)
	if err != nil {
		return err
	}

	err = installRoutingEndpointSlice(ctx, cli, nsName, desiredSlice, svc)
	if err != nil {
		return err
	}

	return cleanupObsoleteRoutingSlices(ctx, cli, nsName, svc, ep, desiredSlice.GetName())
}

// checkRoutingAccess 审查路由协调过程所需执行的操作权限。
//
// Service 和传统 Endpoints 保持其一贯所需的权限。EndpointSlice 则单独进行审查，
// 因为前两者的权限并不覆盖清理过时 Slice 时所需的 list 读取权限。
func checkRoutingAccess(ctx context.Context, cli kubernetes.Interface, nsName string) error {
	err := review.CanDoCreate(ctx,
		cli.AuthorizationV1().SelfSubjectAccessReviews(),
		review.Simples{
			{
				Group:    core.SchemeGroupVersion.Group,
				Version:  core.SchemeGroupVersion.Version,
				Resource: "services",
			},
			{
				Group:    core.SchemeGroupVersion.Group,
				Version:  core.SchemeGroupVersion.Version,
				Resource: "endpoints",
			},
		},
		review.WithRecreateIfDuplicated(),
		review.WithUpdateIfExisted(),
	)
	if err != nil {
		return err
	}

	verbs := []string{"get", "list", "create", "update", "delete"}
	reviews := make(kubereviewself.Reviews, 0, len(verbs)+1)
	for _, verb := range verbs {
		reviews = append(reviews, kubereviewself.Review{
			ResourceAttributes: &authz.ResourceAttributes{
				Verb:      verb,
				Group:     discovery.SchemeGroupVersion.Group,
				Version:   discovery.SchemeGroupVersion.Version,
				Resource:  "endpointslices",
				Namespace: nsName,
			},
		})
	}

	// Slice 的 ownerReference 会阻止路由 Service 被删除，
	// 因此 API Server 在接受该 Slice 前会要求具备更新 Service finalizer 的权限。
	reviews = append(reviews, kubereviewself.Review{
		ResourceAttributes: &authz.ResourceAttributes{
			Verb:        "update",
			Group:       core.SchemeGroupVersion.Group,
			Version:     core.SchemeGroupVersion.Version,
			Resource:    "services",
			Subresource: "finalizers",
			Namespace:   nsName,
		},
	})

	return kubereviewself.CanDo(ctx, cli.AuthorizationV1().SelfSubjectAccessReviews(), reviews)
}

// installRoutingService 创建作为扩展 API Service 前置入口的无 selector Service，
// 并返回集群中实际保存的对象。
func installRoutingService(ctx context.Context, cli kubernetes.Interface, nsName, svcName string, port int32,
	addressType discovery.AddressType,
) (*core.Service, error) {
	expected := newRoutingService(nsName, svcName, port, addressType)
	svcCompareFn := func(actual *core.Service) bool {
		if actual.Spec.Type != expected.Spec.Type {
			return false
		}
		// 仅当期望的 Service 指定了 ClusterIP 时才进行比较。
		// operator 不管理该字段，而是由 API Server 为集群中的 Service 分配；
		// 若将其与期望 Service 中的空 IP 比较，会导致每次启动都删除并重建 Service。
		// 重建 Service 会连带删除当前安装的 EndpointSlice（因为 Slice 作为其从属资源会阻塞删除），
		// 并且还会改变路由地址。
		if expected.Spec.ClusterIP != "" && actual.Spec.ClusterIP != expected.Spec.ClusterIP {
			return false
		}
		if len(actual.Spec.IPFamilies) > 0 && actual.Spec.IPFamilies[0] != expected.Spec.IPFamilies[0] {
			return false
		}
		if actual.Spec.ClusterIP != "" && actual.Spec.ClusterIP != core.ClusterIPNone {
			if actualType, err := routingAddressType(actual.Spec.ClusterIP); err == nil && actualType != addressType {
				return false
			}
		}
		expectedPort := expected.Spec.Ports[0]
		return slices.ContainsFunc(actual.Spec.Ports, func(ap core.ServicePort) bool {
			return ap.Name == expectedPort.Name &&
				ap.Port == expectedPort.Port &&
				(ap.Protocol == "" || ap.Protocol == expectedPort.Protocol)
		})
	}

	actual, err := kubeclientset.Create(ctx, cli.CoreV1().Services(nsName), expected,
		kubeclientset.WithRecreateIfDuplicated(svcCompareFn))
	if err != nil {
		return nil, fmt.Errorf("install fake routing service %q: %w", expected.GetName(), err)
	}

	return actual, nil
}

// newRoutingService 构建系统路由期望的 Service。
//
// 该 Service 没有 selector，因此不会有控制器为其选取端点，
// 下方的 EndpointSlice 是路由到系统的唯一依据。
func newRoutingService(nsName, svcName string, port int32, addressType discovery.AddressType) *core.Service {
	return &core.Service{
		ObjectMeta: meta.ObjectMeta{
			Namespace: nsName,
			Name:      svcName,
			Labels: map[string]string{
				routingLabel: routingLabelValue,
			},
		},
		Spec: core.ServiceSpec{
			Type:       core.ServiceTypeClusterIP,
			IPFamilies: []core.IPFamily{routingIPFamily(addressType)},
			Ports: []core.ServicePort{
				{
					Name:     routingPortName,
					Port:     port,
					Protocol: core.ProtocolTCP,
				},
			},
		},
	}
}

// routingIPFamily 返回与 EndpointSlice 地址类型匹配的 Service IP 地址族。
func routingIPFamily(addressType discovery.AddressType) core.IPFamily {
	if addressType == discovery.AddressTypeIPv6 {
		return core.IPv6Protocol
	}
	return core.IPv4Protocol
}

// installRoutingEndpoints 创建系统路由的传统 Endpoints，并返回集群中实际保存的对象。
//
// 保留传统 Endpoints 是为了兼容仍在读取它们的旧版聚合器（即早于聚合器迁移至 EndpointSlice 的
// Kubernetes 版本）。skip mirror 标签可防止镜像控制器据此再生成第二个 Slice，
// 否则聚合器可能会在两个 Slice 中任选其一。
func installRoutingEndpoints(ctx context.Context, cli kubernetes.Interface, nsName, svcName, address string, port int32) (*core.Endpoints, error) {
	expected := newRoutingEndpoints(nsName, svcName, address, port)
	epAlignFn := func(actual *core.Endpoints) (*core.Endpoints, bool, error) {
		aligned := actual.DeepCopy()
		aligned.SetLabels(mergeLabels(actual.GetLabels(), expected.GetLabels()))
		// 传统 Endpoints 的地址和端口采用替换而非追加方式，
		// 以确保系统迁移到其他地址后不会保留旧值。
		aligned.Subsets = expected.Subsets

		if apiequality.Semantic.DeepEqual(actual.GetLabels(), aligned.GetLabels()) &&
			apiequality.Semantic.DeepEqual(actual.Subsets, aligned.Subsets) {
			return actual, true, nil
		}
		return aligned, false, nil
	}

	actual, err := kubeclientset.Create(ctx, cli.CoreV1().Endpoints(nsName), expected,
		kubeclientset.WithUpdateIfExisted(epAlignFn))
	if err != nil {
		return nil, fmt.Errorf("install fake routing endpoints %q: %w", expected.GetName(), err)
	}

	return actual, nil
}

// newRoutingEndpoints 构建系统路由期望的传统 Endpoints。
func newRoutingEndpoints(nsName, svcName, address string, port int32) *core.Endpoints {
	return &core.Endpoints{
		ObjectMeta: meta.ObjectMeta{
			Namespace: nsName,
			Name:      svcName,
			Labels: map[string]string{
				routingLabel:              routingLabelValue,
				discovery.LabelSkipMirror: "true",
			},
		},
		Subsets: []core.EndpointSubset{
			{
				Addresses: []core.EndpointAddress{
					{
						IP: address,
					},
				},
				Ports: []core.EndpointPort{
					{
						Name: routingPortName,
						Port: port,
					},
				},
			},
		},
	}
}

// mergeLabels 将期望的标签叠加到对象现有标签之上并返回，
// 从而保留其他参与者添加的标签，同时确保期望标签始终生效。
func mergeLabels(actual, wanted map[string]string) map[string]string {
	if len(actual) == 0 && len(wanted) == 0 {
		return nil
	}

	merged := make(map[string]string, len(actual)+len(wanted))
	for k, v := range actual {
		merged[k] = v
	}
	for k, v := range wanted {
		merged[k] = v
	}
	return merged
}
