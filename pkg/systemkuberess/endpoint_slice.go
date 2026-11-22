package systemkuberess

import (
	"context"
	"errors"
	"fmt"
	"net"

	core "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"gpustack.ai/gpustack/pkg/kubeclients/kubernetes"
	"gpustack.ai/gpustack/pkg/kubeclientset"
	"gpustack.ai/gpustack/pkg/kubemeta"
)

const (
	// routingManagedBy 是系统路由 EndpointSlice 的管理者标识。
	//
	// 它采用独立的值，而不是 EndpointSlice 控制器或 EndpointSlice 镜像控制器的标识，
	// 以免二者接管该 Slice，同时也能将本 operator 创建的 Slice 与镜像 Slice 区分开来。
	routingManagedBy = "gpustack-operator-worker"

	// routingEndpointSliceNameSuffix 是系统路由 EndpointSlice 名称的后缀。
	//
	// 该 Slice 的命名与其所服务的 Service 区分开，因为传统 Endpoints 的镜像 Slice
	// 是以 Endpoints 名称加上简短随机后缀命名的，两者绝不能成为同一个对象。
	routingEndpointSliceNameSuffix = "-direct"
)

// addressTypeMismatchError 报告集群中的 EndpointSlice 所携带的地址类型与路由期望的不一致。
//
// EndpointSlice 的地址类型无法更改，因此遇到此类 Slice 时会直接替换而非更新。
type addressTypeMismatchError struct {
	name     string
	actual   discovery.AddressType
	expected discovery.AddressType
}

func (e *addressTypeMismatchError) Error() string {
	return fmt.Sprintf("routing endpoint slice %q has address type %q, want %q",
		e.name, e.actual, e.expected)
}

// newRoutingEndpointSlice 构建系统路由期望的 EndpointSlice。
//
// 聚合器会读取携带该 Service 名称的 Slice，并选取第一个端口名与 Service 端口一致且端点就绪的 Slice。
// 因此，该 Slice 会关联 Service 名称、使用 Service 端口名，并携带指向系统地址的就绪端点。
func newRoutingEndpointSlice(nsName, svcName, address string, port int32) (*discovery.EndpointSlice, error) {
	addressType, err := routingAddressType(address)
	if err != nil {
		return nil, err
	}

	ready, serving, terminating := true, true, false

	return &discovery.EndpointSlice{
		ObjectMeta: meta.ObjectMeta{
			Namespace: nsName,
			Name:      routingEndpointSliceName(svcName),
			Labels: map[string]string{
				discovery.LabelServiceName: svcName,
				discovery.LabelManagedBy:   routingManagedBy,
				routingLabel:               routingLabelValue,
			},
		},
		AddressType: addressType,
		Endpoints: []discovery.Endpoint{
			{
				Addresses: []string{address},
				Conditions: discovery.EndpointConditions{
					Ready:       &ready,
					Serving:     &serving,
					Terminating: &terminating,
				},
			},
		},
		Ports: []discovery.EndpointPort{
			{
				Name:     ptr.To(routingPortName),
				Port:     ptr.To(port),
				Protocol: ptr.To(core.ProtocolTCP),
			},
		},
	}, nil
}

// routingEndpointSliceName 返回系统路由 EndpointSlice 的名称。
func routingEndpointSliceName(svcName string) string {
	return svcName + routingEndpointSliceNameSuffix
}

// routingAddressType 返回能够承载给定地址的 EndpointSlice 地址类型。
//
// 聚合器优先选择与 Service 主地址族一致的 Slice，且 EndpointSlice 仅接受与其自身类型匹配的地址，
// 因此地址类型必须随地址而定。
func routingAddressType(address string) (discovery.AddressType, error) {
	ip := net.ParseIP(address)
	if ip == nil {
		return "", fmt.Errorf("routing address %q is not an IP address", address)
	}
	if ip.To4() != nil {
		return discovery.AddressTypeIPv4, nil
	}
	return discovery.AddressTypeIPv6, nil
}

// installRoutingEndpointSlice 创建或更新系统路由的 EndpointSlice。
//
// EndpointSlice 的地址类型无法修改，因此为另一地址族创建的 Slice 会被替换而非更新。
//
// 同一时间只有一个 worker 拥有集群的路由控制权。由于每次安装的 Namespace 和路由 Service 名称均相同，
// 若两个 worker 指向同一集群，它们会写入同一个 Slice，以最后写入者为准。
// 对于不属于当前 worker 的 Slice，会拒绝修改而不是强行接管。
func installRoutingEndpointSlice(ctx context.Context, cli kubernetes.Interface, nsName string,
	desired *discovery.EndpointSlice, svc *core.Service,
) error {
	sliceCli := cli.DiscoveryV1().EndpointSlices(nsName)

	actual, err := sliceCli.Get(ctx, desired.GetName(), meta.GetOptions{})
	switch {
	case err == nil:
		err = checkRoutingSliceIdentity(actual, svc)
		if err != nil {
			return err
		}
		if actual.AddressType != desired.AddressType {
			err = deleteRoutingSlice(ctx, cli, nsName, actual)
			if err != nil && !kerrors.IsConflict(err) {
				return err
			}
			// 发生冲突意味着在此期间有其他参与者替换了该 Slice。
			// 下面的写入操作会读取当前存在的 Slice 并据此作出处理。
		}
	case kerrors.IsNotFound(err):
		// 该 Slice 尚不存在，将在下方创建。
	default:
		return fmt.Errorf("get routing endpoint slice %q: %w", desired.GetName(), err)
	}

	kubemeta.ControlOn(desired, svc, core.SchemeGroupVersion.WithKind("Service"))

	// 创建时读取到的内容在删除后仍可能显示旧地址族的 Slice，
	// 因此在删除该 Slice 后遇到地址族不匹配时会进行重试。
	for attempt := 0; ; attempt++ {
		_, err = kubeclientset.Create(ctx, sliceCli, desired,
			kubeclientset.WithUpdateIfExisted(routingEndpointSliceAlign(desired, svc)))
		if err == nil {
			return nil
		}

		var mismatch *addressTypeMismatchError
		if !errors.As(err, &mismatch) || attempt >= 2 {
			return fmt.Errorf("install routing endpoint slice %q: %w", desired.GetName(), err)
		}

		stale, getErr := sliceCli.Get(ctx, desired.GetName(), meta.GetOptions{})
		if getErr != nil {
			if kerrors.IsNotFound(getErr) {
				continue
			}
			return fmt.Errorf("get routing endpoint slice %q: %w", desired.GetName(), getErr)
		}

		// 在发现不匹配的读取操作与本次读取之间，该 Slice 可能已被替换，
		// 因此在执行任何删除操作前会再次校验其身份。删除请求的 UID 前置条件
		// 会将其绑定到此处读取到的 Slice，而该 Slice 可能由其他参与者拥有。
		err = checkRoutingSliceIdentity(stale, svc)
		if err != nil {
			return err
		}

		if stale.AddressType == desired.AddressType {
			// 该 Slice 当前已携带期望的地址族，因此协调过程可直接基于它收敛，无需删除。
			continue
		}

		err = deleteRoutingSlice(ctx, cli, nsName, stale)
		if err != nil {
			if !kerrors.IsConflict(err) {
				return err
			}
			// 在此期间有其他参与者替换了该 Slice。下面的写入操作会读取当前存在的 Slice 并据此作出处理。
			continue
		}
	}
}

// checkRoutingSliceIdentity 拒绝不属于当前 worker 的 EndpointSlice。
//
// 当 Slice 带有本 operator 的 managed-by 标签且由路由 Service 控制，或者带有该标签但完全没有所有者
// （即本 operator 早期版本遗留的 Slice 状态）时，该 Slice 才属于当前 worker。
// 其他任何 Slice 都会被拒绝，并在错误信息中指明其所有者。
func checkRoutingSliceIdentity(slice *discovery.EndpointSlice, svc *core.Service) error {
	if slice.GetLabels()[discovery.LabelManagedBy] != routingManagedBy {
		return fmt.Errorf("routing endpoint slice %q is managed by %q, not by this operator",
			slice.GetName(), slice.GetLabels()[discovery.LabelManagedBy])
	}

	owner := kubemeta.GetControllerOfNoCopy(slice)
	switch {
	case owner == nil:
		// 本 operator 早期版本创建的 Slice，当时未设置所有者。
	case routingServiceOwns(slice, svc):
		// 该 Slice 由路由 Service 控制。所有者的 UID 可能属于本次安装中已被重建的旧 Service
		// （Slice 的生命周期长于旧 Service）；此时会接管该 Slice，并将其 ownerReference 重写为当前的 Service。
	default:
		return fmt.Errorf("routing endpoint slice %q is controlled by %s %q with uid %q, not by the routing service",
			slice.GetName(), owner.Kind, owner.Name, owner.UID)
	}

	return nil
}

// routingServiceOwns 报告路由 Service 是否控制该 Slice。
//
// 此处通过引用的 Service 名称而非 UID 来识别所有者，因为每当 spec 变更时路由 Service 都会被重建。
// 过去 Service 所拥有的 Slice 仍然属于当前安装，且该 Namespace 下同名 Service 只有一个，
// 因此名称与 UID 同样能够证明所有权，且在 Service 重建后依然有效。
func routingServiceOwns(slice *discovery.EndpointSlice, svc *core.Service) bool {
	if svc == nil {
		return false
	}

	return controlsRoutingOwner(slice, "Service", svc.GetName())
}

// routingEndpointsOwns 报告传统 Endpoints 是否控制该 Slice。
//
// 此处同样通过名称识别所有者，原因与路由 Service 相同：传统 Endpoints 在被删除后会重建，
// 过去 Endpoints 的镜像 Slice 依然属于当前安装。
func routingEndpointsOwns(slice *discovery.EndpointSlice, ep *core.Endpoints) bool {
	if ep == nil {
		return false
	}

	return controlsRoutingOwner(slice, "Endpoints", ep.GetName())
}

// controlsRoutingOwner 报告该 Slice 的控制器 ownerReference 是否指向给定 kind 和名称的资源。
func controlsRoutingOwner(slice *discovery.EndpointSlice, kind, name string) bool {
	owner := kubemeta.GetControllerOfNoCopy(slice)
	if owner == nil {
		return false
	}

	return owner.APIVersion == core.SchemeGroupVersion.String() &&
		owner.Kind == kind &&
		owner.Name == name
}

// routingEndpointSliceAlign 返回系统路由 EndpointSlice 的对齐函数。
//
// 端点和端口采用替换而非追加方式，以确保系统迁移到其他地址或端口后不会保留旧值。
//
// 不仅在之前读取的对象上，也会在即将写入的对象上校验 Slice 的身份，
// 以防止在读写之间被调包的 Slice 被误改写。
func routingEndpointSliceAlign(desired *discovery.EndpointSlice, svc *core.Service) kubeclientset.AlignWithFn[*discovery.EndpointSlice] {
	return func(actual *discovery.EndpointSlice) (*discovery.EndpointSlice, bool, error) {
		err := checkRoutingSliceIdentity(actual, svc)
		if err != nil {
			return nil, false, err
		}

		if actual.AddressType != desired.AddressType {
			return nil, false, &addressTypeMismatchError{
				name:     actual.GetName(),
				actual:   actual.AddressType,
				expected: desired.AddressType,
			}
		}

		aligned := actual.DeepCopy()
		aligned.SetLabels(mergeLabels(actual.GetLabels(), desired.GetLabels()))
		kubemeta.ControlOn(aligned, svc, core.SchemeGroupVersion.WithKind("Service"))

		// 端点和端口均经过深拷贝，不与 desired 对象共享，
		// 以避免对写入对象的修改影响到下一次尝试写入的对象。
		wanted := desired.DeepCopy()
		aligned.Ports = wanted.Ports
		aligned.Endpoints = wanted.Endpoints

		if apiequality.Semantic.DeepEqual(actual, aligned) {
			return actual, true, nil
		}
		return aligned, false, nil
	}
}

// deleteRoutingSlice 删除系统路由的一个 EndpointSlice。
//
// 删除操作绑定了所读取 Slice 的 UID，从而避免误删在此期间被其他参与者替换的 Slice。
// 若发生替换，API Server 会返回冲突错误，随后由调用方重新读取当前存在的 Slice。
func deleteRoutingSlice(ctx context.Context, cli kubernetes.Interface, nsName string, slice *discovery.EndpointSlice) error {
	err := cli.DiscoveryV1().EndpointSlices(nsName).Delete(ctx, slice.GetName(), meta.DeleteOptions{
		PropagationPolicy: ptr.To(meta.DeletePropagationBackground),
		Preconditions:     &meta.Preconditions{UID: ptr.To(slice.GetUID())},
	})
	if err != nil {
		if kerrors.IsNotFound(err) {
			// 该 Slice 已不存在，已达到删除操作的目标状态。
			return nil
		}
		return fmt.Errorf("delete routing endpoint slice %q: %w", slice.GetName(), err)
	}
	return nil
}

// cleanupObsoleteRoutingSlices 移除属于当前安装且不再需要的 EndpointSlice：
// 包括本 operator 早期版本以其他名称创建的 Slice，以及 EndpointSlice 镜像控制器
// 从传统 Endpoints 镜像生成的 Slice。
//
// 仅移除 ownerReference 指向当前安装的 Service 或传统 Endpoints 的 Slice。
// 带有路由 Service 名称但属于其他所有者的 Slice 会予以保留，因为仅凭标签不足以证明所有权。
func cleanupObsoleteRoutingSlices(ctx context.Context, cli kubernetes.Interface, nsName string,
	svc *core.Service, ep *core.Endpoints, keepName string,
) error {
	if svc == nil || ep == nil {
		// 缺少所有者对象时无法证明所有权，因此不执行任何移除操作。
		return nil
	}

	sliceCli := cli.DiscoveryV1().EndpointSlices(nsName)
	selector := discovery.LabelServiceName + "=" + svc.GetName()
	slices, err := sliceCli.List(ctx, meta.ListOptions{LabelSelector: selector})
	if err != nil {
		return fmt.Errorf("list routing endpoint slices: %w", err)
	}

	for i := range slices.Items {
		slice := &slices.Items[i]
		if slice.GetName() == keepName {
			continue
		}

		if !isObsoleteRoutingSlice(slice, svc, ep) {
			// 不属于当前安装，予以保留。
			continue
		}

		err = deleteObsoleteRoutingSlice(ctx, cli, nsName, slice, svc, ep, keepName)
		if err != nil {
			return err
		}
	}

	return nil
}

// isObsoleteRoutingSlice 报告该 Slice 是否属于当前安装且已不再需要。
func isObsoleteRoutingSlice(slice *discovery.EndpointSlice, svc *core.Service, ep *core.Endpoints) bool {
	switch {
	case routingEndpointsOwns(slice, ep):
		// 当前安装传统 Endpoints 的镜像 Slice。它与直连 Slice 携带相同的地址，
		// 但聚合器可能会任选其一，因此需要用直连 Slice 替代它。
	case slice.GetLabels()[discovery.LabelManagedBy] == routingManagedBy &&
		routingServiceOwns(slice, svc):
		// 当前安装使用旧版本命名创建的直连 Slice，当前版本已不再使用该名称。
	default:
		return false
	}

	return true
}

// deleteObsoleteRoutingSlice 移除过时的 Slice；若在此期间被其他参与者替换，则重新读取当前 Slice。
//
// 删除请求携带所读取 Slice 的 UID，以免误删其他参与者新放入的 Slice。
// 随后会重新读取当前存在的 Slice，若其同样属于我们且已过时，则会一并移除。
func deleteObsoleteRoutingSlice(ctx context.Context, cli kubernetes.Interface, nsName string,
	slice *discovery.EndpointSlice, svc *core.Service, ep *core.Endpoints, keepName string,
) error {
	err := deleteRoutingSlice(ctx, cli, nsName, slice)
	if err == nil || !kerrors.IsConflict(err) {
		return err
	}

	current, err := cli.DiscoveryV1().EndpointSlices(nsName).
		Get(ctx, slice.GetName(), meta.GetOptions{})
	if err != nil {
		if kerrors.IsNotFound(err) {
			// 该 Slice 已不存在，已达到删除操作的目标状态。
			return nil
		}
		return fmt.Errorf("get routing endpoint slice %q: %w", slice.GetName(), err)
	}

	if current.GetName() == keepName || !isObsoleteRoutingSlice(current, svc, ep) {
		// 保留当前存在的 Slice，它要么不属于我们，要么正是当前安装正在使用的 Slice。
		return nil
	}

	return deleteRoutingSlice(ctx, cli, nsName, current)
}
