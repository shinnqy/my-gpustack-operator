package apis

import (
	"context"
	"fmt"
	"time"

	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	apiregistration "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	GroupName        = "worker.gpustack.ai"
	StorageVersion   = "v1alpha1"
	ExtensionVersion = "v1"
	DevicesResource  = "devices"
	DevicesKind      = "Devices"
	apiServiceName   = ExtensionVersion + "." + GroupName
)

func NewDevices() *unstructured.Unstructured {
	object := &unstructured.Unstructured{}
	object.SetGroupVersionKind(schema.GroupVersionKind{Group: GroupName, Version: StorageVersion, Kind: DevicesKind})
	return object
}

// InstallCRDs 建立本章使用的 Devices 存储 API，并等待它可以被发现。
func InstallCRDs(ctx context.Context, kube client.Client) error {
	name := DevicesResource + "." + GroupName
	crd := &apiextensions.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: apiextensions.CustomResourceDefinitionSpec{
			Group: GroupName,
			Scope: apiextensions.ClusterScoped,
			Names: apiextensions.CustomResourceDefinitionNames{
				Plural:   DevicesResource,
				Singular: DevicesResource,
				Kind:     DevicesKind,
				ListKind: DevicesKind + "List",
			},
			Versions: []apiextensions.CustomResourceDefinitionVersion{{
				Name:    StorageVersion,
				Served:  true,
				Storage: true,
				// 先保存设备字段，后续章节再加入具体的结构和校验。
				Schema: &apiextensions.CustomResourceValidation{OpenAPIV3Schema: &apiextensions.JSONSchemaProps{
					Type: "object",
					Properties: map[string]apiextensions.JSONSchemaProps{
						"spec":   {Type: "object", XPreserveUnknownFields: ptr.To(true)},
						"status": {Type: "object", XPreserveUnknownFields: ptr.To(true)},
					},
				}},
				Subresources: &apiextensions.CustomResourceSubresources{
					Status: &apiextensions.CustomResourceSubresourceStatus{},
				},
			}},
		},
	}
	// 已有集群可能具有完整的 Devices 定义，应保留其中的 schema。
	if err := kube.Create(ctx, crd); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	err := wait.PollUntilContextTimeout(ctx, time.Second, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		if err := kube.Get(ctx, client.ObjectKey{Name: name}, crd); err != nil {
			return false, err
		}
		for _, condition := range crd.Status.Conditions {
			if condition.Type == apiextensions.Established && condition.Status == apiextensions.ConditionTrue {
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil {
		return fmt.Errorf("wait for %s to be established: %w", name, err)
	}
	return nil
}

// InstallServices 将 v1 聚合 API 指向 Worker 的 HTTPS Service。
func InstallServices(ctx context.Context, kube client.Client, namespace, service string, port int32, caBundle []byte) error {
	hook := &apiregistration.APIService{ObjectMeta: metav1.ObjectMeta{Name: apiServiceName}}
	_, err := controllerutil.CreateOrUpdate(ctx, kube, hook, func() error {
		hook.Spec = apiregistration.APIServiceSpec{
			Group:                GroupName,
			Version:              ExtensionVersion,
			GroupPriorityMinimum: 100,
			VersionPriority:      100,
			Service:              &apiregistration.ServiceReference{Namespace: namespace, Name: service, Port: &port},
			CABundle:             caBundle,
		}
		return nil
	})
	return err
}

// WaitForServicesReady 等待 Kubernetes 聚合层确认 APIService 可访问。
func WaitForServicesReady(ctx context.Context, reader client.Reader) error {
	service := &apiregistration.APIService{}
	err := wait.PollUntilContextTimeout(ctx, 2*time.Second, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		if err := reader.Get(ctx, client.ObjectKey{Name: apiServiceName}, service); err != nil {
			return false, err
		}
		for _, condition := range service.Status.Conditions {
			if condition.Type == apiregistration.Available {
				return condition.Status == apiregistration.ConditionTrue, nil
			}
		}
		return false, nil
	})
	if err != nil {
		return fmt.Errorf("APIService %q is not available: %w", apiServiceName, err)
	}
	return nil
}
