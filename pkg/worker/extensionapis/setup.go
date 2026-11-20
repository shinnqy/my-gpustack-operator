package extensionapis

import (
	"net/http"

	apidiscovery "k8s.io/api/apidiscovery/v2"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/endpoints/discovery"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"gpustack.ai/gpustack/pkg/worker/apis"
)

// Setup 注册聚合 API 的发现路由，并把读取转发到 CRD 的存储版本。
func Setup(server *genericapiserver.GenericAPIServer, codecs runtime.NegotiatedSerializer, reader client.Reader) {
	gv := schema.GroupVersion{Group: apis.GroupName, Version: apis.ExtensionVersion}
	discoveryVersion := metav1.GroupVersionForDiscovery{GroupVersion: gv.String(), Version: gv.Version}
	group := metav1.APIGroup{
		Name:             gv.Group,
		Versions:         []metav1.GroupVersionForDiscovery{discoveryVersion},
		PreferredVersion: discoveryVersion,
	}
	resources := []metav1.APIResource{{
		Name:         apis.DevicesResource,
		SingularName: apis.DevicesResource,
		Kind:         apis.DevicesKind,
		Verbs:        metav1.Verbs{"get", "list"},
	}}

	groupPath := "/apis/" + gv.Group
	versionPath := groupPath + "/" + gv.Version
	devicesPath := versionPath + "/" + apis.DevicesResource

	mux := server.Handler.NonGoRestfulMux
	server.DiscoveryGroupManager.AddGroup(group)
	server.AggregatedDiscoveryGroupManager.AddGroupVersion(gv.Group, apidiscovery.APIVersionDiscovery{
		Version:   gv.Version,
		Freshness: apidiscovery.DiscoveryFreshnessCurrent,
		Resources: []apidiscovery.APIResourceDiscovery{{
			Resource:         apis.DevicesResource,
			ResponseKind:     &metav1.GroupVersionKind{Group: gv.Group, Version: gv.Version, Kind: apis.DevicesKind},
			Scope:            apidiscovery.ScopeCluster,
			SingularResource: apis.DevicesResource,
			Verbs:            resources[0].Verbs,
		}},
	})
	mux.Handle(groupPath, discovery.NewAPIGroupHandler(codecs, group))
	mux.Handle(versionPath, discovery.NewAPIVersionHandler(codecs, gv,
		discovery.APIResourceListerFunc(func() []metav1.APIResource { return resources })))

	handlers := http.NewServeMux()
	handlers.HandleFunc("GET "+devicesPath, func(w http.ResponseWriter, r *http.Request) {
		var options metav1.ListOptions
		query := r.URL.Query()
		if err := metav1.Convert_url_Values_To_v1_ListOptions(&query, &options, nil); err != nil {
			responsewriters.ErrorNegotiated(apierrors.NewBadRequest(err.Error()), codecs, metav1.SchemeGroupVersion, w, r)
			return
		}
		if options.Watch {
			responsewriters.ErrorNegotiated(apierrors.NewBadRequest("watch Devices through the v1alpha1 storage API"), codecs, metav1.SchemeGroupVersion, w, r)
			return
		}

		list := &unstructured.UnstructuredList{}
		list.SetGroupVersionKind(schema.GroupVersionKind{Group: apis.GroupName, Version: apis.StorageVersion, Kind: apis.DevicesKind + "List"})
		listOptions := &client.ListOptions{Raw: &options, Limit: options.Limit, Continue: options.Continue}
		if err := reader.List(r.Context(), list, listOptions); err != nil {
			responsewriters.ErrorNegotiated(err, codecs, metav1.SchemeGroupVersion, w, r)
			return
		}
		list.SetAPIVersion(gv.String())
		for i := range list.Items {
			list.Items[i].SetAPIVersion(gv.String())
		}
		responsewriters.WriteRawJSON(http.StatusOK, list, w)
	})
	handlers.HandleFunc("GET "+devicesPath+"/{name}", func(w http.ResponseWriter, r *http.Request) {
		device := apis.NewDevices()
		if err := reader.Get(r.Context(), client.ObjectKey{Name: r.PathValue("name")}, device); err != nil {
			responsewriters.ErrorNegotiated(err, codecs, metav1.SchemeGroupVersion, w, r)
			return
		}
		device.SetAPIVersion(gv.String())
		responsewriters.WriteRawJSON(http.StatusOK, device, w)
	})
	mux.Handle(devicesPath, handlers)
	mux.HandlePrefix(devicesPath+"/", handlers)
}
