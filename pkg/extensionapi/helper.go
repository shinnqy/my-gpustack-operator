package extensionapi

import (
	"context"
	"fmt"

	"github.com/davecgh/go-spew/spew"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	ctrl "sigs.k8s.io/controller-runtime"
)

type _APIOptions struct {
	ResourceStorage     rest.Storage
	SubResourceStorages map[string]rest.Storage
}

// ExecuteSetup 执行给定的 setup 以安装扩展 API。
func ExecuteSetup(
	ctx context.Context,
	srv *genericapiserver.GenericAPIServer,
	srvScheme *runtime.Scheme,
	srvParameterCodec runtime.ParameterCodec,
	srvCodec serializer.CodecFactory,
	mgr ctrl.Manager,
	setups []Setup,
) error {
	// 初始化所有 handler。
	apiOpts := make(map[string]map[string]map[string]_APIOptions)
	for i := range setups {
		opts := SetupOptions{Manager: mgr}
		gvr, srs, err := setups[i].SetupHandler(ctx, opts)
		if err != nil {
			return fmt.Errorf("extension api setup: %s: %w", spew.Sdump(setups[i]), err)
		}

		if apiOpts[gvr.Group] == nil {
			apiOpts[gvr.Group] = make(map[string]map[string]_APIOptions)
		}
		if apiOpts[gvr.Group][gvr.Version] == nil {
			apiOpts[gvr.Group][gvr.Version] = make(map[string]_APIOptions)
		}
		apiOpts[gvr.Group][gvr.Version][gvr.Resource] = _APIOptions{
			ResourceStorage:     setups[i],
			SubResourceStorages: srs,
		}
	}

	agis := make([]*genericapiserver.APIGroupInfo, 0, len(apiOpts))
	for _, gn := range sets.List(sets.KeySet(apiOpts)) {
		agi := genericapiserver.NewDefaultAPIGroupInfo(gn, srvScheme, srvParameterCodec, srvCodec)
		for _, vn := range sets.List(sets.KeySet(apiOpts[gn])) {
			if len(apiOpts[gn][vn]) == 0 {
				continue
			}
			agi.VersionedResourcesStorageMap[vn] = make(map[string]rest.Storage)
			for _, rn := range sets.List(sets.KeySet(apiOpts[gn][vn])) {
				igv := schema.GroupVersion{Group: gn, Version: runtime.APIVersionInternal}
				egv := schema.GroupVersion{Group: gn, Version: vn}
				stg := apiOpts[gn][vn][rn].ResourceStorage
				// TODO(thxCode): 引入 conversion-gen 后移除此临时处理。
				// 注册内部版本。
				srvScheme.AddKnownTypes(igv, stg.New())
				if v, ok := stg.(rest.Lister); ok {
					srvScheme.AddKnownTypes(igv, v.NewList())
				}
				// 注册主存储。
				agi.VersionedResourcesStorageMap[vn][rn] = stg
				// 若存在 status 子资源则进行注册。
				if _, ok := stg.New().(ObjectWithStatusSubResource); ok {
					pstg := stg.(StatusSubResourceParentStore)
					stg := AsStatusSubResourceStorage(pstg)
					agi.VersionedResourcesStorageMap[vn][rn+"/status"] = stg
				}
				// 如果存在 scale 子资源，则注册。
				if _, ok := stg.New().(ObjectWithScaleSubResource); ok {
					pstg := stg.(ScaleSubResourceParentStore)
					stg := AsScaleSubResourceStorage(pstg)
					agi.VersionedResourcesStorageMap[vn][rn+"/scale"] = stg
				}
				// 如果存在任意子资源，则注册。
				{
					srs := apiOpts[gn][vn][rn].SubResourceStorages
					for srn := range srs {
						sstg := srs[srn]
						// 注册内部版本。
						srvScheme.AddKnownTypes(igv, sstg.New())
						// 注册外部版本。
						srvScheme.AddKnownTypes(egv, sstg.New())
						// 注册存储。
						agi.VersionedResourcesStorageMap[vn][rn+"/"+srn] = sstg
						// 注册可选的 getter 选项。
						if v, ok := sstg.(rest.GetterWithOptions); ok {
							optionsObj, _, _ := v.NewGetOptions()
							if optionsObj != nil {
								// 注册内部版本。
								srvScheme.AddKnownTypes(igv, optionsObj)
								// 注册外部版本。
								srvScheme.AddKnownTypes(egv, optionsObj)
							}
						}
						// 注册可选的 connector 选项。
						if v, ok := sstg.(rest.Connecter); ok {
							optionsObj, _, _ := v.NewConnectOptions()
							if optionsObj != nil {
								// 注册内部版本。
								srvScheme.AddKnownTypes(igv, optionsObj)
								// 注册外部版本。
								srvScheme.AddKnownTypes(egv, optionsObj)
							}
						}
					}
				}
			}
		}
		agis = append(agis, &agi)
	}

	// 安装。
	return srv.InstallAPIGroups(agis...)
}
