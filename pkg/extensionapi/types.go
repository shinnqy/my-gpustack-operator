package extensionapi

import (
	"context"

	autoscaling "k8s.io/api/autoscaling/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/registry/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
)

type (
	// SetupOptions 是用于初始化 handler 的选项。
	SetupOptions struct {
		// Manager 是 controller-runtime 的 manager。
		Manager ctrl.Manager
	}

	// Setup 是用于初始化 handler 的接口。
	Setup interface {
		rest.Storage
		// SetupHandler 用于初始化 handler。
		//
		// SetupHandler 会在 Cache 启动前被调用，
		// 因此不应执行任何依赖 Cache 已启动的操作。
		// 相反，可以在此配置 Cache，例如调用 IndexField 等。
		SetupHandler(ctx context.Context, opts SetupOptions) (gvr schema.GroupVersionResource, srs map[string]rest.Storage, err error) // nolint: lll
	}
)

type (
	// ObjectInfo 是用于描述扩展 API 对象的接口。
	ObjectInfo interface {
		rest.Scoper
		rest.KindProvider
		rest.SingularNameProvider
		rest.CategoriesProvider
		rest.ShortNamesProvider
	}

	// ObjectWithStatusSubResource 是带有 status 子资源的扩展 API 对象接口，
	// 表示该资源包含 status 子资源。
	ObjectWithStatusSubResource interface {
		meta.ObjectMetaAccessor
		runtime.Object
		CopyStatusTo(runtime.Object)
	}

	// ObjectWithScaleSubResource 是带有 scale 子资源的扩展 API 对象接口，
	// 表示该资源包含 scale 子资源。
	ObjectWithScaleSubResource interface {
		meta.ObjectMetaAccessor
		runtime.Object
		GetScale() *autoscaling.Scale
		SetScale(*autoscaling.Scale)
	}
)

type (
	// MetaObject 是带有元数据的对象接口。
	MetaObject = ctrlcli.Object
	// MetaObjectList 是带有元数据的对象列表接口。
	MetaObjectList = ctrlcli.ObjectList
)
