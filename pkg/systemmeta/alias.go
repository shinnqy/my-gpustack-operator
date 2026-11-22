package systemmeta

import (
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
)

type (
	// MetaObject 是带有元数据的对象接口。
	MetaObject = ctrlcli.Object
)
