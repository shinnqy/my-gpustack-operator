package systemmeta

import (
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
)

type (
	// MetaObject is the interface for the object with metadata.
	MetaObject = ctrlcli.Object
)
