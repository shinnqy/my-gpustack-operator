package worker

import (
	"k8s.io/apimachinery/pkg/api/resource"
)

func itoa(v int64) string {
	return resource.NewQuantity(v, resource.DecimalSI).String()
}
