package service

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

// TestAggregatedInstanceTypeMirrorsEveryStatusView 核对网关手写聚合结果是否保留 Worker 资源状态中的每个资源维度，
// 使新增维度不会在跨集群查询中静默消失。
func TestAggregatedInstanceTypeMirrorsEveryStatusView(t *testing.T) {
	views := fieldNamesOfType(
		reflect.TypeFor[workercore.InstanceTypeStatus](),
		reflect.TypeFor[workercore.InstanceTypeResource](),
	)

	cases := []struct {
		name   string
		mirror reflect.Type
		fields []reflect.Type
	}{
		{
			name:   "overview bundle carries one dimension per view",
			mirror: reflect.TypeFor[AggregatedInstanceTypeOverviewResource](),
			fields: []reflect.Type{
				reflect.TypeFor[resource.Quantity](),
				reflect.TypeFor[[]workercore.AcceleratorProfileCount](),
			},
		},
		{
			name:   "candidate carries one resource per view",
			mirror: reflect.TypeFor[AggregatedInstanceTypeOnceMaxRequestCandidate](),
			fields: []reflect.Type{reflect.TypeFor[AggregatedInstanceTypeResource]()},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.ElementsMatch(t, views, fieldNamesOfType(c.mirror, c.fields...),
				"%s must mirror exactly the InstanceTypeStatus views", c.mirror.Name())
		})
	}
}

// fieldNamesOfType 返回直接以指定类型声明的字段名，也识别匿名嵌入所需类型的资源包装。
func fieldNamesOfType(in reflect.Type, fields ...reflect.Type) []string {
	names := make([]string, 0, in.NumField())
	for i := range in.NumField() {
		for _, field := range fields {
			if declaresType(in.Field(i).Type, field) {
				names = append(names, in.Field(i).Name)
				break
			}
		}
	}
	return names
}

// declaresType 判断一个字段的类型是否等于目标类型，或者通过结构体字段匿名嵌入了它。
func declaresType(t, field reflect.Type) bool {
	if t == field {
		return true
	}
	if t.Kind() != reflect.Struct {
		return false
	}
	for i := range t.NumField() {
		if f := t.Field(i); f.Anonymous && f.Type == field {
			return true
		}
	}
	return false
}
