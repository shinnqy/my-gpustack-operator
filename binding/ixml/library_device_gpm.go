package ixml

import (
	"sync"
	"time"
)

// GpmQueryDeviceSupportV 用于获取设备上的 GPU 性能指标（GPM）支持信息，返回一个处理器，可用于访问不同版本
// 的 GPM 支持信息。
func (l Device) GpmQueryDeviceSupportV() GpmSupportHandler {
	return GpmSupportHandler(l)
}

type GpmSupportHandler Device

func (l GpmSupportHandler) V1() (GpmSupport, Return) {
	if l.so.Lookup("nvmlGpmQueryDeviceSupport") != nil {
		return GpmSupport{}, ERROR_FUNCTION_NOT_FOUND
	}

	var gpmSupport GpmSupport
	gpmSupport.Version = GPM_SUPPORT_VERSION
	ret := nvmlGpmQueryDeviceSupport(l.handle, &gpmSupport)
	return gpmSupport, ret
}

// GpmMetricsGetV 用于获取设备的性能指标，以进行 GPU 性能监控（GPM），并返回一个处理程序，可用于访问不同版
// 本的 GPM 指标信息。
func (l Device) GpmMetricsGetV() GpmMetricsGetHandler {
	return GpmMetricsGetHandler{l, false}
}

type GpmMetricsGetHandler struct {
	Device
	isMig bool
}

var gpmLock sync.Mutex

func (l GpmMetricsGetHandler) V1(duration time.Duration, metricIds ...GpmMetricId) ([]GpmMetric, Return) {
	if l.so.Lookup("nvmlGpmMetricsGet") != nil {
		return nil, ERROR_FUNCTION_NOT_FOUND
	}

	var gpmSample1 nvmlGpmSample
	ret := nvmlGpmSampleAlloc(&gpmSample1)
	if !ret.IsSuccess() {
		return nil, ret
	}
	defer func() {
		_ = nvmlGpmSampleFree(gpmSample1)
	}()

	var gpmSample2 nvmlGpmSample
	ret = nvmlGpmSampleAlloc(&gpmSample2)
	if !ret.IsSuccess() {
		return nil, ret
	}
	defer func() {
		_ = nvmlGpmSampleFree(gpmSample2)
	}()

	ret = func() Return {
		gpmLock.Lock()
		defer gpmLock.Unlock()

		ret = nvmlGpmSampleGet(l.handle, gpmSample1)
		if ret.IsSuccess() {
			time.Sleep(duration)
			ret = nvmlGpmSampleGet(l.handle, gpmSample2)
		}
		return ret
	}()
	if !ret.IsSuccess() {
		return nil, ret
	}

	var gpmMetrics nvmlGpmMetricsGetType
	gpmMetrics.Version = STRUCT_VERSION(gpmMetrics, 1)
	gpmMetrics.NumMetrics = uint32(len(metricIds))
	for i := range metricIds {
		gpmMetrics.Metrics[i].MetricId = uint32(metricIds[i])
	}
	gpmMetrics.Sample1 = gpmSample1
	gpmMetrics.Sample2 = gpmSample2

	ret = nvmlGpmMetricsGet(&gpmMetrics)
	return gpmMetrics.Metrics[:], ret
}
