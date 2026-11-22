package nvml

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

// GpmMetricsGetV 用于获取设备的性能指标，以进行 GPU 性能监控（GPM），并返回一个处理程序，可用于访问不同版
// 本的 GPM 指标信息。
func (l _MigDevice) GpmMetricsGetV() GpmMetricsGetHandler {
	return GpmMetricsGetHandler{l.Device, true}
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

		if l.isMig {
			var gpuInstanceId uint32
			ret = nvmlDeviceGetGpuInstanceId(l.handle, &gpuInstanceId)
			if !ret.IsSuccess() {
				return ret
			}

			ret = nvmlGpmMigSampleGet(l.handle, gpuInstanceId, gpmSample1)
			if ret.IsSuccess() {
				time.Sleep(duration)
				ret = nvmlGpmMigSampleGet(l.handle, gpuInstanceId, gpmSample2)
			}
			return ret
		}

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
	// GPM 使用普通版本号（nvml.h：`#define NVML_GPM_METRICS_GET_VERSION 1`），而不是那些使用大小编码
	// 的 STRUCT_VERSION 方案——那些使用大小版本化的结构体（内存信息、配置信息）使用的是该方案。发送大
	// 小编码值会导致 nvmlGpmMetricsGet 以 ARGUMENT_VERSION_MISMATCH 错误拒绝调用，因此 GPM 永远不会返
	// 回指标数据。
	gpmMetrics.Version = GPM_METRICS_GET_VERSION
	gpmMetrics.NumMetrics = uint32(len(metricIds))
	for i := range metricIds {
		gpmMetrics.Metrics[i].MetricId = uint32(metricIds[i])
	}
	gpmMetrics.Sample1 = gpmSample1
	gpmMetrics.Sample2 = gpmSample2

	ret = nvmlGpmMetricsGet(&gpmMetrics)
	return gpmMetrics.Metrics[:], ret
}
