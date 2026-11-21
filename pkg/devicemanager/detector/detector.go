package detector

import (
	"gpustack.ai/gpustack/pkg/device"
	klog "k8s.io/klog/v2"
	"time"
)

var logger = klog.Background().WithName("detector")

// MonitorSnapshot is the latest accelerator metrics sample stored by the monitor loop.
//
// The envelope carries its own Timestamp (the time the device manager stored the sample) because
// device.MetricsGroupList has no top-level timestamp — each MetricsGroup timestamps itself — and a
// single store time makes staleness checks (one monitor period) a one-field comparison.
type MonitorSnapshot struct {
	// Timestamp is the time when the sample was stored,
	// zero before the first monitor tick.
	Timestamp time.Time `json:"timestamp"`
	// PeriodSeconds is the monitor period in effect when the sample was stored,
	// so consumers can scale their staleness bound to the configured cadence.
	PeriodSeconds int64 `json:"periodSeconds"`
	// Groups is the latest accelerator metrics sample,
	// empty before the first monitor tick.
	Groups device.MetricsGroupList `json:"groups"`
	// Slices is what each Instance holds of the accelerators the groups above describe as wholes,
	// absent when the pass could not measure — which is not the same claim as an empty section,
	// and is why the field is a pointer. Its SchemaVersion is what lets a consumer tell a
	// producer that predates slice reporting from hardware that cannot be measured.
	Slices *MonitorSliceSection `json:"slices,omitempty"`
}
