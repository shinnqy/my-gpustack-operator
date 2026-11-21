package device

import (
	"time"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

// Structures for device metadata.
type (
	// AcceleratorAllocation is an accelerator's runtime allocation row — its mode, credit budget
	// and, for a hardware-partitioned accelerator, its per-profile partition ledger.
	AcceleratorAllocation = workercore.AcceleratorAllocation
	// AcceleratorStatus represents the status of the accelerator device,
	// including its health status and other status information.
	AcceleratorStatus = workercore.AcceleratorStatus
)

// Structures for device metrics.
type (
	// AcceleratorMetrics represents the metrics of the accelerator device,
	// including its memory usage, core usage, temperature, and power consumption.
	AcceleratorMetrics struct {
		// ID is the universally unique identifier for this device.
		ID string `json:"id" yaml:"id"`
		// Memory is the memory size of the device in MiB.
		Memory uint64 `json:"memory" yaml:"memory"`
		// MemoryUsage is the memory used by the device in MiB.
		MemoryUsage uint64 `json:"memoryUsage" yaml:"memoryUsage"`
		// MemoryUtilization is the percentage of memory being used by the device, in the range of [0, 100].
		MemoryUtilization uint32 `json:"memoryUtilization" yaml:"memoryUtilization"`
		// CoresUtilization is the percentage of cores being used by the device, in the range of [0, 100].
		CoresUtilization uint32 `json:"coresUtilization" yaml:"coresUtilization"`
		// Temperature is the temperature of the device in Celsius.
		Temperature uint32 `json:"temperature" yaml:"temperature"`
		// PowerUsage is the power used by the device in Watts.
		PowerUsage uint32 `json:"powerUsage" yaml:"powerUsage"`
		// Unhealthy indicates whether the device is healthy or not.
		Unhealthy bool `json:"unhealthy" yaml:"unhealthy"`
	}
	// MetricsGroup represents a group of device metrics.
	MetricsGroup struct {
		// Manufacturer is the name of the device manufacturer.
		Manufacturer string `json:"manufacturer" yaml:"manufacturer"`
		// Timestamp is the time when the metrics were collected.
		Timestamp time.Time `json:"timestamp" yaml:"timestamp"`
		// Accelerators is the list of the accelerator device metrics in this group.
		Accelerators []AcceleratorMetrics `json:"accelerators" yaml:"accelerators"`
	}
	// MetricsGroupList represents a list of device metrics groups.
	MetricsGroupList = []MetricsGroup
)

// Structures for per-process device usage.
//
// These rows are producer-internal: they name host processes, so they are aggregated per Pod and
// container before anything is published and never travel on a wire themselves.
type (
	// AcceleratorProcessReason names why a per-process figure could not be produced. The values
	// are stable strings because they are published as a label beside the absence they explain,
	// and an absence without a named reason is indistinguishable from an idle device.
	AcceleratorProcessReason string
)

const (
	// AcceleratorProcessReasonNone is the absence of a refusal: the entry point answered.
	AcceleratorProcessReasonNone AcceleratorProcessReason = ""
	// AcceleratorProcessReasonUnsupported means the driver does not serve this query for this
	// device. It is a property of the node, not of this sample.
	AcceleratorProcessReasonUnsupported AcceleratorProcessReason = "unsupported"
	// AcceleratorProcessReasonPermission means the query needs privileges the process does not
	// have. Like the reason above it will not clear on its own.
	AcceleratorProcessReasonPermission AcceleratorProcessReason = "permission"
	// AcceleratorProcessReasonDriverError means the query failed for a reason that may not
	// repeat, so support is not disproven by it and the next sample is worth taking.
	AcceleratorProcessReasonDriverError AcceleratorProcessReason = "transient_driver_error"
	// AcceleratorProcessReasonTruncated means the driver kept asking for a larger buffer than
	// the read would accept, so the row list could not be completed. A truncated list read as a
	// complete one turns processes that exist into processes that do not.
	AcceleratorProcessReasonTruncated AcceleratorProcessReason = "truncated"
)
