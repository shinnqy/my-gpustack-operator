package detector

import (
	"gpustack.ai/gpustack/pkg/device"
	"k8s.io/utils/ptr"
	"slices"
)

// MonitorSliceSchemaVersion is the version of the slice section's schema, and
// MonitorSliceSchemaVersionMin is the oldest one this consumer still reads.
//
// The version exists so that a consumer can tell version skew from unsupported hardware. Without it,
// a newer consumer reading an older device manager sees a valid, fresh snapshot carrying no slice
// data and has no way to know whether this node's accelerators cannot be measured or the producer
// simply predates the feature — and reporting the first as the second is exactly the "absent read as
// idle" error the whole section exists to prevent.
//
// The consumer accepts a RANGE rather than its own version alone, because the two run in separate
// Pods upgraded at separate times: during a rollout a new worker reads an old device manager, and
// refusing that whole section would throw away the figures the old producer does serve in order to
// report a newer kind it never wrote. So each version is added here when a consumer can read it as
// what it is, and a version outside the range answers nothing — which is the honest claim about a
// section whose meaning this build cannot know.
//
// Version 1 carries the per-process usages, the device diagnostics and the hardware partitions.
const (
	MonitorSliceSchemaVersion    = 1
	MonitorSliceSchemaVersionMin = 1
)

// KnownMonitorSliceSchema reports whether a section's schema version is one this build reads.
func KnownMonitorSliceSchema(version int) bool {
	return version >= MonitorSliceSchemaVersionMin && version <= MonitorSliceSchemaVersion
}

const SliceReasonVersionSkew device.AcceleratorProcessReason = "version_skew"

// MonitorSliceSection is the per-Instance share of every accelerator the node measured this pass,
// keyed by (Pod UID, container name, device ID).
//
// It is a section of its own rather than a field inside device.AcceleratorMetrics, which stays what
// it is: one card's readings. The raw per-process rows this is aggregated from never appear here —
// they name host processes, and both the privacy boundary and the section's size bound depend on
// them staying producer-internal.
//
// A consumer must not read the absence of a record as an absence of usage. A record is emitted only
// for a container that held processes; a container with none, on a device Devices reports as
// measured, used nothing — which is zero. Figures makes that decision once, so both surfaces make
// it identically.
type MonitorSliceSection struct {
	// SchemaVersion is MonitorSliceSchemaVersion as of the producer that wrote the section. A
	// consumer that does not know the version must report every slice figure absent with a
	// version-skew reason rather than treating the section as empty; one it knows but that predates
	// a kind of record reads the records that version carries and reports the rest absent.
	SchemaVersion int `json:"schemaVersion"`
	// Truncated reports that the bound was reached and some devices were dropped whole. Those
	// devices carry their reason in Devices, so a dropped device reads as absent rather than idle.
	Truncated bool `json:"truncated,omitempty"`
	// Usages holds one record per (Pod UID, container name, device ID) that was measured.
	Usages []SliceUsage `json:"usages"`
	// Devices holds one entry per accelerator the per-process pass covered, whether or not any
	// record came out of it. An accelerator absent from here was not covered at all.
	Devices []SliceDeviceDiagnostics `json:"devices"`
	// Partitions holds one record per hardware partition an Instance holds, read on the partition's
	// own device handle. It is a list of its own rather than a field on Usages because it comes from
	// a different source and survives that source's failures independently — see Figures.
	Partitions []SlicePartition `json:"partitions,omitempty"`
}

// SliceUsage is one Instance container's measured share of one accelerator.
//
// The two figures are pointers because absent and zero are different claims: absent means the
// figure was not measurable here, zero means it was measured and the container was idle.
type SliceUsage struct {
	// Manufacturer, PodUID, Container and DeviceID are the key. Container is the container's NAME
	// rather than its runtime ID: the allocation a consumer joins this against is recorded per
	// container name, and the runtime ID — like the process ids behind it — stays on the node.
	Manufacturer string `json:"manufacturer"`
	PodUID       string `json:"podUID"`
	Container    string `json:"container"`
	DeviceID     string `json:"deviceID"`

	// MemoryUsedMiB is the device memory this container's processes hold on this device, summed
	// in the vendor's native unit and converted once. Absent when the manufacturer served no
	// usable figure for it.
	MemoryUsedMiB *uint64 `json:"memoryUsedMiB,omitempty"`
	// CoresUtilizationPercent is the share of the WHOLE card's compute this container's processes
	// were last measured using. Absent when the manufacturer serves no per-process utilization.
	CoresUtilizationPercent *uint32 `json:"coresUtilizationPercent,omitempty"`
}

// SlicePartition is one Instance container's hardware partition of one accelerator, measured on the
// partition's OWN device handle.
//
// A partition is not aggregated from the parent card's processes, and that is what makes this a
// record rather than a SliceUsage: the partition is named by what the allocation recorded — a profile
// and a placement — so an idle partition is still a partition that can be found and reported as zero,
// and a process another tenant holds on a sibling partition cannot poison it.
type SlicePartition struct {
	// Manufacturer, PodUID, Container and DeviceID are the key, the same one SliceUsage carries.
	// DeviceID is the PARENT accelerator's, which is what the allocation records and what a consumer
	// joins by.
	Manufacturer string `json:"manufacturer"`
	PodUID       string `json:"podUID"`
	Container    string `json:"container"`
	DeviceID     string `json:"deviceID"`

	// ID is the partition's OWN identifier, as its handle reports it. A consumer reports the partition
	// under this rather than under DeviceID above, because the partition is what its holder was
	// granted — and because two partitions of one card held by one Instance are one entry under the
	// card's identifier and two under theirs. Empty when the handle could not be reached.
	ID string `json:"id,omitempty"`

	// MemoryTotalMiB and MemoryUsedMiB are the partition's own capacity and what is held of it, both
	// read on its handle and converted once. The capacity comes from the driver rather than from the
	// profile name because the name rounds: a "1g.10gb" partition carries 9856 MiB, not 10240. Absent
	// when the handle could not be read; used is zero when it was read and the partition is idle.
	MemoryTotalMiB *uint64 `json:"memoryTotalMiB,omitempty"`
	MemoryUsedMiB  *uint64 `json:"memoryUsedMiB,omitempty"`

	// CoresPercent is the partition's own compute utilization, read on its handle. Absent on every
	// manufacturer whose library serves no per-partition compute figure, which is most of them; see
	// device.AcceleratorPartition.CoresPercent.
	CoresPercent *uint32 `json:"coresPercent,omitempty"`

	// MemoryReason names why the memory figures above are absent, and is empty when they were
	// produced. CoresReason names why CoresPercent is absent, and is likewise empty when it was read.
	MemoryReason device.AcceleratorProcessReason `json:"memoryReason,omitempty"`
	CoresReason  device.AcceleratorProcessReason `json:"coresReason,omitempty"`
}

// SliceDeviceDiagnostics explains one accelerator's sample: why a figure is absent, and what the
// pass saw. Without the counts an aggregate-only section makes an undercount indistinguishable from
// a genuinely idle device, which is the one confusion this feature cannot afford.
type SliceDeviceDiagnostics struct {
	Manufacturer string `json:"manufacturer"`
	DeviceID     string `json:"deviceID"`

	// MemoryReason and CoresReason name why that figure is absent on EVERY record of this device,
	// and are empty when the figure was produced. They are separate because a driver commonly
	// serves process memory while refusing process utilization.
	MemoryReason device.AcceleratorProcessReason `json:"memoryReason,omitempty"`
	CoresReason  device.AcceleratorProcessReason `json:"coresReason,omitempty"`

	// RowsReturned is how many per-process rows the manufacturer returned for this device.
	RowsReturned uint32 `json:"rowsReturned"`
	// RowsAttributed is how many of them were resolved to an Instance's Pod and container.
	RowsAttributed uint32 `json:"rowsAttributed"`
	// RowsNonInstance is how many belonged to a Pod the node runs that backs no Instance. Their
	// usage is real and nobody's slice, so dropping them leaves the device measurable.
	RowsNonInstance uint32 `json:"rowsNonInstance"`
	// RowsUnreadable is how many could not be read from /proc at all, or whose identity moved
	// while it was read: exited, permission, unreadable, zombie, unstable, invisible.
	RowsUnreadable uint32 `json:"rowsUnreadable"`
	// RowsAmbiguous is how many were read but could not be turned into one known Pod and
	// container: a host process, two Pod ancestors, a Pod or container the node's index does not
	// carry, a process holding the device on others' behalf.
	RowsAmbiguous uint32 `json:"rowsAmbiguous"`
	// ReadsTruncated is how many of this device's entry points could not deliver a complete row
	// list. A truncated list read as a complete one turns processes that exist into processes
	// that do not, so it makes its figure absent rather than partial.
	ReadsTruncated uint32 `json:"readsTruncated"`
}

// SliceFigures is what a consumer publishes for one (Pod, container, device): each figure either
// measured — possibly zero — or absent.
type SliceFigures struct {
	MemoryUsedMiB *uint64

	// CoresUtilizationPercent is a share of the WHOLE card in every mode but Partitioned, where it is
	// the partition's own — read on the partition's handle, which knows nothing of the parent. The
	// consumer restates the former against the holder's cap and leaves the latter alone, since a
	// partition makes no compute request and so carries no cap to restate against.
	CoresUtilizationPercent *uint32

	// ID and MemoryTotalMiB describe what was measured rather than what it was measured using, and
	// they are here for the one case where the producer is what knows them: a hardware partition
	// carries its own identifier and its own capacity, both read off its handle. Empty and absent for
	// every other mode, whose identity is the accelerator asked about and whose quota the consumer
	// reads off the allocation.
	ID             string
	MemoryTotalMiB *uint64
}

// Figures resolves one container's figures on one device, and reports whether the section can
// answer for that device at all.
//
// It holds the absent-versus-zero decision so that both consuming surfaces make it identically: a
// device the section does not carry answers nothing; a device it carries with no record for this
// container answers zero for each figure the device measured, because a container that held no
// process on a measured device used none of it.
//
// A PARTITION'S FIGURES ARE RESOLVED INDEPENDENTLY OF THE PER-PROCESS PASS, and that is deliberate
// rather than an oversight in the rule above. They come from a source of their own — the partition's
// own device handle — so a failure in the manufacturer's per-process query says nothing about them: a
// row nobody could attribute poisons memory and cores for the whole device and must not take a
// partition with it, and on a MIG-enabled card the per-process query is precisely the one that cannot
// answer. So a partition record alone is enough to make this an answer.
//
// A partition record supersedes the per-process pass for its own key rather than merging with it. The
// two measure the same memory on two different handles, so a per-field mixture would publish a figure
// nobody can recompute; the partition's own handle is the narrower and more reliable of the two, and
// it is the only one that survives a sibling tenant's unattributable process.
func (s *MonitorSliceSection) Figures(
	manufacturer, podUID, container, deviceID string,
) (SliceFigures, bool) {
	if s == nil {
		return SliceFigures{}, false
	}
	if !KnownMonitorSliceSchema(s.SchemaVersion) {
		// The API carries no reason field, by design, so this surface's share of making the skew
		// discoverable is saying it once per pass rather than going quiet: an operator comparing a
		// console's empty figures against the exporter's version_skew gauge has both halves of the
		// same story, and neither is a guess about the hardware.
		logger.V(2).Info("a device manager wrote a slice section this build cannot read,"+
			" reporting no slice figures", "reason", SliceReasonVersionSkew,
			"sectionVersion", s.SchemaVersion, "supported", MonitorSliceSchemaVersion,
			"manufacturer", manufacturer, "device", deviceID)
		return SliceFigures{}, false
	}

	figures := SliceFigures{}
	if p := slices.IndexFunc(s.Partitions, func(p SlicePartition) bool {
		return p.Manufacturer == manufacturer && p.PodUID == podUID &&
			p.Container == container && p.DeviceID == deviceID
	}); p >= 0 {
		figures.ID = s.Partitions[p].ID
		figures.MemoryTotalMiB = s.Partitions[p].MemoryTotalMiB
		figures.MemoryUsedMiB = s.Partitions[p].MemoryUsedMiB
		// A partition's compute is already stated against the partition, so it travels as-is: the
		// consumer's cap-relative restatement is a no-op for a mode that makes no compute request.
		// Absent on every manufacturer whose library serves no per-partition figure.
		figures.CoresUtilizationPercent = s.Partitions[p].CoresPercent
		return figures, true
	}

	i := slices.IndexFunc(s.Devices, func(d SliceDeviceDiagnostics) bool {
		return d.Manufacturer == manufacturer && d.DeviceID == deviceID
	})
	if i < 0 {
		return figures, false
	}
	diag := s.Devices[i]

	j := slices.IndexFunc(s.Usages, func(u SliceUsage) bool {
		return u.Manufacturer == manufacturer && u.PodUID == podUID &&
			u.Container == container && u.DeviceID == deviceID
	})
	if j >= 0 {
		figures.MemoryUsedMiB = s.Usages[j].MemoryUsedMiB
		figures.CoresUtilizationPercent = s.Usages[j].CoresUtilizationPercent
		return figures, true
	}

	if diag.MemoryReason == device.AcceleratorProcessReasonNone {
		figures.MemoryUsedMiB = ptr.To[uint64](0)
	}
	if diag.CoresReason == device.AcceleratorProcessReasonNone {
		figures.CoresUtilizationPercent = ptr.To[uint32](0)
	}
	return figures, true
}
