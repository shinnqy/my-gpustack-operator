package device

// PartitionLedgerReady reports whether an accelerator's allocation row carries a usable
// per-profile partition ledger. A missing row, or one reporting neither allocated nor remaining
// instances, is the not-yet-published state: the runtime status is rebuilt from the accelerator's
// cached placement geometry, which an older device manager did not record.
//
// Every reader of the ledger has to agree on this, because they all take the same fallback to the
// accelerator's static capability ceiling when it is false — publishing zero instead would read as
// a full accelerator on a working node.
func PartitionLedgerReady(alloc *AcceleratorAllocation) bool {
	return alloc != nil && (len(alloc.AllocatedProfiles) > 0 || len(alloc.RemainingProfiles) > 0)
}
