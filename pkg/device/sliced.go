package device

// AggregateAcceleratorSlicedDetail 从逐卡状态汇总设备组的切分能力。逻辑切片数相加，
// 采用支持软切的卡给出的算力超配标志。物理规格按名称累计实例数，整组硬切上限按卡相加。
func AggregateAcceleratorSlicedDetail(accelerators []Accelerator) AcceleratorSlicedDetail {
	var detail AcceleratorSlicedDetail
	profileIndex := make(map[string]int)
	for i := range accelerators {
		st := accelerators[i].Status

		detail.Logical.Count += st.LogicalSliced.Count
		if st.LogicalSliced.Count > 0 && st.LogicalSliced.CoresPercentageOvercommit {
			detail.Logical.CoresPercentageOvercommit = true
		}

		detail.Physical.Count += st.PhysicalSliced.Count
		for _, p := range st.PhysicalSliced.Profiles {
			if idx, ok := profileIndex[p.Name]; ok {
				detail.Physical.Profiles[idx].Count += p.Count
				continue
			}
			profileIndex[p.Name] = len(detail.Physical.Profiles)
			detail.Physical.Profiles = append(detail.Physical.Profiles, AcceleratorSlicedPhysicalDetailProfile{
				Name:      p.Name,
				Count:     p.Count,
				MemoryMib: p.MemoryMib,
			})
		}
	}
	return detail
}

// SetGroupSlicedDetails 在探测结束时将逐卡切分能力汇总到各设备组。
func SetGroupSlicedDetails(groups DevicesGroupList) {
	for i := range groups {
		groups[i].AcceleratorSlicedDetail = AggregateAcceleratorSlicedDetail(groups[i].Accelerators)
	}
}
