package hygon

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gpustack.ai/gpustack/binding"
	"gpustack.ai/gpustack/binding/dmi"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/utils/loggerx"
)

// migConfigDir 是厂商的实时实例注册表。驱动程序在计算实例创建后，会在此处写入一个文件。而工作负载容器也会
// 绑定使用同一个文件来利用该实例——因此它既满足此检测器的需求，也是分配器提供的资源。这是一个变量，因此测
// 试可以针对固定值进行验证。
var migConfigDir = "/etc/dmi_mig_config"

// _MigInstanceIDPrefix 是厂商自己的工具在列表中对实例 UUID 所采用的前缀形式，在 DMI_MIG_VISIBLE_DEVICE 值
// 中，容器也选择使用这种前缀形式。分区标识符也采用这种形式，原因相同：这种带前缀的形式是操作员在其他地方
// 会看到的格式。
const _MigInstanceIDPrefix = "MIG-"

// _MigConfNameRegex 匹配厂商的计算实例文件名，该文件名包含了用于标识实例的三个数字：设备自身的索引、其
// GPU 实例以及其计算实例。
var _MigConfNameRegex = regexp.MustCompile(`^dev([0-9]+)gi([0-9]+)ci([0-9]+)\.conf$`)

// migInstanceRef 以驱动程序枚举的方式处理某个计算实例。
//
// 设备索引会被保留，即使下面的查找仅基于实例对匹配：GPU 实例 ID 在其卡内是唯一的，因此两个卡都可以拥有 gi
// 0，如果移除其中一张卡，它们就会变成相同的键。
type migInstanceRef struct {
	deviceIndex uint32
	gpuInstance uint32
	computeInst uint32
}

// MonitorAcceleratorPartitions 实现了 device.AcceleratorPartitionDetector 接口：它会在分区自己的设备句柄
// 上报告每个被请求分区所包含的内容。
//
// 通过其分配记录的标识符可以找到分区，该识别方式通过厂商实例注册表进行，而非通过哪个实例包含我们的进程来
// 确定。这种差异正是其存在的原因：闲置分区不包含任何进程，而先查找进程的做法则无法找到句柄，并会报告不存
// 在的情况，而实际上为零。
//
// 与这里的其他制造商不同，这里确实报告了计算利用率。该库根据分区自己的句柄来计算每个实例的利用率——在运行
// matmul 循环的实例上，该利用率为 95%，而同一卡上另外三个处于空闲状态的实例则读取量为 0——因此这一数值是真
// 实的测量结果，而非无理由的缺失。
func (in *hygon) MonitorAcceleratorPartitions(
	noPciCheck bool, requests []device.AcceleratorPartitionRequest,
) (_ device.AcceleratorPartitionsGroup, err error) {
	grp := device.AcceleratorPartitionsGroup{Manufacturer: Manufacturer}

	defer loggerx.RecoverWithStackScanner(func(s loggerx.Scanner, e error) {
		in.logger.Error(e, "failed to monitor hygon device partitions")
		for s.Scan() {
			in.logger.Error(nil, s.Text())
		}
		err = e
	})

	if len(requests) == 0 {
		return grp, nil
	}

	grp.Timestamp = time.Now()

	pciDevs := binding.GetPCIDevices([]string{_PciVendor}, nil)
	if !noPciCheck && len(pciDevs) == 0 {
		// 分配名称指的是卡上某个分区，没有 Hygon PCI 设备可以响应。每个请求仍然会被响应——带有原因
		// 的响应使得其缺失变得合理。
		in.logger.Info("no hygon pci devices found for allocated partitions", "partitions", len(requests))
		grp.Partitions = unreadAcceleratorPartitions(requests)
		return grp, nil
	}

	in.init()

	cnt, ret := in.rsmi.GetDeviceCount()
	if !ret.IsSuccess() || cnt == 0 {
		in.logger.V(3).Error(ret, "failed to count devices for allocated partitions",
			"count", cnt, "partitions", len(requests))
		grp.Partitions = unreadAcceleratorPartitions(requests)
		return grp, nil
	}

	// 一次性阅读整个过程。分区的标识符是其 UUID，而 UUID 仅存在于此处：该库根本没有 GetUUID 方法，因
	// 此没有这个注册表，请求就无法被解析为句柄。
	refs, err := readMigInstanceRefs(migConfigDir)
	if err != nil {
		in.logger.V(3).Error(err, "failed to read the hygon multi-instance registry",
			"dir", migConfigDir, "partitions", len(requests))
		grp.Partitions = unreadAcceleratorPartitions(requests)
		return grp, nil
	}

	byCard := partitionRequestsByCard(requests)
	grp.Partitions = make([]device.AcceleratorPartition, 0, len(requests))
	for i := 0; i < cnt; i++ {
		logger := in.logger.WithValues("index", i)

		dev := in.rsmi.GetDeviceHandleByIndex(i)

		// 身份首先被解析，因此没有分区处理的卡片只需一次便宜的调用，完全不需要进行实例枚举。
		uuid, ret := dev.GetUniqueId()
		if !ret.IsSuccess() {
			logger.V(3).Error(ret, "failed to get device UUID")
			continue
		}
		cardRequests, ok := byCard[uuid]
		if !ok {
			continue
		}
		delete(byCard, uuid)

		pciBusID, ret := dev.GetPciId()
		if !ret.IsSuccess() {
			logger.V(3).Error(ret, "failed to get device PCI ID for allocated partitions")
			grp.Partitions = append(grp.Partitions, unreadAcceleratorPartitions(cardRequests)...)
			continue
		}

		partitions := resolvePartitions(cardRequests, refs, in.migPartitionSource(pciBusID))
		for j := range partitions {
			if partitions[j].MemoryReason != device.AcceleratorProcessReasonNone {
				logger.V(3).Info("no partition memory", "device", uuid,
					"profile", partitions[j].Profile, "reason", partitions[j].MemoryReason)
			}
		}
		grp.Partitions = append(grp.Partitions, partitions...)
	}

	// 一个请求，其卡片没有列出对应的处理句柄——即一个失败的处理句柄或 UUID 调用，或者一张卡片已离开机
	// 器，而分配操作仍指向其上的某个分区。
	for _, cardRequests := range byCard {
		grp.Partitions = append(grp.Partitions, unreadAcceleratorPartitions(cardRequests)...)
	}

	return grp, nil
}

// partitionRequestsByCard 根据每个父加速器名称对请求进行分组，因此一张卡会被计数一次，无论该节点包含其多
// 少个分区。
func partitionRequestsByCard(
	requests []device.AcceleratorPartitionRequest,
) map[string][]device.AcceleratorPartitionRequest {
	byCard := make(map[string][]device.AcceleratorPartitionRequest)
	for _, req := range requests {
		byCard[req.DeviceID] = append(byCard[req.DeviceID], req)
	}
	return byCard
}

// unreadAcceleratorPartitions 对所有请求均不返回数据，适用于那些根本无法到达分区句柄的路径。
//
// 原因总是短暂的，而非不可持续的。它是固定的，而非传递的，因为这些路径上没有任何因素能证明该库无法满足查
// 询需求：每一个——没有 PCI 设备，没有设备数量，无法读取的注册表，没有处理器的卡——都是本次遍历中无法触及的
// 卡，而能力探针不应由此得出“此节点无法完成”的结论。计算相关的原因与内存相同，与那些库完全不服务于计算功
// 能的制造商不同：这个库确实如此，因此它的缺失与内存的短暂差异有关，而非库本身的特性。
func unreadAcceleratorPartitions(
	requests []device.AcceleratorPartitionRequest,
) []device.AcceleratorPartition {
	unread := make([]device.AcceleratorPartition, 0, len(requests))
	for _, req := range requests {
		unread = append(unread, device.AcceleratorPartition{
			DeviceID:     req.DeviceID,
			Profile:      req.Profile,
			Placements:   req.Placements,
			MemoryReason: device.AcceleratorProcessReasonDriverError,
			CoresReason:  device.AcceleratorProcessReasonDriverError,
		})
	}
	return unread
}

// migPartitionRead 是某个计算实例自身句柄所报告的关于该实例的信息。
type migPartitionRead struct {
	memoryTotalBytes uint64
	memoryUsedBytes  uint64
	coresPercent     uint32
}

// migPartitionSource 是解析过程的面向驱动器的部分：它针对由注册表解析请求标识符对应的实例对所标识的卡片进
// 行响应。
//
// 这是一个闭包，因此匹配过程本身无需依赖硬件，且可进行单元测试，因为该包的规格定义本身就是如此。bool 参数
// 用于指示卡片是否包含该实例，这与读取失败的情况不同。
type migPartitionSource func(ref migInstanceRef) (migPartitionRead, device.AcceleratorProcessReason, bool)

// resolvePartitions 会按照请求的顺序，响应来自一张卡片的每一个请求。
//
// 身份、记忆和计算都源自对分区自身句柄的读取，因此一个分区要么被完整报告——包括名称、大小和测量数据——要么
// 被报告为缺失状态并给出原因。半句回答则指的是那种无法被任何人测量大小的授予。
func resolvePartitions(
	requests []device.AcceleratorPartitionRequest,
	refs map[string]migInstanceRef,
	src migPartitionSource,
) []device.AcceleratorPartition {
	partitions := make([]device.AcceleratorPartition, 0, len(requests))
	for _, req := range requests {
		partition := device.AcceleratorPartition{
			DeviceID:   req.DeviceID,
			Profile:    req.Profile,
			Placements: req.Placements,
		}

		ref, known := refs[normalizeMigInstanceID(req.ID)]
		if req.ID == "" || !known {
			// 没有需要处理的内容，或者是一个厂商注册表中没有记录的标识符：一个在实时分配下被
			// 销毁的实例，或者一个节点已不再拥有该分配的实例。属于临时状态而非无支持状态——驱
			// 动程序能够很好地满足查询需求，只是找不到这个分区——且是缺失而非近似，因为兄弟实
			// 例的占用情况会占用一个租户的内存供另一个使用。
			partition.MemoryReason = device.AcceleratorProcessReasonDriverError
			partition.CoresReason = device.AcceleratorProcessReasonDriverError
			partitions = append(partitions, partition)
			continue
		}

		read, reason, found := src(ref)
		if !found && reason == device.AcceleratorProcessReasonNone {
			// 注册表列出了该实例的名称，但驱动程序并未对其进行枚举。同样的处理，同样的原因：
			// 两者存在分歧，因此关于这个分区无法被任何一方所信。
			reason = device.AcceleratorProcessReasonDriverError
		}
		if found && reason == device.AcceleratorProcessReasonNone {
			partition.ID = _MigInstanceIDPrefix + trimMigInstanceIDPrefix(req.ID)
			partition.MemoryTotalBytes = &read.memoryTotalBytes
			partition.MemoryUsedBytes = &read.memoryUsedBytes
			partition.CoresPercent = &read.coresPercent
		}
		partition.MemoryReason = reason
		partition.CoresReason = reason
		partitions = append(partitions, partition)
	}
	return partitions
}

// normalizeMigInstanceID 会将标识符简化为注册表所依赖的纯 UUID 格式，因此持有不同拼写形式的调用者也能解析
// 到同一个实例。
func normalizeMigInstanceID(id string) string {
	return strings.ToLower(trimMigInstanceIDPrefix(id))
}

// trimMigInstanceIDPrefix 会移除厂商的显示前缀（如果存在）。
func trimMigInstanceIDPrefix(id string) string {
	return strings.TrimPrefix(id, _MigInstanceIDPrefix)
}

// migPartitionSource 将解析结果绑定到某张卡的 Multi-Instance 句柄上。
//
// 每张卡片的实例处理函数在每次遍历时最多被处理一次，在需要其处理的第一次读取时就会进行处理。这种处理会消
// 耗一次对节点-wide 索引空间的索引的调用成本，而未被分区的卡片则不会为此支付费用。
func (in *hygon) migPartitionSource(pciBusID string) migPartitionSource {
	var (
		handles map[migInstanceRef]dmi.Device
		reason  device.AcceleratorProcessReason
		walked  bool
	)

	walk := func() {
		walked = true
		handles = make(map[migInstanceRef]dmi.Device)

		dev, ret := in.dmi.GetDeviceHandleByPciBusId(pciBusID)
		if !ret.IsSuccess() {
			reason = migPartitionReason(ret)
			return
		}

		migs, ret := dev.MigDevices()
		if !ret.IsSuccess() {
			reason = migPartitionReason(ret)
			return
		}

		index, ret := dev.GetIndex()
		if !ret.IsSuccess() {
			reason = migPartitionReason(ret)
			return
		}

		for _, md := range migs {
			gi, ret := md.GetGpuInstanceID()
			if !ret.IsSuccess() {
				continue
			}
			ci, ret := md.GetComputeInstanceID()
			if !ret.IsSuccess() {
				continue
			}
			handles[migInstanceRef{deviceIndex: index, gpuInstance: gi, computeInst: ci}] = md
		}
	}

	return func(ref migInstanceRef) (migPartitionRead, device.AcceleratorProcessReason, bool) {
		if !walked {
			walk()
		}
		if reason != device.AcceleratorProcessReasonNone {
			return migPartitionRead{}, reason, false
		}

		md, ok := handles[ref]
		if !ok {
			return migPartitionRead{}, device.AcceleratorProcessReasonNone, false
		}

		mem, ret := md.GetMemoryInfo()
		if !ret.IsSuccess() {
			return migPartitionRead{}, migPartitionReason(ret), true
		}
		util, ret := md.GetUtilizationRates()
		if !ret.IsSuccess() {
			return migPartitionRead{}, migPartitionReason(ret), true
		}

		return migPartitionRead{
			memoryTotalBytes: mem.Total,
			memoryUsedBytes:  mem.Used,
			coresPercent:     util.Gpu,
		}, device.AcceleratorProcessReasonNone, true
	}
}

// migPartitionReason 类用于分类库对分区问题的回应内容。
func migPartitionReason(ret dmi.Return) device.AcceleratorProcessReason {
	switch {
	case ret.IsSuccess():
		return device.AcceleratorProcessReasonNone
	case ret.IsAPIUnavailable(), ret == dmi.ERROR_NOT_SUPPORTED:
		return device.AcceleratorProcessReasonUnsupported
	case ret == dmi.ERROR_NO_PERMISSION:
		return device.AcceleratorProcessReasonPermission
	case ret == dmi.ERROR_INSUFFICIENT_SIZE:
		return device.AcceleratorProcessReasonTruncated
	}
	return device.AcceleratorProcessReasonDriverError
}

// readMigInstanceRefs 功能会将厂商的实例注册表读取到分区 UUID 对应的三个数字中，以便进行查找。
//
// 缺失的目录实际上是一个空的注册表，而非错误：它仅在节点处于多实例模式时存在，而未被分区的节点则没有需要
// 处理的分区。无法解析的文件会被跳过，而不会导致整个流程失败——一个不可读取的条目不应使读者对节点上的其他
// 实例视而不见——而完全无法读取的目录则确实属于错误，因为此时无法得知任何信息，报告为空注册表则会导致所有
// 请求都返回“没有此类分区”的回答。
func readMigInstanceRefs(dir string) (map[string]migInstanceRef, error) {
	ciDir := filepath.Join(dir, "ci")

	entries, err := os.ReadDir(ciDir)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]migInstanceRef{}, nil
		}
		return nil, fmt.Errorf("reading %s: %w", ciDir, err)
	}

	refs := make(map[string]migInstanceRef, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		ref, ok := migInstanceRefOf(entry.Name())
		if !ok {
			continue
		}
		uuid, err := readMigConfUUID(filepath.Join(ciDir, entry.Name()))
		if err != nil || uuid == "" {
			continue
		}
		refs[strings.ToLower(uuid)] = ref
	}
	return refs, nil
}

// migInstanceRefOf 从计算实例文件名中提取出三个数字。
func migInstanceRefOf(name string) (migInstanceRef, bool) {
	m := _MigConfNameRegex.FindStringSubmatch(name)
	if m == nil {
		return migInstanceRef{}, false
	}
	nums := make([]uint32, 3)
	for i := range nums {
		n, err := strconv.ParseUint(m[i+1], 10, 32)
		if err != nil {
			return migInstanceRef{}, false
		}
		nums[i] = uint32(n)
	}
	return migInstanceRef{deviceIndex: nums[0], gpuInstance: nums[1], computeInst: nums[2]}, true
}

// readMigConfUUID 返回计算实例文件所携带的实例 UUID。
//
// 该文件是其 GPU 实例字段与自身字段的拼接结果，因此包含多个重复的键。UUID 仅出现一次，且仅存在于计算实例
// 部分，因此可以通过键而非位置来读取它。
func readMigConfUUID(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()

	s := bufio.NewScanner(f)
	for s.Scan() {
		key, value, ok := strings.Cut(s.Text(), ":")
		if !ok || strings.TrimSpace(key) != "mig_uuid" {
			continue
		}
		return strings.TrimSpace(value), nil
	}
	return "", s.Err()
}
