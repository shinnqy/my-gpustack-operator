package detector

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/binding"
)

// IMPORTANT: 从 Linux /sys 枚举本机网卡。
// 扫描 /sys/class/net 和 /sys/class/infiniband，把本机所有网卡（包括物理网卡 PF、SR-IOV 虚拟功能 VF、虚拟网卡）的名字、PCI 路径、Switch、NUMA 节点，
// 以及是否绑定了 RDMA 设备全部读出来，填进 Devices.Spec.Interfaces

const (
	// sysfsAttrMaxBytes 限制了一个属性的读取量。sysfs 属性要么是一个单一值，要么是一个简短列表。任何
	// 超过此值的数值都意味着该路径并非预期要读取的属性，而限制此读取量正是为了防止这种情况发生。
	sysfsAttrMaxBytes   = 64 << 10
	sysfsDevicesDir     = "devices"
	sysfsNetClassDir    = "class/net"
	sysfsInfinibandDir  = "class/infiniband"
	sysfsVirtualDevices = "virtual"
)

// sysfsTree 读取一个 sysfs 根目录下的属性，并一次性解析该根目录自身的符号链接。
//
// 根路径是一个参数，而非可配置的编译进常量：选择真实根路径的文件仅限于 Linux 环境，因此无法在该代码所运行
// 的平台上进行编译、lint 或测试。输入根路径则允许整个流程在 fixtures 环境下运行，这是该流程到达节点之前唯
// 一的验证方式。
type sysfsTree struct {
	realRoot string

	// rdmaClass 是反向 RDMA 布局，对于此树结构，最多只能读取一次。参见 rdmaClassIndex。
	rdmaClass *rdmaClassIndex
}

// rdmaClassIndex 是建立的 RDMA 类目录中的一项列表项。
//
// 它回答了反向布局的问题——“哪个 RDMA 设备指向这个 PCI 设备？”——适用于一次遍历中的所有接口和虚拟功能。
type rdmaClassIndex struct {
	// 列出的是该类目录是否可以被列出。
	listed bool
	// existsUnlisted 表示它是否存在且无法被列出，这属于读取失败，而非不存在。
	existsUnlisted bool
	// byDevice 将每个候选者的 RESOLVED 设备路径映射到存储该路径的 RDMA 目录中。
	byDevice map[string]string
	// `unresolvable`表示某个候选者的设备链接无法解析。如果 byDevice 中出现错误，就不能被理解为“该接口
	// 没有 RDMA 设备”，因为未能解析的那个设备可能就是正确的选项。
	unresolvable bool
}

func newSysfsTree(root string) (*sysfsTree, error) {
	realRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return nil, fmt.Errorf("resolve sysfs root %q: %w", root, err)
	}
	return &sysfsTree{realRoot: realRoot}, nil
}

// resolve 会将相对于 sysfs 根目录的路径转换为绝对路径，遵循符号链接，然后检查它们最终落在了哪里。
//
// 在这里无法避免后续的符号链接——sysfs 中每一个有趣的路径都是如此——因此验证 RESOLVED 路径就是整个工作的重
// 点。没有它，树中的某个符号链接将决定它读取的是哪个文件。
func (s *sysfsTree) resolve(rel string) (string, error) {
	resolved, err := filepath.EvalSymlinks(filepath.Join(s.realRoot, rel))
	if err != nil {
		return "", err
	}
	if resolved != s.realRoot && !strings.HasPrefix(resolved, s.realRoot+string(os.PathSeparator)) {
		return "", fmt.Errorf("sysfs path %q resolves to %q, outside %q", rel, resolved, s.realRoot)
	}
	return resolved, nil
}

// relTo 将已解析的绝对路径重新转换为以根目录为基准的相对路径，因此后续的读取操作会经过解析过程，而非绕行。
func (s *sysfsTree) relTo(abs string) (string, error) {
	rel, err := filepath.Rel(s.realRoot, abs)
	if err != nil {
		return "", err
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return "", fmt.Errorf("path %q is outside %q", abs, s.realRoot)
	}
	return rel, nil
}

// exists 报告路径是否存在，但不会跟随该路径执行，这正是“这里没有 RDMA”与“RDMA 树存在但无法被读取”之间的区
// 别所在。
func (s *sysfsTree) exists(rel string) bool {
	_, err := os.Lstat(filepath.Join(s.realRoot, rel))
	return err == nil
}

// isDir 报告相对路径是否解析为树中的目录。与 `exists` 不同，它遵循链接规则，因为调用者需要知道的是该条目
// 是什么，而不是名称是否被占用。
//
// 该错误会被返回，而不是被合并到 bool 中，这就是它有两个结果的原因。单一的 false 会将“被证明不是目录”转化
// 为“无法读取”，无论哪种情况，调用者都会跳过该条目——这会使不可读取的接口变成不存在的接口，并发布部分库存
// 信息。F3 恰恰禁止了这种情况：部分缺失会被视为不存在的硬件，因此不可读取的条目必须结束处理流程，而不是缩
// 短列表。
//
// 消失的条目并非错误。目录列表与此次调用之间可以移除一个接口，这只是一个普通的竞争情况，而非无法读取的树
// 结构，因此 os.IsNotExist 返回值为 (false, nil)——直接跳过它，不要导致失败。
func (s *sysfsTree) isDir(rel string) (bool, error) {
	p, err := s.resolve(rel)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	info, err := os.Stat(p)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return info.IsDir(), nil
}

func (s *sysfsTree) readDir(rel string) ([]os.DirEntry, error) {
	p, err := s.resolve(rel)
	if err != nil {
		return nil, err
	}
	return os.ReadDir(p)
}

// attr 读取一个属性，经过限制和修剪后。
func (s *sysfsTree) attr(rel string) (string, error) {
	p, err := s.resolve(rel)
	if err != nil {
		return "", err
	}
	f, err := os.Open(p)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()

	buf, err := io.ReadAll(io.LimitReader(f, sysfsAttrMaxBytes+1))
	if err != nil {
		return "", err
	}
	if len(buf) > sysfsAttrMaxBytes {
		return "", fmt.Errorf("sysfs attribute %q exceeds %d bytes", rel, sysfsAttrMaxBytes)
	}
	return strings.TrimSpace(string(buf)), nil
}

// attrOrEmpty 读取一个属性，若无法读取则将其视为空值。在使用的任何地方，空值都表示“未知”，因此调用者无需
// 处理该错误。
func (s *sysfsTree) attrOrEmpty(rel string) string {
	v, err := s.attr(rel)
	if err != nil {
		return ""
	}
	return v
}

// attrInt32 用于读取数值属性。若无法读取或无法解析，则值为零。对于该字段而言，零意味着“无法读取”——没有任
// 何接口会报告零的 MTU 值。
func (s *sysfsTree) attrInt32(rel string) int32 {
	v, err := s.attr(rel)
	if err != nil {
		return 0
	}
	n, err := strconv.ParseInt(v, 10, 32)
	if err != nil {
		return 0
	}
	return int32(n)
}

// numaAffinity 读取一个 NUMA 节点 ID，将内核的“无亲和性”哨兵映射为空值。
//
// “空”表示未知。不得将其标准化为节点 0，因为那样会发布一个无人阅读的关联信息。此外，哨兵值也不应被发布——“
// -1”并非节点 ID。
func (s *sysfsTree) numaAffinity(rel string) string {
	if v := s.attrOrEmpty(rel); v != "" && v != "-1" {
		return v
	}
	return ""
}

// readUp 用于读取接口的运行状态。
//
// 当可以读取时，operstate 具有权威性。而 carrier 则作为后备方案。无法读取的配对会报告为“not-up”：该值仅用
// 于描述接口，从不用于标记门控，因此从不存在的文件中猜测出该值就相当于创造了一个状态。
func (s *sysfsTree) readUp(ifaceRel string) bool {
	if v, err := s.attr(filepath.Join(ifaceRel, "operstate")); err == nil {
		return v == "up"
	}
	if v, err := s.attr(filepath.Join(ifaceRel, "carrier")); err == nil {
		return v == "1"
	}
	return false
}

// classifyBus 用于标识设备树中接口目录所在的那条总线，这也是虚拟接口被识别的方式：内核会将此类资源归类为
// `devices/virtual` 目录。
//
// 一个既非虚拟也不位于 PCI 上的接口会报告其自己的总线，而不是什么都没有，因此其缺失的 PCI 坐标会被视为一
// 种接口，而非查找失败的结果。正是出于这个原因，本次枚举会先列出接口：以 PCI 为根节点的遍历根本无法发现它。
func (s *sysfsTree) classifyBus(resolvedIfaceDir string) (bus string, virtual bool) {
	rel, err := s.relTo(resolvedIfaceDir)
	if err != nil {
		return "", false
	}
	parts := strings.Split(rel, string(os.PathSeparator))
	if len(parts) < 2 || parts[0] != sysfsDevicesDir {
		return "", false
	}
	switch segment := parts[1]; {
	case segment == sysfsVirtualDevices:
		return sysfsVirtualDevices, true
	case strings.HasPrefix(segment, "pci"):
		return "pci", false
	default:
		return segment, false
	}
}

// busFromSubsystem 指的是来自设备自身 `subsystem` 链接的总线，其目标位于 /sys/bus 下的总线目录中——因此其
// 基础是内核所指定的总线名称。
//
// “空”意味着无法解析链接，调用者将其理解为“没有比已提供的路径更好的答案”，而非独立的总线。
func (s *sysfsTree) busFromSubsystem(deviceRel string) string {
	target, err := s.resolve(filepath.Join(deviceRel, "subsystem"))
	if err != nil {
		return ""
	}
	return filepath.Base(target)
}

// resolveRDMA 会找到绑定在 PCI 设备上的 RDMA 设备，并返回其相对于 sysfs 根目录的目录路径——因为设备的名称
// 就是该路径的基础，所以该路径也包含了调用者所需的所有信息。
//
// 返回路径而非名称，使得链接能够在不假设布局的情况下检查端口：此函数已经知道两个布局中哪个得到了响应，否
// 则检查过程将不得不重新猜测，并可能在另一个分支找到的布局上出错。
//
// 它会返回所有尝试过的布局结果，因此负面回答可以说明所寻找的内容。readFailed 将“此处没有 RDMA 设备”这一普
// 通回答，与“树存在但无法读取”的情况区分开来——如果把后者当作前者，就会因为读取失败而认为该节点没有 RDMA。
func (s *sysfsTree) resolveRDMA(pciRel string) (dir string, tried []string, readFailed bool) {
	// RDMA 设备位于 PCI 设备下方的布局。
	underDevice := filepath.Join(pciRel, "infiniband")
	tried = append(tried, underDevice)
	entries, err := s.readDir(underDevice)
	switch {
	case err == nil && len(entries) > 0:
		return filepath.Join(underDevice, entries[0].Name()), tried, false
	case err != nil && s.exists(underDevice):
		readFailed = true
	}

	// 仅显示反向绑定的布局：遍历 RDMA 类，并将每个条目的设备与这个设备进行匹配。
	//
	// 沿着这条路径发生的每一次失败都会使 readFailed 状态成立，原因与设备本地布局的情况相同：在这里返
	// 回 false 会基于一个存在但无法读取的目录，从而发布 `rdma: false` —— “该接口没有 RDMA 设备”—— 这
	// 同样是一种不受支持的声明，就像在枚举失败后发布空清单一样，而此功能在其他地方也拒绝这种做法。
	tried = append(tried, sysfsInfinibandDir)
	index := s.rdmaClassIndex()
	switch {
	case index.existsUnlisted:
		readFailed = true
	case index.listed:
		want, werr := s.resolve(pciRel)
		if werr != nil {
			// 该接口自身的设备路径无法解析，因此没有条目可以匹配回它。是否会有条目匹配尚未确
			// 定，故不回答“否”。
			return "", tried, true
		}
		if dir, ok := index.byDevice[want]; ok {
			return dir, tried, false
		}
		// 没有匹配结果，因此一个无法解决的候选项现在仍具有承载作用：它可能是那个答案。只有在出现
		// 错误后才会进行查询，这就是为什么即使在某些其他条目不可读的树状结构中，找到匹配结果时仍
		// 不会报告失败。
		if index.unresolvable {
			readFailed = true
		}
	}

	return "", tried, readFailed
}

// rdmaClassIndex 会一次性读取反向 RDMA 布局，并响应后续所有查询。
//
// 该功能每个 TREE 执行一次，而非每次调用都执行。该树仅能支持一次枚举过程。resolveRDMA 每个接口和每个虚拟
// 函数调用一次，之前列出的目录在每次调用中都会解析其中的所有条目——因此使用这种布局的主机若拥有 N 个 RDMA
// 虚拟函数，则需要进行 N 次枚举和最多 N*N 次路径解析，且每次枚举过程都会执行这些操作。现在，该过程在
// Device Manager 的监控周期中运行，而非每次加速器发现时执行一次，这导致成本随时间 tick 速率的增加而上升。
//
// 生命周期机制使得这保持为备忘录而非缓存。过期的索引会指向已失效的硬件，而库存系统会报告无法打开的设备。
// 而每次处理的树状结构则能避免这种情况，使其变得不太可能。
func (s *sysfsTree) rdmaClassIndex() *rdmaClassIndex {
	if s.rdmaClass != nil {
		return s.rdmaClass
	}

	index := &rdmaClassIndex{byDevice: map[string]string{}}
	entries, err := s.readDir(sysfsInfinibandDir)
	switch {
	case err != nil:
		// 缺失的答案是不合适的。既存在又不可读的答案也不行，只有第二种情况才可能导致调用方出现读
		// 取失败的情况。
		index.existsUnlisted = s.exists(sysfsInfinibandDir)
	default:
		index.listed = true
		for _, entry := range entries {
			dir := filepath.Join(sysfsInfinibandDir, entry.Name())
			device, derr := s.resolve(filepath.Join(dir, "device"))
			if derr != nil {
				index.unresolvable = true
				continue
			}
			// 第一个写入者获胜，与替换后的循环一致：它返回了设备匹配的第一个条目，而
			// os.ReadDir 按名称对条目进行排序。
			if _, seen := index.byDevice[device]; !seen {
				index.byDevice[device] = dir
			}
		}
	}

	s.rdmaClass = index
	return index
}

// readVirtualFunctions 会收集该物理函数的虚拟函数，这些函数嵌套于其之下，并按总线编号（P8/P9）排序。
//
// 此处会返回失败的读取结果，而不会像其他属性读取那样被忽略。它们之间的区别在于降级记录会声称的内容：不可
// 读的属性会降级为 "unknown"，这是真实的。而不可读的 virtfn 设置则会降级为“此物理函数没有虚拟函数”。调用
// 方已经从响应文件中将 `sriov: true` 提交，因此读取结果会被描述为“一个未配置 VFs 的 PF”——实际上，这种分类
// 与“不是 PF”这一说法是有区别的。由于同样的原因，被丢弃的 virtfn 也会低估相同数量，因此这些结果也会被返回，
// 而不是被跳过。
//
// 在仅通过虚拟函数暴露 RDMA 的主机上，这种缺失表现为节点所拥有的所有 RDMA 设备。如果无法列出该目录，该节
// 点将取消 `rdma.capable` 的设定。此路径的错误处理机制会保留之前的清单和标签，这也是当前情况的结果。
func (s *sysfsTree) readVirtualFunctions(
	pciRel string,
) ([]workercore.DeviceInterfaceVirtualFunction, error) {
	entries, err := s.readDir(pciRel)
	if err != nil {
		return nil, fmt.Errorf("list the virtual functions of %s: %w", pciRel, err)
	}

	var out []workercore.DeviceInterfaceVirtualFunction
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "virtfn") {
			continue
		}
		vfDir, err := s.resolve(filepath.Join(pciRel, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("resolve %s of %s: %w", entry.Name(), pciRel, err)
		}
		vfRel, err := s.relTo(vfDir)
		if err != nil {
			return nil, fmt.Errorf("locate %s of %s under the sysfs root: %w", entry.Name(), pciRel, err)
		}

		vf := workercore.DeviceInterfaceVirtualFunction{
			PciBusID:     filepath.Base(vfDir),
			NumaAffinity: s.numaAffinity(filepath.Join(vfRel, "numa_node")),
			CpuAffinity:  s.attrOrEmpty(filepath.Join(vfRel, "local_cpulist")),
		}
		if netEntries, nerr := s.readDir(filepath.Join(vfRel, "net")); nerr == nil && len(netEntries) > 0 {
			vf.Name = netEntries[0].Name()
			ifaceRel := filepath.Join(vfRel, "net", vf.Name)
			vf.MTU = s.attrInt32(filepath.Join(ifaceRel, "mtu"))
			vf.Up = s.readUp(ifaceRel)
		}
		// 该链接的验证原因与位于顶层接口相同，因此遗漏的影响比通常更大：虚拟设备从顶层列表中被移
		// 除，因此这里的“nil”判定结果意味着一个绑定了 RDMA 设备的记录中没有任何“ANYWHERE”的判定。
		// 在仅通过虚拟函数暴露 RDMA 的配置下，即节点所拥有的所有 RDMA 设备。
		if rdmaDir, tried, readFailed := s.resolveRDMA(vfRel); rdmaDir != "" {
			vf.RDMA, vf.RDMADevice = true, filepath.Base(rdmaDir)
			vf.Link = s.checkRDMALink(rdmaDir)
		} else if readFailed {
			vf.Link = &workercore.DeviceInterfaceLink{
				State:  workercore.DeviceInterfaceLinkStateUnverified,
				Reason: "the RDMA tree is present but could not be read; tried " + strings.Join(tried, ", "),
			}
		}
		out = append(out, vf)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].PciBusID < out[j].PciBusID })
	return out, nil
}

// readInterface 用于构建某个接口的记录。
//
// isVF 报告称，该接口属于一个虚函数，调用者在顶层会跳过它，因为该虚函数被收集在其实际功能之下。
//
// 无法读取的属性会破坏记录，而不是将其删除：接口仍然具有名称，而带有空属性的名称会显示为“unknown”，若省略
// 接口则显示为“not present”——这是一种不同的且更糟糕的情况。
//
// 如果返回的失败记录声称了某些内容，则操作将失败并终止。有两种情况：一是虚拟函数集，它没有“unknown”这一选
// 项（参见 readVirtualFunctions）。二是设备链接。在非虚拟接口上，无法解析的 `device` 会留下 `rdma: false`，
// 没有结果，且不是“unknown”——这是普通以太网网卡产生的记录，因此节点的 `rdma.capable` 属性会被撤销，因为存
// 在无法读取的符号链接。若使用“unverified”进行合成则会导致另一种失败情况，即在一个可能完全没有 RDMA 的节
// 点上创建端点。由于两种说法都不可用，因此操作失败，之前发布的清单和标签仍然有效。
func (s *sysfsTree) readInterface(
	name string,
) (iface workercore.DeviceInterface, isVF bool, err error) {
	iface.Name = name
	ifaceRel := filepath.Join(sysfsNetClassDir, name)
	iface.MTU = s.attrInt32(filepath.Join(ifaceRel, "mtu"))
	iface.Up = s.readUp(ifaceRel)

	resolvedIface, rerr := s.resolve(ifaceRel)
	if rerr != nil {
		return iface, false, nil //nolint:nilerr // degraded on purpose: a named interface, coordinates unknown
	}
	iface.Bus, iface.Virtual = s.classifyBus(resolvedIface)
	if iface.Virtual {
		// 虚拟接口在此处停止，位于下方的 RDMA 解析之前。这是一个值得提及的限制：此读取器中所有通
		// 往 RDMA 设备的路径都始于 PCI 设备目录，而绑定、VLAN 或 veth 则没有此类路径。因此，此读
		// 取器无法在任何主机上创建承载 RDMA 设备或链接判定的虚拟接口。
		//
		// 这是软件 RDMA 的一个缺陷：rxe 和 siw 将 RDMA 设备绑定到普通的 netdev 上，而 netdev 可能
		// 是绑定或 VLAN。端点可以正常工作，但在此处不可见。移动这个返回指针无济于事，因为另一个分
		// 支无法访问，未被跳过。修复它需要通过 netdev 名称进行 RDMA 查找，例如通过
		// /sys/class/infiniband/<设备>/ports/*/gid_attrs/ndevs`/* 来实现，而不是通过 PCI 查找。
		//
		// triggersDetect 仍然仅豁免那些没有 RDMA 记录的虚拟接口，这看起来就像这些记录确实存在一样。
		// 它故意写在“判定”上而非“虚拟性”上：谓词是“无法影响门”，因此如果未来的读者通过非 PCI 设备
		// 访问到 RDMA，触发器会自动处理，无需查找和更改。如今该分支无法访问，此处说明这一点正是关
		// 键——对于一种无法产生任何效果的形式，豁免措施就被视为覆盖。
		return iface, false, nil
	}

	deviceDir, rerr := s.resolve(filepath.Join(ifaceRel, "device"))
	if rerr != nil {
		return iface, false, fmt.Errorf("resolve the device of %s: %w", name, rerr)
	}
	deviceRel, rerr := s.relTo(deviceDir)
	if rerr != nil {
		return iface, false, fmt.Errorf("locate the device of %s under the sysfs root: %w", name, rerr)
	}

	// 一个虚拟函数命名其物理功能。找到该函数即可确认此接口不是顶级入口。
	if s.exists(filepath.Join(deviceRel, "physfn")) {
		return iface, true, nil
	}

	// 路径告诉我们设备位于何处。其自身的`subsystem`链接代表了内核对其身份的认定，而两者在 PCI 相关的
	// 任何方面都存在分歧。USB 网卡通过 devices/pci0000:00/.../usb1/1-1/1-1:1.0/net/eth0 进行定位，因
	// 此路径派定的名称被称为 PCI——其设备目录的基名（"1-1:1.0"）则被存储为 PCI 地址，随后在没有任何设
	// 备的设备上执行桥接遍历，并根据此过程计算距离坐标。当子系统给出答案时，优先采用该答案。若未给出，
	// 则采用路径派定的答案，因为无法读取的链接并不能证明存在不同的总线。
	if bus := s.busFromSubsystem(deviceRel); bus != "" {
		iface.Bus = bus
	}

	iface.NumaAffinity = s.numaAffinity(filepath.Join(deviceRel, "numa_node"))
	iface.CpuAffinity = s.attrOrEmpty(filepath.Join(deviceRel, "local_cpulist"))

	if iface.Bus == "pci" {
		// 与加速器端共享，而非在此处重新推导。这些坐标仅在与制造商所写的坐标相等时才有用，因此某
		// 种实现方式使得两者在构造上而非规范上保持一致。
		iface.PciBusID = filepath.Base(deviceDir)
		iface.PciRootID, iface.PciSwitches = binding.ResolvePCITopology(deviceDir)
		iface.PciVendor = s.attrOrEmpty(filepath.Join(deviceRel, "vendor"))
		iface.PciDevice = s.attrOrEmpty(filepath.Join(deviceRel, "device"))
	}

	// sriov_numvfs 确实存在于某个物理功能上，包括那些没有配置 VFs 的功能。因此，该标志是从文件的存在
	// 性中读取的，其计数也无法由此推断出来：“具有零 VFs 的 PF”和“不是 PF”是两种不同的情况。
	//
	// 关注点，而非可读性。读取文件并将答案视为 `nil` 错误，导致生成了不可读但可见的标记报告“不是 PF”，
	// 进而完全跳过 VF 枚举——在那些 RDMA 仅存在于虚拟函数上的主机上，当读取失败时 `rdma.capable` 会被
	// 清除。通过关注点检查后，标记仍显示为“PF”，任何无法列出函数的现象都会从 readVirtualFunctions 中
	// 传递出来，而不会在这里被忽略。
	if s.exists(filepath.Join(deviceRel, "sriov_numvfs")) {
		iface.SRIOV = true
		vfs, verr := s.readVirtualFunctions(deviceRel)
		if verr != nil {
			return iface, false, fmt.Errorf("read the virtual functions of %s: %w", name, verr)
		}
		iface.VirtualFunctions = vfs
	}

	rdmaDir, tried, readFailed := s.resolveRDMA(deviceRel)
	switch {
	case rdmaDir != "":
		iface.RDMA, iface.RDMADevice = true, filepath.Base(rdmaDir)
		// RDMA 设备是一个限制条件，并非有效的连接，两者在真实硬件层面存在差异：一个端口可以完全配
		// 置好，包含地址和网关，而其连接则处于断开状态。
		iface.Link = s.checkRDMALink(rdmaDir)
	case readFailed:
		// 树存在，但无法读取。未经验证，从未失败：失败会留下节点标签，而标签绝不能被隐藏，因为文
		// 件无法被读取。
		iface.Link = &workercore.DeviceInterfaceLink{
			State:  workercore.DeviceInterfaceLinkStateUnverified,
			Reason: "the RDMA tree is present but could not be read; tried " + strings.Join(tried, ", "),
		}
	}

	return iface, false, nil
}

// enumerateInterfaces 会按名称排序，列出给定 sysfs 根目录下的每个网络接口。
//
// 它枚举了接口，并将每个接口的 PCI 设备作为属性进行标识，而不是遍历 PCI 总线进行关联——非 PCI 互连上的接口
// 对后者来说是不可见的，而这正是该清单在平台上最需要的用途。
//
// 未能列举是一种错误，而非空列表。空清单与从未进行分析的节点并无区别，因此返回没有接口也没有错误，这是通
// 过疏漏得以避免的一种失败模式。出于相同原因，这也适用于部分缺失的情况，这就是为什么某个接口的不可读虚拟
// 函数集会结束该遍历过程，而不是缩短列表。
func enumerateInterfaces(root string) ([]workercore.DeviceInterface, error) {
	tree, err := newSysfsTree(root)
	if err != nil {
		return nil, err
	}

	entries, err := tree.readDir(sysfsNetClassDir)
	if err != nil {
		return nil, fmt.Errorf("enumerate network interfaces: %w", err)
	}

	out := make([]workercore.DeviceInterface, 0, len(entries))
	for _, entry := range entries {
		// 网络类目录并非仅包含接口。当绑定驱动程序加载时，它还包含 `bonding_masters` 这个普通的
		// FILE 文件，它是驱动程序的控制界面。在两个 RDMA 主机上，这是唯一的非符号链接条目。将其作
		// 为接口读取时，会得到一个仅包含名称的条目，这意味着该接口的总线无法被确定。
		//
		// 跳过它并非本次拒绝的“隐藏存在”：无法指向目录的条目根本不是接口，因此记录一个也会造成存
		// 在，反之亦然。只有经过验证的非目录条目才会被跳过——无法分类的条目会结束该过程，因为跳过
		// 它最终也会显示出缺失。
		isDir, derr := tree.isDir(filepath.Join(sysfsNetClassDir, entry.Name()))
		if derr != nil {
			return nil, fmt.Errorf("classify the net class entry %s: %w", entry.Name(), derr)
		}
		if !isDir {
			continue
		}

		iface, isVF, ierr := tree.readInterface(entry.Name())
		if ierr != nil {
			return nil, ierr
		}
		if isVF {
			continue
		}
		out = append(out, iface)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}
