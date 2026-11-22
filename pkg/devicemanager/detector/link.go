package detector

import (
	"fmt"
	"path/filepath"
	"strings"

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubemeta"
)

// IMPORTANT: 本文件负责：RDMA 链路健康检查 + 监控防抖

// interfacesChanged 报告新枚举的接口记录是否与上次发布的记录不同，这也会提示监控循环进行下一次检测。
//
// reportedKnown 表示所报告的内容是否属于一个基准值，这是一个独立的参数，因为该切片无法包含这一信息。没有
// 基准值且没有基准值的切片是相同的，但对应的答案却截然相反：如果没有基准值，则没有任何内容被发布，因此任
// 何成功的枚举操作都必须满足这一条件。如果没有这个参数，那么对于触发子集为空的所有主机来说，答案都会是
// `"未改变"`——每个接口都是虚拟的，没有包含 RDMA 记录，而这正是 Device Manager 在 Pod 网络命名空间中所看到
// 的状态——因此，只要加速器密钥保持不变，库存信息就会一直不存在。
//
// 失败的枚举操作并不构成变化，这比缺失的基准情况更为重要：对不可读取的读取进行循环处理，在没有证据的情况
// 下会重新报告结果，而报告路径在出现一次这种情况后就会拒绝发布空清单——因此双方同意将失败的读取视为“无明确
// 信息”。因此，恢复过程由第一个成功的枚举操作驱动，而非失败本身，这也是为什么永久不可读取的 sysfs 不会进
// 行循环处理的原因。
//
// 首次出现次数不参与比较，它们在这里被移除，而非由调用者自行处理。这些数据来自存储对象，而非机器，因此包
// 含它们的比较会在每次 tick 时产生差异，并持续重新报告正确的数据——这是仅由写入量引起的缺陷。若将其本地保
// 存，则未来的调用者无法通过提交已被合并的记录来重新引入这些数据。
func interfacesChanged(
	reported []workercore.DeviceInterface, reportedKnown bool,
	detected []workercore.DeviceInterface, detectedErr error,
) bool {
	if detectedErr != nil {
		return false
	}
	if !reportedKnown {
		return true
	}
	return !kubemeta.DeepEqual(
		withoutLinkFirstSeen(triggersDetect(reported)),
		withoutLinkFirstSeen(triggersDetect(detected)),
	)
}

// triggersDetect 保留那些变化值得再次检测的接口。
//
// 临时性的虚拟端点会被移除，这种缺失正是关键所在。每个在节点上启动或停止的 Pod，都会在
// /sys/devices/virtual/net 下添加或移除一个 `veth`，因此对比整个列表会将 Pod 的生命周期转化为硬件变更信号：
// 该循环会离开监控范围，重新运行制造商的驱动检测功能，并在每个 Pod 事件中对集群范围内的 Devices 对象进行
// 重写，每款制造商的 DaemonSet 会触发一次这样的操作。在繁忙的节点上，这种情况永远不会停止。这种对比所引入
// 的成本与之前的情况类似，只是从另一个方向实现——与时间戳情况不同，数据确实发生了变化，因此对象中的值看起
// 来没有错误。
//
// 无论是否包含 RDMA 设备或链接裁决的虚拟接口，都将被保留。条件是“无法影响门限”，而非“是虚拟的”，因此带有
// RDMA 裁决的绑定或覆盖接口会像其他情况一样进行循环处理。
//
// 当前没有这样的接口：readInterface 在解析 RDMA 之前返回的是一个虚拟接口，因为所有通往 RDMA 设备的路径都
// 始于 PCI 设备目录。因此该分支无法访问，但保留它，因为该条件是正确的——通过其他方式最终到达 RDMA 的读取器
// 无需在此进行任何修改。说明如下，以免被误认为是覆盖情况。
//
// 已发布的清单仍然记录所有接口，包括临时接口，这是该功能的边界：只有那些迫使需要重新读取的内容才会被缩小。
// 代价是，纯虚拟接口只有在其他变更报告后才会被发布，这就是其价值——精确到秒的 veth 列表，不如一个在每次
// Pod 启动时都不重新写入其对象的节点有价值。
func triggersDetect(in []workercore.DeviceInterface) []workercore.DeviceInterface {
	out := make([]workercore.DeviceInterface, 0, len(in))
	for i := range in {
		if in[i].Virtual && !carriesRDMARecord(&in[i]) {
			continue
		}
		out = append(out, in[i])
	}
	return out
}

// carriesRDMARecord 报告该接口或其任何虚拟函数是否绑定了 RDMA 设备，或者是否有记录下链接 verdict——这两个
// 信息都是节点标签的由来。
func carriesRDMARecord(iface *workercore.DeviceInterface) bool {
	if iface.RDMA || iface.Link != nil {
		return true
	}
	for i := range iface.VirtualFunctions {
		if vf := &iface.VirtualFunctions[i]; vf.RDMA || vf.Link != nil {
			return true
		}
	}
	return false
}

// withoutLinkFirstSeen 返回一份记录副本，其中每个链接的首次出现时间都被清除，适用于所有级别。
func withoutLinkFirstSeen(in []workercore.DeviceInterface) []workercore.DeviceInterface {
	out := cloneInterfaces(in)
	for i := range out {
		if out[i].Link != nil {
			out[i].Link.FirstSeenTime = nil
		}
		for j := range out[i].VirtualFunctions {
			if vf := &out[i].VirtualFunctions[j]; vf.Link != nil {
				vf.Link.FirstSeenTime = nil
			}
		}
	}
	return out
}

// cloneInterfaces 返回一个记录，该记录与给定的记录没有任何共享内容，因此在调用该方法的监视器循环中所比较
// 的对象，无法被后续执行的合并操作所修改。
func cloneInterfaces(in []workercore.DeviceInterface) []workercore.DeviceInterface {
	if in == nil {
		return nil
	}
	out := make([]workercore.DeviceInterface, len(in))
	for i := range in {
		in[i].DeepCopyInto(&out[i])
	}
	return out
}

// sysfsEnumName 返回 sysfs 枚举对象名称的前半部分，由 RDMA 子系统以 "<序号>: <名称>" 的形式输出——例如“4:
// ACTIVE”或“5: LinkUp”。没有序号的值则原样返回。
//
// 比较必须针对整个名称，因为这些名称是彼此的前缀：将 `ACTIVE` 作为子串进行匹配时，也允许使用 ACTIVE_DEFER，
// 这是一个已失去连接且不再承载用户流量的端口。将此状态报告为 `ok`，则会在连接上发布节点的 RDMA 标签，而任
// 何设备都无法使用该连接。而声称拥有可用的连接则属于错误，下游设备无法发现此问题，这也是为什么该文件从无
// 法读取的文件状态中从未变为 `failed`。
func sysfsEnumName(v string) string {
	if _, name, found := strings.Cut(v, ":"); found {
		return strings.TrimSpace(name)
	}
	return strings.TrimSpace(v)
}

// checkRDMALink 会验证相对于系统文件系统根目录的给定路径，该 RDMA 设备的连接情况。
//
// 该检查机制是每个端口对应两次属性读取——即端口的传输状态及其物理连接状态——并且是故意不按厂商进行分发的。
// 现有技术中，对于需要检查 NPU RoCE 端口的某一平台，会读取这两个文件。这些文件属于 RDMA 子系统的自身属性，
// 而非任何厂商的：按厂商分类的表格将仅有一条记录，从而产生一种“未针对该厂商实施检查”的状态，而这种情况在
// 其他情况下不会出现。
//
// 这两个属性都会被参考，因为它们解答不同的问题：端口可以报告传输层的活动状态，而物理连接在管理上则处于关
// 闭状态。
//
// 不可读文件所呈现的状态，正是此处需要做出的重要决定。`failed` 字段会保留节点标签，因此只有所有端口都被读
// 取且没有端口携带链接时，该状态才会被触发。无法读取的端口会使“所有端口均关闭”这一状态保持未确定状态，将
// 其报告为 `failed` 会基于无法打开文件的依据，从调度中移除该节点。
//
// 判断结果是按设备进行的，调用者会将其复制到所有指向该设备的接口上。这在设备端口对应一个 netdev 时是正确
// 的——未使用的端口对接口没有任何影响——但在 InfiniBand 模式下则错误，因为一个设备的两个端口对应两个 IPoIB
// netdev：健康的端口会掩盖处于关闭状态的端口，而接口的行数据则夸大了这一情况。将 netdev 分配给其对应的端
// 口需要使用 ports/*/gid_attrs/ndevs/*，但此处没有可用的机器来呈现这种形态，因此这种映射会处于未使用状态，
// 其失效模式是无法提供标签。这个限制是命名的，而不是半固定的。
func (s *sysfsTree) checkRDMALink(rdmaRel string) *workercore.DeviceInterfaceLink {
	portsRel := filepath.Join(rdmaRel, "ports")

	// 错误被丢弃，而非分支处理：无法读取的端口目录、空的端口目录，以及每个端口都拒绝被读取的端口，都
	// 是同一情况——没有读取到任何端口状态——它们通过下面的循环处理，但没有任何结果。
	entries, _ := s.readDir(portsRel)

	// os.ReadDir 按名称对条目进行排序，因此下面的排序也是按照此规则进行的。若在此处再次进行排序，则能
	// 再次确保同一效果，而未经排序的排序方式在没有任何更改的情况下会导致结果不一致。
	var (
		observed   []string
		unreadable []string
	)
	for _, entry := range entries {
		portRel := filepath.Join(portsRel, entry.Name())
		state, serr := s.attr(filepath.Join(portRel, "state"))
		physState, perr := s.attr(filepath.Join(portRel, "phys_state"))
		if serr != nil || perr != nil {
			unreadable = append(unreadable, entry.Name())
			continue
		}

		// 这两个值都原封不动地填入原因中。它们是内核自己的表述，而读取被隐藏标签的运算符需要的是
		// 所读取的内容，而不是我们的总结。
		observed = append(observed,
			fmt.Sprintf("port %s: state=%q phys_state=%q", entry.Name(), state, physState))

		if sysfsEnumName(state) == "ACTIVE" && sysfsEnumName(physState) == "LinkUp" {
			return &workercore.DeviceInterfaceLink{State: workercore.DeviceInterfaceLinkStateOK}
		}
	}

	switch {
	case len(observed) == 0:
		return &workercore.DeviceInterfaceLink{
			State:  workercore.DeviceInterfaceLinkStateUnverified,
			Reason: fmt.Sprintf("the RDMA port state could not be read under %q", portsRel),
		}
	case len(unreadable) > 0:
		return &workercore.DeviceInterfaceLink{
			State: workercore.DeviceInterfaceLinkStateUnverified,
			Reason: fmt.Sprintf("the RDMA link could not be verified: %s; unreadable: %s",
				strings.Join(observed, "; "), strings.Join(unreadable, ", ")),
		}
	default:
		return &workercore.DeviceInterfaceLink{
			State:  workercore.DeviceInterfaceLinkStateFailed,
			Reason: "the RDMA link is not usable: " + strings.Join(observed, "; "),
		}
	}
}

// carryLinkFirstSeen 根据已存储的数据填充每个检测到的链接的首次可见时间，并会就地重写被检测到的内容。
//
// 它必须在检测到的库存与存储的库存进行比较之前运行，这种排序逻辑正是该函数存在的原因。该功能的两个要求会
// 相互冲突：当故障持续存在时，首次出现的时刻必须保持稳定。而如果没有发现任何新情况，通过测试的结果也必须
// 与存储的数据相等。在每次测试中都记录当前时间，可以满足第一个要求，但会破坏第二个要求——因为比较结果永远
// 不会一致，所以对象将在每次测试中都被重新写入，从而始终保持正确的数据。写入量将是唯一的迹象。
//
// 只有故障会具有时间属性，其关键在于 STATE 字段，而非具体原因：第二个端口故障会改变原因，但故障本身仍保持
// 不变，这正是“这个问题已经存在多久了？”所询问的内容。存储的故障如果没有时间属性，则会被标记而非持续存在，
// 因此在此字段之前写入的对象会合并，而不是保持空白。
//
// 传递一个空的存储库存是创建路径：此时每个失败情况都会首次被看到。
func carryLinkFirstSeen(stored, detected []workercore.DeviceInterface, now meta.Time) {
	previous := make(map[string]*workercore.DeviceInterfaceLink, len(stored))
	for i := range stored {
		if stored[i].Link != nil {
			previous[stored[i].Name] = stored[i].Link
		}
		// 虚函数也受到同样的处理，因为它们携带的是相同的判断结果。每个迭代中需要重新更新的虚函数
		// 的计时器会永久地重写对象，而唯一的错误值就是时间戳，这种记录是无人会去比较的。
		for j := range stored[i].VirtualFunctions {
			vf := &stored[i].VirtualFunctions[j]
			if vf.Link != nil {
				previous[vfLinkKey(stored[i].Name, vf)] = vf.Link
			}
		}
	}

	for i := range detected {
		carryOneLinkFirstSeen(previous, detected[i].Name, detected[i].Link, now)
		for j := range detected[i].VirtualFunctions {
			vf := &detected[i].VirtualFunctions[j]
			carryOneLinkFirstSeen(previous, vfLinkKey(detected[i].Name, vf), vf.Link, now)
		}
	}
}

// vfLinkKey 用于标识某个虚拟函数的链接记录在不同迭代中。
//
// 总线 ID 是身份标识，而非名称：没有配置网络设备的虚拟设备根本没有名称，而属于同一物理功能的两个设备会共
// 享一个密钥，并互相传递故障计时器。
//
// 总线 ID 始终存在，这是用户的行为，而非该函数的职责：readVirtualFunctions 从 sysfs 目录名称中获取它，因
// 此如果此包没有生成该 ID，记录就会缺失。没有该 ID 的记录会回退到名称，而同时没有两者的记录则会退化为共享
// 键——正是这种冲突被上一段话所避免——因此设置总线 ID 的行为是必要的，而非偶然的。若在此处生成身份，则需要
// 在不同版本中保持稳定，而这样的记录根本无法做到这一点。
//
// 预检查报告按照相同的规则（vfInterfaceName）来命名 VF 行，而不是通过该函数进行命名：一个用于标识中断计时
// 器，另一个用于指代读取器，它们今天达成一致，因为总线 ID 是解答这两个问题的最佳方案。
func vfLinkKey(ifaceName string, vf *workercore.DeviceInterfaceVirtualFunction) string {
	id := vf.PciBusID
	if id == "" {
		id = vf.Name
	}
	return ifaceName + "/" + id
}

// carryOneLinkFirstSeen 对一条链接记录应用该规则。
//
// 通过调用者的密钥进行匹配，该密钥即记录的身份。按位置进行匹配则会导致一个接口的中断计时器在库存增加或减
// 少时立即被另一个接口占用。
func carryOneLinkFirstSeen(
	previous map[string]*workercore.DeviceInterfaceLink,
	key string, link *workercore.DeviceInterfaceLink, now meta.Time,
) {
	if link == nil {
		return
	}
	if link.State != workercore.DeviceInterfaceLinkStateFailed {
		// 没有中断，因此没有需要作为首次出现时间的起点。
		link.FirstSeenTime = nil
		return
	}

	if was := previous[key]; was != nil &&
		was.State == workercore.DeviceInterfaceLinkStateFailed && was.FirstSeenTime != nil {
		link.FirstSeenTime = was.FirstSeenTime.DeepCopy()
		return
	}
	link.FirstSeenTime = now.DeepCopy()
}
