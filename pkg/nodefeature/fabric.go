package nodefeature

import (
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/utils/strconvx"
)

const (
	// FabricFeatureLabelPrefix 用于作为该文件所写入的每一个标签的前缀，它也是使该集合能够被识别为集合
	// 的关键。
	FabricFeatureLabelPrefix = FeatureLabelPrefix + "fabric."
	// NodeFabricDomainLabelKey 指定了该节点上每个加速器所属的纵向扩展互联域，其格式为 `<kind>-<id>`——
	// 在将域划分为可相互寻址的子集的制造商中，也采用 `<kind>-<id>-<clique>` 的格式。
	//
	// 该值属于值的一部分，而不仅仅是键的一部分，因为域 ID 仅能在其自身互联结构中进行比较：一个 Ascend
	// 超级节点 ID 是一个小整数，而一个 AMD XGMI 目录 ID 是一个 64 位数字，因此 `7` 可以同时用于这两个
	// ID。仅基于 ID 的选择器会使得作业在两个不相关的架构之间被调度。
	//
	// 该群集之所以处于该数值，是因为它位于同一层级之下。两个共享互联网络集群唯一标识符的 NVIDIA 加速
	// 器虽然位于同一互联网络内，但并未形成群集，因此无法互相通信。仅命名该集群也无法实现分区内的共位，
	// 更无法保证该集群能服务于位于分区另一端的节点——而这正是该数值被比较的地方。如果制造商不报告该群
	// 集的存在，那么这种两部分的组合形式将保持不变。
	//
	// 仅当节点上的所有加速器报告相同的域时才会触发此行为，这使其可用作相等性选择器：位于不同域的加速
	// 器节点没有唯一的答案，发布其中任何一个都会宣传硬件所不提供的共位情况。这样的节点没有密钥，需要
	// 按每个加速器获取信息的用户会读取包含该信息的 `Devices` 对象。
	NodeFabricDomainLabelKey = FabricFeatureLabelPrefix + "domain"
	// NodeFabricMembersLabelKey 表示该域报告的成员数量。
	//
	// 仅用于信息说明。选择器采用相等匹配方式，因此无法通过它询问“至少 128 个成员”的相关问题，只能询问
	// “成员数量 == 384”。发布该内容可以回答操作器关于节点爆炸半径的问题。但无法回答调度器的问题。
	NodeFabricMembersLabelKey = FabricFeatureLabelPrefix + "members"
)

// ConstructFabricNodeLabels 根据一次检测流程的库存信息，构建扩展互联网络标签。
//
// 这两个键都描述了节点的整体特性，因此除非节点有明确的答案，否则都不会显示。关于为何部分答案不如没有答案
// 好，请参考 NodeFabricDomainLabelKey。
//
// 对于没有此类互联网络的节点，它返回的是一个空映射，而非 nil。在写入这些对象的场景下，空结果具有实际意义：
// 写入器会移除 pass 未报告的以该前缀为键的键，因此从超级容器中移除的节点会失去标签，而不是保留其原有的域。
// 这就是为何选择不给出决策而非保持沉默的原因——这也是为什么减少操作必须针对整个节点进行，因为只有当所有访
// 问该对象的写入器都计算出相同的答案时，移除操作才是有效的。
func ConstructFabricNodeLabels(groups device.DevicesGroupList) map[string]string {
	labels := map[string]string{}

	domain, members, ok := soleFabricDomain(groups)
	if !ok {
		return labels
	}

	// 域值是复合的，因此清理器无法作为安全网：它将标签值限制在 63 个字符以内，而截断后的
	// `<kind>-<id>` 会显示其他节点可能处于的不同的域。因此，在清理后无法正常显示的域将不会获得任何键，
	// 这也是 NodeRDMANumaLabelKey 对其自身设置值所遵循的规则。
	if kubemeta.SanitizeLabelValue(domain) != domain {
		return labels
	}
	labels[NodeFabricDomainLabelKey] = domain

	// “零”表示制造商未报告尺寸，也从未报告没有成员的域名。
	if members > 0 {
		labels[NodeFabricMembersLabelKey] = kubemeta.SanitizeLabelValue(strconvx.FormatUint(uint64(members), 10))
	}

	return labels
}

// soleFabricDomain 报告了该节点上每个加速器所属于的域，该域的成员数量，以及是否有一个明确无误的答案。
//
// 一个没有互联网络记录的加速器，与拥有其他域的加速器一样，会发表相同的意见：一半的加速器位于互联网络上，
// 另一半则不在，这样的节点没有节点级域。若将那部分不活跃的节点视为同意，就会发布一个承诺整个节点的密钥。
//
// 该群集是价值的一部分，制造商在此报告一个值，因此同一个互联网络中的两个不同群集中的加速器在此存在分歧，
// 这与不同互联网络中的两个加速器的情况相同——这是同时拥有两个节点的节点应给出的答案，因为双方都无法解决对
// 方的问题。关于为何分区应属于该值中而非其旁边，请参考 NodeFabricDomainLabelKey。
//
// 关于大小的争议较为简短，仅涉及大小问题。该领域指的是加速器所对比的对象，因此当各节点报告不同的成员数量
// 时——比如某节点使用过时的测量标准进行报告，或是在可重新计算期间取得的读数——该领域就确定下来了，但大小却
// 未知。采用迭代过程中首先得到的那个数量，可以让一个超级节点内的两个节点分别报告不同的大小，而无需记录输
// 入数据存在分歧，因此该数量被保留不报。零就是 ConstructFabricNodeLabels 中那种没有人报告过的大小。
func soleFabricDomain(groups device.DevicesGroupList) (domain string, members uint32, ok bool) {
	sizeAgreed := true
	for gi := range groups {
		accels := groups[gi].Accelerators
		for ai := range accels {
			fabric := accels[ai].Topology.Fabric
			if fabric == nil || fabric.Kind == "" || fabric.ID == "" {
				return "", 0, false
			}

			seen := fabric.Kind + "-" + fabric.ID
			if fabric.CliqueID != "" {
				seen += "-" + fabric.CliqueID
			}
			if domain == "" {
				domain, members = seen, fabric.MemberCount
				continue
			}
			if seen != domain {
				return "", 0, false
			}
			if fabric.MemberCount != members {
				sizeAgreed = false
			}
		}
	}

	if !sizeAgreed {
		members = 0
	}
	return domain, members, domain != ""
}
