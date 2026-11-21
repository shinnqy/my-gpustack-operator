package device

import (
	"os"
	"regexp"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"

	"gpustack.ai/gpustack/binding"
	"gpustack.ai/gpustack/pkg/utils/quantityx"
	"gpustack.ai/gpustack/pkg/utils/strconvx"
	"gpustack.ai/gpustack/pkg/utils/typex"
)

// 控制各查询采用的读取方式。
var (
	constructGroupIDWithMemory bool
)

func init() {
	constructGroupIDWithMemory = os.Getenv("GPUSTACK_DEVICES_GROUP_ID_WITH_MEMORY") == "true"
}

var (
	// _CPUNameTrademarkMarkerReplacer 去掉 CPU 商品名里的商标标记。
	_CPUNameTrademarkMarkerReplacer = strings.NewReplacer("(R)", "", "(r)", "", "(TM)", "", "(tm)", "")

	// _CPUNamePrefixCruft 匹配 Genuine、11th Gen 等 CPU 名称前缀。
	_CPUNamePrefixCruft = regexp.MustCompile(`^(Genuine |[0-9]+(st|nd|rd|th) Gen )+`)

	// _CPUNameCoreCountSuffix 匹配 CPU 商品名末尾的核心数，例如 64-Core、16-Cores。
	_CPUNameCoreCountSuffix = regexp.MustCompile(`(?i) [0-9]+-cores?$`)
)

// NormalizeName 将设备或 CPU 商品名变为可用作 Kubernetes 标签的短名称。它转成小写，只保留字母、数字与 -_.，
// 合并连续分隔符，并去掉匹配的厂商前缀和首尾分隔符。
// stripCruft 可进一步去掉频率、商标、代际及核心数等 CPU 修饰，maxLength 为正时按字符数截断。
func NormalizeName(name, prefix string, maxLength int, stripCruft bool) string {
	if stripCruft {
		name, _, _ = strings.Cut(name, " @ ")
		name = _CPUNameTrademarkMarkerReplacer.Replace(name)
		name = _CPUNamePrefixCruft.ReplaceAllString(name, "")
		name = strings.TrimSpace(name)
		name = strings.TrimSuffix(name, " CPU")
		name = strings.TrimSuffix(name, " Processor")
		name = _CPUNameCoreCountSuffix.ReplaceAllString(name, "")
	}

	if maxLength <= 0 {
		maxLength = len(name)
	}

	buf := make([]rune, 0, min(len(name), maxLength))
	var pr rune
	for _, r := range name {
		switch {
		case r >= 'A' && r <= 'Z':
			r += 'a' - 'A'
		case r >= 'a' && r <= 'z':
		case r >= '0' && r <= '9':
		case r == ' ':
			r = '-'
			fallthrough
		case r == '-' || r == '_' || r == '.':
			if len(buf) == 0 {
				continue
			}
			if pr == '-' || pr == '_' || pr == '.' {
				continue
			}
		default:
			continue
		}
		buf = append(buf, r)
		pr = r

		// 去掉匹配的名称前缀。
		if len(buf) == len(prefix) {
			if strings.EqualFold(string(buf), prefix) {
				buf = buf[:0]
				pr = 0
			}
		}

		// 达到最大长度后停止继续写入。
		if len(buf) >= maxLength {
			break
		}
	}

	if len(buf) == 0 {
		return ""
	}

	// 清除结尾的非字母数字字符。
	if c := buf[len(buf)-1]; c == '-' || c == '_' || c == '.' {
		buf = buf[:len(buf)-1]
	}

	return string(buf)
}

// CalculateUtilization 根据已用量和总量计算使用百分比。
func CalculateUtilization[U, T typex.Integer](usage U, total T) uint32 {
	if usage <= 0 || total <= 0 {
		return 0
	}
	return uint32(float64(usage) / float64(total) * 100)
}

// API 统一用 MiB。`>> 20` 等价于对非负字节数做整数除法 ÷ 1,048,576
// ConvertBytesToMiB 将字节换算为 MiB。
func ConvertBytesToMiB[T typex.Integer](bytes T) uint64 {
	return uint64(bytes) >> 20
}

// ConvertKiBToMiB 将 KiB 换算为 MiB。
func ConvertKiBToMiB[T typex.Integer](kib T) uint64 {
	return uint64(kib) >> 10
}

// NormalizeVersion 将版本号规范为主版本和次版本。
func NormalizeVersion(ver string) string {
	if ver == "" {
		return ""
	}
	ps := strings.Split(ver, ".")
	if len(ps) < 2 {
		return ver
	}
	return ps[0] + "." + ps[1]
}

// ConstructGroupID 去掉名称的厂商前缀，并按配置将显存容量编码进设备组标识。
func ConstructGroupID(manufacturer, name string, memory uint64) string {
	// 设备组名由厂商、规范化型号及可选显存后缀组成，总长度不超过 63 个字符。
	budget := 63 - len(manufacturer) - 1
	if constructGroupIDWithMemory {
		budget -= 7 // 为以连字符起始的显存后缀预留最多 7 个字符。
	}
	n := NormalizeName(name, manufacturer, budget, false)
	if !constructGroupIDWithMemory {
		return n
	}
	m := formatMemory(memory)
	return n + "-" + m
}

// ConstructTopology 将 PCI 地址、根桥、设备类别和上游桥链组合成设备拓扑。复制桥链并保持从近到远的顺序，
// 可防止调用方复用切片后改变已记录的拓扑。遍历顺序稳定才能避免每轮上报都产生虚假差异。
func ConstructTopology(pciBusId, pciRootId, pciClass string, pciSwitches []string) Topology {
	return Topology{
		PciBusID:     pciBusId,
		PciRootID:    pciRootId,
		PciClass:     pciClass,
		PciSwitches:  slices.Clone(pciSwitches),
		NumaAffinity: binding.GetNumaNodeByBDF(pciBusId),
		CpuAffinity:  binding.MapNumaNodeStrToCPUAffinity(binding.GetNumaNodeByBDF(pciBusId)),
	}
}

// formatMemory 将以 MiB 计的显存转换成名称后缀。
func formatMemory(mib uint64) string {
	switch {
	case mib == 0:
		return "0g"
	case mib < 1024:
		return "1g"
	}
	r := strings.ToLower(quantityx.Format(resource.MustParse(strconvx.Itoa(mib) + "Mi")))
	return r[:len(r)-1] // 从单位 gi 中去掉 i，以便生成紧凑的标签。
}

// RuntimeMajor 提取规范化运行时版本的主版本，如 12.4 取 12。版本为空时采用备用值。
func RuntimeMajor(runtimeVersion, fallback string) string {
	if runtimeVersion == "" {
		return fallback
	}
	return strings.SplitN(runtimeVersion, ".", 2)[0]
}
