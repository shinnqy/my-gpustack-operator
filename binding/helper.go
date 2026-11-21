package binding

import (
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/exp/constraints"
	"k8s.io/apimachinery/pkg/util/sets"
	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/pkg/utils/osx"
)

var (
	bitsSize           int
	cpuSize            int
	cpuSetSize         int
	numaNodeSize       int
	numaNodeSetSize    int
	cpuNumaNodeMapping []int
	numaNodeCpuMapping map[int][]int
)

func init() {
	bitsSize = getBitsSize()
	cpuSize = getCPUSize()
	cpuSetSize = (cpuSize + bitsSize) / bitsSize
	numaNodeSize = getNumaNodeSize()
	numaNodeSetSize = (numaNodeSize + bitsSize) / bitsSize
	cpuNumaNodeMapping = getCPUNumaNodeMapping()
	numaNodeCpuMapping = getNumaNodeCPUMapping()
}

// GetNumaNodeByBDF 按完整 PCI 地址查询 NUMA 节点。地址无效、sysfs 返回 -1 或读不到亲和关系时返回空串。
// 空串表示未知，节点 0 则是有效测量。
func GetNumaNodeByBDF(bdf string) string {
	return getNumaNodeByBDF(bdf)
}

// MapNumaNodeStrToCPUAffinity 把 NUMA 节点范围（如 0-1,3）转换为 CPU 核心范围，无有效输入时返回空字符串。
func MapNumaNodeStrToCPUAffinity(node string) string {
	if node == "" {
		return ""
	}

	nodes := StrRangeToList(node)

	cpuSet := map[int]struct{}{}
	for _, n := range nodes {
		for _, c := range numaNodeCpuMapping[n] {
			cpuSet[c] = struct{}{}
		}
	}
	if len(cpuSet) == 0 {
		return ""
	}

	var list []int
	for c := range cpuSet {
		list = append(list, c)
	}

	return ListToStrRange(list)
}

// GoStringN 将有界 C 字符数组转换为 Go 字符串，遇到首个 NUL 或数组末尾即停止。
// 即使驱动塞满整个数组也不会越界。类型参数同时兼容有符号和无符号 char。
func GoStringN[C ~int8 | ~uint8](chars []C) string {
	buf := make([]byte, 0, len(chars))
	for i := range chars {
		if chars[i] == 0 {
			break
		}
		buf = append(buf, byte(chars[i]))
	}
	return string(buf)
}

// ListToStrRange 把连续整数压缩成区间，例如 [0,1,2,4,5,7] 转为 0-2,4-5,7。
func ListToStrRange[I constraints.Integer](indices []I) string {
	sets.NewInt()
	if len(indices) == 0 {
		return ""
	}

	slices.Sort(indices)

	start := indices[0]
	end := start

	var parts []string
	for _, v := range indices[1:] {
		if v == end+1 {
			end = v
		} else {
			parts = append(parts, formatRange(start, end))
			start, end = v, v
		}
	}
	parts = append(parts, formatRange(start, end))

	return strings.Join(parts, ",")
}

// StrRangeToList 展开逗号分隔的整数或区间，例如 0-2,4-6,7 展开为 [0,1,2,4,5,6,7]。
func StrRangeToList(s string) []int {
	if s == "" {
		return nil
	}

	set := map[int]struct{}{}

	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)

		if strings.Contains(part, "-") {
			p := strings.Split(part, "-")
			lo := safeInt(p[0], -1)
			hi := safeInt(p[1], -1)
			if lo >= 0 && hi >= lo {
				for i := lo; i <= hi; i++ {
					set[i] = struct{}{}
				}
			}
		} else {
			i := safeInt(part, -1)
			if i >= 0 {
				set[i] = struct{}{}
			}
		}
	}

	out := make([]int, 0, len(set))
	for v := range set {
		out = append(out, v)
	}
	sort.Ints(out)
	return out
}

type (
	// PCIDevice 保存 PCI 设备身份和计算拓扑、亲和性所需的属性。
	PCIDevice struct {
		// Address 是完整 PCI 地址，格式为 domain:bus:device.function，例如 0000:00:1f.2。
		Address PCIDeviceAddress
		// Class 是十六进制 PCI 类别，如 020000 表示网络控制器。
		Class string
		// Vendor 是 PCI 厂商编号，例如 Intel 使用 8086。
		Vendor string
		// Device 是十六进制 PCI 设备编号，例如 1234。
		Device string
		// Path 指向该 PCI 设备的 sysfs 目录，例如 /sys/bus/pci/devices/0000:00:1f.2。
		Path string
		// Root 记录最外层 PCI 桥的地址。直接连到根复合体的设备记录自身地址。
		// 两个设备的 Root 相同表示它们位于同一最外层桥下。此值不存放 pci0000:00 一类的根复合体名称。
		Root string
		// Config 保存从 sysfs 的 config 文件读出的原始 PCI 配置空间。
		Config []byte
		// SubVendor 是 PCI 子系统厂商编号。
		SubVendor string
		// SubDevice 是 PCI 子系统设备编号。
		SubDevice string
		// Switches 保存从设备向上经过的 PCI 桥地址，用于比较两台设备共享的交换路径。
		Switches []string
	}
	// PCIDeviceAddress 是 domain:bus:device.function 形式的 PCI 地址，例如 0000:00:1f.2。
	PCIDeviceAddress = string
	// PCIDevices 按 PCI 地址索引设备信息。
	PCIDevices map[PCIDeviceAddress]PCIDevice
)

// GetPCIDevices 返回满足厂商和类别条件的 PCI 设备。
func GetPCIDevices(vendors, classPrefixes []string) PCIDevices {
	return getPCIDevices(vendors, classPrefixes)
}

// ResolvePCITopology 从解析过符号链接的 sysfs 设备路径提取最外层桥及中间桥链。桥链从近到远排列。无上游桥时，
// Root 取设备本身的地址。函数只接收路径，让加速器和网卡用同一套坐标比较拓扑。
func ResolvePCITopology(resolvedDevPath string) (root string, switches []string) {
	current := resolvedDevPath
	for {
		// PCI 桥目录名中的 BDF 含两个冒号，根复合体目录名如 pci0000:00 只有一个。沿路径向上遇到后者就停止。
		parent := filepath.Dir(current)
		name := filepath.Base(parent)
		if strings.Count(name, ":") != 2 {
			break
		}
		switches = append(switches, name)
		current = parent
	}

	return filepath.Base(current), switches
}

type (
	// PCIDeviceNameID 用厂商和设备编号识别 PCI 产品。
	PCIDeviceNameID struct {
		// Vendor 是十六进制 PCI 厂商编号，例如 8086。
		Vendor string
		// Device 是十六进制 PCI 设备编号，例如 1234。
		Device string
	}
	// PCIDeviceName 保存 PCI 设备及其子设备的可读名称。
	PCIDeviceName struct {
		// Name 是设备的可读名称，例如 Intel(R) Ethernet Controller X550。
		Name string
		// SubDevices 把子系统厂商和设备编号映射到对应名称。
		SubDevices PCIDeviceNames
	}
	// PCIDeviceNames 将 PCI 标识映射为可读名称。
	PCIDeviceNames map[PCIDeviceNameID]PCIDeviceName
)

// GetName 根据厂商和设备编号返回可读的 PCI 产品名。
func (n PCIDeviceNames) GetName(vendor, device, subvendor, subdevice string) string {
	nameID := PCIDeviceNameID{
		Vendor: vendor,
		Device: device,
	}
	if name, ok := n[nameID]; ok {
		if len(name.SubDevices) != 0 && subvendor != "" && subdevice != "" {
			subnameID := PCIDeviceNameID{
				Vendor: subvendor,
				Device: subdevice,
			}
			if subname, ok := name.SubDevices[subnameID]; ok {
				return subname.Name
			}
		}
		return name.Name
	}
	return ""
}

// GetPCIDeviceNames 按指定厂商加载 PCI 名称映射。
func GetPCIDeviceNames(vendors []string) PCIDeviceNames {
	return getPCIDeviceNames(vendors)
}

// GetLibFromPaths 在候选路径中选择存在的共享库。
func GetLibFromPaths(paths []string) string {
	if len(paths) == 0 {
		return ""
	}

	klog.V(4).InfoS("searching for library in paths", "paths", paths)

	for i := range paths {
		if filepath.IsAbs(paths[i]) {
			if osx.Exists(paths[i]) {
				klog.V(4).InfoS("found library in absolute path", "path", paths[i])
				return paths[i]
			}
			continue
		}
		if path := getLibFromEnv(paths[i]); path != "" {
			klog.V(4).InfoS("found library in environment variable", "name", paths[i], "path", path)
			return path
		}
		if path := getLibFromLdCache(paths[i]); path != "" {
			klog.V(4).InfoS("found library in ld cache", "name", paths[i], "path", path)
			return path
		}
	}

	klog.V(4).InfoS("used first library", "name", paths[0])
	return paths[0]
}

func formatRange[T constraints.Integer](lo, hi T) string {
	if lo == hi {
		return strconv.FormatUint(uint64(lo), 10)
	}
	return strconv.FormatUint(uint64(lo), 10) + "-" + strconv.FormatUint(uint64(hi), 10)
}

func getBitsSize() int {
	if ^uint(0)>>32 > 0 {
		return 64
	}
	return 32
}

func getNumaNodeCPUMapping() map[int][]int {
	res := map[int][]int{}
	for cpu, node := range cpuNumaNodeMapping {
		if node >= 0 {
			res[node] = append(res[node], cpu)
		}
	}
	return res
}

func safeInt(s string, def int) int {
	if v, err := strconv.Atoi(strings.TrimSpace(s)); err == nil {
		return v
	}
	return def
}

func readText(path string) (string, error) { // nolint:unused
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}

func readBinary(path string) ([]byte, error) { // nolint:unused
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// hasPrefix 检查字符串是否具有任一指定前缀，前缀集合为空时接受所有字符串。
func hasPrefix(s string, prefixes []string) bool { // nolint:unused
	if len(prefixes) == 0 {
		return true
	}
	idx := slices.IndexFunc(prefixes, func(p string) bool {
		return strings.HasPrefix(s, p)
	})
	return idx >= 0
}

// contains 检查字符串是否属于给定集合，集合为空时接受所有字符串。
func contains(s string, strs []string) bool { // nolint:unused
	if len(strs) == 0 {
		return true
	}

	return slices.Contains(strs, s)
}
