package binding

import (
	"bufio"
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"gpustack.ai/gpustack/pkg/utils/osx"
)

// 控制各查询采用的读取方式。
var (
	getPCIDevicesDefaultClassPrefixes []string

	// sysfsPCIDevicesPath 指向内核发布 PCI 属性的目录。测试可替换它，
	// 分别检查有效 NUMA 节点、-1 哨兵值和不可解析文本。
	sysfsPCIDevicesPath = "/sys/bus/pci/devices"
)

func init() {
	for _, p := range strings.Split(os.Getenv("GPUSTACK_PCI_CLASS_PREFIXES"), ",") {
		if p = strings.TrimSpace(p); p != "" {
			getPCIDevicesDefaultClassPrefixes = append(getPCIDevicesDefaultClassPrefixes, p)
		}
	}
	if len(getPCIDevicesDefaultClassPrefixes) == 0 {
		// 默认只查显示及加速器相关的 PCI 类别。类别编号见 https://admin.pci-ids.ucw.cz/read/PD。
		getPCIDevicesDefaultClassPrefixes = []string{"02", "03", "0b", "12"}
	}
}

func getCPUSize() int {
	entries, err := os.ReadDir("/sys/devices/system/cpu")
	if err != nil {
		return 0
	}

	count := 0
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, "cpu") {
			if _, err := strconv.Atoi(name[3:]); err == nil {
				count++
			}
		}
	}
	return count
}

func getNumaNodeSize() int {
	maxNode := 0

	s, err := readText("/sys/devices/system/node/possible")
	if err != nil {
		return 1
	}

	for _, part := range strings.Split(s, ",") {
		if strings.Contains(part, "-") {
			hi := strings.Split(part, "-")[1]
			maxNode = max(maxNode, safeInt(hi, 0))
		} else {
			maxNode = max(maxNode, safeInt(part, 0))
		}
	}

	return maxNode + 1
}

func getCPUNumaNodeMapping() []int {
	mapping := make([]int, cpuSize)
	for i := 0; i < cpuSize; i++ {
		mapping[i] = -1
	}

	for i := 0; i < numaNodeSize; i++ {
		path := "/sys/devices/system/node/node" + strconv.Itoa(i) + "/cpulist"

		s, err := readText(path)
		if err != nil {
			continue
		}

		for _, part := range strings.Split(s, ",") {
			if strings.Contains(part, "-") {
				lohi := strings.Split(part, "-")
				lo := safeInt(lohi[0], -1)
				hi := safeInt(lohi[1], -1)

				for c := lo; c <= hi; c++ {
					if c >= 0 && c < cpuSize {
						v := i
						mapping[c] = v
					}
				}
			} else {
				c := safeInt(part, -1)
				if c >= 0 && c < cpuSize {
					v := i
					mapping[c] = v
				}
			}
		}
	}

	return mapping
}

func getNumaNodeByBDF(bdf string) string {
	if bdf == "" {
		return ""
	}

	s, err := readText(filepath.Join(sysfsPCIDevicesPath, bdf, "numa_node"))
	if err != nil {
		return ""
	}

	// 节点 0 是有效结果。-1 表示内核未报告亲和关系，解析失败也按未知处理。保留这一区别，
	// 避免下游误判网卡与加速器同属节点 0。
	n := safeInt(s, -1)
	if n < 0 {
		return ""
	}

	return strconv.Itoa(n)
}

func getPCIDevices(vendors, classPrefixes []string) PCIDevices {
	devices := PCIDevices{}

	const sysfsPCIPath = "/sys/bus/pci/devices"

	if _, err := os.Stat(sysfsPCIPath); err != nil {
		return devices
	}

	if classPrefixes == nil {
		classPrefixes = getPCIDevicesDefaultClassPrefixes
	}

	entries, err := os.ReadDir(sysfsPCIPath)
	if err != nil {
		return devices
	}

	for _, entry := range entries {
		devAddress := strings.ToLower(entry.Name())

		devPath := filepath.Join(sysfsPCIPath, entry.Name())

		// 读取厂商编号。
		devVendor, err := readText(filepath.Join(devPath, "vendor"))
		if err != nil {
			continue
		} else {
			if len(devVendor) < 2 {
				continue
			}
			devVendor = devVendor[2:]
			if !contains(devVendor, vendors) {
				continue
			}
		}

		// 读取设备类别。
		devClass, err := readText(filepath.Join(devPath, "class"))
		if err != nil {
			continue
		} else {
			if len(devClass) < 2 {
				continue
			}
			devClass = devClass[2:]
			if !hasPrefix(devClass, classPrefixes) {
				continue
			}
		}

		// 读取设备编号。
		devDevice, err := readText(filepath.Join(devPath, "device"))
		if err != nil {
			continue
		} else {
			if len(devDevice) < 2 {
				continue
			}
			devDevice = devDevice[2:]
		}

		// 读取配置空间。
		devConfig, err := readBinary(filepath.Join(devPath, "config"))
		if err != nil {
			continue
		}

		// 解析设备目录的符号链接。
		resolvedPath, err := filepath.EvalSymlinks(devPath)
		if err != nil {
			continue
		}

		// 读取子系统厂商编号。
		devSubVendor, _ := readText(filepath.Join(devPath, "subsystem_vendor"))
		if len(devSubVendor) < 2 {
			devSubVendor = ""
		} else {
			devSubVendor = devSubVendor[2:]
		}

		// 读取子系统设备编号。
		devSubDevice, _ := readText(filepath.Join(devPath, "subsystem_device"))
		if len(devSubDevice) < 2 {
			devSubDevice = ""
		} else {
			devSubDevice = devSubDevice[2:]
		}

		// 提取交换路径和最外层 PCI 桥，确保加速器和网卡使用相同的拓扑坐标。
		root, switches := ResolvePCITopology(resolvedPath)

		devices[devAddress] = PCIDevice{
			Address:   devAddress,
			Class:     devClass,
			Vendor:    devVendor,
			Device:    devDevice,
			Path:      resolvedPath,
			Root:      root,
			Config:    devConfig,
			SubVendor: devSubVendor,
			SubDevice: devSubDevice,
			Switches:  switches,
		}
	}

	return devices
}

func getPCIDeviceNames(vendors []string) PCIDeviceNames {
	names := PCIDeviceNames{}

	var pciIdPath string
	for _, p := range []string{
		"/usr/share/hwdata/pci.ids",
		"/usr/share/pci.ids",
		"/usr/share/misc/pci.ids",
	} {
		if osx.Exists(p) {
			pciIdPath = p
			break
		}
	}
	if pciIdPath == "" {
		return names
	}

	mm, err := osx.OpenMmapFile(pciIdPath)
	if err != nil {
		return names
	}
	defer osx.Close(mm)

	bs := mm.Bytes()
	bsLen := mm.Len()

	var nameID PCIDeviceNameID
	for i := int64(0); i < bsLen; {
		j := i
		for j < bsLen && bs[j] != '\n' {
			j++
		}

		line := bs[i:j]
		i = j + 1

		if len(line) == 0 {
			continue
		}

		// 解析厂商行。
		if line[0] != '\t' {
			fields := bytes.Fields(line)
			if len(fields) > 1 {
				nameID.Vendor = strings.ToLower(string(fields[0]))
				if !contains(nameID.Vendor, vendors) {
					nameID.Vendor = ""
				}
			}
			continue
		}
		if nameID.Vendor == "" {
			continue
		}

		// 解析设备型号行。
		if len(line) > 1 && line[0] == '\t' && line[1] != '\t' {
			fields := bytes.Fields(bytes.TrimSpace(line))
			if len(fields) > 1 {
				nameID.Device = strings.ToLower(string(fields[0]))
				name := PCIDeviceName{
					Name:       string(bytes.Join(fields[1:], []byte(" "))),
					SubDevices: PCIDeviceNames{},
				}
				names[nameID] = name
			}
			continue
		}
		if nameID.Device == "" {
			continue
		}

		// 解析子设备行。
		fields := bytes.Fields(bytes.TrimSpace(line))
		if len(fields) > 2 {
			subVendor := strings.ToLower(string(fields[0]))
			subDevice := strings.ToLower(string(fields[1]))
			subNameID := PCIDeviceNameID{
				Vendor: subVendor,
				Device: subDevice,
			}
			subName := PCIDeviceName{
				Name: string(bytes.Join(fields[2:], []byte(" "))),
			}
			names[nameID].SubDevices[subNameID] = subName
		}
	}

	return names
}

func getLibFromEnv(libName string) string {
	libPaths := strings.Split(os.Getenv("LD_LIBRARY_PATH"), ":")
	for i := range libPaths {
		libPath := filepath.Join(libPaths[i], libName)
		if osx.Exists(libPath) {
			return libPath
		}
	}
	return ""
}

func getLibFromLdCache(libName string) string {
	cmd := exec.Command("ldconfig", "--print-cache")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return ""
	}
	if err = cmd.Start(); err != nil {
		return ""
	}
	defer func() {
		_ = cmd.Wait()
	}()

	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.Contains(line, libName) {
			continue
		}

		parts := strings.SplitN(line, "=>", 2)
		if len(parts) != 2 {
			continue
		}

		fields := strings.Fields(parts[0])
		for _, f := range fields {
			if f != libName {
				continue
			}
			_ = cmd.Process.Kill()
			return strings.TrimSpace(parts[1])
		}
	}
	return ""
}
