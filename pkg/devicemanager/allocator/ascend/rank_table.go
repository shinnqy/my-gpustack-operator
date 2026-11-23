package ascend

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/utils/json"
)

// rankTableCapability 标记本机 HCCL rank table 的格式检查。
const rankTableCapability = "hccl-ranktable"

// hcclRootInfoPath 是 HCCL 查找 rank table 的路径。宿主机存在这个文件时，厂商运行时会把它挂进容器。
const hcclRootInfoPath = "/etc/hccl_rootinfo.json"

// rankTableVersion950 是 A5 所需的 rank table 格式版本，取决于芯片代际。
const rankTableVersion950 = "2.0"

// checkRankTable 读取宿主机上可能注入容器的 rank table。A5 接受 2.0。确认文件不存在也可继续。
// 已有旧版表或读不到宿主机时，报告具体原因。
func checkRankTable(path string) device.PreflightCheck {
	c := device.PreflightCheck{
		Capability: rankTableCapability,
		State:      device.PreflightStateUnavailable,
		Depth:      device.PreflightDepthDeclared,
	}

	// 路径为空表示没有可用的宿主机根目录，此时无法判断宿主机有没有 rank table。
	if path == "" {
		c.Reason = fmt.Sprintf("this pass has no host root, so the host's %s was never looked for "+
			"and whether it would break HCCL here is unknown. Mount the host's root filesystem, or "+
			"read this row from a pass that has one", hcclRootInfoPath)
		return c
	}

	// 报告符号链接的目标，交给宿主机核实。绝对链接可能解析到容器根目录，相对链接也可能越过挂载点。
	if target, err := os.Readlink(path); err == nil {
		c.Reason = fmt.Sprintf("%s is a symbolic link to %s, which this command cannot follow the way "+
			"the host does -- an absolute target resolves inside this container instead -- so whether "+
			"the host carries a ranktable is unknown while the vendor runtime follows that link and "+
			"mounts whatever it finds. Read the link's target on the host",
			hcclRootInfoPath, target)
		return c
	}

	content, err := os.ReadFile(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		if dir := filepath.Dir(path); !isDir(dir) {
			c.Reason = fmt.Sprintf("%s was never looked for: %s is not a readable directory, so the "+
				"host's own %s is not visible to this command and a missing file here says nothing "+
				"about the host. Mount the host root before reading this row",
				hcclRootInfoPath, dir, filepath.Dir(hcclRootInfoPath))
			return c
		}
		c.State = device.PreflightStateOK
		c.Detail = fmt.Sprintf("%s is absent, so the vendor runtime mounts no ranktable and HCCL "+
			"negotiates its communicator through rootInfo", hcclRootInfoPath)
		return c
	case err != nil:
		c.Reason = fmt.Sprintf("%s could not be read, so whether it would break HCCL on this node is "+
			"unknown -- the vendor runtime mounts it whenever it exists, whatever this could make of "+
			"it: %v", hcclRootInfoPath, err)
		return c
	}

	var table struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal(content, &table); err != nil {
		c.Reason = fmt.Sprintf("%s is not readable as JSON, so the ranktable generation it carries is "+
			"unknown while the vendor runtime mounts it regardless: %v", hcclRootInfoPath, err)
		return c
	}

	if table.Version == "" {
		c.Reason = fmt.Sprintf("%s carries no version entry, so the ranktable generation it carries "+
			"is unknown while the vendor runtime mounts it regardless", hcclRootInfoPath)
		return c
	}

	if table.Version != rankTableVersion950 {
		c.Reason = fmt.Sprintf("%s is a version %s ranktable and A5 accepts only %s, so the vendor "+
			"runtime mounting it into every container makes multi-card HCCL fail to initialise with "+
			"Config_Error_Ranktable (EI0014); single-card allocations are unaffected. Remove the file "+
			"or replace it with a %s table -- no environment variable disables the check",
			hcclRootInfoPath, table.Version, rankTableVersion950, rankTableVersion950)
		return c
	}

	c.State = device.PreflightStateOK
	c.Detail = fmt.Sprintf("%s is a version %s ranktable, the generation A5 accepts",
		hcclRootInfoPath, rankTableVersion950)
	return c
}

// isDir 仅在进程能确认路径是目录时返回真，用于区分确实缺少文件和父目录本身不可见。
func isDir(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}
