package ascend

import (
	"errors"
	"fmt"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

// errShareUnsupported 表示容器共享调用失败，原因可能是该驱动程序没有相应的 dcmi 入口点，或者 libdcmi 根本
// 没有加载。与因设备或操作时机问题导致的失败不同，这种失败无法通过重试、写入或 `npu-smi` 命令来解决，因此
// 预检查会直接拒绝该操作，且无法对设备进行任何处理，也无法提供有效的修复方案。
//
// 这是一个包哨兵，而非 dcmi 返回码，因为下方的核心代码是在 Darwin 系统上编译的，在该系统中无法链接绑定
// /dcmi 相关组件。带有构建标签的驱动程序会对代码进行分类并标记错误，而核心代码则读取这个标记。
var errShareUnsupported = errors.New("this driver has no dcmi container-share API")

// errShareNotDeclared 表示容器共享调用失败，因为该驱动程序所处理的 API 代次中没有此标志——这与拥有该标志却
// 无法处理它的驱动程序形成对比，也与之相反。
//
// DCMI API 的 V2 代次没有任何容器共享入口点。没有用于读取的标记，也没有用于写入的标记，因此预检查阶段无需
// 进行任何操作，分配过程也能正常进行。如果拒绝的话，就会基于其 API 未定义的标记，拒绝该代次下的所有共同租
// 户分配。
//
// 在返回代码中无法看到这种区别，因此无法将其归为某一类。提供 V2 服务的驱动程序仍然会导出 V1 容器共享入口
// 点，并回应 NOT_SUPPORT —— 该驱动程序描述的是一个设备，因此 errShareUnsupported 并不适用——而预检查会将其
// 视为值得重试、写入或再次拒绝的失败情况，最终会要求操作员执行一个无法添加该代次所特有的标志的命令。
var errShareNotDeclared = errors.New("this driver's dcmi API generation declares no container-share flag")

// shareModeError 会显示容器共享路径上驱动调用失败的情况，当 apiUnavailable 表明无法进行该调用时，会标记为
// errShareUnsupported。
//
// 该判断结果以 bool 类型呈现，因为只有带有 build 标签的驱动程序能够读取 dcmi 返回代码，而此文件必须能够被
// Darwin 测试二进制文件导入。以这种方式拆分文件，使得标记本身也受到测试：接缝部分保留了“单行代码到 bool
// 类型的分类”，而悬挂在其后的决策则在此得到验证。
func shareModeError(what string, cause error, apiUnavailable bool) error {
	if apiUnavailable {
		return fmt.Errorf("%s: %w: %w", what, cause, errShareUnsupported)
	}
	return fmt.Errorf("%s: %w", what, cause)
}

// shareNotDeclaredError 会执行容器共享调用，当调用方的 API 代次没有该标志时，该调用会失败，并标记为
// errShareNotDeclared。
//
// 驾驶员的原始代码被保留在消息中，与标记一起。是代次导致了旗帜的缺失，但代码正是驾驶员实际所说的内容，而
// 删除该代码的日志行则无法用于进行分类判断。
//
// shareModeError 上的是第二个构造函数，而非第二个布尔值，因此这两种判断结果无法合并：调用操作要么与存在的
// 标志相关，要么与 API 从未定义的标志相关。
func shareNotDeclaredError(what string, cause error) error {
	return fmt.Errorf("%s: %w: %w", what, cause, errShareNotDeclared)
}

// shareDriver 是 _linux.go/_other.go 构建标签背后的 dcmi 执行器接缝：真正的实现负责在 Linux 上处理绑定
// /dcmi 操作。由于存在 Darwin stub 错误，因此无 dcmi 的注入核心通过虚拟驱动程序进行表测试。设备通过（卡片、
// 设备-卡片）这对 dcmi 名称来标识，该名称已由加速器上的检测器记录。
type shareDriver interface {
	// GetShareEnabled 报告该设备的容器共享模式是否处于开启状态。
	GetShareEnabled(cardID, deviceID int32) (bool, error)
	// SetShareEnabled 用于开启或关闭设备的容器共享模式。它会写入会持续存在于进程之外的主机驱动状态，
	// 因此调用方只有在读取到该标志与预期不符时才会进行写入操作。
	SetShareEnabled(cardID, deviceID int32, enabled bool) error
}

// ensureShareEnabled 会启用驱动程序标志，该标志允许一个设备被挂载到多个容器中，对于加速器来说，即意味着即
// 将进行逻辑切片操作。该标志先被读取，仅在关闭时才会被写入，因此已经包含切片的加速器只需进行一次查询即可。
//
// 如果没有设置标志，加速器上的第二个容器仍会启动，随后其工作负载会在设备上的容器中失败，并出现一个不属于
// 加速器或标志的驱动程序内部错误。拒绝分配会迫使进行诊断，因此无法设置标志的加速器会被拒绝而非允许使用。
//
// 该读取操作被归类为需要处理的事件，而非单独视为致命错误，分为三种结果而非两种。如果读取报告显示 dcmi 入
// 口点缺失，则不会进行分配，且不会触及设备——包括那些标志已开启的设备，因为无法使用此代码查询的驱动程序也
// 无法管理此类设备。如果读取报告显示驱动程序的 API 代次完全不存在，则允许分配，因为没有需要设置的标志，也
// 没有可以设置该标志的命令。其他任何读取失败情况均不说明 API 是否存在，因此仍会尝试写入操作：写入操作会使
// 标志被知晓，因此使用此代码无法信任的读取结果并非拒绝分配的理由，因为写入操作本已完成。只有如果写入操作
// 也失败，才需要给出拒绝理由。
func (s *server) ensureShareEnabled(accel *workercore.Accelerator) error {
	if s.share == nil {
		return fmt.Errorf("container-share actuator not configured")
	}
	// Ascend 检测器将 dcmi 自身的地址信息记录在 PhysicalIndexes 中，格式为{物理 ID，卡片 ID，卡片中的
	// 设备 ID}。
	if len(accel.PhysicalIndexes) < 3 {
		return fmt.Errorf("accelerator %q carries no dcmi card/device index", accel.ID)
	}
	cardID, deviceID := int32(accel.PhysicalIndexes[1]), int32(accel.PhysicalIndexes[2])

	enabled, readErr := s.share.GetShareEnabled(cardID, deviceID)
	switch {
	case errors.Is(readErr, errShareNotDeclared):
		// 在以下拒绝操作之前，以及任何写入操作之前，请检查：此代次没有标志，因此没有可读取的内容、
		// 可设置的内容，也没有可使用的 `npu-smi` 命令——该命令无法添加驱动程序未携带的入口点，其输
		// 出的数字也是本绑定自身的 V2 寻址方式，而非操作器可输入的数值。
		//
		// 这些记录是按加速器进行的，而非默默通过，因为这是共同租户分配得以实施的地方，而无需驾驶
		// 员确认可能性的情况下。关于该代次是否真的需要这种确认，目前尚未在硬件层面得到验证，因此
		// 这些记录就是所有依赖该假设的分配的记录。
		s.Logger.Info("allowing the allocation without a container-share flag",
			"accelerator", accel.ID, "card", cardID, "device", deviceID, "reason", readErr.Error())
		return nil
	case errors.Is(readErr, errShareUnsupported):
		return fmt.Errorf("card %d device %d cannot be shared: %w", cardID, deviceID, readErr)
	case readErr == nil && enabled:
		return nil
	}

	err := s.share.SetShareEnabled(cardID, deviceID, true)
	if errors.Is(err, errShareNotDeclared) {
		// 当读取成功而写入失败时可访问：dcmi 独立解析每个符号，且代次判断是每次调用时确定，而非缓
		// 存。
		s.Logger.Info("allowing the allocation without a container-share flag",
			"accelerator", accel.ID, "card", cardID, "device", deviceID, "reason", err.Error())
		return nil
	}
	if err == nil {
		if readErr != nil {
			// 现在旗帜已升起，因此分配操作有效——但读取此代码时不能信任它，因为在下一次分配时
			// 它将再次失败，每次都会重新写入主机状态，因此必须得到确认。
			s.Logger.Info("enabled container-share mode without a trustworthy read",
				"card", cardID, "device", deviceID, "readError", readErr.Error())
		}
		return nil
	}
	if readErr != nil {
		err = fmt.Errorf("%w (the flag read failed too: %w)", err, readErr)
	}
	// 写入端可以报告读取端未发现的缺失情况：dcmi 会独立解析每个符号。在这种情况下提供命令会引导操作员
	// 进行修复，而该修复不会添加任何缺失的符号，因此这两种结果在这里也被区分开来。
	if errors.Is(err, errShareUnsupported) {
		return fmt.Errorf("card %d device %d cannot be shared: %w", cardID, deviceID, err)
	}
	return fmt.Errorf(
		"enable container-share mode of card %d device %d: %w; "+
			"enable it on the host with `npu-smi set -t device-share -i %d -c %d -d 1`",
		cardID, deviceID, err, cardID, deviceID,
	)
}
