package ascend

import (
	"sync"

	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/binding"
	"gpustack.ai/gpustack/binding/dcmi"
)

// newShareDriver 返回了真正的基于 dcmi 的容器共享驱动程序。该驱动程序仅适用于 Linux：设备管理器也仅在
// Linux 上运行，若将 cgo 绑定/ dcmi 链接到 Darwin 测试二进制文件中（该二进制文件会链接 Go 的插件包），在
// 加载未解析的 DCMI 符号时会导致 dyld 崩溃，因此 Darwin 构建版本使用 share_driver_other.go 中的 stub 实现。
//
// 在此处初始化无需与检测器进行协调，检测器会按照自己的时间表初始化整个进程范围内的库。包装器不会卸载其已
// 持有的库，因此任何一方都无需清空另一方的函数指针，也无需先运行。
func newShareDriver(logger klog.Logger) shareDriver {
	l := dcmi.New(binding.WithLogger(logger))
	return &dcmiShareDriver{lib: l, logger: logger, initRet: l.Init(logger)}
}

// dcmiShareDriver 就是真正的 shareDriver，它根据（卡片或卡片内设备）这对 dcmi 名称来标识设备。其对真实驱
// 动程序的行为可通过 e2e 运行验证：一个 darwin 测试二进制文件根本无法进行绑定/dcmi 操作。
type dcmiShareDriver struct {
	lib    *dcmi.DCMI
	logger klog.Logger

	// mu 保护 initRet，该值在构造过程中会被重新尝试，而非固定不变。一个驱动实例由切片、共享和可见性服
	// 务器共享，每个服务器都拥有自己的 gRPC 监听器，因此两次 Allocate 调用可以同时到达。
	mu sync.Mutex
	// initRet 是库的最后初始化结果，当调用无法继续时，会以该结果作为根本原因进行报告，而不是让单纯的
	// 缺失符号错误来替代它。
	initRet dcmi.Return
}

func (d *dcmiShareDriver) device(cardID, deviceID int32) (dcmi.Device, error) {
	if err := d.ready(); err != nil {
		return dcmi.Device{}, err
	}
	return d.lib.GetDeviceHandleByCardAndIndex(cardID, deviceID), nil
}

// ready 报告库是否已初始化，在未初始化时每次调用都会重新尝试一次。在构建过程中短暂处于未就绪状态的驱动程
// 序，否则会使得节点上的所有分配操作失败，直到设备管理器重启为止。这种重试操作既便宜又安全，因为包装器永
// 远不会卸载其已持有的库——它只重新执行厂商初始化过程，而这是可能出问题的部分。
func (d *dcmiShareDriver) ready() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.initRet.IsSuccess() {
		return nil
	}
	if d.initRet = d.lib.Init(d.logger); !d.initRet.IsSuccess() {
		// 如果 libdcmi 从未加载，也不会提供容器共享 API，因此预处理阶段必须像处理不存在的入口点一
		// 样进行读取，而不是试图绕过它进行写入。
		return shareModeError("dcmi init failed", d.initRet, d.initRet.IsAPIUnavailable())
	}
	return nil
}

// shareFlagUndeclared 用于报告此失败是否意味着 API 代次完全没有容器共享标志。该函数接受代次作为参数，而非
// 直接读取它，因此整个判断结果完全取决于其两个输入参数，并且可以通过表格进行测试。其调用者则负责确定这些
// 参数的来源。
//
// 这一裁决是故意采用**并列结构**的，两半部分都具有承载功能：
//
//   - 该代次必须是 V2，因为正是该代次的头文件没有声明容器共享入口点。没有返回码能单独说明这一点：V2 驱动
//     程序仍然会导出 V1 的入口点，并返回 NOT_SUPPORT 响应，而这是 V1 代次也用来表示“该设备不执行此操作”的
//     代码，因此单凭代码无法区分两者。
//   - 该代码还必须能够表明此调用未被处理——例如 NOT_SUPPORT，这是 V2 驱动程序针对 V1 入口点所响应的内容。
//     或者 FUNCTION_NOT_FOUND，即根本不存在的入口点。如果没有这一机制，V2 主机上的所有失败情况都会显示为“
//     无此类标志”：那些 DM 系统没有权限查询的设备（OPER_NOT_PERMITTED），或是超时的设备，都会被当作 API
//     被调用了，而判决旁记录的原因也会与之矛盾。这些是普通的失败情况，现有的分类机制可以处理它们——通过写
//     入操作重新尝试，如果仍失败则给出解决方案。
//
// LIBRARY_NOT_FOUND 被特意排除，即使在 V2 版本中也是如此：一个从未加载的库对任何代次都毫无信息提供，而那
// 个路径必须被拒绝。
//
// 调用方在每次调用时读取该代次，而不是在构建过程中保留它，这与绑定规则一致：此驱动程序每次调用都会重新执
// 行初始化，因此即使初始化失败，该代次仍可被识别。
func shareFlagUndeclared(version dcmi.APIVersion, ret dcmi.Return) bool {
	if version != dcmi.APIVersionV2 {
		return false
	}
	return ret == dcmi.ERROR_NOT_SUPPORT || ret == dcmi.ERROR_FUNCTION_NOT_FOUND
}

func (d *dcmiShareDriver) GetShareEnabled(cardID, deviceID int32) (bool, error) {
	dev, err := d.device(cardID, deviceID)
	if err != nil {
		return false, err
	}
	enabled, ret := dev.GetShareEnabled()
	if !ret.IsSuccess() {
		if shareFlagUndeclared(d.lib.APIVersion(), ret) {
			return false, shareNotDeclaredError("dcmi get device share enable", ret)
		}
		return false, shareModeError("dcmi get device share enable", ret, ret.IsAPIUnavailable())
	}
	return enabled, nil
}

func (d *dcmiShareDriver) SetShareEnabled(cardID, deviceID int32, enabled bool) error {
	dev, err := d.device(cardID, deviceID)
	if err != nil {
		return err
	}
	if ret := dev.SetShareEnabled(enabled); !ret.IsSuccess() {
		if shareFlagUndeclared(d.lib.APIVersion(), ret) {
			return shareNotDeclaredError("dcmi set device share enable", ret)
		}
		return shareModeError("dcmi set device share enable", ret, ret.IsAPIUnavailable())
	}
	return nil
}
