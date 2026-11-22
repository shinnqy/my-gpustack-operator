package deviceplugin

import (
	"fmt"
	"strings"
	"sync"

	core "k8s.io/api/core/v1"
	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/pkg/utils/osx"
)

// IMPORTANT: 自动决定当前节点走 CDI 通道 还是 环境变量通道

// 注入策略决定了哪个通道将授予的加速器传递给容器。共有两种策略，其中哪种有效取决于节点本身，而非此操作符。
//
//   - 制造商的 *_VISIBLE_DEVICES 变量由该制造商的容器运行时读取，因此只有当该运行时位于 Pod 路径上时，该
//     变量才会被容器访问。在通用的 OCI 处理程序中，该变量处于无效状态：容器启动时没有加速器，也没有任何错
//     误记录。
//   - 此响应中的 CDI 请求将由容器引擎读取，该引擎会根据节点的规范进行解析，并注入设备节点及驱动库。它基于
//     通用处理程序运行，如果引擎无法解析名称，则会导致以该名称创建的容器在消息中失败，而不会扩大授予范围。
type InjectionStrategy string

const (
	// InjectionEnvvar 是默认值：这是制造商的`visible-devices`变量，在第二个通道存在之前，这个操作符就
	// 使用这个变量。
	InjectionEnvvar InjectionStrategy = "envvar"
	// InjectionCDI 将授予的加速器作为 CDI 设备使用。
	InjectionCDI InjectionStrategy = "cdi-annotations"
	// InjectionAuto 会读取节点并选择其中一个，不会进行猜测。可选设置：一个节点可以同时承载通用处理程
	// 序、厂商处理程序和 CDI 配置的处理程序，因此并非每个 Pod 都适用全局节点解决方案。
	InjectionAuto InjectionStrategy = "auto"
)

// InjectionConfig 是制造商的分配器所提供的用于获取整个分辨率设置的内容。每个字段都是该制造商自己的术语。
// 不需要其他任何内容。
type InjectionConfig struct {
	// 制造商指定了该策略的环境变量以及 CDI 请求的注释后缀。
	Manufacturer string
	// CDIKind 是厂商及制造商自制的类，该厂商的生成器会基于此类发布加速器。在分配时刻被实体化的子设备
	// 具有其独特的特性，没有预生成的规范能够描述它，因此处理一个子设备的推理服务永远不会查询这个解析
	// 器。
	CDIKind string
	// VisibleDevicesEnv 是制造商容器运行时用于读取设备列表的变量。
	VisibleDevicesEnv string
}

// InjectionStrategyEnv 指定单个厂商的设备注入策略。通过 Helm chart 的 deviceManager.env 将它传给该厂商的
// Device Manager DaemonSet。
func InjectionStrategyEnv(manufacturer string) string {
	return "GPUSTACK_" + strings.ToUpper(manufacturer) + "_DEVICE_INJECTION_STRATEGY"
}

// ParseInjectionStrategy 会读取已配置的策略。若识别不到的值，将引发错误，而非静默回退到默认值：这是部署错
// 误，且不同渠道中“grant”的拼写也有所不同。
func ParseInjectionStrategy(value string) (InjectionStrategy, error) {
	switch s := InjectionStrategy(strings.TrimSpace(value)); s {
	case "":
		return InjectionEnvvar, nil
	case InjectionEnvvar, InjectionCDI, InjectionAuto:
		return s, nil
	default:
		return "", fmt.Errorf("unrecognized %q: want one of %q, %q, %q",
			value, InjectionEnvvar, InjectionCDI, InjectionAuto)
	}
}

// InjectionResolver 负责根据容器确定哪个通道会传递授权信息。
//
// 节点事实会被读取并缓存一次，因为每次在分配过程中读取它们会导致分配路径中出现文件 I/O 操作。推理引擎的配
// 置属于节点的静态状态。当节点硬件发生变化时，加载的 CDI 规范会重新生成，因此在该 DaemonSet 启动后出现的
// 规范设置会保留环境变量通道，直到节点重启。这种处理方式是安全的——它选择不切换而非错误地切换——而重启
// DaemonSet 正是检测器自身启动过程中已读取所需的处理方式。
type InjectionResolver struct {
	cfg      InjectionConfig
	strategy InjectionStrategy

	once   sync.Once
	specs  CDISpecs
	engine ContainerEngineFacts

	// 已报告解决的记录，由所确定的策略进行标记。在自动模式下，答案以容器为单位——它读取 Pod 自身的
	// runtimeClassName 以及获得的加速器——因此仅报告第一个记录即可描述一个 Pod，而隐藏所有采用其他通道
	// 的后续记录。该决策的全部内容就是哪个通道获得了授权。采用标记而非无限制：答案涉及两个值。
	logged sync.Map
}

// NewInjectionResolver 会读取制造商配置的策略。如果配置值错误，会在启动阶段出现问题，而非首次分配时。一个
// 解析器会服务于制造商的每一个响应者，因此该节点会被读取一次，每个解析后的通道也会被记录一次。
func NewInjectionResolver(cfg InjectionConfig) (*InjectionResolver, error) {
	env := InjectionStrategyEnv(cfg.Manufacturer)
	strategy, err := ParseInjectionStrategy(osx.Getenv(env))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", env, err)
	}

	return &InjectionResolver{cfg: cfg, strategy: strategy}, nil
}

// 当配置的策略无法被读取时，分配器会回退使用 DefaultInjectionResolver：即默认通道，此时没有任何节点被读取。
func DefaultInjectionResolver(cfg InjectionConfig) *InjectionResolver {
	return &InjectionResolver{cfg: cfg, strategy: InjectionEnvvar}
}

// Apply 通过已解析的通道将授予的加速器写入响应中。
//
// 只能有一个通道，绝不要两个：一个容器有两个实时注入路径，这就是为何一个容器会持有无人授予的硬件。
func (r *InjectionResolver) Apply(
	logger klog.Logger,
	resp *ContainerAllocateResponse,
	pod *core.Pod,
	ids []string,
) error {
	strategy, evidence := r.resolve(pod, ids)

	// 每次回答中而非每次分配中：一个始终以相同方式解析的节点在其生命周期内会记录一行日志。而一个自动
	// 将不同 Pods 发送到不同通道的节点会分别记录这些日志。
	if _, seen := r.logged.LoadOrStore(strategy, struct{}{}); !seen {
		logger.Info("resolved the device-injection strategy", "strategy", strategy, "evidence", evidence)
	}

	if strategy == InjectionEnvvar {
		if resp.Envs == nil {
			resp.Envs = make(map[string]string, 1)
		}
		resp.Envs[r.cfg.VisibleDevicesEnv] = strings.Join(ids, ",")

		return nil
	}

	names := CDIDeviceNames(r.cfg.CDIKind, ids)

	// 明确配置的 CDI 策略仍然有效，因为使用非默认命名策略生成的规范中不包含以加速器 ID 命名的设备。如
	// 果出现失败，则会指出是加速器的问题，而容器引擎只会将 CDI 字符串作为名称。
	//
	// 找到的名称结束了它：这是一个积极的事实，任何无法读取的文件都无法找到它。
	r.readNode()
	switch missing := r.specs.Missing(names); {
	case len(missing) == 0:
	// 根本无法看到任何内容，或者缺失的部分可能存在于无法读取的文件中：空或部分视图仅代表证据缺失，而
	// 非证据不存在的迹象。拒绝处理该问题会破坏一个工作节点。推理引擎作为后备方案——无法解析的名称会导
	// 致容器创建失败，相关消息中也会提及这一点，因此请求被移交，漏洞也被记录下来。
	case len(r.specs.Names) == 0 || r.specs.Unreadable:
		logger.Info("the requested devices could not be validated against a CDI specification before "+
			"the container engine sees them",
			"directories", r.specs.Dirs, "devices", missing, "someSpecificationUnreadable", r.specs.Unreadable)
	default:
		return fmt.Errorf("no CDI specification under %v names %s; the accelerator cannot be "+
			"requested as a CDI device", r.specs.Dirs, strings.Join(missing, ", "))
	}

	SetCDIRequest(resp, "gpustack-"+r.cfg.Manufacturer, names)

	return nil
}

// resolve 返回该容器加速器所注入的通道，以及做出该选择的依据。它永远不会返回错误：不可读的节点是继续当前
// 行为的理由，而不是导致分配失败的理由。
func (r *InjectionResolver) resolve(pod *core.Pod, ids []string) (InjectionStrategy, string) {
	switch r.strategy {
	case "", InjectionEnvvar:
		return InjectionEnvvar, "configured"
	case InjectionCDI:
		return r.strategy, "configured"
	}

	r.readNode()

	// 名为 Pod 的组件请求了运行时处理器的注入，而此解析器无法读取处理器的配置来知道其将执行什么操作。
	// 这由处理器自行决定。
	if pod != nil && pod.Spec.RuntimeClassName != nil && *pod.Spec.RuntimeClassName != "" {
		return InjectionEnvvar, fmt.Sprintf("the pod names runtimeClassName %q, whose handler owns injection",
			*pod.Spec.RuntimeClassName)
	}
	if !r.engine.Known {
		return InjectionEnvvar, fmt.Sprintf("the container engine configuration could not be read at %q, "+
			"which %s names", r.engine.Path, ContainerdConfigDirEnv)
	}
	// 名为 no handler 的 Pod 没有处理器，因此会在推理引擎的默认配置下运行。如果默认配置是厂商的运行时
	// 环境，则 visible-devices 变量会指向该环境，此时 CDI 请求就会成为一个容器内的第二种注入路径。
	if r.engine.DefaultHandlerIsVendor {
		return InjectionEnvvar, fmt.Sprintf("the engine's default runtime handler %q is a vendor runtime",
			r.engine.DefaultHandler)
	}
	if !r.engine.ResolvesCDI {
		return InjectionEnvvar, "the container engine does not resolve CDI requests"
	}
	// 每一个获准的加速器，而不仅仅是其中一个：如果请求中提及的设备规格与容器中的规格不符，则整个请求
	// 会失败，因此部分匹配也毫无帮助。
	//
	// 在下面的不可读检查之前进行询问，而非之后。找到的名称是自动所需的积极事实，且无法读取的文件也无
	// 法找到它——尽管这些目录与其他制造商的生成器共享，因此描述该操作员从未听说过的硬件的畸形规格也足
	// 以使工作节点恢复正常运行。
	missing := r.specs.Missing(CDIDeviceNames(r.cfg.CDIKind, ids))
	if len(missing) != 0 {
		// 无法解析的规范并非什么都不命名的规范：它是那个缺失名称可能存在的位置，因此其缺失并未被
		// 确定，而非被确立。而自动功能仅会将节点从今天的积极行为中移除。
		if r.specs.Unreadable {
			return InjectionEnvvar, fmt.Sprintf("a CDI specification under %v could not be read, so whether "+
				"the node names %s cannot be established", r.specs.Dirs, strings.Join(missing, ", "))
		}

		return InjectionEnvvar, fmt.Sprintf("no loaded CDI specification names %s", strings.Join(missing, ", "))
	}

	return InjectionCDI, fmt.Sprintf("the pod names no runtimeClassName, the engine's default handler %q "+
		"resolves CDI per %q, and the specifications under %v name every granted accelerator",
		r.engine.DefaultHandler, r.engine.Path, r.specs.Dirs)
}

// readNode 会自动决定读取所有内容，且仅在解析器生命周期内执行一次。
func (r *InjectionResolver) readNode() {
	r.once.Do(func() {
		r.specs = LoadCDISpecs()
		r.engine = ReadContainerEngineFacts()
	})
}
