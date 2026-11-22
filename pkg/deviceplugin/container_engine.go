package deviceplugin

import (
	"path/filepath"
	"sort"

	"github.com/BurntSushi/toml"

	"gpustack.ai/gpustack/pkg/utils/osx"
)

// IMPORTANT: 读节点上的 containerd 配置，判断引擎是否开启了 CDI 以及默认 RuntimeHandler 是什么

// ContainerdConfigDirEnv 指的是存储容器推理引擎配置的目录，Device Manager 会读取该目录以确定该引擎是否能
// 解析 CDI 请求，以及名为 Pod 的容器在何种运行时处理程序下运行，且是否包含 RuntimeClass。
//
// 这是一种设置，而非搜索：各发行版将文件放在他们喜欢的任何位置——k3s 和 RKE2 将其保存在 /var/lib/rancher
// 下，而管理员则可以将它放在任何地方——如果有一个设备管理器去查找，就必须提供它可能找到的路径，这其实就是
// 那种不够诚实的设置。错误的或未被挂载的目录会被识别为未知推理引擎，而未知推理引擎则会在其已运行的通道上
// 保留一个节点。
const ContainerdConfigDirEnv = "GPUSTACK_CONTAINERD_CONFIG_DIR"

// defaultContainerdConfigDir 和 containerdConfigFile 分别是 containerd 的默认配置目录和文件名。手动启动
// Device Manager 且未显式设置时使用这两个默认值。Helm chart 会传入该设置。
const (
	defaultContainerdConfigDir = "/etc/containerd"
	containerdConfigFile       = "config.toml"
)

// ContainerEngineFacts 描述了容器引擎配置中关于容器如何访问其加速器的信息，以及是否可读取的标记。不可读取
// 的事实并非错误事实。
type ContainerEngineFacts struct {
	// 有报告说明配置是否已被读取和解析，以及该文件来自的路径。
	Known bool
	Path  string

	// ResolvesCDI 报告了推理引擎是否能解析 CDI 请求。
	ResolvesCDI bool

	// DefaultHandler 是 Pod 的运行时处理程序，没有 runtimeClassName 可以运行在它之下。而
	// DefaultHandlerIsVendor 则决定该处理程序是厂商提供的运行时，还是通用的 OCI 运行时——后者决定了
	// `visible-devices`变量能否独立产生任何效果。
	DefaultHandler         string
	DefaultHandlerIsVendor bool
}

// ReadContainerEngineFacts 用于读取容器推理引擎的配置。每次读取都是尽力获取：无法读取的配置将被报告为未知。
func ReadContainerEngineFacts() ContainerEngineFacts {
	path := filepath.Join(osx.Getenv(ContainerdConfigDirEnv, defaultContainerdConfigDir), containerdConfigFile)

	var cfg map[string]any
	if _, err := toml.DecodeFile(path, &cfg); err != nil {
		// 即使没有读取到任何内容，该路径仍然被保留：这是当节点没有按预期接收通道时，操作员需要看
		// 到的唯一信息，而记录下来的解决措施也无其他可命名的依据。
		return ContainerEngineFacts{Path: path}
	}

	f := ContainerEngineFacts{Known: true, Path: path}
	f.DefaultHandler, _ = findNestedString(cfg, "default_runtime_name")
	f.DefaultHandlerIsVendor = isVendorRuntimeHandler(f.DefaultHandler)

	if enabled, ok := findNestedBool(cfg, "enable_cdi"); ok {
		// 无论配置版本将它放在何处，显式的切换方式都能发挥作用。
		f.ResolvesCDI = enabled
	} else if version, ok := cfg["version"].(int64); ok && version >= 3 {
		// containerd 2.x 版本支持配置版本 3，并移除了开关设置：CDI 始终处于开启状态。2 及以下版本
		// 默认会将其关闭。
		f.ResolvesCDI = true
	}

	return f
}

// isVendorRuntimeHandler 用于指示运行时处理器的名称是否不是通用的 OCI 运行时。其他情况则被假定为厂商级运
// 行时，该运行时会注入自己的加速器，因为错误判断会破坏当前行为，而不会为某个容器增加第二个注入路径。
func isVendorRuntimeHandler(name string) bool {
	switch name {
	case "", "runc", "crun":
		return false
	default:
		return true
	}
}

// findNestedString 和 findNestedBool 可以从已解码的 TOML 文档中读取任意一个键，无论该键位于何处。容器推理
// 引擎在不同配置版本之间移动了其插件部分，因此通过完整的路径来引用一个键会将其绑定到其中一个版本中。
//
// Subtree 的遍历采用排序顺序进行，而非映射顺序。在两种地方都包含键的配置文件属于病态情况，但 Go 对映射迭
// 代进行了随机化处理，因此无序遍历会在两次重新读取同一文件时产生不同的结果——而且一个不随节点变化而改变的
// 属性，正是解析器绝不能有的特性。
func findNestedString(node map[string]any, key string) (string, bool) {
	if v, ok := node[key].(string); ok {
		return v, true
	}
	for _, name := range sortedKeys(node) {
		if child, ok := node[name].(map[string]any); ok {
			if found, ok := findNestedString(child, key); ok {
				return found, true
			}
		}
	}

	return "", false
}

func findNestedBool(node map[string]any, key string) (bool, bool) {
	if v, ok := node[key].(bool); ok {
		return v, true
	}
	for _, name := range sortedKeys(node) {
		if child, ok := node[name].(map[string]any); ok {
			if found, ok := findNestedBool(child, key); ok {
				return found, true
			}
		}
	}

	return false, false
}

func sortedKeys(node map[string]any) []string {
	names := make([]string, 0, len(node))
	for name := range node {
		names = append(names, name)
	}
	sort.Strings(names)

	return names
}
