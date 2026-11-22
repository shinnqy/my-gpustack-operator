// 本文件保存所有引擎通用的注入信息，以及仅 vLLM 系列使用的产物：其配置文件。
// “vLLM 系列”指两个引擎，它们使用两个连接器名称，以及分别限定于不同键集合的两个读取器，
// 因此下文按读取器而非按系列说明各项取值。
//
// 下文配置针对自身不存储任何数据的 Mooncake 客户端渲染。这是一种角色设定而非容量设置：
// 拥有本地缓冲区且无全局段的客户端可执行 Get 和 Put，同时不贡献内存，这正是推理引擎所需的。
// 本文件的组织方式旨在防止意外声明其他形态，因此声明该角色的键会一并写入，绝不单独写入。
package inject

import (
	"gpustack.ai/gpustack/pkg/systemname"
)

// ClientConfigAnnotationKey 用于在使用配置文件的引擎对应的 Pod 上承载渲染后的客户端配置。
// 该文件是此 annotation 的 downwardAPI 投影，因此调用方无需创建 ConfigMap，无需相关 RBAC 权限，
// 也不会留下需要垃圾回收的资源：配置的生命周期与 Pod 完全一致。
//
// 它声明于此，而非放在调用方读取的 annotation 旁边，因为该键是渲染的输出结果。
// 拒绝用户传入该值的调用方会从此处读取该常量。
const ClientConfigAnnotationKey = "kvcache." + systemname.LabelPrefix + "client-config"

const (
	// ConfigVolumeName 和 ConfigMountPath 指定渲染文件在容器内的挂载卷名与路径。该挂载为只读：
	// 容器内没有任何理由写入该文件，而可写投影会让进程篡改已注入配置的记录。
	ConfigVolumeName = "gpustack-kvcache-config"
	ConfigMountPath  = "/etc/gpustack/kvcache"

	// ConfigFileName 是挂载目录内的文件名，ConfigFilePath 是两者的拼接结果——
	// 即引擎自身的路径变量所设置的值。
	ConfigFileName = "mooncake.json"
	ConfigFilePath = ConfigMountPath + "/" + ConfigFileName
)
