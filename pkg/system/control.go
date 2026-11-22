package system

import (
	"k8s.io/apimachinery/pkg/util/sets"

	"gpustack.ai/gpustack/pkg/utils/varx"
)

var (
	// BootstrapPassword 是用于引导初始化系统的密码。
	BootstrapPassword = varx.NewOnce("")

	// DisableAuths 是用于禁用认证的标志。
	DisableAuths = varx.NewOnce(false)

	// DisableApplications 是不允许安装的应用集合。
	DisableApplications = varx.NewOnce(sets.New[string]())

	// ModelManagerServiceAccount 是系统 Namespace 中 model-manager 插件运行所使用的
	// ServiceAccount：它是唯一允许写入 NodeModelStore 状态的身份。
	ModelManagerServiceAccount = varx.NewOnce("gpustack-operator-model-manager")
)

// ConfigureControl 配置系统控制功能。
func ConfigureControl(
	bootstrapPassword string,
	disableAuths bool,
	disableApps []string,
) {
	BootstrapPassword.Configure(bootstrapPassword)
	DisableAuths.Configure(disableAuths)
	DisableApplications.Configure(sets.New[string](disableApps...))
}
