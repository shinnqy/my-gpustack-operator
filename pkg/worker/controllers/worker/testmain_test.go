package worker

import (
	"os"
	"testing"

	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"gpustack.ai/gpustack/pkg/kubeclients/kubernetes/scheme"
	"gpustack.ai/gpustack/pkg/kubediscovery"
	"gpustack.ai/gpustack/pkg/system"
)

// TestMain 为控制器测试准备读取设置的客户端。这里没有配置 Secret，测试会使用默认设置。
func TestMain(m *testing.M) {
	system.LoopbackCtrlClient.Configure(ctrlfake.NewClientBuilder().WithScheme(scheme.Scheme).Build())
	// 调谐级别的渲染断言默认基于原生 sidecar 形态，这里配置满足最低要求的集群版本以启用该形态；
	// 传统形态的校验则在渲染器级别单独覆盖。
	system.LoopbackKubeVersion.Configure(kubediscovery.Version{
		Major: "1", Minor: "31", GitVersion: "v1.31.0",
	})
	os.Exit(m.Run())
}
