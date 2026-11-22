package worker

import (
	"os"
	"testing"

	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"gpustack.ai/gpustack/pkg/kubeclients/kubernetes/scheme"
	"gpustack.ai/gpustack/pkg/system"
)

// TestMain 为控制器测试准备读取设置的客户端。这里没有配置 Secret，测试会使用默认设置。
func TestMain(m *testing.M) {
	system.LoopbackCtrlClient.Configure(ctrlfake.NewClientBuilder().WithScheme(scheme.Scheme).Build())
	os.Exit(m.Run())
}
