package deviceplugin

import (
	"context"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

// 在容器创建过程中会调用 Allocate 操作，以便设备插件能够执行特定于设备的操作，并指示 Kubelet 执行使设备在
// 容器中可用的步骤。
//
// 当前，每个已授予的令牌都被解析回 Devices.spec.interfaces[] 中的端点，没有任何内容被写入任何地方：没有账
// 本记录，没有 Pod 注释，也没有预留。因此，响应内容始终基于当前的记录状态生成，这使得两个共享的家族能够使
// 用相同的 HCAs，而不会互相减少其能力。
//
// 无法解析或未指向所服务家族中任何端点的令牌将被拒绝：一种悄声给出的、低于所请求数量的响应，便是该资源存
// 在的、用于终止的半许诺行为。
func (s *rdmaServer) Allocate(ctx context.Context, req *AllocateRequest) (*AllocateResponse, error) {
	// 第 14 章补上此函数的实现。
	return nil, grpcstatus.Error(grpccodes.Unimplemented, "RDMA allocation is pending")
}
