package deviceplugin

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

// 分配响应要读取宿主机的 sysfs 与设备目录。使用可替换的根路径，便于单测用临时目录验证相同的解析和存在性检查。
var (
	// rdmaSysfsRoot 是内核公布 RDMA 设备关系的 sysfs 根目录。
	rdmaSysfsRoot = "/sys"

	// rdmaDevNodesDir 是 kubelet 分配响应引用的设备节点目录，
	// 生产环境使用 /dev/infiniband。
	rdmaDevNodesDir = devInfinibandDir
)

// rdmaCmName 是节点共用的 RDMA 连接管理设备。宿主机提供它时，每个容器响应只附带一次。
const rdmaCmName = "rdma_cm"

// ncclIbHCAEnv 将授予容器的 RDMA 设备名传给 NCCL。名称按排序后的 token 顺序用逗号连接。
const ncclIbHCAEnv = "NCCL_IB_HCA"

// Allocate 在 kubelet 创建容器时返回设备分配结果。
// 每次调用都从当前 Devices.spec.interfaces 找到 token 对应的端点。
// 整端点和共享模式各自发布 token。分配器不写 Pod 注解或进程内预留，也不扣减另一种模式的 token 数量。
// token 格式错误或端点不属于当前模式时，整个请求返回错误。成功时只返回设备注入信息。
func (s *rdmaServer) Allocate(ctx context.Context, req *AllocateRequest) (*AllocateResponse, error) {
	ctrRequests := req.GetContainerRequests()
	if len(ctrRequests) == 0 {
		// gRPC 请求可能不含容器。先检查再取第一项，避免一次异常请求通过数组越界导致整个设备管理器退出。
		return nil, grpcstatus.Error(grpccodes.InvalidArgument,
			"the allocate request carries no container requests")
	}
	ctrReq := ctrRequests[0]

	deviceIDs := ctrReq.GetDevicesIds()
	sort.Strings(deviceIDs)

	ctrResp, err := s.allocateContainer(ctx, deviceIDs)
	if err != nil {
		return nil, err
	}

	s.Logger.Info("allocate response", "response", ctrResp)
	return &AllocateResponse{ContainerResponses: []*ContainerAllocateResponse{ctrResp}}, nil
}

// allocateContainer 根据 kubelet 给一个容器选中的 token，
// 查找当前可分配的端点、对应的 verbs 设备和节点共用的连接管理设备，并填写容器的 NCCL_IB_HCA。
func (s *rdmaServer) allocateContainer(
	ctx context.Context,
	deviceIDs []string,
) (*ContainerAllocateResponse, error) {
	devs, err := s.Reconciler.getDevices(ctx)
	if err != nil {
		return nil, fmt.Errorf("read the interface inventory: %w", err)
	}

	endpoints, err := s.grantedEndpoints(devs, deviceIDs)
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(endpoints))
	for i := range endpoints {
		names = append(names, endpoints[i].RDMADevice)
	}

	ctrResp := &ContainerAllocateResponse{
		Envs: map[string]string{ncclIbHCAEnv: strings.Join(names, ",")},
	}

	// 逐个解析获授端点的 uverbs 路径，只把这些设备写进响应。整目录挂载会把同节点的其他适配器也交给容器。
	// 任一已选端点无法解析或对应设备路径不存在时，应报告具体端点并拒绝本次分配。
	for i := range endpoints {
		verbs, err := verbsDevicePath(rdmaSysfsRoot, endpoints[i].RDMADevice)
		if err != nil {
			return nil, fmt.Errorf("resolve the verbs character device of endpoint %s: %w",
				endpoints[i].Resource, err)
		}
		node := rdmaDeviceNodePath(verbs)
		spec := NewRWDevice(node)
		if spec == nil {
			return nil, fmt.Errorf("endpoint %s has no verbs device node %q",
				endpoints[i].Resource, node)
		}
		ctrResp.Devices = append(ctrResp.Devices, spec)
	}

	// 多个端点可以共用同一个连接管理设备。放在循环之后，宿主机上存在 rdma_cm 时只添加一次。
	if cm := NewRWDevice(filepath.Join(rdmaDevNodesDir, rdmaCmName)); cm != nil {
		ctrResp.Devices = append(ctrResp.Devices, cm)
	}

	return ctrResp, nil
}

// grantedEndpoints 按排好序的 token 依次解析端点。一个共享端点可以有多个 token，
// 生成注入清单时只保留首次出现的端点。空清单直接返回错误。
func (s *rdmaServer) grantedEndpoints(
	devs *workercore.Devices,
	deviceIDs []string,
) ([]rdmaEndpoint, error) {
	endpoints := make([]rdmaEndpoint, 0, len(deviceIDs))
	seen := make(map[Resource]struct{}, len(deviceIDs))
	for _, id := range deviceIDs {
		token, err := ParseResourceToken(id)
		if err != nil {
			s.Logger.Error(err, "convert device id", "device id", id)
			return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "invalid device id %q: %v", id, err)
		}
		if _, dup := seen[token.Resource]; dup {
			continue
		}
		endpoint, ok := s.grantedEndpoint(devs, token.Resource)
		if !ok {
			return nil, grpcstatus.Errorf(grpccodes.InvalidArgument,
				"device id %q names no RDMA endpoint of the %s family", id, s.AllocationMode)
		}
		seen[token.Resource] = struct{}{}
		endpoints = append(endpoints, endpoint)
	}
	if len(endpoints) == 0 {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "the request grants no RDMA device")
	}
	return endpoints, nil
}

// grantedEndpoint 在当前接口清单中，按服务的分配模式和端点标识查找设备。
// 上轮分配的结果不会覆盖本轮观察到的绑定关系。
func (s *rdmaServer) grantedEndpoint(
	devs *workercore.Devices,
	res Resource,
) (rdmaEndpoint, bool) {
	for i := range devs.Spec.Interfaces {
		iface := &devs.Spec.Interfaces[i]
		for _, endpoint := range rdmaEndpoints(iface, s.AllocationMode) {
			if endpoint.Resource == res {
				return endpoint, true
			}
		}
	}
	return rdmaEndpoint{}, false
}

// rdmaDeviceNodePath 取解析出的 uverbs 条目名并拼到设备根目录，
// 得到本次响应应使用的宿主机设备路径。
func rdmaDeviceNodePath(verbsPath string) string {
	return filepath.Join(rdmaDevNodesDir, filepath.Base(verbsPath))
}
