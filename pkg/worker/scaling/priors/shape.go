package priors

import (
	"fmt"
	"strings"

	"gpustack.ai/gpustack/pkg/nodefeature"
)

// Shape 是唯一标识一种部署形态的六元组。
//
// 它是 profile 的识别依据，因为按名称标识 profile 的 catalog 可能让
// 两个 profile 声明同一名称，而且按名称选择 profile 的部署必须在了解
// catalog 的其他任何信息之前就预先知道该名称。
//
// 这六个要素分别是角色、模型、量化方式、引擎、硬件和拓扑，因为
// 在一种引擎、一种加速卡和一种形态下对单个模型和单个配置所做的
// 测量即构成一条独立的测量。
type Shape struct {
	// Role 是该 shape 所承担的服务角色。
	Role RoleKind `json:"role"`

	// Model 是规范化的模型仓库标识。
	Model string `json:"model"`

	// Quantization 是规范化的量化标识。
	Quantization string `json:"quantization"`

	// Engine 是推理引擎系列。
	Engine string `json:"engine"`

	// Hardware 是规范化的加速卡系列。
	Hardware string `json:"hardware"`

	// Topology 是单个扩缩容副本的标准化拓扑形态。
	Topology Topology `json:"topology"`
}

// Shape 返回 profile 对应的形态。
//
// 它会对每个要素进行规范化，因为用不同拼写记录的模型 profile
// 与部署实际上指向同一种形态。
func (p Profile) Shape() Shape {
	return Shape{
		Role:         p.Role,
		Model:        CanonicalModelName(p.Identity.Model.Name),
		Quantization: CanonicalQuantization(p.Identity.Model.Quantization),
		Engine:       CanonicalEngineFamily(p.Identity.Engine.Image),
		Hardware:     CanonicalAcceleratorFamily(p.Identity.Hardware.Accelerator),
		Topology:     p.Topology.Normalized(),
	}
}

// DocumentShape 返回所编写 profile 的 shape。
//
// 该函数会对每个元素进行规范化，因为 shape 是识别 profile 的依据，
// 读取方无论从文件还是从运行时获取，都必须得到相同的 shape。
func DocumentShape(d ProfileDocument) Shape {
	return Shape{
		Role:         RoleKind(d.Role),
		Model:        CanonicalModelName(d.Model),
		Quantization: CanonicalQuantization(d.Quantization),
		Engine:       CanonicalEngineFamily(d.Engine),
		Hardware:     CanonicalAcceleratorFamily(d.Hardware),
		Topology:     NormalizeTopologyDocument(d.Topology),
	}
}

// NormalizeTopologyDocument 返回所编写拓扑的规范形式。
//
// 该函数会将未声明的宽度替换为 1，因为源中未声明的宽度即为 1，
// 而宽度为 0 不对应任何部署。
func NormalizeTopologyDocument(d TopologyDocument) Topology {
	topology := Topology(d)
	return topology.Normalized()
}

// Shape 返回请求所指定的 shape。
//
// 该方法读取的是请求而非 profile，因为请求代表部署所声明的内容，
// 而 profile 则是目录中保存的内容。
func (r Request) Shape() Shape {
	return Shape{
		Role:         r.Role,
		Model:        CanonicalModelName(r.Identity.Model.Name),
		Quantization: CanonicalQuantization(r.Identity.Model.Quantization),
		Engine:       CanonicalEngineFamily(r.Identity.Engine.Image),
		Hardware:     CanonicalAcceleratorFamily(r.Identity.Hardware.Accelerator),
		Topology:     r.Topology.Normalized(),
	}
}

// String 以稳定格式呈现该 shape。
//
// 该方法会列出全部六个元素，因为当解析失败并被拒绝时，
// 在拒绝信息中给出无法解析的 shape 可以提示读者需要修改哪些内容。
func (s Shape) String() string {
	return fmt.Sprintf(
		"%s/%s/%s/%s/%s/gpus=%d,tp=%d,ep=%d,pp=%d,dp=%d,ldp=%d,cp=%d",
		s.Role, s.Model, s.Quantization, s.Engine, s.Hardware,
		s.Topology.GPUsPerReplica, s.Topology.TensorParallel, s.Topology.ExpertParallel,
		s.Topology.PipelineParallel, s.Topology.DataParallel, s.Topology.LocalDataParallel,
		s.Topology.ContextParallel,
	)
}

// shapeMatches 判断 profile 的 shape 是否与请求指定的 shape 匹配。
//
// 该函数按名称比较模型，因为部署指定的模型名称不含组织名，
// 而目录中记录的模型名称包含组织名。
//
// 该函数允许请求未声明某些元素，因为未声明量化方式等参数的部署
// 既没有产生冲突，也没有做出选择。当多个 profile 同时匹配时，选择逻辑会予以拒绝，
// 因此未声明的元素绝不会擅自选中某个 profile。
func shapeMatches(want, got Shape) bool {
	if want.Role != got.Role {
		return false
	}
	if !ModelNameMatches(want.Model, got.Model) {
		return false
	}
	if want.Quantization != "" && want.Quantization != got.Quantization {
		return false
	}
	if want.Engine != "" && want.Engine != got.Engine {
		return false
	}
	if want.Hardware != "" && want.Hardware != got.Hardware {
		return false
	}
	return want.Topology == got.Topology
}

// CanonicalEngineFamily 返回引擎名称所属的引擎系列。
//
// 对于当前 operator 未知的引擎，该函数会原样保留其名称，
// 因为即使 operator 无法调度该引擎，它仍然是一个独立的引擎，
// 两个不同的未知引擎对应两个不同的 shape。
func CanonicalEngineFamily(image string) string {
	if family := EngineFamily(image); family != "" {
		return family
	}
	return strings.TrimSpace(image)
}

// CanonicalAcceleratorFamily 返回加速器名称所属的加速器系列。
//
// 对于当前 operator 未知的加速器，该函数会原样保留其名称，
// 因为即使 operator 无法识别该名称，它仍然是一种独立的加速器。
func CanonicalAcceleratorFamily(name string) string {
	if family := nodefeature.ResolveAcceleratorFamily(name); family != "" {
		return family
	}
	return strings.TrimSpace(name)
}
