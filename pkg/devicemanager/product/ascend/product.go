// ascend 包通过驱动辨识 A5 节点的产品形态，包括超节点、服务器和推理卡。探测与分配需要使用同一规则，
// 避免对同一硬件作出不同判断。
package ascend

import (
	"fmt"
	"sync"
)

// Family 是 A5 芯片共用的型号族标识。Ascend950PR、Ascend950DT 等后缀持续扩展，探测器统一归入 950。
const Family = "950"

// Type 用可读名称表示 A5 形态，便于探测器判断超节点归属，并供分配器选择对应拓扑。
type Type string

const (
	// TypeServer8P 表示八芯片服务器形态。是否实际隶属超节点还要结合设备坐标判断。
	TypeServer8P Type = "server-8p"
	// TypePod1D 表示一维布线的超节点。
	TypePod1D Type = "pod-1d"
	// TypePod2D 表示二维布线的超节点。
	TypePod2D Type = "pod-2d"
	// TypeServer16P 表示独立的十六芯片服务器。
	TypeServer16P Type = "server-16p"
	// TypeServer32P 表示独立的三十二芯片服务器。
	TypeServer32P Type = "server-32p"
	// TypeCard1P 表示通过主板编号识别的单芯片推理卡。
	TypeCard1P Type = "card-1p"
	// TypeCard4P 表示通过主板编号识别的四芯片推理卡。
	TypeCard4P Type = "card-4p"
)

// InSuperPod 结合产品形态和实测坐标判断超节点归属。无效域 ID 一律排除。已知超节点形态可直接确认。
// 8P 服务器或推理卡还须有有效大小。16P、32P 服务器及未知形态不发布域。
func (t Type) InSuperPod(superPodID, superPodSize uint32) bool {
	if superPodID == InvalidSuperPodID {
		return false
	}

	switch t {
	case TypePod1D, TypePod2D:
		return true
	case TypeServer8P, TypeCard1P, TypeCard4P:
		return superPodSize != InvalidSuperPodSize
	default:
		// 16P、32P 独立服务器，以及本版本尚未识别的产品形态。
		return false
	}
}

// 厂商对超节点产品形态使用的编号。
const (
	codeServer8P  uint32 = 0
	codePod1D     uint32 = 1
	codePod2D     uint32 = 2
	codeServer16P uint32 = 3
	codeServer32P uint32 = 4
	codeCard1P    uint32 = 5
	codeCard4P    uint32 = 6
)

// InvalidSuperPodID 和 InvalidSuperPodSize 是厂商对无效坐标的标记。查询成功仍需检查这两个值，
// 避免把普通服务器判为成员。
const (
	InvalidSuperPodID   uint32 = 0xffffffff
	InvalidSuperPodSize uint32 = 0xffffffff
)

// InvalidSuperPodCoordinate 使用同一个全 1 值筛掉无效的服务器及机箱坐标。零是可用坐标，应当保留。
const InvalidSuperPodCoordinate = InvalidSuperPodID

// A5 载板编号区分 300I 推理卡的单芯片和四芯片形态。训练底板使用相同编号体系里的其他 ID，
// 需要继续查询其产品类型。
const (
	mainboardIDCard1P uint32 = 0x68
	mainboardIDCard4P uint32 = 0x6c
)

// typeCodes 把已知厂商编号映射为产品名称。未知编号保留原始数值，Type 留空。
var typeCodes = map[uint32]Type{
	codeServer8P:  TypeServer8P,
	codePod1D:     TypePod1D,
	codePod2D:     TypePod2D,
	codeServer16P: TypeServer16P,
	codeServer32P: TypeServer32P,
	codeCard1P:    TypeCard1P,
	codeCard4P:    TypeCard4P,
}

// Product 同时保留可读形态和驱动原始编号，使未知产品也能诊断，未来增加映射时仍能解释旧数据。
type Product struct {
	// Type 为产品形态，无法识别厂商编号时为空。
	Type Type
	// Code 记录厂商返回的产品类型编号。
	Code uint32
}

// Driver 提供识别节点产品形态所需的两种读取操作。调用方传入该加速器在 DCMI 中的卡号和卡内索引。
type Driver interface {
	// MainboardID 读取设备所在主板的编号。
	MainboardID(cardID, deviceID int32) (uint32, error)
	// SuperPodType 查询设备所在超节点的产品类型。
	SuperPodType(cardID, deviceID int32) (uint32, error)
}

// Resolver 首次成功时缓存 A5 节点的产品形态。主板和超节点类型是节点级事实，探测不同加速器时可共用。
// 失败时留待下一次读取。
type Resolver struct {
	driver Driver

	// mu 保护缓存和已完成标志。一个 Resolver 可被多个设备操作共用，须串行首次读取。
	mu       sync.Mutex
	resolved bool
	product  Product
}

func NewResolver(driver Driver) *Resolver {
	return &Resolver{driver: driver}
}

// Resolve 返回已确认的产品形态。成功后复用，驱动读取失败时继续允许重试。
func (r *Resolver) Resolve(cardID, deviceID int32) (Product, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.resolved {
		return r.product, nil
	}

	product, err := r.read(cardID, deviceID)
	if err != nil {
		return Product{}, err
	}
	r.product, r.resolved = product, true

	return r.product, nil
}

// 先读取主板编号识别推理卡。其他主板再查询超节点类型，由设备对自身形态给出答案。
func (r *Resolver) read(cardID, deviceID int32) (Product, error) {
	mainboardID, err := r.driver.MainboardID(cardID, deviceID)
	if err != nil {
		return Product{}, fmt.Errorf("read mainboard id of card %d device %d: %w", cardID, deviceID, err)
	}
	switch mainboardID {
	case mainboardIDCard1P:
		return Product{Type: TypeCard1P, Code: codeCard1P}, nil
	case mainboardIDCard4P:
		return Product{Type: TypeCard4P, Code: codeCard4P}, nil
	}

	code, err := r.driver.SuperPodType(cardID, deviceID)
	if err != nil {
		return Product{}, fmt.Errorf("read super pod type of card %d device %d: %w", cardID, deviceID, err)
	}

	return Product{Type: typeCodes[code], Code: code}, nil
}
