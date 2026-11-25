// 排空（drain）接缝：下线协议在获准提交删除操作之前，向副本引擎询问的唯一问题。
//
// 传输实现属于后续任务，因此本文件先定义问题并拒绝回答。在后端接入真实采集器之前，生产接线默认返回 Unsupported，这是安全方向：协议会停留在 Draining 状态而不删除任何对象。如果未接线时默认返回 Idle，就会把“无人观测”变成“队列为空”，而这正是本类型要从根本上杜绝的逻辑倒置。
//
// 它被定义为接口而非直接拨号，原因与服务视图和缓存抓取器相同：这里必须处理好的状态是各类失败场景，而真实采集器无法按需稳定制造出缺失指标序列、不可绑定采集结果或传输错误。
//
// 读取按单成员执行，且通过类型系统使其成为硬性约束而非仅靠口头约定。组级聚合值只能代表组本身，绝不能代表单个成员为零，因此这里的单次调用绝不能通过跨成员求和来回答“该副本已空闲”。一旦搞错，就会让繁忙成员搭空闲兄弟节点的便车被移出选择集。
package worker

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/types"
)

// modelDeploymentDrainState 表示单个成员的引擎对其当前仍持有工作负载的声明。
//
// 这四个状态对应四条指令。Idle 表示协议获准据此行动的声明；Busy 表示禁止行动的声明；Unknown 表示缺少有效答案并触发挂起；Unsupported 表示该引擎不存在此类度量指标，它触发挂起的原因不同——该形态处于未度量而非未观测状态，后续传输实现可改变此状态，而偶发抖动的采集器不会。
type modelDeploymentDrainState string

const (
	modelDeploymentDrainIdle        modelDeploymentDrainState = "Idle"
	modelDeploymentDrainBusy        modelDeploymentDrainState = "Busy"
	modelDeploymentDrainUnknown     modelDeploymentDrainState = "Unknown"
	modelDeploymentDrainUnsupported modelDeploymentDrainState = "Unsupported"
)

// modelDeploymentDrainTarget 指明本次读取所针对的唯一成员。容器名与引擎类型随目标直接携带，而不在调用点再行查找，避免读取器拿到某个成员后却悄然替其另一个容器作答。
type modelDeploymentDrainTarget struct {
	PodUID    types.UID
	Container string
	Role      string
	Engine    string
	// DeploymentUID 是代表其读取该成员所属的部署 UID。只有当实时对象仍属于指明它的那次操作时，该成员才是同一个成员；以相同名称重建过属主的 Pod 属于不同的属主。
	DeploymentUID types.UID
}

// modelDeploymentDrainAnswer 刻意采用三值语义并自带完整性标记，因为“我读到的数值为零”和“我读全了所有关键数值”是两个不同的事实，只有两者同时成立才是协议可据此行动的零。
type modelDeploymentDrainAnswer struct {
	State  modelDeploymentDrainState
	Reason string
	// Complete 表示是否每个预期序列都出现在与该成员匹配的格式良好的信封中。无论读到的数值是多少，不完整的读取一律视为 Unknown，因此只抓取了四个 SGLang gauge 中两个的读取器不能通过对已发现部分求和来上报零值。
	Complete bool
	// Series 保存本次读取观测到的序列值，用于填充状态原因，使排查“为何被挂起”的读者能看到具体数值而不仅仅是结论。
	Series map[string]float64
}

// modelDeploymentDrainReader 读取单个成员的引擎在途（in-flight）活动状态。
type modelDeploymentDrainReader interface {
	// Drain 报告该成员当前仍在途的工作量。按设计它是逐成员进行的：组级聚合值只能代表组，绝不能用来证明单个成员为零。
	Drain(ctx context.Context, target modelDeploymentDrainTarget) (modelDeploymentDrainAnswer, error)
}

// IMPORTANT: 是给 r.drainReader 加的“防空指针 + 安全拦截”默认替身（设计模式里叫 Null Object 模式）
//
// unwiredDrainReader 是在真实采集器接入前使用的默认生产读取器。它会点名引擎并拒绝作答，
// 使此期间的状态信息明确指出哪款引擎尚未接入度量，而不是误报队列为空。
type unwiredDrainReader struct{}

// Drain 返回 Unsupported，且永不返回 error：未接线属于已知状态而非故障，若返回 error 会使
// 所有调用方将其当作可重试故障不断重试。
func (unwiredDrainReader) Drain(
	_ context.Context, target modelDeploymentDrainTarget,
) (modelDeploymentDrainAnswer, error) {
	return modelDeploymentDrainAnswer{
		State:  modelDeploymentDrainUnsupported,
		Reason: fmt.Sprintf("no engine in-flight collector is wired for %s yet", target.Engine),
	}, nil
}

// modelDeploymentDrainReaderOf 解析本轮调谐使用的读取器。字段为 nil 时采用生产默认实现，
// 即上方的拒绝型读取器。
func (r *ModelDeploymentReconciler) modelDeploymentDrainReaderOf() modelDeploymentDrainReader {
	if r.drainReader == nil {
		return unwiredDrainReader{}
	}

	return r.drainReader
}
