package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/utils/json"
)

// [IMPORTANT: elastic EP] 本文件负责把操作写入 ConfigMap，做持久化

// 弹性扩缩容操作由持久化记录及作用于其上的纯决策函数构成。之所以需要记录，
// 是因为无法通过询问引擎当前状况来恢复扩缩容：请求超时、网关返回 408 或 500，
// 以及请求根本未发出，在任何观测者看来都是无法区分的三种状态；若对其作不同处理，
// 就可能对已经应用过变更的引擎重放扩缩容请求。
//
// 因此，命令意图会在请求发出之前写入，且记录命令已发送绝不能作为再次发送的依据。
// 重启后的重建逻辑会读取该记录与最新事实，推导出与重启前完全一致的下一步动作。

// elasticOperationState 表示一次宽度变更目前所处的阶段，下列状态按可能的推进顺序排列。
//
// 调用引擎前先写记录，并把即将发送的命令记入状态。此后每次推进都需要新的观测，
// 即使发送请求的控制器已经重启。
type elasticOperationState string

const (
	elasticStateRecorded      elasticOperationState = "Recorded"
	elasticStateCommandSent   elasticOperationState = "CommandSent"
	elasticStateAwaitingWidth elasticOperationState = "AwaitingWidth"
	elasticStateNativeDone    elasticOperationState = "NativeComplete"
	elasticStateWithdrawn     elasticOperationState = "Withdrawn"
	elasticStateReleased      elasticOperationState = "Released"
	elasticStateCompleted     elasticOperationState = "Completed"
	elasticStateAbandoned     elasticOperationState = "Abandoned"
)

// elasticWidth 保存这次变更的旧宽度和目标宽度。两个值都必须为正数，退到零需要走其他生命周期操作。
type elasticWidth struct {
	Old    int `json:"old"`
	Target int `json:"target"`
}

// Upward 判断本次变更是否增加成员。
func (w elasticWidth) Upward() bool { return w.Target > w.Old }

// elasticCapturedIdentity 保存操作开始时一个成员的名称和 UID。
//
// 同名重建的 Pod 会有新的 UID。退场时同时核对名称和 UID，才能避免删除刚接替旧成员的 Pod。
type elasticCapturedIdentity struct {
	Name string    `json:"name"`
	UID  types.UID `json:"uid"`
}

// elasticOperation 是一次宽度变更的持久记录。
type elasticOperation struct {
	// Name 保存 ModelDeployment 的名称，用于核对记录所属的部署。
	// 同一部署的操作 ConfigMap 由部署 UID 唯一命名。
	Name string `json:"name"`
	// DeploymentUID 绑定本次操作所属的 ModelDeployment，并与 ConfigMap 的控制属主核对。即使名称相同，
	// 其他 UID 的记录也不能用于这次部署。
	DeploymentUID types.UID `json:"deploymentUID"`
	// Namespace 是记录所在的命名空间，与部署一致。
	Namespace string `json:"namespace"`
	// Generation 保存做出变更决定时的 spec 版本。spec 随后有更新时，需要先处理旧操作，
	// 再按新的意图决策。
	Generation int64 `json:"generation"`
	// Width 保存变更前的宽度和本次请求的目标宽度。
	Width elasticWidth `json:"width"`
	// Workers 列出这次操作允许退场的成员。master 单独记录，以保证退场流程始终保留它。
	Workers []elasticCapturedIdentity         `json:"workers"`
	Master  elasticCapturedIdentity           `json:"master"`
	Head    elasticCapturedIdentity           `json:"head,omitempty"`
	Members []elasticCapturedIdentity         `json:"members,omitempty"`
	Release *modelDeploymentRetirementRelease `json:"release,omitempty"`
	State   elasticOperationState             `json:"state"`
	// CommandIntent 在请求引擎前写明将要发送的命令。控制器重启后通过这份记录识别已发起的操作，
	// 避免对结果不明的请求直接重发。
	CommandIntent string `json:"commandIntent"`
	// CommandSent 与发送意图一同持久化。看到该标记时，引擎就有可能已经收到命令。
	CommandSent bool `json:"commandSent"`
	// ScaleAttempts 统计引擎明确拒绝的次数。达到上限时放弃本次操作。
	// 未收到确定应答时无法判断引擎是否已执行命令，因此不扣减此额度。
	ScaleAttempts int `json:"scaleAttempts,omitempty"`
	// ScaleLastError 保存上次明确拒绝的引擎错误。等待重试期间持续保留它，重试获接受后清除，
	// 耗尽次数时用于报告最终原因。
	ScaleLastError string `json:"scaleLastError,omitempty"`
	// ScaleLastFailedAt 记录最近一次失败的时间，用来计算下次可重试的时刻。
	// 为空表示无需等待这个退避间隔。
	ScaleLastFailedAt *time.Time `json:"scaleLastFailedAt,omitempty"`
	// ScaleAmbiguousAttempts 统计重发后遇到断连、调用方超时或回执无法解析的次数。
	// 这些情况会延长退避间隔，直到达到间隔上限，但不会耗用引擎明确拒绝的次数。
	ScaleAmbiguousAttempts int `json:"scaleAmbiguousAttempts,omitempty"`
	// ResourceVersion 保存读到的 ConfigMap 版本。更新必须针对这一版，才能发现并发写入。
	ResourceVersion string `json:"resourceVersion"`
}

// SentState 根据记录所处的阶段判断命令是否已经进入发送阶段。
//
// 发送阶段和 CommandSent 记录同一事实，判断以阶段为准。读取持久记录时还要校验二者一致，
// 防止一个错误的布尔值触发命令重放。
//
// Abandoned 也表示命令发出过。该阶段代表明确拒绝次数已耗尽，可以直接报告终态。
func (eo *elasticOperation) SentState() bool {
	switch eo.State {
	case elasticStateCommandSent, elasticStateAwaitingWidth, elasticStateNativeDone,
		elasticStateWithdrawn, elasticStateReleased, elasticStateCompleted, elasticStateAbandoned:
		return true
	default:
		return false
	}
}

// envelopeDrift 返回更新试图改动的不可变内容，全部一致时返回空串。
//
// 本次操作依据原先的起止宽度、generation 和成员身份作出决定。修改其中任一项，
// 就可能借旧记录对集群尚未准入的目标执行另一条命令。
func (eo *elasticOperation) envelopeDrift(next *elasticOperation) string {
	if eo.Head != next.Head || !reflect.DeepEqual(eo.Members, next.Members) || !reflect.DeepEqual(eo.Release, next.Release) {
		return "the captured runtime and allocation identities cannot change"
	}
	if eo.DeploymentUID != next.DeploymentUID {
		return "the record cannot be moved to another deployment"
	}
	if eo.Generation != next.Generation {
		return "the record cannot be rebound to another spec generation"
	}
	if eo.Width != next.Width {
		return "the record cannot be retargeted to another width"
	}
	if len(eo.Workers) != len(next.Workers) {
		return "the record cannot capture a different number of workers"
	}
	for i := range eo.Workers {
		if eo.Workers[i] != next.Workers[i] {
			return "the record cannot capture a different worker identity"
		}
	}
	if eo.Master != next.Master {
		return "the record cannot capture a different master identity"
	}
	// 命令意图一旦写入就只能保留。清除已发出的标记会让后续调谐再次发送同一命令。
	if eo.CommandIntent != "" && next.CommandIntent != eo.CommandIntent {
		return "the record cannot change the command it already recorded"
	}
	if eo.CommandSent && !next.CommandSent {
		return "the record cannot forget that a command was sent"
	}

	return ""
}

// elasticOperationFromData 解码一份持久记录，文档格式或记录内容有误时返回错误。
func elasticOperationFromData(body string) (*elasticOperation, error) {
	if strings.TrimSpace(body) == "" {
		return nil, fmt.Errorf("%w: the stored record is empty", errElasticOperation)
	}

	decoder := json.NewDecoder(strings.NewReader(body))
	decoder.DisallowUnknownFields()
	op := new(elasticOperation)
	if err := decoder.Decode(op); err != nil {
		return nil, fmt.Errorf("%w: %w", errElasticOperation, err)
	}
	if err := decoder.Decode(new(json.RawMessage)); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%w: the stored record carries a trailing JSON value", errElasticOperation)
	}

	return op, nil
}

// elasticOperationLabel 标记存储对象，使其可被检索且不会被误认为普通 ConfigMap。
const elasticOperationLabel = "gpustack.ai/elastic-operation"

// elasticOperationDataKey 是 ConfigMap 中保存整份操作文档的键。只接受一份 JSON 文档，
// 后面再跟一个值会被判为格式错误。
const elasticOperationDataKey = "operation.json"

// elasticControllerRef 为每个属主引用返回单独的 true 指针，避免共享可被其他调用方改写的变量。
func elasticControllerRef() *bool {
	yes := true

	return &yes
}

// errElasticOperation 标记记录本身无效。调用方可以通过错误链把这类问题和读取存储失败区分开。
var errElasticOperation = errors.New("elastic resize operation is not usable")

// validate 检查记录中的字段能否完整描述一次宽度变更。
//
// 必须能唯一确定部署，并知道有效的旧宽度和目标宽度。缺少任一项都不能据此执行变更。
func (eo *elasticOperation) validate() error {
	if eo.DeploymentUID == "" {
		return fmt.Errorf("%w: it names no deployment", errElasticOperation)
	}
	if eo.Generation <= 0 {
		return fmt.Errorf("%w: it is bound to no spec generation", errElasticOperation)
	}
	if eo.Width.Old <= 0 || eo.Width.Target <= 0 {
		return fmt.Errorf(
			"%w: widths %d to %d are not a resize", errElasticOperation, eo.Width.Old, eo.Width.Target,
		)
	}
	if eo.Width.Old == eo.Width.Target {
		return fmt.Errorf("%w: the target width equals the old one", errElasticOperation)
	}
	if eo.State == "" {
		return fmt.Errorf("%w: it carries no state", errElasticOperation)
	}
	// 发送阶段和 CommandSent 必须一致。发现矛盾时拒绝记录，避免依据其中一个字段重放命令。
	if eo.CommandSent && !eo.SentState() {
		return fmt.Errorf("%w: it says a command was sent in a state that has not", errElasticOperation)
	}
	if !eo.CommandSent && eo.SentState() {
		return fmt.Errorf("%w: it says no command was sent in a state that has", errElasticOperation)
	}
	if eo.Master.UID == "" || eo.Master.Name == "" {
		return fmt.Errorf("%w: it captured no master identity", errElasticOperation)
	}
	if eo.Name == "" {
		return fmt.Errorf("%w: it names no deployment", errElasticOperation)
	}
	if eo.Namespace == "" {
		return fmt.Errorf("%w: it names no namespace to store the record in", errElasticOperation)
	}
	for i, worker := range eo.Workers {
		if worker.UID == "" {
			return fmt.Errorf("%w: captured worker %d has no identity", errElasticOperation, i)
		}
		if worker.Name == "" {
			return fmt.Errorf("%w: captured worker %d has no name", errElasticOperation, i)
		}
		if worker.UID == eo.Master.UID {
			return fmt.Errorf("%w: the master is also captured as retirable", errElasticOperation)
		}
	}

	return nil
}

// retractableWorkers 返回已捕获成员中允许退场的集合，始终排除 master。
func (eo *elasticOperation) retractableWorkers() []elasticCapturedIdentity {
	retractable := make([]elasticCapturedIdentity, 0, len(eo.Workers))
	for _, worker := range eo.Workers {
		if worker.UID != eo.Master.UID {
			retractable = append(retractable, worker)
		}
	}

	return retractable
}

// elasticOperationStore 将记录保存到以部署为控制属主的 ConfigMap 中，Kubernetes 据此在删除部署时回收记录。
type elasticOperationStore struct {
	client ctrlcli.Client
	// now 提供创建记录时使用的时间，在测试中可以指定固定值。
	now func() time.Time
}

// newElasticOperationStore 构建记录存储。若没有配置客户端，各项读写会返回错误，
// 避免因接线错误导致整个控制器崩溃。
func newElasticOperationStore(client ctrlcli.Client) *elasticOperationStore {
	return &elasticOperationStore{client: client, now: time.Now}
}

// elasticOperationRecordName 根据部署 UID 计算其唯一操作记录名。如果两个操作同时试图创建记录，
// 第二个创建会冲突。
func elasticOperationRecordName(deploymentUID types.UID) string {
	return "elastic-resize-" + string(deploymentUID)
}

// Create 在存储前校验记录，字段不满足约束时返回错误。
//
// 发出任何引擎请求前要先持久化操作。调用方取得已存储的记录后，才能继续登记并发送命令。
func (s *elasticOperationStore) Create(
	ctx context.Context, op *elasticOperation,
) (*elasticOperation, error) {
	if err := op.validate(); err != nil {
		return nil, err
	}
	if s.client == nil {
		return nil, fmt.Errorf("%w: no client is configured", errElasticOperation)
	}

	document, err := json.Marshal(op)
	if err != nil {
		return nil, fmt.Errorf("encode the elastic resize record: %w", err)
	}

	stored := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      elasticOperationRecordName(op.DeploymentUID),
			Namespace: op.Namespace,
			Labels: map[string]string{
				elasticOperationLabel: "true",
			},
			// 将 ModelDeployment 的 UID 写入控制属主，防止同名重建的部署误用旧操作。
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: workercore.GroupVersion.String(),
				Kind:       "ModelDeployment",
				Name:       op.Name,
				UID:        op.DeploymentUID,
				Controller: elasticControllerRef(),
			}},
		},
		Data: map[string]string{elasticOperationDataKey: string(document)},
	}
	if s.now != nil {
		stored.CreationTimestamp = metav1.NewTime(s.now())
	}

	if err := s.client.Create(ctx, stored); err != nil {
		return nil, fmt.Errorf("store the elastic resize record: %w", err)
	}
	op.ResourceVersion = stored.ResourceVersion

	return op, nil
}

// Read 读取指定部署的操作记录，同时核对其归属。
//
// 属主缺失或不符、正文为空、JSON 损坏、解码后字段缺失或尾部有第二个值均属错误。
// 只有对象确实不存在才能按无待办操作处理，否则会丢失尚未完成的决定。
func (s *elasticOperationStore) Read(
	ctx context.Context, namespace, name string, deploymentUID types.UID,
) (*elasticOperation, error) {
	if s.client == nil {
		return nil, fmt.Errorf("%w: no client is configured", errElasticOperation)
	}

	stored := new(corev1.ConfigMap)
	if err := s.client.Get(ctx, ctrlcli.ObjectKey{Namespace: namespace, Name: elasticOperationRecordName(deploymentUID)}, stored); err != nil {
		return nil, err
	}

	// 用调用方预期的名称、UID、API 版本和类型核对控制属主，
	// 防止把另一种资源或另一个部署的记录当作当前操作。
	if err := elasticControllingOwnerIs(stored, name, deploymentUID); err != nil {
		return nil, err
	}
	owner := metav1.GetControllerOf(stored)

	body, found := stored.Data[elasticOperationDataKey]
	if !found || strings.TrimSpace(body) == "" {
		return nil, fmt.Errorf("%w: the stored record is empty", errElasticOperation)
	}

	decoder := json.NewDecoder(strings.NewReader(body))
	decoder.DisallowUnknownFields()
	op := new(elasticOperation)
	if err := decoder.Decode(op); err != nil {
		return nil, fmt.Errorf("%w: %w", errElasticOperation, err)
	}
	if err := decoder.Decode(new(json.RawMessage)); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%w: the stored record carries a trailing JSON value", errElasticOperation)
	}

	// 比较文档自己保存的身份与属主。若用属主值覆盖再比较，就发现不了记录原先指向另一部署。
	if op.DeploymentUID != "" && op.DeploymentUID != owner.UID {
		return nil, fmt.Errorf(
			"%w: the record inside names deployment %s while its owner is %s",
			errElasticOperation, op.DeploymentUID, owner.UID,
		)
	}
	if op.Name != "" && op.Name != owner.Name {
		return nil, fmt.Errorf(
			"%w: the record inside names %s while its owner is %s",
			errElasticOperation, op.Name, owner.Name,
		)
	}
	op.Namespace = stored.Namespace
	op.ResourceVersion = stored.ResourceVersion
	op.Name = owner.Name
	if err := op.validate(); err != nil {
		return nil, err
	}
	if op.DeploymentUID != deploymentUID {
		return nil, fmt.Errorf(
			"%w: the record inside is bound to %s rather than %s",
			errElasticOperation, op.DeploymentUID, deploymentUID,
		)
	}

	return op, nil
}

// elasticControllingOwnerIs 检查记录的控制属主是否完整匹配调用方指定的部署。
//
// 读取和更新共用这项检查。期望的部署身份必须完整，缺失值不能充当通配符，否则可能覆盖别的部署的数据。
func elasticControllingOwnerIs(stored *corev1.ConfigMap, name string, deploymentUID types.UID) error {
	if name == "" || deploymentUID == "" {
		return fmt.Errorf(
			"%w: no expected deployment identity was given to match the stored owner", errElasticOperation,
		)
	}

	owner := metav1.GetControllerOf(stored)
	if owner == nil {
		return fmt.Errorf("%w: the stored record has no controlling owner", errElasticOperation)
	}
	if owner.Kind != "ModelDeployment" {
		return fmt.Errorf(
			"%w: the stored record is owned by a %s rather than a ModelDeployment",
			errElasticOperation, owner.Kind,
		)
	}
	if owner.APIVersion != workercore.GroupVersion.String() {
		return fmt.Errorf(
			"%w: the stored record's owner is %s rather than %s",
			errElasticOperation, owner.APIVersion, workercore.GroupVersion.String(),
		)
	}
	if owner.Name != name {
		return fmt.Errorf(
			"%w: the stored record is owned by %s rather than the expected %s",
			errElasticOperation, owner.Name, name,
		)
	}
	if owner.UID != deploymentUID {
		return fmt.Errorf(
			"%w: the stored record is owned by %s rather than %s",
			errElasticOperation, owner.UID, deploymentUID,
		)
	}

	return nil
}

// Update 只更新先前读到的那一个对象版本。
//
// 写入失败时保留调用方记录的原版本。并发冲突时需要重新读取并判断最新状态，
// 不能把先前的决定套到更新后的版本上。
func (s *elasticOperationStore) Update(ctx context.Context, op *elasticOperation) error {
	if err := op.validate(); err != nil {
		return err
	}
	if s.client == nil {
		return fmt.Errorf("%w: no client is configured", errElasticOperation)
	}

	stored := new(corev1.ConfigMap)
	if err := s.client.Get(ctx, ctrlcli.ObjectKey{
		Namespace: op.Namespace, Name: elasticOperationRecordName(op.DeploymentUID),
	}, stored); err != nil {
		return err
	}
	// 拒绝未携带 resourceVersion 的更新，防止直接覆盖在决策之后发生的修改。
	if op.ResourceVersion == "" {
		return fmt.Errorf(
			"%w: the record was not read, so it carries no version to write against", errElasticOperation,
		)
	}
	if stored.ResourceVersion != op.ResourceVersion {
		return apierrors.NewConflict(
			corev1.Resource("configmaps"), elasticOperationRecordName(op.DeploymentUID),
			fmt.Errorf("the record moved from %s to %s", op.ResourceVersion, stored.ResourceVersion),
		)
	}

	// 写回前重新读取并核对当前控制属主。对象在先前读取和现在之间可能已被改写，
	// 旧记录中的属主不足以授权写入。
	if err := elasticControllingOwnerIs(stored, op.Name, op.DeploymentUID); err != nil {
		return err
	}

	// 普通更新锁定部署、generation、宽度及成员身份，只允许推进操作状态。命令意图一旦写入就须保留。
	current, err := elasticOperationFromData(stored.Data[elasticOperationDataKey])
	if err != nil {
		return err
	}
	if reason := current.envelopeDrift(op); reason != "" {
		return fmt.Errorf("%w: %s", errElasticOperation, reason)
	}

	document, err := json.Marshal(op)
	if err != nil {
		return fmt.Errorf("encode the elastic resize record: %w", err)
	}
	stored.Data = map[string]string{elasticOperationDataKey: string(document)}

	if err := s.client.Update(ctx, stored); err != nil {
		return err
	}
	op.ResourceVersion = stored.ResourceVersion

	return nil
}

// RecoverInterrupted 在一次写入中把被打断的记录替换为恢复记录。
//
// 此处允许替换原来的宽度及捕获名单。若旧 generation 的扩容没有推进引擎，先重新证明旧宽度，
// 再用只负责释放的记录接手多占的成员。摘除流量、证明 actor 已退出及核对资源归还仍走普通流程。
// 替换前重新读取对象并核对版本、属主和旧记录内容，出现并发修改就保留现场。
func (s *elasticOperationStore) RecoverInterrupted(
	ctx context.Context, current, recovered *elasticOperation,
) error {
	if err := current.validate(); err != nil {
		return err
	}
	if err := recovered.validate(); err != nil {
		return err
	}
	if recovered.Name != current.Name || recovered.Namespace != current.Namespace ||
		recovered.DeploymentUID != current.DeploymentUID {
		return fmt.Errorf("%w: the recovery record is not this deployment's", errElasticOperation)
	}
	if s.client == nil {
		return fmt.Errorf("%w: no client is configured", errElasticOperation)
	}

	stored := new(corev1.ConfigMap)
	if err := s.client.Get(ctx, ctrlcli.ObjectKey{
		Namespace: current.Namespace, Name: elasticOperationRecordName(current.DeploymentUID),
	}, stored); err != nil {
		return err
	}
	if current.ResourceVersion == "" {
		return fmt.Errorf(
			"%w: the record was not read, so it carries no version to write against", errElasticOperation,
		)
	}
	if stored.ResourceVersion != current.ResourceVersion {
		return apierrors.NewConflict(
			corev1.Resource("configmaps"), elasticOperationRecordName(current.DeploymentUID),
			fmt.Errorf("the record moved from %s to %s", current.ResourceVersion, stored.ResourceVersion),
		)
	}
	if err := elasticControllingOwnerIs(stored, current.Name, current.DeploymentUID); err != nil {
		return err
	}
	storedRecord, err := elasticOperationFromData(stored.Data[elasticOperationDataKey])
	if err != nil {
		return err
	}
	if reason := storedRecord.envelopeDrift(current); reason != "" {
		return fmt.Errorf("%w: %s", errElasticOperation, reason)
	}

	document, err := json.Marshal(recovered)
	if err != nil {
		return fmt.Errorf("encode the elastic resize record: %w", err)
	}
	stored.Data = map[string]string{elasticOperationDataKey: string(document)}

	if err := s.client.Update(ctx, stored); err != nil {
		return err
	}
	recovered.ResourceVersion = stored.ResourceVersion

	return nil
}

// Delete 删除已处理完的操作记录，使部署可以开始下一次宽度变更。
func (s *elasticOperationStore) Delete(ctx context.Context, op *elasticOperation) error {
	if s.client == nil || op == nil || op.ResourceVersion == "" {
		return fmt.Errorf("%w: deletion requires a read record", errElasticOperation)
	}
	stored := new(corev1.ConfigMap)
	if err := s.client.Get(ctx, ctrlcli.ObjectKey{
		Namespace: op.Namespace, Name: elasticOperationRecordName(op.DeploymentUID),
	}, stored); err != nil {
		return err
	}
	if err := elasticControllingOwnerIs(stored, op.Name, op.DeploymentUID); err != nil {
		return err
	}
	if stored.ResourceVersion != op.ResourceVersion {
		return apierrors.NewConflict(corev1.Resource("configmaps"), stored.Name,
			fmt.Errorf("the operation record changed before deletion"))
	}
	return s.client.Delete(ctx, stored, ctrlcli.Preconditions{
		UID: &stored.UID, ResourceVersion: &op.ResourceVersion,
	})
}
