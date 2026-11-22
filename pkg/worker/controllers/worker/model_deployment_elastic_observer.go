package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"

	_ "embed"
)

// 本观测器读取的身份接缝已冻结：即承载所属 Pod UID 的 Ray 节点标签。工作负载渲染时通过 `ray start --labels` 传入该标签，Ray 将其持久化在 GcsNodeInfo.labels 中，本文件执行的每一次关联（join）都经由此标签完成。基于 Pod IP 或 Pod 名称的关联在同名重建后会误连，因此刻意不予使用。
const modelDeploymentElasticPodUIDLabelKey = "gpustack-pod-uid"

//go:embed model_deployment_elastic_observer.py
var modelDeploymentElasticObserverScript string

// 单次观测可声称的规模上限。它们是解析器上限，而非传输上限：exec 传输层有自身的字节上限，而这些上限用于防止格式畸形但看似合法的文档被无界遍历。
const (
	modelDeploymentElasticMaxNodes  = 4096
	modelDeploymentElasticMaxActors = 4096
	modelDeploymentElasticMaxPGs    = 4096

	// 单次 Observe 调用的总时间预算。调用方更早的截止时间仍优先生效：该预算通过 context.WithTimeout 叠加，因此先到期者触发。
	modelDeploymentElasticObservationBudget = 45 * time.Second

	// 传递给脚本的单次 exec 执行上限，使其自身的写入器在触及传输层上限之前主动停止，从而返回明确拒绝（退出码 4）而非被截断的流。
	modelDeploymentElasticScriptMaxOutputBytes = 6 << 20

	modelDeploymentElasticScriptDeadlineSeconds = 20.0

	// head 自身控制面默认监听的 GCS 地址。调用方从渲染后的 head spec 中提供该地址；捕获为空时直接拒绝。
	modelDeploymentElasticDefaultGCSAddress = "127.0.0.1:6379"
)

// 原生 DP actor 类。rank 证明必须由其中精确一类的 actor 满足；成员上所有其他存活的 actor 仍会否决该成员的 actor-free 事实。
const (
	modelDeploymentElasticActorClassEngineCore  = "EngineCoreActor"
	modelDeploymentElasticActorClassDPMoEEngine = "DPMoEEngineCoreActor"
)

// 原生 placement-group 名称格式为 dp_rank_N，由固定版本的 vLLM 引擎在 v1/engine/utils.py:739（初始化）和 :843（扩容）处生成。
const modelDeploymentElasticRankPGPrefix = "dp_rank_"

// 解析器接受的原始 GCS 枚举值。除此之外的任何值均视为 Unknown，绝不回退为默认值：未识别的状态属于本文件拒绝解释的 schema 漂移。
const (
	elasticRayActorDependenciesUnready = 0
	elasticRayActorPendingCreation     = 1
	elasticRayActorAlive               = 2
	elasticRayActorRestarting          = 3
	elasticRayActorDead                = 4

	elasticRayPGPending      = 0
	elasticRayPGPrepared     = 1
	elasticRayPGCreated      = 2
	elasticRayPGRemoved      = 3
	elasticRayPGRescheduling = 4
)

// modelDeploymentElasticMemberRole 是捕获到的单个受管 Pod 的角色。
type modelDeploymentElasticMemberRole string

const (
	modelDeploymentElasticRoleHead   modelDeploymentElasticMemberRole = "head"
	modelDeploymentElasticRoleMaster modelDeploymentElasticMemberRole = "master"
	modelDeploymentElasticRoleWorker modelDeploymentElasticMemberRole = "worker"
)

// modelDeploymentElasticCapturedMember 表示一个已捕获的受管 Pod：控制器在观测前记录的名称、UID、容器和角色。本文件产出的每一项运行时事实都会通过非缓存读取绑定回这些捕获记录。
type modelDeploymentElasticCapturedMember struct {
	PodUID    types.UID
	PodName   string
	Container string
	Role      modelDeploymentElasticMemberRole
}

// 传输线格式（wire shape）。必需字段均使用指针，以便缺失或 null 字段被拒绝而不是被当作零值读取：在这里零值意味着声明什么都不存在，而协议会对零值采取行动。
type elasticRayNodeWire struct {
	NodeIDHex           *string           `json:"node_id_hex"`
	Alive               *bool             `json:"alive"`
	NodeManagerAddress  string            `json:"node_manager_address"`
	NodeManagerHostname string            `json:"node_manager_hostname"`
	NodeManagerPort     *int              `json:"node_manager_port"`
	NodeName            string            `json:"node_name"`
	DeathReason         *int              `json:"death_reason"`
	DeathReasonMessage  string            `json:"death_reason_message"`
	Labels              map[string]string `json:"labels"`
	Resources           map[string]string `json:"resources"`
}

type elasticRayActorWire struct {
	ActorIDHex          *string `json:"actor_id_hex"`
	State               *int    `json:"state"`
	StateName           string  `json:"state_name"`
	ClassName           string  `json:"class_name"`
	NodeIDHex           string  `json:"node_id_hex"`
	PlacementGroupIDHex string  `json:"placement_group_id_hex"`
}

type elasticRayBundleWire struct {
	BundleIndex   *int              `json:"bundle_index"`
	NodeIDHex     string            `json:"node_id_hex"`
	UnitResources map[string]string `json:"unit_resources,omitempty"`
}

type elasticRayPGWire struct {
	PlacementGroupIDHex *string                `json:"placement_group_id_hex"`
	Name                *string                `json:"name"`
	State               *int                   `json:"state"`
	StateName           string                 `json:"state_name"`
	Bundles             []elasticRayBundleWire `json:"bundles"`
}

type elasticRayErrorWire struct {
	Table string `json:"table"`
	Error string `json:"error"`
}

type elasticRayDocumentWire struct {
	SchemaVersion   *int                   `json:"schema_version"`
	RayVersion      *string                `json:"ray_version"`
	GCSAddress      string                 `json:"gcs_address"`
	CollectedAtMS   *int64                 `json:"collected_at_ms"`
	Nodes           *[]elasticRayNodeWire  `json:"nodes"`
	Actors          *[]elasticRayActorWire `json:"actors"`
	PlacementGroups *[]elasticRayPGWire    `json:"placement_groups"`
	Errors          []elasticRayErrorWire  `json:"errors"`
}

// rejectDuplicateJSONKeys 遍历单个 JSON 文档的 token 流，并在遇到第一个重复声明同一键的对象时拒绝该文档。encoding/json 本身会静默保留最后一个重复键，否则类似 `{"state":2,"state":4}` 的载荷就会按解码器最后看到的值解析。
func rejectDuplicateJSONKeys(dec *json.Decoder) error {
	return walkDuplicateJSONKeys(dec, 0)
}

func walkDuplicateJSONKeys(dec *json.Decoder, depth int) error {
	if depth > 64 {
		return fmt.Errorf("json nesting exceeds 64")
	}
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	delim, ok := tok.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]bool{}
		for dec.More() {
			keyTok, err := dec.Token()
			if err != nil {
				return err
			}
			key, ok := keyTok.(string)
			if !ok {
				return fmt.Errorf("object key is not a string")
			}
			if seen[key] {
				return fmt.Errorf("duplicate JSON field %q", key)
			}
			seen[key] = true
			if err := walkDuplicateJSONKeys(dec, depth+1); err != nil {
				return err
			}
		}
		_, err := dec.Token()
		return err
	case '[':
		for dec.More() {
			if err := walkDuplicateJSONKeys(dec, depth+1); err != nil {
				return err
			}
		}
		_, err := dec.Token()
		return err
	default:
		return nil
	}
}

// parseElasticRayDocument 严格解码单个采集器文档：不允许未知字段、不允许重复键、不允许尾随多余值，所有必需的表与字段必须存在，且表大小在解析器上限之内。任一检查失败都会返回 error，调用方会将其转为 Unknown 而非视为空集群。
func parseElasticRayDocument(data []byte) (*elasticRayDocumentWire, error) {
	scanner := json.NewDecoder(strings.NewReader(string(data)))
	if err := rejectDuplicateJSONKeys(scanner); err != nil {
		return nil, fmt.Errorf("collector document is not strict JSON: %w", err)
	}

	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	var doc elasticRayDocumentWire
	if err := decoder.Decode(&doc); err != nil {
		return nil, fmt.Errorf("collector document is not an observation: %w", err)
	}
	if err := decoder.Decode(new(json.RawMessage)); err != io.EOF {
		return nil, errors.New("collector document carries a trailing JSON value")
	}

	if doc.SchemaVersion == nil || *doc.SchemaVersion != 1 {
		return nil, errors.New("collector document carries no schema_version 1")
	}
	if doc.RayVersion == nil || *doc.RayVersion == "" {
		return nil, errors.New("collector document carries no ray_version")
	}
	if doc.CollectedAtMS == nil {
		return nil, errors.New("collector document carries no collected_at_ms")
	}
	if doc.Nodes == nil || doc.Actors == nil || doc.PlacementGroups == nil {
		return nil, errors.New("collector document is missing a required table")
	}
	if len(doc.Errors) > 0 {
		return nil, fmt.Errorf("collector refused %d table reads", len(doc.Errors))
	}
	if len(*doc.Nodes) > modelDeploymentElasticMaxNodes {
		return nil, fmt.Errorf("collector document carries %d nodes", len(*doc.Nodes))
	}
	if len(*doc.Actors) > modelDeploymentElasticMaxActors {
		return nil, fmt.Errorf("collector document carries %d actors", len(*doc.Actors))
	}
	if len(*doc.PlacementGroups) > modelDeploymentElasticMaxPGs {
		return nil, fmt.Errorf("collector document carries %d placement groups", len(*doc.PlacementGroups))
	}

	for i := range *doc.Nodes {
		if err := elasticRayNodeRequired(&(*doc.Nodes)[i]); err != nil {
			return nil, fmt.Errorf("node %d: %w", i, err)
		}
	}
	for i := range *doc.Actors {
		if err := elasticRayActorRequired(&(*doc.Actors)[i]); err != nil {
			return nil, fmt.Errorf("actor %d: %w", i, err)
		}
	}
	for i := range *doc.PlacementGroups {
		if err := elasticRayPGRequired(&(*doc.PlacementGroups)[i]); err != nil {
			return nil, fmt.Errorf("placement group %d: %w", i, err)
		}
	}
	return &doc, nil
}

func elasticRayNodeRequired(node *elasticRayNodeWire) error {
	if node.NodeIDHex == nil || *node.NodeIDHex == "" {
		return errors.New("carries no node_id_hex")
	}
	if node.Alive == nil {
		return errors.New("carries no alive")
	}
	if node.Labels == nil {
		return errors.New("carries no labels table")
	}
	return nil
}

func elasticRayActorRequired(actor *elasticRayActorWire) error {
	if actor.ActorIDHex == nil || *actor.ActorIDHex == "" {
		return errors.New("carries no actor_id_hex")
	}
	if actor.State == nil {
		return errors.New("carries no state")
	}
	switch *actor.State {
	case elasticRayActorDependenciesUnready,
		elasticRayActorPendingCreation,
		elasticRayActorAlive,
		elasticRayActorRestarting,
		elasticRayActorDead:
	default:
		return fmt.Errorf("unknown actor state %d", *actor.State)
	}
	return nil
}

func elasticRayPGRequired(pg *elasticRayPGWire) error {
	if pg.PlacementGroupIDHex == nil || *pg.PlacementGroupIDHex == "" {
		return errors.New("carries no placement_group_id_hex")
	}
	if pg.Name == nil {
		return errors.New("carries no name")
	}
	if pg.State == nil {
		return errors.New("carries no state")
	}
	switch *pg.State {
	case elasticRayPGPending, elasticRayPGPrepared, elasticRayPGCreated,
		elasticRayPGRemoved, elasticRayPGRescheduling:
	default:
		return fmt.Errorf("unknown placement group state %d", *pg.State)
	}
	return nil
}

// modelDeploymentElasticActorFreeFact 是按已捕获 UID 记录的 actor-free 结论：布尔值必须结合其 reason 才有意义，且为 false 时绝不删除任何对象。
type modelDeploymentElasticActorFreeFact struct {
	ActorFree bool
	Reason    string
}

// modelDeploymentElasticObservation 为扩缩容控制器提供观测输入：独立判明的宽度、rank 到 Pod UID 的映射、完整的 actor 身份信息，以及按已捕获 UID 记录且带明确原因的 actor-free 事实。它本身绝不直接授权扩缩容、删除或完成操作。
type modelDeploymentElasticObservation struct {
	SchemaVersion int
	RayVersion    string
	GCSAddress    string
	CollectedAtMS int64

	WidthKnown         bool
	RayWidth           int
	WidthUnknownReason string
	RankToPodUID       map[int]types.UID

	// RegisteredGPU 是四层关联中的节点侧指标：当前有多少个已捕获的 GPU 成员正各自占用整张 GPU 落在唯一的 ALIVE Ray 节点上。它刻意独立于 RayWidth——扩容时容量会先于原生 rank 注册完成——且已知的零表示确实没有捕获到 GPU 成员。
	RegisteredGPUKnown         bool
	RegisteredGPU              int
	RegisteredGPUUnknownReason string

	ActorsComplete bool

	ActorFree map[types.UID]modelDeploymentElasticActorFreeFact

	UnknownReasons []string
}

// isNativeElasticActorClass 报告某个 GCS actor 类是否属于可满足 rank 证明的原生 DP 引擎 actor 类之一。
func isNativeElasticActorClass(class string) bool {
	return class == modelDeploymentElasticActorClassEngineCore ||
		class == modelDeploymentElasticActorClassDPMoEEngine
}

// elasticRayRankOfName 严格解析 dp_rank_N：必须具备精确前缀且整数之后没有任何字符。仅匹配前缀或带有后缀的名称均不算作 rank。
func elasticRayRankOfName(name string) (int, bool) {
	rest, ok := strings.CutPrefix(name, modelDeploymentElasticRankPGPrefix)
	if !ok || rest == "" {
		return 0, false
	}
	rank := 0
	for _, r := range rest {
		if r < '0' || r > '9' {
			return 0, false
		}
		rank = rank*10 + int(r-'0')
		if rank > 1<<20 {
			return 0, false
		}
	}
	return rank, true
}

// joinModelDeploymentElasticRayIdentity 将单个采集器文档与已捕获的成员关联起来。下方的每条规则都导向挂起而非零值：文档未能证明的任何事实都会带有明确的 Unknown 原因，因为缺失证明正是下线安全性的基石。
func joinModelDeploymentElasticRayIdentity(
	doc *elasticRayDocumentWire,
	members []modelDeploymentElasticCapturedMember,
	expectedWidth int32,
	tensorParallelSize int,
) modelDeploymentElasticObservation {
	obs := modelDeploymentElasticObservation{
		SchemaVersion:  *doc.SchemaVersion,
		RayVersion:     *doc.RayVersion,
		GCSAddress:     doc.GCSAddress,
		CollectedAtMS:  *doc.CollectedAtMS,
		WidthKnown:     true,
		RankToPodUID:   map[int]types.UID{},
		ActorFree:      map[types.UID]modelDeploymentElasticActorFreeFact{},
		UnknownReasons: []string{},
	}

	memberByUID := map[types.UID]modelDeploymentElasticCapturedMember{}
	for _, member := range members {
		if _, dup := memberByUID[member.PodUID]; dup {
			obs.WidthKnown = false
			obs.WidthUnknownReason = "captured members repeat Pod UID " + string(member.PodUID)
			return obs
		}
		memberByUID[member.PodUID] = member
	}

	// 节点关联。只有 ALIVE 节点才会纳入声明：DEAD 行只是历史记录——例如重启后 Pod 的旧节点或已下线的外部机器——既不得计入已注册成员数，也不得与当前携带相同 Pod UID 的 ALIVE 节点重复。DEAD 行仍保留在原始查找表中，以便解析仍驻留在其上的 actor。
	nodeByHex := map[string]*elasticRayNodeWire{}
	nodeHexByUID := map[types.UID]string{}
	headHexByUID := map[types.UID]string{}
	duplicateClaimUIDs := map[types.UID]bool{}
	for i := range *doc.Nodes {
		node := &(*doc.Nodes)[i]
		if _, dup := nodeByHex[*node.NodeIDHex]; dup {
			obs.addUnknown(fmt.Sprintf("nodes repeat node id %s", *node.NodeIDHex))
			continue
		}
		nodeByHex[*node.NodeIDHex] = node
		if !*node.Alive {
			continue
		}
		uid, claimed := node.Labels[modelDeploymentElasticPodUIDLabelKey]
		if !claimed {
			continue
		}
		member, known := memberByUID[types.UID(uid)]
		if !known {
			obs.addUnknown(fmt.Sprintf("node %s claims foreign Pod UID %s", *node.NodeIDHex, uid))
			continue
		}
		if member.Role == modelDeploymentElasticRoleHead {
			if _, dup := headHexByUID[member.PodUID]; dup {
				obs.addUnknown(fmt.Sprintf("head Pod UID %s is claimed by two Ray nodes", uid))
				continue
			}
			headHexByUID[member.PodUID] = *node.NodeIDHex
			continue
		}
		if _, dup := nodeHexByUID[member.PodUID]; dup {
			duplicateClaimUIDs[member.PodUID] = true
			obs.addUnknown(fmt.Sprintf("Pod UID %s is claimed by two ALIVE Ray nodes (replacement)", uid))
			continue
		}
		nodeHexByUID[member.PodUID] = *node.NodeIDHex
	}
	for _, member := range members {
		if member.Role == modelDeploymentElasticRoleHead {
			continue
		}
		if _, joined := nodeHexByUID[member.PodUID]; !joined {
			obs.addUnknown(fmt.Sprintf(
				"captured %s Pod %s (uid %s) is claimed by no live Ray node",
				member.Role, member.PodName, member.PodUID,
			))
		}
	}

	// 已注册 GPU 容量是四层关联中的节点侧指标，独立于原生 rank 侧：即 ALIVE Ray 节点当前为多少个已捕获的 GPU 成员各持有一整张 GPU。扩容时容量会在引擎创建新 rank 之前完成注册，因此该值必须能够独立判明，否则内核会因等待命令尚未创建的 rank 而死锁。没有已捕获 GPU 成员是已知的零；若某个已捕获 GPU 成员当前没有对应的 ALIVE 节点，则结果为 Unknown，绝不能按缺额计数。
	obs.RegisteredGPUKnown = true
	gpuMembers := 0
	for _, member := range members {
		if member.Role == modelDeploymentElasticRoleHead {
			continue
		}
		gpuMembers++
		nodeHex, joined := nodeHexByUID[member.PodUID]
		if !joined || duplicateClaimUIDs[member.PodUID] {
			obs.RegisteredGPUKnown = false
			obs.RegisteredGPUUnknownReason = fmt.Sprintf(
				"registered GPU capacity unknown: captured %s Pod %s has no unique current ALIVE Ray node",
				member.Role, member.PodName,
			)
			continue
		}
		rawGPU, carried := nodeByHex[nodeHex].Resources["GPU"]
		value, err := strconv.ParseFloat(rawGPU, 64)
		switch {
		case !carried:
			obs.RegisteredGPUKnown = false
			obs.RegisteredGPUUnknownReason = fmt.Sprintf(
				"registered GPU capacity unknown: node %s states no GPU resource", nodeHex,
			)
		case err != nil:
			obs.RegisteredGPUKnown = false
			obs.RegisteredGPUUnknownReason = fmt.Sprintf(
				"registered GPU capacity unknown: node %s GPU resource %q is not a number", nodeHex, rawGPU,
			)
		case math.IsInf(value, 0) || math.IsNaN(value):
			obs.RegisteredGPUKnown = false
			obs.RegisteredGPUUnknownReason = fmt.Sprintf(
				"registered GPU capacity unknown: node %s GPU resource is not finite", nodeHex,
			)
		case value != float64(tensorParallelSize):
			obs.RegisteredGPUKnown = false
			obs.RegisteredGPUUnknownReason = fmt.Sprintf(
				"registered GPU capacity unknown: node %s carries %v GPUs, not exactly %d", nodeHex, value, tensorParallelSize,
			)
		default:
			obs.RegisteredGPU++
		}
	}
	if gpuMembers == 0 {
		obs.RegisteredGPU = 0
	}

	// Actor 清单。重复条目属于 schema 漂移；未知状态行已在解析阶段被拒绝。每个 actor 的节点都必须能够解析，否则完整 actor 身份标记将为 false。
	actorByID := map[string]*elasticRayActorWire{}
	actorsByNode := map[string][]*elasticRayActorWire{}
	actorsByPG := map[string][]*elasticRayActorWire{}
	actorsComplete := true
	for i := range *doc.Actors {
		actor := &(*doc.Actors)[i]
		if _, dup := actorByID[*actor.ActorIDHex]; dup {
			obs.addUnknown(fmt.Sprintf("actors repeat actor id %s", *actor.ActorIDHex))
			actorsComplete = false
			continue
		}
		actorByID[*actor.ActorIDHex] = actor
		actorsByNode[actor.NodeIDHex] = append(actorsByNode[actor.NodeIDHex], actor)
		actorsByPG[actor.PlacementGroupIDHex] = append(actorsByPG[actor.PlacementGroupIDHex], actor)
		if _, known := nodeByHex[actor.NodeIDHex]; !known {
			obs.addUnknown(fmt.Sprintf(
				"actor %s (%s) sits on unknown node %s",
				*actor.ActorIDHex, actor.ClassName, actor.NodeIDHex,
			))
			actorsComplete = false
			continue
		}
		// 停留在 DEAD 节点上的非终态 actor 属于集群尚未清理妥当的未决存活工作进程；DEAD 节点上的 DEAD actor 只是历史记录。
		if node := nodeByHex[actor.NodeIDHex]; !*node.Alive && *actor.State != elasticRayActorDead {
			obs.addUnknown(fmt.Sprintf(
				"nonterminal actor %s (%s) in state %s sits on DEAD node %s",
				*actor.ActorIDHex, actor.ClassName, actor.StateName, actor.NodeIDHex,
			))
			actorsComplete = false
		}
	}
	obs.ActorsComplete = actorsComplete

	// Rank 证明。每个预期 rank 都需要其 dp_rank_N placement group 处于 CREATED 状态，
	// 其上调度有一个 ALIVE 状态的原生 actor，且该 actor 所在节点存活并已加入捕获的 GPU 成员。
	// pending、restarting、removed 和 rescheduled 状态均会阻塞。
	pgByRank := map[int]*elasticRayPGWire{}
	for i := range *doc.PlacementGroups {
		pg := &(*doc.PlacementGroups)[i]
		rank, isRank := elasticRayRankOfName(*pg.Name)
		if !isRank {
			continue
		}
		if *pg.State == elasticRayPGRemoved {
			for _, actor := range actorsByPG[*pg.PlacementGroupIDHex] {
				if *actor.State != elasticRayActorDead {
					obs.addUnknown(fmt.Sprintf("nonterminal actor %s remains on removed rank %d", *actor.ActorIDHex, rank))
					obs.ActorsComplete = false
				}
			}
			continue
		}
		if _, dup := pgByRank[rank]; dup {
			obs.addUnknown(fmt.Sprintf("placement groups repeat rank %d (name %s)", rank, *pg.Name))
			continue
		}
		pgByRank[rank] = pg
	}

	// addWidthReason 即 addUnknown：未能闭环的 rank 证明属于由原因字符串指明的未知状态，
	// 而非更小的宽度。
	addWidthReason := obs.addUnknown

	for rank := 0; rank < int(expectedWidth); rank++ {
		pg, known := pgByRank[rank]
		if !known {
			addWidthReason(fmt.Sprintf("missing native rank %d placement group", rank))
			continue
		}
		if *pg.State != elasticRayPGCreated {
			addWidthReason(fmt.Sprintf(
				"rank %d placement group is %s, not CREATED", rank, pg.StateName,
			))
			continue
		}
		alive := 0
		proofUID := types.UID("")
		for _, actor := range actorsByPG[*pg.PlacementGroupIDHex] {
			if *actor.State == elasticRayActorDead {
				// rank 组上的 DEAD 状态 actor 属于重启历史，而非有效的 rank 声明。
				continue
			}
			if *actor.State != elasticRayActorAlive {
				addWidthReason(fmt.Sprintf(
					"rank %d carries nonterminal native actor %s in state %s",
					rank, *actor.ActorIDHex, actor.StateName,
				))
				continue
			}
			if !isNativeElasticActorClass(actor.ClassName) {
				continue
			}
			node, known := nodeByHex[actor.NodeIDHex]
			if !known || node.Alive == nil || !*node.Alive {
				addWidthReason(fmt.Sprintf(
					"rank %d native actor %s is not at an alive Ray node", rank, *actor.ActorIDHex,
				))
				continue
			}
			uid, claimed := node.Labels[modelDeploymentElasticPodUIDLabelKey]
			member, knownMember := memberByUID[types.UID(uid)]
			if !claimed || !knownMember || member.Role == modelDeploymentElasticRoleHead {
				addWidthReason(fmt.Sprintf(
					"rank %d native actor %s is not at a captured GPU member", rank, *actor.ActorIDHex,
				))
				continue
			}
			alive++
			proofUID = member.PodUID
		}
		if alive != 1 || proofUID == "" {
			addWidthReason(fmt.Sprintf("rank %d has no single alive native actor at a captured GPU member", rank))
			continue
		}
		if tensorParallelSize > 1 {
			if reason := elasticTPBundlesAgree(pg, nodeHexByUID[proofUID], tensorParallelSize); reason != "" {
				addWidthReason(fmt.Sprintf("rank %d: %s", rank, reason))
				continue
			}
		}
		obs.RankToPodUID[rank] = proofUID
	}
	for rank, pg := range pgByRank {
		// 超出预期宽度的 REMOVED 状态 rank 是本次观测所跟踪扩缩容的排空残留；
		// 它属于历史记录，而非正在形成的新宽度。超出预期宽度的任何其他状态
		// 都是引擎仍在持有的原生 rank，会阻塞流程。
		if rank >= int(expectedWidth) && *pg.State != elasticRayPGRemoved {
			addWidthReason(fmt.Sprintf(
				"unexpected native rank %d placement group is %s", rank, pg.StateName,
			))
		}
	}

	obs.RayWidth = len(obs.RankToPodUID)

	// 按捕获的 GPU 成员记录无 actor 事实。成员节点上的所有 actor（无论何种类型）均计入：
	// 非原生残留正是排空绝不能遗留的对象。DEAD actor 属于历史记录；所有非终态都会阻塞。
	// 该事实要求捕获 UID 对应唯一的当前 ALIVE 节点——存在重复声明的节点不能用于读取该事实。
	for _, member := range members {
		if member.Role == modelDeploymentElasticRoleHead {
			continue
		}
		nodeHex, joined := nodeHexByUID[member.PodUID]
		if !joined || duplicateClaimUIDs[member.PodUID] {
			obs.ActorFree[member.PodUID] = modelDeploymentElasticActorFreeFact{
				ActorFree: false,
				Reason: fmt.Sprintf(
					"no unique current ALIVE Ray node claims Pod %s (uid %s)", member.PodName, member.PodUID,
				),
			}
			continue
		}
		blocking := 0
		reason := ""
		for _, actor := range actorsByNode[nodeHex] {
			if *actor.State == elasticRayActorDead {
				continue
			}
			blocking++
			if reason == "" {
				reason = fmt.Sprintf("actor %s (%s) in state %s",
					*actor.ActorIDHex, actor.ClassName, actor.StateName)
			}
		}
		if blocking > 0 {
			obs.ActorFree[member.PodUID] = modelDeploymentElasticActorFreeFact{
				ActorFree: false,
				Reason: fmt.Sprintf(
					"%d nonterminal actor(s) remain on Pod %s: %s",
					blocking, member.PodName, reason,
				),
			}
			continue
		}
		obs.ActorFree[member.PodUID] = modelDeploymentElasticActorFreeFact{ActorFree: true}
	}
	return obs
}

func (o *modelDeploymentElasticObservation) addUnknown(reason string) {
	// 每个未知状态同时也是宽度阻塞：无法证明结构事实的观测同样无法证明宽度，
	// 而在存在结构未知状态的情况下判定出的宽度正是本文件所要杜绝的虚假零值。
	o.UnknownReasons = append(o.UnknownReasons, reason)
	o.WidthKnown = false
	if o.WidthUnknownReason != "" {
		o.WidthUnknownReason += "; "
	}
	o.WidthUnknownReason += reason
}

// unknownElasticObservation 是所有 Observe 阻塞情况返回的形态：未证明任何结论，
// 每个接入点的 Unknown 原因都携带相同的根本原因。
func unknownElasticObservation(reason string) modelDeploymentElasticObservation {
	return modelDeploymentElasticObservation{
		UnknownReasons:             []string{reason},
		WidthUnknownReason:         reason,
		RegisteredGPUUnknownReason: reason,
		RankToPodUID:               map[int]types.UID{},
		ActorFree:                  map[types.UID]modelDeploymentElasticActorFreeFact{},
	}
}

// modelDeploymentElasticObserver 通过现有 exec 传输通道读取单个弹性部署的 Ray 身份信息。
// 它不新增第二套 exec 实现：排空收集器的有界 SPDY 流即为其传输通道，
// 本 observer 仅负责执行脚本、围绕它的 K8s 身份校验以及严格的解析与关联。
type modelDeploymentElasticObserver struct {
	client ctrlcli.Client
	fresh  ctrlcli.Reader
	exec   modelDeploymentDrainExec
	budget time.Duration
}

// newModelDeploymentElasticObserver 基于排空收集器使用的同一传输通道构建生产环境 observer。
// exec 为 nil 时会安装排空收集器的真实实现；测试可通过同一字段进行替换。
func newModelDeploymentElasticObserver(
	client ctrlcli.Client, fresh ctrlcli.Reader, restConfig *rest.Config,
	core corev1client.CoreV1Interface,
) *modelDeploymentElasticObserver {
	drain := newModelDeploymentDrainCollector(client, fresh, restConfig, core)
	return &modelDeploymentElasticObserver{
		client: client,
		fresh:  fresh,
		exec:   drain.exec,
		budget: modelDeploymentElasticObservationBudget,
	}
}

// modelDeploymentElasticIdentity 是读取操作所绑定的前后身份标识：
// 实时部署的 UID 和 generation 在两次非缓存读取之间不得发生变化。
type modelDeploymentElasticIdentity struct {
	UID        types.UID
	Generation int64
}

// Observe 采集并关联一次弹性观测。它采用故障关闭（fail-closed）策略：若成员发生变更、
// 容器未处于运行状态、文档无法解析，或部署身份在首尾两次边界读取之间发生漂移，
// 则返回带有明确原因的 Unknown 状态，绝不将其视为空集群。
func (o *modelDeploymentElasticObserver) Observe(
	ctx context.Context,
	md *workercore.ModelDeployment,
	members []modelDeploymentElasticCapturedMember,
	expectedWidth int32,
	gcsAddress string,
) (modelDeploymentElasticObservation, error) {
	if o.exec == nil {
		return modelDeploymentElasticObservation{}, errors.New("no exec transport is configured")
	}
	if gcsAddress == "" {
		gcsAddress = modelDeploymentElasticDefaultGCSAddress
	}

	ctx, cancel := context.WithTimeout(ctx, o.budget)
	defer cancel()

	before, drift := o.liveIdentity(ctx, md, nil)
	if drift != "" {
		return unknownElasticObservation(drift), nil
	}

	// 在执行 exec 之前，每个捕获的成员都会与实时 Pod 绑定：名称相同、UID 相同、归属当前部署、
	// 且引擎容器正在运行。随后在捕获的 CPU head 容器中执行读取——head 以同样方式
	// 证明其自身捕获的身份，但它绝不计入 GPU 宽度成员。
	var head *corev1.Pod
	for _, member := range members {
		pod, drift := o.liveMember(ctx, md, &before, member)
		if drift != "" {
			return unknownElasticObservation(drift), nil
		}
		if member.Role == modelDeploymentElasticRoleHead {
			head = pod
		}
	}
	if head == nil {
		reason := "captured members name no CPU head to read through"
		return unknownElasticObservation(reason), nil
	}

	argv := []string{
		"python3", "-c", modelDeploymentElasticObserverScript,
		"--gcs", gcsAddress,
		"--deadline-seconds", fmt.Sprintf("%g", modelDeploymentElasticScriptDeadlineSeconds),
		"--max-output-bytes", fmt.Sprintf("%d", modelDeploymentElasticScriptMaxOutputBytes),
	}
	body, err := o.exec(ctx, head, headContainerOf(members), argv)
	if err != nil {
		reason := fmt.Sprintf("ray identity exec failed: %v", err)
		return unknownElasticObservation(reason), nil
	}

	doc, err := parseElasticRayDocument([]byte(body))
	if err != nil {
		reason := fmt.Sprintf("ray identity document refused: %v", err)
		return unknownElasticObservation(reason), nil
	}

	after, drift := o.liveIdentity(ctx, md, &before)
	if drift != "" {
		return unknownElasticObservation(drift), nil
	}
	for _, member := range members {
		if _, drift := o.liveMember(ctx, md, &after, member); drift != "" {
			return unknownElasticObservation(drift), nil
		}
	}

	tp, err := ModelDeploymentElasticTensorParallelSize(ModelDeploymentElasticRole(md))
	if err != nil {
		return unknownElasticObservation(err.Error()), nil
	}
	return joinModelDeploymentElasticRayIdentity(doc, members, expectedWidth, tp), nil
}

// liveIdentity 以非缓存方式读取部署；当 expected 非 nil 时，若 UID 或 generation 发生变更则拒绝继续。
func (o *modelDeploymentElasticObserver) liveIdentity(
	ctx context.Context, md *workercore.ModelDeployment, expected *modelDeploymentElasticIdentity,
) (modelDeploymentElasticIdentity, string) {
	live := &workercore.ModelDeployment{}
	if err := o.fresh.Get(ctx, ctrlcli.ObjectKey{Namespace: md.Namespace, Name: md.Name}, live); err != nil {
		return modelDeploymentElasticIdentity{}, fmt.Sprintf("live deployment read failed: %v", err)
	}
	current := modelDeploymentElasticIdentity{UID: live.UID, Generation: live.Generation}
	if expected != nil && (current.UID != expected.UID || current.Generation != expected.Generation) {
		return modelDeploymentElasticIdentity{}, fmt.Sprintf(
			"deployment identity drifted during observation (uid %s->%s generation %d->%d)",
			expected.UID, current.UID, expected.Generation, current.Generation,
		)
	}
	return current, ""
}

// liveMember 将单个捕获成员绑定到其实时 Pod：名称相同、UID 相同、归属上述捕获的部署，
// 且容器存在并正在运行。
func (o *modelDeploymentElasticObserver) liveMember(
	ctx context.Context, md *workercore.ModelDeployment,
	deployment *modelDeploymentElasticIdentity, member modelDeploymentElasticCapturedMember,
) (*corev1.Pod, string) {
	pod := &corev1.Pod{}
	if err := o.fresh.Get(ctx, ctrlcli.ObjectKey{Namespace: md.Namespace, Name: member.PodName}, pod); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Sprintf("captured Pod %s is gone", member.PodName)
		}
		return nil, fmt.Sprintf("live Pod %s read failed: %v", member.PodName, err)
	}
	if pod.UID != member.PodUID {
		return nil, fmt.Sprintf(
			"Pod %s carries uid %s, captured uid is %s (same-name replacement)",
			member.PodName, pod.UID, member.PodUID,
		)
	}
	owned := false
	for _, ref := range pod.OwnerReferences {
		if ref.UID == deployment.UID {
			owned = true
			break
		}
	}
	if !owned {
		return nil, fmt.Sprintf("Pod %s is not owned by deployment uid %s", member.PodName, deployment.UID)
	}
	running := false
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == member.Container && status.State.Running != nil {
			running = true
			break
		}
	}
	if !running {
		return nil, fmt.Sprintf("Pod %s container %s is not running", member.PodName, member.Container)
	}
	return pod, ""
}

// headContainerOf 选择运行 exec 的容器：即捕获的 CPU head 自身容器。
// head 是 Ray 控制面及其 GCS 监听所在的位置。
func headContainerOf(members []modelDeploymentElasticCapturedMember) string {
	for _, member := range members {
		if member.Role == modelDeploymentElasticRoleHead {
			return member.Container
		}
	}
	return ""
}

// elasticTPBundlesAgree 要求引擎 actor 所在节点上存在完整的 TP 组。
func elasticTPBundlesAgree(pg *elasticRayPGWire, nodeID string, expected int) string {
	seen := map[int]bool{}
	gpus := 0
	for _, bundle := range pg.Bundles {
		if bundle.BundleIndex == nil || *bundle.BundleIndex < 0 || seen[*bundle.BundleIndex] {
			return "TP placement contains missing or duplicate bundle indices"
		}
		seen[*bundle.BundleIndex] = true
		if len(bundle.UnitResources) == 0 {
			return "TP placement bundle states no resources"
		}
		raw, gpu := bundle.UnitResources["GPU"]
		if !gpu {
			continue
		}
		value, err := strconv.ParseFloat(raw, 64)
		if err != nil || math.IsNaN(value) || math.IsInf(value, 0) || value != 1 {
			return "TP placement requires unit-GPU bundles"
		}
		if nodeID == "" || bundle.NodeIDHex != nodeID {
			return "TP placement spans nodes or differs from the native actor's node"
		}
		gpus++
	}
	if gpus != expected {
		return fmt.Sprintf("TP placement carries %d GPU bundles, expected %d", gpus, expected)
	}
	return ""
}
