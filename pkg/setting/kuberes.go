package setting

import (
	"context"
	"strings"

	"github.com/google/uuid"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/pkg/kubeclients/kubernetes"
	"gpustack.ai/gpustack/pkg/kubeclientset"
	"gpustack.ai/gpustack/pkg/kubeclientset/review"
	"gpustack.ai/gpustack/pkg/systemmeta"
	"gpustack.ai/gpustack/pkg/systemname"
)

var (
	// DelegatedSecretNamespace 是用于托管配置项的 Kubernetes Secret 所在 Namespace。
	DelegatedSecretNamespace = systemname.NamespaceName

	// DelegatedSecretName 是用于托管配置项的 Kubernetes Secret 名称。
	DelegatedSecretName = "gpustack-settings"
)

// DefaultTrackingName 是一个 Setting 的名称，其逗号分隔的值列出了遵循 operator 内置默认值
// 而非使用管理员存储值的各项 Setting。
//
// 对齐过程会按该名称在配置注册表中解析它，而注册表也以同一名称注册该项。两者必须保持一致：
// 若以其他名称注册，读取逻辑与追踪列表均无法匹配到它，从而导致没有任何 Setting 被追踪。
const DefaultTrackingName = "settings-default-tracking"

// Initialize 初始化配置项所需的 Kubernetes 资源。
//
// Initialize 会创建用于托管配置项的 Kubernetes Secret。
func (h Settings) Initialize(ctx context.Context, cli kubernetes.Interface) error {
	err := review.CanDoUpdate(ctx,
		cli.AuthorizationV1().SelfSubjectAccessReviews(),
		review.Simples{
			{
				Group:    core.SchemeGroupVersion.Group,
				Version:  core.SchemeGroupVersion.Version,
				Resource: "secrets",
			},
		},
		review.WithCreateIfNotExisted(),
	)
	if err != nil {
		return err
	}

	secCli := cli.CoreV1().Secrets(DelegatedSecretNamespace)

	eSec := &core.Secret{
		ObjectMeta: meta.ObjectMeta{
			Name: DelegatedSecretName,
		},
		Data: map[string][]byte{},
	}
	eResType := "settings"
	eNotes := map[string]string{}
	for name, setting := range h {
		eSec.Data[name] = []byte(setting.defVal)
		eNotes[name+"-uid"] = uuid.NewString()
	}
	systemmeta.NoteResource(eSec, eResType, eNotes)
	alignFn := func(aSec *core.Secret) (_ *core.Secret, skip bool, err error) {
		skip = true
		// 对齐托管元信息。
		if !systemmeta.EqualResourceTypeAndNotes(eSec, aSec) {
			systemmeta.NoteResource(aSec, eResType, eNotes)
			skip = false
		}
		// 对齐数据。在评估任何规则之前，先回填缺失的键，以便默认值为空的新注册配置项仍能写入，
		// 且早于追踪配置项创建的 Secret 也能在同一轮对齐中补齐该键。
		for k := range eSec.Data {
			if _, ok := aSec.Data[k]; !ok {
				aSec.Data[k] = eSec.Data[k]
				skip = false
			}
		}
		tracking := parseTrackedSettings(h, trackingValue(aSec))
		for name, s := range h {
			stored := string(aSec.Data[name])
			desired, rule := s.desiredValue(tracking)
			if desired == "" || desired == stored {
				// 这些规则绝不会写入空值，且相等的值不构成覆盖，
				// 因此未发生任何变更的对齐过程不会产生日志。
				continue
			}
			aSec.Data[name] = []byte(desired)
			skip = false
			klog.InfoS("setting overridden on start", s.overrideLogPairs(stored, desired, rule)...)
		}
		return aSec, skip, nil
	}

	_, err = kubeclientset.Update(ctx, secCli, eSec,
		kubeclientset.WithUpdateAlign(alignFn),
		kubeclientset.WithCreateIfNotExisted[*core.Secret]())
	if err != nil {
		return err
	}

	return nil
}

// sensitiveValuesWithheld 在覆盖日志行中同时替代敏感 Setting 的新旧两个值。它是一个任何实际值
// 都不会与之相等的字面量，使阅读者能够将其与空值（表示 Setting 未保存内容）以及缺失的键值对
// （表示 operator 从未写入）区分开来。
const sensitiveValuesWithheld = "[withheld: sensitive setting]"

// overrideLogPairs 根据被替换的存储值、新写入的值以及触发覆盖的规则，返回该 Setting 在
// 覆盖日志行中携带的键值对。
//
// 敏感 Setting 的日志行不会以任何形式包含这两个值。日志的可见范围比托管 Secret 更广：
// 通常具有更高的读取权限覆盖面，且会被收集至日志聚合器、CI 输出和备份中，因此 operator 刻意
// 避免泄露的内容绝不能出现在日志里。配置名称、规则与环境变量名仍会保留，因为这三者足以向
// 运维人员说明哪个 Setting 发生了变更及其原因，且都不包含配置值本身。
//
// 非敏感 Setting 则按原有的键值对及顺序保留与此前一致的日志内容。
func (s Setting) overrideLogPairs(stored, desired, rule string) []any {
	pairs := []any{"setting", s.name}
	if s.Sensitive() {
		pairs = append(pairs, "values", sensitiveValuesWithheld)
	} else {
		pairs = append(pairs, "stored", stored, "value", desired)
	}
	pairs = append(pairs, "rule", rule)
	if s.envName != "" {
		pairs = append(pairs, "envName", s.envName)
	}
	return pairs
}

// trackingValue 从当前 Secret 中返回原始追踪列表，若 Secret 中无此键则返回空字符串。
// Secret 是对齐过程唯一查询的来源，因为此前的回填步骤已经将内置默认值写入早于该配置项
// 创建的 Secret 中，所以此时 Secret 中保存的要么是管理员配置的列表，要么就是该默认值。
func trackingValue(aSec *core.Secret) string {
	return string(aSec.Data[DefaultTrackingName])
}

// parseTrackedSettings 返回原始追踪列表中所指明的已注册配置项集合。它会按逗号分割、去除各段
// 首尾空白，并丢弃空段、重复项、追踪配置项自身名称以及注册表中不存在的名称，从而防止存储值
// 追踪不存在的配置项。每个被丢弃的片段都会以 verbosity 4 级别记录一次日志并附带原因。
func parseTrackedSettings(h Settings, raw string) sets.Set[string] {
	tracked := sets.New[string]()
	for _, segment := range strings.Split(raw, ",") {
		name := strings.TrimSpace(segment)
		reason := ""
		switch {
		case name == "":
			reason = "blank"
		case tracked.Has(name):
			reason = "duplicate"
		case name == DefaultTrackingName:
			reason = "self-reference"
		default:
			if _, ok := h.Index(name); !ok {
				reason = "not registered"
			}
		}
		if reason != "" {
			klog.V(4).InfoS("tracked setting segment dropped",
				"setting", DefaultTrackingName, "segment", segment, "reason", reason)
			continue
		}
		tracked.Insert(name)
	}
	return tracked
}

// desiredValue 返回 operator 对该 Setting 持有的期望值及主张该值的规则名称。
// 若规则为空，表示 operator 对此不作管控，沿用管理员存储的值。
//
// 当某项 Setting 的种子值非空，且环境变量显式声明了该项或追踪列表包含该项时，operator 会写入该值。
// 若环境变量存在但其值为空，则会得到空种子值——因为内置默认值仅在环境变量缺失时才进行替换，
// 所以即使该 Setting 拥有默认值，空值声明也不会触发任何写入。
func (s Setting) desiredValue(tracked sets.Set[string]) (value, rule string) {
	if isBlank(s.defVal) {
		return "", ""
	}
	if s.declared {
		return s.defVal, "env-declared"
	}
	if tracked.Has(s.name) {
		return s.defVal, "default-tracked"
	}
	return "", ""
}
