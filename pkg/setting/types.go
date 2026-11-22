package setting

import (
	"context"
	"fmt"
	"strconv"
	"time"

	cache "github.com/patrickmn/go-cache"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"

	"gpustack.ai/gpustack/pkg/kubeclientset"
	"gpustack.ai/gpustack/pkg/system"
	"gpustack.ai/gpustack/pkg/utils/funcx"
)

// Prop 表示配置项的属性，可以是 editable、private 和 sensitive 的组合。
type Prop uint8

const (
	PropEditable Prop = 1 << iota
	PropPrivate
	PropSensitive
)

type (
	// Setting 表示服务端配置项，包含名称、描述、默认值、准入校验函数以及属性。
	Setting struct {
		name        string
		description string
		defVal      string
		// envName 是注册期间读取的环境变量名称；若未使用环境变量则为空。
		envName string
		// declared 表示注册期间 envName 是否存在（包括其值为空的情况）。
		declared bool
		admit    Admission
		props    Prop
	}

	// IndexFunc 是一个函数类型，接收配置项名称并返回对应的 Setting 及表示该配置项是否存在的布尔值。
	IndexFunc = func(name string) (Setting, bool)
)

// Name 返回设置项的名称。
func (s Setting) Name() string {
	return s.name
}

// Description 返回设置项的描述。
func (s Setting) Description() string {
	return s.description
}

func (s Setting) DefaultValue() string {
	return s.defVal
}

// Editable 在设置项可编辑时返回 true。
func (s Setting) Editable() bool {
	return s.props&PropEditable == PropEditable
}

// Sensitive 在设置项为敏感项时返回 true。
func (s Setting) Sensitive() bool {
	return s.props&PropSensitive == PropSensitive
}

// Private 在设置项为私有项时返回 true。
func (s Setting) Private() bool {
	return s.props&PropPrivate == PropPrivate
}

// Configure 配置设置项的值。
func (s Setting) Configure(ctx context.Context, newVal string) error {
	lpCli := system.LoopbackKubeClient.Get()

	// 更新。
	eSec := &core.Secret{
		ObjectMeta: meta.ObjectMeta{
			Namespace: DelegatedSecretNamespace,
			Name:      DelegatedSecretName,
		},
	}
	alignFn := func(aSec *core.Secret) (*core.Secret, bool, error) {
		var oldVal string
		if aSec.Data != nil {
			oldVal = string(aSec.Data[s.name])
		}
		if admitErr := s.admit(ctx, oldVal, newVal); admitErr != nil {
			// NB(thxCode): 如果新值无效，则跳过更新。
			return nil, false, admitErr
		}
		if oldVal == newVal {
			// 如果新值与旧值相同，则跳过更新。
			return nil, true, nil
		}
		// 更新设置项的值。
		aSec.Data[s.name] = []byte(newVal)
		return aSec, false, nil
	}

	secCli := lpCli.CoreV1().Secrets(DelegatedSecretNamespace)
	_, err := kubeclientset.Update(ctx, secCli, eSec,
		kubeclientset.WithUpdateAlign(alignFn))
	if err != nil {
		return fmt.Errorf("configure setting %s: %w", s.name, err)
	}

	return nil
}

// ReadCacheTTL 是读取设置值时从缓存提供服务的时长：变更在保存后最多经过此时间即可到达读取方。
const ReadCacheTTL = 30 * time.Second

// _Cache 是设置项值的缓存，
// 用于减少对 Kubernetes API server 的 API 调用次数。
var _Cache = cache.New(ReadCacheTTL, cache.NoExpiration)

// ValueFromRemote 通过直接访问 Kubernetes API server 中的托管 Secret 返回设置项的值，
// 用于远程访问且不经过 controller-runtime 客户端缓存。
//
// 这适用于 controller-runtime 客户端缓存尚未同步完成，
// 或因某些原因需要绕过缓存的场景。
// 但这可能会带来性能影响，应谨慎使用。
//
// 如果在托管 Secret 中未找到该值，则返回设置项的默认值。
func (s Setting) ValueFromRemote(ctx context.Context) (string, error) {
	if val, found := _Cache.Get(s.name); found {
		return val.(string), nil
	}

	lpCli := system.LoopbackKubeClient.Get()

	sec, err := lpCli.CoreV1().
		Secrets(DelegatedSecretNamespace).
		Get(ctx, DelegatedSecretName,
			meta.GetOptions{})
	if err != nil {
		return s.defVal, fmt.Errorf("get value of setting %s: %w", s.name, err)
	}

	if sec.Data == nil || sec.Data[s.name] == nil {
		return s.defVal, fmt.Errorf("get value of setting %s: not found", s.name)
	}

	ret := string(sec.Data[s.name])
	_Cache.Set(s.name, ret, cache.DefaultExpiration)
	return ret, nil
}

// ShouldValueFromRemote 从远程返回设置项的值并忽略 error。
func (s Setting) ShouldValueFromRemote(ctx context.Context) string {
	return funcx.NoError(s.ValueFromRemote(ctx))
}

// ValueBoolFromRemote 从远程读取并返回设置项的布尔值（参见 ValueFromRemote）。
// 相比将 ShouldValueFromRemote 与字面量 "true" 比较，更推荐使用此方法：布尔准入校验
// （AllowBool）接受任意 strconv.ParseBool 形式（"1"/"TRUE"/...），因此它与 ValueBool 保持一致。
func (s Setting) ValueBoolFromRemote(ctx context.Context) (bool, error) {
	// 即使发生 error，ValueFromRemote 也会在 valStr 中返回设置项的默认值，因此无条件解析 valStr
	// 以遵循“未找到时使用默认值”的契约，同时一并返回获取时的 error。
	valStr, err := s.ValueFromRemote(ctx)
	b, perr := strconv.ParseBool(valStr)
	if err != nil {
		return b, err
	}
	return b, perr
}

// ShouldValueBoolFromRemote 从远程返回设置项的布尔值并忽略 error。
func (s Setting) ShouldValueBoolFromRemote(ctx context.Context) bool {
	return funcx.NoError(s.ValueBoolFromRemote(ctx))
}

// Value 返回设置项的值。
//
// 如果在托管 Secret 中未找到该值，则返回设置项的默认值。
func (s Setting) Value(ctx context.Context) (string, error) {
	if val, found := _Cache.Get(s.name); found {
		return val.(string), nil
	}

	lpCli := system.LoopbackCtrlClient.Get()

	sec := &core.Secret{
		ObjectMeta: meta.ObjectMeta{
			Name:      DelegatedSecretName,
			Namespace: DelegatedSecretNamespace,
		},
	}
	err := lpCli.Get(ctx, ctrlcli.ObjectKeyFromObject(sec), sec,
		&ctrlcli.GetOptions{
			Raw: &meta.GetOptions{
				ResourceVersion: "0",
			},
		})
	if err != nil {
		return s.defVal, fmt.Errorf("get value of setting %s: %w", s.name, err)
	}

	if sec.Data == nil || sec.Data[s.name] == nil {
		return s.defVal, fmt.Errorf("get value of setting %s: not found", s.name)
	}

	ret := string(sec.Data[s.name])
	_Cache.Set(s.name, ret, cache.DefaultExpiration)
	return ret, nil
}

// ShouldValue 返回设置项的值并忽略 error。
func (s Setting) ShouldValue(ctx context.Context) string {
	return funcx.NoError(s.Value(ctx))
}

// ValueBool 返回设置项的布尔值。
//
// 读取失败时会回退到设置项的默认值而非 false，并将读取 error 一并返回。
// 调用方通常通过 ShouldValueBool 读取并丢弃 error，如果返回 false，单次读取失败就会让
// 所有默认开启的设置项变成“关闭”行为，且读取它的 reconcile 会将该行为写回。
// 若调用方要求读取错误必须视为 false（例如授予访问权限的门控），应自行检查该 error。
func (s Setting) ValueBool(ctx context.Context) (bool, error) {
	// Value 在出错时会在 valStr 中返回默认值，因此与 ValueBoolFromRemote 一样
	// 对其进行无条件解析。
	valStr, err := s.Value(ctx)
	b, perr := strconv.ParseBool(valStr)
	if err != nil {
		return b, err
	}
	return b, perr
}

// ShouldValueBool 返回设置项的布尔值并忽略 error。
func (s Setting) ShouldValueBool(ctx context.Context) bool {
	return funcx.NoError(s.ValueBool(ctx))
}

type Settings map[string]Setting

// New 根据给定的名称、描述、属性、初始化器和准入校验器创建新的设置项，
// 并将其注册到 settings 映射中。
func (h Settings) New(
	name, description string, props Prop,
	init Initializer, admits ...Admission,
) Setting {
	defVal, envName, declared := init(name)
	s := Setting{
		name:        name,
		defVal:      defVal,
		envName:     envName,
		declared:    declared,
		admit:       AdmitWith(admits...),
		description: description,
		props:       props,
	}
	h[name] = s
	return s
}

// NewEditable 根据给定的名称、描述、初始化器和准入校验器创建新的可编辑设置项。
// 该设置项可在 UI 上编辑并由用户修改其值，但不会在 UI 和日志中隐藏。
func (h Settings) NewEditable(
	name, description string,
	init Initializer, admits ...Admission,
) Setting {
	return h.New(
		name, description, PropEditable,
		init, admits...,
	)
}

// Index 返回指定名称的设置项。
func (h Settings) Index(name string) (Setting, bool) {
	s, ok := h[name]
	return s, ok
}
