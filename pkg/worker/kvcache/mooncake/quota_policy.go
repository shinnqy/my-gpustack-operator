package mooncake

import (
	"fmt"
	"strings"
	"unicode"

	yaml "gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation/field"

	"gpustack.ai/gpustack/pkg/utils/quantityx"
)

// quotaPolicyVersion 始终写入，当前策略文件格式只支持该版本。
//
// 如果缺少该文件，系统不会崩溃——主节点会抛出 `std::`runtime_error，导致加载器中断并终止，这在刚才还健康的
// Pod 上表现为 CrashLoopBackOff。因此，这是编写者规定的固定值，并非输入字段可以省略的部分。
const quotaPolicyVersion = 1

// QuotaPolicyTenant 表示配额策略的一项，包含复用域名称及其申请的字节上限。
//
// 配额是一个 resource.Quantity 对象，而非计数，因为这是该 API 的存储形式；渲染器是将其转换为字节的地方，而
// quantityx.OverflowsInt64 负责保护这一转换过程。
type QuotaPolicyTenant struct {
	Name  string
	Quota resource.Quantity
}

// quotaPolicyDocument 与主节点加载的文件一致。这里的字段顺序与输出顺序相同，这使得一个版本的渲染与另一个版
// 本的渲染能够清晰区分。
type quotaPolicyDocument struct {
	Version int                   `yaml:"version"`
	Tenants []quotaPolicyDocEntry `yaml:"tenants"`
}

type quotaPolicyDocEntry struct {
	Name string `yaml:"name"`
	// 用 int64 表示字节数，与 master 处理 Admin API 写入后重写文件的格式一致。例如手写的 `quota: 1GB`
	// 会变成 `quota: 1073741824`。直接写入字节数可避免双方反复改写表示形式。
	Quota int64 `yaml:"quota"`
}

// RenderQuotaPolicy 从租户列表生成完整的配额策略文件。
//
// 先调用 ValidateQuotaPolicyTenants 校验全部租户。任何条目无效时返回错误，不生成残缺文件。空列表是合法的，
// 可表示尚无 Binding 的 Pool。
//
// Webhook 和渲染器调用同一组校验规则，确保通过准入的策略也能交给 leader 加载。
func RenderQuotaPolicy(tenants []QuotaPolicyTenant) ([]byte, error) {
	if errs := ValidateQuotaPolicyTenants(tenants, field.NewPath("tenants")); len(errs) > 0 {
		return nil, errs.ToAggregate()
	}

	doc := quotaPolicyDocument{
		Version: quotaPolicyVersion,
		Tenants: make([]quotaPolicyDocEntry, 0, len(tenants)),
	}
	for _, t := range tenants {
		doc.Tenants = append(doc.Tenants, quotaPolicyDocEntry{
			Name:  t.Name,
			Quota: t.Quota.Value(),
		})
	}

	out, err := yaml.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("marshalling the tenant quota policy: %w", err)
	}
	return out, nil
}

// ValidateQuotaPolicyTenants 在 fldPath 下返回所有字段错误，涵盖会被 leader 拒绝或导致它无法加载的输入。
//
// 在准入时就报告这些错误，避免控制器先发布无效策略，直到管理接口返回 400 或进程无法启动时才暴露问题。下面逐
// 项检查 leader 自身的限制。
//
//   - 租户名必须非空、在本组内唯一，且不能以 "_" 开头。leader 会以 `Missing or invalid tenant_id` 拒绝无效
//     名称，并为内部用途保留下划线前缀。
//   - 名称不能含 NUL 或控制字符，以便用策略文件写入并由 leader 读回。
//   - 配额必须严格大于零，并可表示为 int64 字节数。leader 会返回 `Tenant quota must be positive`。超出
//     int64 上限的值转换后可能表现为非正数，详见 quantityx.OverflowsInt64。
//
// 汇总所有有问题的条目，使管理员一次提交就能看到全部字段错误。
func ValidateQuotaPolicyTenants(tenants []QuotaPolicyTenant, fldPath *field.Path) field.ErrorList {
	var errs field.ErrorList

	seen := make(map[string]struct{}, len(tenants))
	for i, t := range tenants {
		namePath := fldPath.Index(i).Child("name")
		if nameErrs := ValidateQuotaPolicyTenantName(t.Name, namePath); len(nameErrs) > 0 {
			errs = append(errs, nameErrs...)
		} else if _, ok := seen[t.Name]; ok {
			// 唯一性应保留在此处，而非按名称规则来定义，因为这是集合的属性，而非某个特定名称的
			// 属性。第二个主张被拒绝，而非第一个：每个名称对应一个账本记录，意味着“最后胜者合并”
			// 会让该域获得其从未要求的绑定配额。
			errs = append(errs, field.Duplicate(namePath, t.Name))
		} else {
			seen[t.Name] = struct{}{}
		}

		errs = append(errs, ValidateQuotaPolicyQuota(t.Quota, fldPath.Index(i).Child("quota"))...)
	}

	return errs
}

// ValidateQuotaPolicyTenantName 报告某个名称为何不能作为策略文件中的租户名。
//
// 这里检查的是下界——即 leader 自身会拒绝的条件——而非本项目偏好的命名形态。
// 调用方可以对名称施加更严格的限制；例如复用域要求满足 DNS-1123 标签规范，
// 它恰好已经满足此处的全部规则。这使得该调用如今看起来有些多余，但仍坚持调用它的原因
// 正在于此：leader 的规则集中在一处维护，一旦其中新增规则，无需任何人记住去复制，
// 即可自动覆盖所有准入路径。
func ValidateQuotaPolicyTenantName(name string, fldPath *field.Path) field.ErrorList {
	switch {
	case name == "":
		return field.ErrorList{field.Required(fldPath,
			"a reuse domain with no name is not a tenant the leader can hold quota for")}
	case strings.HasPrefix(name, "_"):
		return field.ErrorList{field.Invalid(fldPath, name,
			`must not start with "_": the leader reserves that prefix and answers `+
				`"Missing or invalid tenant_id"`)}
	// NUL 在此处与其他控制字符无异，仅因它会截断而非造成乱码而在错误消息中单独提及。
	case strings.ContainsFunc(name, unicode.IsControl):
		return field.ErrorList{field.Invalid(fldPath, name,
			"must not contain NUL or any other control character: it does not survive the "+
				"policy file the leader loads and rewrites")}
	}

	return nil
}

// ValidateQuotaPolicyQuota 报告某个数量为何不能作为策略文件中的配额。
//
// 将其导出是因为字节上限会在不止一处被拒绝——包括 Binding 自身的配额以及限制它的
// 池子总量——且各处必须出于相同的原因拒绝。如果把这两条规则再复制一份，它们就可能
// 朝"放行"方向漂移，而这种漂移是无人会察觉的：对象会被准入，故障则推迟到 master
// 无法启动时才暴露。
func ValidateQuotaPolicyQuota(quota resource.Quantity, fldPath *field.Path) field.ErrorList {
	switch {
	// 在判断符号之前先检查溢出，因为超过 int64 最大值的数量会报告为 MinInt64 或 0——
	// 若以"非正数"拒绝它，错误信息里就会给出一个输入中并不存在的数字。
	case quantityx.OverflowsInt64(quota):
		return field.ErrorList{field.Invalid(fldPath, quota.String(),
			"must not exceed 9223372036854775807 (2^63-1) bytes: the policy file carries a "+
				"signed 64-bit count, and a larger one does not survive the conversion")}
	case quota.CmpInt64(0) <= 0:
		return field.ErrorList{field.Invalid(fldPath, quota.String(),
			`must be greater than 0: the leader answers "Tenant quota must be positive", and a `+
				"tenant with no quota is one that can write nothing")}
	}

	return nil
}
