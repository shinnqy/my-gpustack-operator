package mooncake

import (
	"strings"
	"unicode"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation/field"

	"gpustack.ai/gpustack/pkg/utils/quantityx"
)

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
