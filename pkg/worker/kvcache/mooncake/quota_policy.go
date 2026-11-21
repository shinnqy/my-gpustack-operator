package mooncake

import (
	"strings"
	"unicode"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation/field"

	"gpustack.ai/gpustack/pkg/utils/quantityx"
)

// ValidateQuotaPolicyTenantName reports why one name could not be a tenant in the policy file.
//
// It is the LOWER BOUND — what the leader itself refuses — and not a shape this project prefers. A
// caller may hold a name to something stricter; a reuse domain is held to a DNS-1123 label, which
// happens to satisfy every rule here. That makes this call look redundant today, and it is the point
// of calling it anyway: the leader's rules live in one place, so a rule added to them reaches every
// admitting path without anyone remembering to copy it.
func ValidateQuotaPolicyTenantName(name string, fldPath *field.Path) field.ErrorList {
	switch {
	case name == "":
		return field.ErrorList{field.Required(fldPath,
			"a reuse domain with no name is not a tenant the leader can hold quota for")}
	case strings.HasPrefix(name, "_"):
		return field.ErrorList{field.Invalid(fldPath, name,
			`must not start with "_": the leader reserves that prefix and answers `+
				`"Missing or invalid tenant_id"`)}
	// NUL is a control character like any other here, and is called out in the message only because
	// it is the one that truncates rather than garbles.
	case strings.ContainsFunc(name, unicode.IsControl):
		return field.ErrorList{field.Invalid(fldPath, name,
			"must not contain NUL or any other control character: it does not survive the "+
				"policy file the leader loads and rewrites")}
	}

	return nil
}

// ValidateQuotaPolicyQuota reports why one quantity could not be a quota in the policy file.
//
// It is exported because a byte ceiling is refused in more than one place — a Binding's own, and the
// pool total that bounds it — and each has to refuse for the same reasons. A second copy of these two
// rules would drift in the ADMITTING direction, which is the one nothing notices: an object gets in,
// and the fault appears later as a master that will not start.
func ValidateQuotaPolicyQuota(quota resource.Quantity, fldPath *field.Path) field.ErrorList {
	switch {
	// Asked BEFORE the sign, because a quantity above the int64 maximum reports itself as MinInt64
	// or as 0 — refusing it as "not positive" would name a number the input does not contain.
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
