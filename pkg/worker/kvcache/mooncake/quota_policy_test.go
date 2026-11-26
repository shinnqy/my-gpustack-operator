package mooncake

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// TestQuotaPolicy 在每一行中同时控制两个入口点：即 webhook 调用的验证器以及控制器调用的渲染器。这就是规范中
// 的 webhook_and_reconciler_same_verdict 情况，该情况适用于所有输入，而非仅一次——当两者对某个裁决意见不一致
// 时，webhook 会准入该文件，而 Master 则会将其中止。
//
// 每一行拒绝记录都表明没有任何数据返回，因为包含被删除不良条目的文件不存在任何配额限制。
func TestQuotaPolicy(t *testing.T) {
	cases := []struct {
		name    string
		tenants []QuotaPolicyTenant
		// 整个文件都符合要求，确切地说：只有当渲染器的输出可比较时，它才具有可对比性。它仅在被接受
		// 的行中才有意义。
		want string
		// wantFields 是拒绝必须命名的字段路径，按顺序排列。为空表示接受。
		wantFields []string
	}{
		{
			name: "renders_version_unconditionally",
			tenants: []QuotaPolicyTenant{
				{Name: "team-a", Quota: resource.MustParse("1Gi")},
			},
			want: "version: 1\ntenants:\n    - name: team-a\n      quota: 1073741824\n",
		},
		{
			name: "renders_quota_in_bytes",
			tenants: []QuotaPolicyTenant{
				{Name: "team-a", Quota: resource.MustParse("1G")},
			},
			want: "version: 1\ntenants:\n    - name: team-a\n      quota: 1000000000\n",
		},
		{
			name: "empty_tenant_set",
			want: "version: 1\ntenants: []\n",
		},
		{
			name: "quota_overflows_int64",
			tenants: []QuotaPolicyTenant{
				{Name: "team-a", Quota: resource.MustParse("9223372036854775808")},
				{Name: "team-b", Quota: resource.MustParse("1e30")},
			},
			wantFields: []string{"tenants[0].quota", "tenants[1].quota"},
		},
		{
			name: "name_empty",
			tenants: []QuotaPolicyTenant{
				{Name: "", Quota: resource.MustParse("1Gi")},
			},
			wantFields: []string{"tenants[0].name"},
		},
		{
			name: "name_leading_underscore",
			tenants: []QuotaPolicyTenant{
				{Name: "_bad", Quota: resource.MustParse("1Gi")},
			},
			wantFields: []string{"tenants[0].name"},
		},
		{
			name: "name_duplicate",
			tenants: []QuotaPolicyTenant{
				{Name: "team-a", Quota: resource.MustParse("1Gi")},
				{Name: "team-a", Quota: resource.MustParse("2Gi")},
			},
			wantFields: []string{"tenants[1].name"},
		},
		{
			name: "name_control_character",
			tenants: []QuotaPolicyTenant{
				{Name: "team\x01a", Quota: resource.MustParse("1Gi")},
			},
			wantFields: []string{"tenants[0].name"},
		},
		{
			name: "name_nul",
			tenants: []QuotaPolicyTenant{
				{Name: "team\x00a", Quota: resource.MustParse("1Gi")},
			},
			wantFields: []string{"tenants[0].name"},
		},
		{
			name: "quota_zero",
			tenants: []QuotaPolicyTenant{
				{Name: "team-a", Quota: resource.MustParse("0")},
			},
			wantFields: []string{"tenants[0].quota"},
		},
		{
			name: "quota_negative",
			tenants: []QuotaPolicyTenant{
				{Name: "team-a", Quota: resource.MustParse("-1")},
			},
			wantFields: []string{"tenants[0].quota"},
		},
		{
			name: "one_bad_entry_yields_no_file",
			tenants: []QuotaPolicyTenant{
				{Name: "team-a", Quota: resource.MustParse("1Gi")},
				{Name: "_bad", Quota: resource.MustParse("2Gi")},
			},
			wantFields: []string{"tenants[1].name"},
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			errs := ValidateQuotaPolicyTenants(c.tenants, field.NewPath("tenants"))
			got, err := RenderQuotaPolicy(c.tenants)

			if len(c.wantFields) > 0 {
				fields := make([]string, 0, len(errs))
				for _, e := range errs {
					fields = append(fields, e.Field)
				}
				assert.Equal(t, c.wantFields, fields, "the validator must name every refused input")
				require.Error(t, err, "the renderer must refuse what the validator refuses")
				assert.Nil(t, got, "a refused input yields no file at all, not a shortened one")
				return
			}

			assert.Empty(t, errs, "the validator must accept what the renderer renders")
			require.NoError(t, err)
			assert.Equal(t, c.want, string(got))
		})
	}
}

// TestRenderQuotaPolicyRefusalNamesEveryEntry 检查拒绝消息保留逐项字段错误及其索引，管理员由此可以定位每个无效租户。
func TestRenderQuotaPolicyRefusalNamesEveryEntry(t *testing.T) {
	_, err := RenderQuotaPolicy([]QuotaPolicyTenant{
		{Name: "", Quota: resource.MustParse("1Gi")},
		{Name: "team-b", Quota: resource.MustParse("0")},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "tenants[0].name")
	assert.Contains(t, err.Error(), "tenants[1].quota")
}

// TestRenderQuotaPolicyBeginsWithVersion 会断言 Master 自行终止的属性：省略版本号 1 会导致抛出
// std::runtime_error 和 CrashLoopBackOff 异常，因此任何输入都无法输出不以该版本号开头的结果。
func TestRenderQuotaPolicyBeginsWithVersion(t *testing.T) {
	for _, tenants := range [][]QuotaPolicyTenant{
		nil,
		{},
		{{Name: "team-a", Quota: resource.MustParse("1Gi")}},
	} {
		got, err := RenderQuotaPolicy(tenants)
		require.NoError(t, err)
		assert.True(t, strings.HasPrefix(string(got), "version: 1\n"), "got %q", string(got))
	}
}
