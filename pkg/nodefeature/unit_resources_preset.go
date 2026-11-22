package nodefeature

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/yaml"

	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/utils/strconvx"

	_ "embed"
)

//go:embed unit_resources_preset.yaml
var _UnitResourcesPresetYAML []byte

// _UnitResourcesPresetTable 保存解析并校验过的预设表。格式错误属于程序配置错误，加载时会触发 panic。
// 表在包初始化时加载，发布前需要通过校验测试发现错误。
var _UnitResourcesPresetTable = mustLoadUnitResourcesPresets(_UnitResourcesPresetYAML)

// _UnitResourcesPresetTierNames 按从小到大的顺序列出档位。识别出的产品必须高于未识别产品的默认档位。
var _UnitResourcesPresetTierNames = []string{"fallback", "small", "medium", "large", "xlarge"}

// _UnitResourcesPresetManufacturers 列出表中允许的厂商键。包级变量先于 init 初始化，
// 这时 knowns.go 中的厂商表还未填充，因此直接使用厂商常量。单元测试核对两者一致。
var _UnitResourcesPresetManufacturers = sets.New(
	ManufacturerAMD,
	ManufacturerAscend,
	ManufacturerCambricon,
	ManufacturerHygon,
	ManufacturerIluvatar,
	ManufacturerMetaX,
	ManufacturerMThreads,
	ManufacturerNVIDIA,
	ManufacturerTHead,
)

const (
	// _UnitResourcesPresetFallbackTierName 是产品未匹配时使用的档位名。
	_UnitResourcesPresetFallbackTierName = "fallback"

	// _UnitResourcesPresetRAMSuffix 是 InstanceType 准入 webhook 对 RAM 唯一接受的单位后缀，
	// 大小写必须一致。
	_UnitResourcesPresetRAMSuffix = "Gi"

	// _UnitResourcesPresetTokenSeparators 列出 device.NormalizeName 在产品名各词之间保留的分隔符。
	_UnitResourcesPresetTokenSeparators = "-_."
)

// _UnitResourcesPresetFallbackTier 固定未识别产品的默认资源量，沿用引入预设表前的值，
// 防止修改表项时意外改变既有行为。
var _UnitResourcesPresetFallbackTier = unitResourcesPresetTier{CPU: "4", RAM: "16Gi"}

// unitResourcesPresetTier 表示一个档位，保存每张整卡对应的 CPU 和 RAM。
// 两者都使用 InstanceType 中的字符串格式，加载时即可按准入 webhook 的规则校验。
type unitResourcesPresetTier struct {
	CPU string `json:"cpu"`
	RAM string `json:"ram"`
}

// unitResourcesPresetEntry 对应一家厂商的一个加速器系列。Prefix 是完整词的序列，在同一厂商内唯一，
// 也不会成为其他条目的词前缀，因此产品最多匹配一项。By 按顺序区分同一芯片的具体变体，
// 是表中唯一依赖声明顺序的部分。
type unitResourcesPresetEntry struct {
	Prefix string                       `json:"prefix"`
	Family string                       `json:"family"`
	Tier   string                       `json:"tier"`
	By     []unitResourcesPresetVariant `json:"by,omitempty"`
}

// unitResourcesPresetVariant 在产品键中出现完整的 Token 时细分档位，例如容量标识 80gb。
type unitResourcesPresetVariant struct {
	Token  string `json:"token"`
	Family string `json:"family"`
	Tier   string `json:"tier"`
}

// unitResourcesPresets 包含所有档位、需要剥离的品牌词前缀，以及按厂商组织的产品匹配项。
type unitResourcesPresets struct {
	Tiers    map[string]unitResourcesPresetTier    `json:"tiers"`
	Strip    []string                              `json:"strip"`
	Families map[string][]unitResourcesPresetEntry `json:"families"`
}

// PresetUnitResources 根据加速器厂商和产品返回派生 InstanceType 的每卡 CPU 和 RAM。
// 规范化后的产品名最多匹配一个前缀，随后按变体声明顺序确定档位。
// 厂商未收录、产品未匹配或输入无法规范化时，沿用引入预设前的默认资源值。
// 返回值满足准入 webhook 的规格规则，整个查找无需 I/O，相同输入得到相同结果。
//
// 调用 device.NormalizeName 时把 maxLength 设为零，避免在词中间截断名称、丢失末尾的容量标识。
// 它仅在当前缓冲区长度恰好等于厂商名长度时去掉厂商名，
// 多词品牌（如 Meta X、Moore Threads）要靠剥离规则处理。它也不检查词边界，因此厂商为 amd 时，
// AMD64 Accelerator 会变为 64-accelerator。
func PresetUnitResources(manufacturer, product string) (cpu, ram string) {
	return _UnitResourcesPresetTable.lookup(manufacturer, product)
}

// ResolveAcceleratorFamily 将加速器原始名称解析为产品系列名。
//
// 基准测试通常只给出完整的产品名。名称本身是系列名时直接返回，带厂商前缀时在该厂商内查找，
// 其余情况跨厂商匹配。结果对应多个不同系列时视为无法识别。
func ResolveAcceleratorFamily(raw string) string {
	return _UnitResourcesPresetTable.resolveAcceleratorFamily(raw)
}

// acceleratorFamily 在指定厂商的配置中查找产品对应的系列。
func (t *unitResourcesPresets) acceleratorFamily(manufacturer, product string) string {
	entries := t.Families[manufacturer]
	if len(entries) == 0 {
		return ""
	}

	key := device.NormalizeName(product, manufacturer, 0, false)
	tokens := t.stripLeadingMarketing(splitPresetTokens(key))
	if len(tokens) == 0 {
		return ""
	}

	for _, e := range entries {
		if !hasTokenPrefix(tokens, splitPresetTokens(e.Prefix)) {
			continue
		}
		return presetEntryFamily(e, tokens)
	}
	return ""
}

// presetEntryFamily 解析一个条目的系列名。如果有变体，
// 则使用列表中第一个在产品键里完整出现的词所对应的系列。
func presetEntryFamily(e unitResourcesPresetEntry, tokens []string) string {
	for _, v := range e.By {
		if slices.Contains(tokens, v.Token) {
			return v.Family
		}
	}
	return e.Family
}

// resolveAcceleratorFamily 将原始加速器名称解析为表中声明的系列。
func (t *unitResourcesPresets) resolveAcceleratorFamily(raw string) string {
	tokens := splitPresetTokens(device.NormalizeName(raw, "", 0, false))
	if len(tokens) == 0 {
		return ""
	}

	// 名称本身已是系列名时直接返回，使保存了系列名的目录能够原样读回。
	if name := strings.Join(tokens, "-"); t.declaresFamily(name) {
		return name
	}

	// 名称带厂商前缀时，只在该厂商的条目中解析产品。
	if _UnitResourcesPresetManufacturers.Has(tokens[0]) {
		return t.acceleratorFamily(tokens[0], raw)
	}

	return t.resolveFamilyTokens(tokens)
}

// resolveFamilyTokens 根据产品名称的词序列查找系列。
//
// 产品名前面可能有不属于任何厂商的词，因此逐个尝试后缀，并要求所有匹配结果指向同一系列。
func (t *unitResourcesPresets) resolveFamilyTokens(tokens []string) string {
	stripped := t.stripLeadingMarketing(tokens)
	if family := t.matchFamily(stripped); family != "" {
		return family
	}

	families := sets.New[string]()
	for i := 1; i < len(stripped); i++ {
		if family := t.matchFamily(stripped[i:]); family != "" {
			families.Insert(family)
		}
	}
	if families.Len() != 1 {
		return ""
	}
	return families.UnsortedList()[0]
}

// matchFamily 返回与给定词序列匹配的系列。
//
// 同一个名称可能指向不同系列，例如 v100 既指 NVIDIA 产品，也可能是天数智芯板卡名称的末尾。
// 遇到这种歧义时返回空值。
func (t *unitResourcesPresets) matchFamily(tokens []string) string {
	families := sets.New[string]()
	for _, entries := range t.Families {
		for _, e := range entries {
			prefix := splitPresetTokens(e.Prefix)
			if !hasTokenPrefix(tokens, prefix) && !hasTokenSuffix(prefix, tokens) {
				continue
			}
			families.Insert(presetEntryFamily(e, tokens))
		}
	}
	if families.Len() != 1 {
		return ""
	}
	return families.UnsortedList()[0]
}

// declaresFamily 检查该名称是否是表中声明的系列。
func (t *unitResourcesPresets) declaresFamily(name string) bool {
	for _, entries := range t.Families {
		for _, e := range entries {
			if e.Family == name {
				return true
			}
			for _, v := range e.By {
				if v.Family == name {
					return true
				}
			}
		}
	}
	return false
}

// lookup 根据厂商和产品查找预设。只有厂商在表中有条目时才规范化产品名。
// 加载校验保证表中厂商与可探测厂商一致。
func (t *unitResourcesPresets) lookup(manufacturer, product string) (cpu, ram string) {
	fallback := t.Tiers[_UnitResourcesPresetFallbackTierName]

	entries := t.Families[manufacturer]
	if len(entries) == 0 {
		return fallback.CPU, fallback.RAM
	}

	key := device.NormalizeName(product, manufacturer, 0, false)
	tokens := t.stripLeadingMarketing(splitPresetTokens(key))
	if len(tokens) == 0 {
		return fallback.CPU, fallback.RAM
	}

	for _, e := range entries {
		if !hasTokenPrefix(tokens, splitPresetTokens(e.Prefix)) {
			continue
		}
		for _, v := range e.By {
			if slices.Contains(tokens, v.Token) {
				tier := t.Tiers[v.Tier]
				return tier.CPU, tier.RAM
			}
		}
		tier := t.Tiers[e.Tier]
		return tier.CPU, tier.RAM
	}

	return fallback.CPU, fallback.RAM
}

// stripLeadingMarketing 反复去掉匹配的品牌词前缀。
// 例如 AMD Radeon Instinct MI300X OAM 要依次去掉 radeon 和 instinct，才能匹配 mi300x。
// 加载时已确认所有剥离序列非空，每轮都能前进。
func (t *unitResourcesPresets) stripLeadingMarketing(tokens []string) []string {
	for stripped := true; stripped; {
		stripped = false
		for _, s := range t.Strip {
			seq := splitPresetTokens(s)
			if !hasTokenPrefix(tokens, seq) {
				continue
			}
			tokens = tokens[len(seq):]
			stripped = true
			break
		}
	}
	return tokens
}

// splitPresetTokens 将规范化名称按词拆开。device.NormalizeName 会保留连字符、下划线和点，
// 因此海光的 K100_AI 和 K100-AI 会拆出相同的词。
func splitPresetTokens(s string) []string {
	return strings.FieldsFunc(s, func(r rune) bool {
		return strings.ContainsRune(_UnitResourcesPresetTokenSeparators, r)
	})
}

// hasTokenPrefix 按完整词比较前缀，避免把 a10 匹配到 a100，
// 也允许 h20 与 h200、l4 与 l40 或 l40s 分别作为有效条目。
func hasTokenPrefix(tokens, prefix []string) bool {
	return len(prefix) <= len(tokens) && slices.Equal(tokens[:len(prefix)], prefix)
}

// hasTokenSuffix 按完整词检查末尾的序列，避免用单独的 v100 指代 bi-v100 板卡和 v100 芯片这两种产品。
func hasTokenSuffix(prefix, suffix []string) bool {
	return len(suffix) <= len(prefix) && slices.Equal(prefix[len(prefix)-len(suffix):], suffix)
}

// mustLoadUnitResourcesPresets 加载内嵌的预设表，表格式有误时触发 panic。
func mustLoadUnitResourcesPresets(data []byte) *unitResourcesPresets {
	t, err := loadUnitResourcesPresets(data)
	if err != nil {
		panic("loading unit-resources preset table: " + err.Error())
	}
	return t
}

// loadUnitResourcesPresets 严格解码并校验预设表，拼错字段名会立即报错。
func loadUnitResourcesPresets(data []byte) (*unitResourcesPresets, error) {
	t := new(unitResourcesPresets)
	if err := yaml.UnmarshalStrict(data, t); err != nil {
		return nil, fmt.Errorf("decoding unit-resources presets: %w", err)
	}
	if err := t.validate(); err != nil {
		return nil, err
	}
	return t, nil
}

// validate 拒绝可能让同一产品匹配多个档位、依赖条目声明顺序或产生无效 InstanceType 规格值的配置。
func (t *unitResourcesPresets) validate() error {
	if err := t.validateTiers(); err != nil {
		return err
	}
	if err := t.validateStrip(); err != nil {
		return err
	}
	return t.validateFamilies()
}

// validateTiers 校验档位集合、固定的默认值、档位递增关系，以及每个值是否符合准入规则。
func (t *unitResourcesPresets) validateTiers() error {
	if len(t.Tiers) != len(_UnitResourcesPresetTierNames) {
		return fmt.Errorf("tiers: must define exactly %v, got %d", _UnitResourcesPresetTierNames, len(t.Tiers))
	}
	var prevCPU, prevRAM int32
	for i, name := range _UnitResourcesPresetTierNames {
		tier, ok := t.Tiers[name]
		if !ok {
			return fmt.Errorf("tiers: %q is missing", name)
		}
		if name == _UnitResourcesPresetFallbackTierName && tier != _UnitResourcesPresetFallbackTier {
			return fmt.Errorf("tiers[%s]: must stay %s CPU / %s, the value stamped before presets existed",
				name, _UnitResourcesPresetFallbackTier.CPU, _UnitResourcesPresetFallbackTier.RAM)
		}
		cpu, err := parsePresetUnitCPU(tier.CPU)
		if err != nil {
			return fmt.Errorf("tiers[%s].cpu: %w", name, err)
		}
		ram, err := parsePresetUnitRAM(tier.RAM)
		if err != nil {
			return fmt.Errorf("tiers[%s].ram: %w", name, err)
		}
		switch {
		case i == 1 && (cpu <= prevCPU || ram <= prevRAM):
			return fmt.Errorf("tiers[%s]: must be strictly larger than %q on both axes, so an identified "+
				"product is never sized at or below an unidentified one", name, _UnitResourcesPresetTierNames[i-1])
		case i > 1 && (cpu < prevCPU || ram < prevRAM):
			return fmt.Errorf("tiers[%s]: must not be smaller than %q on either axis",
				name, _UnitResourcesPresetTierNames[i-1])
		}
		prevCPU, prevRAM = cpu, ram
	}
	return nil
}

// validateStrip 检查品牌词序列的格式和冗余。如果一个序列是另一个的词前缀，
// 先匹配短序列可能导致长序列永远匹配不到，因此拒绝依赖列表顺序的配置。
func (t *unitResourcesPresets) validateStrip() error {
	seen := sets.New[string]()
	for _, s := range t.Strip {
		if err := validatePresetSequence(s); err != nil {
			return fmt.Errorf("strip[%q]: %w", s, err)
		}
		if seen.Has(s) {
			return fmt.Errorf("strip[%q]: is listed twice", s)
		}
		seen.Insert(s)
	}
	for i, outer := range t.Strip {
		for j, inner := range t.Strip {
			if i == j {
				continue
			}
			if hasTokenPrefix(splitPresetTokens(inner), splitPresetTokens(outer)) {
				return fmt.Errorf("strip[%q]: is a token-prefix of strip[%q], which makes stripping "+
					"depend on the order the two are listed in", outer, inner)
			}
		}
	}
	return nil
}

// validateFamilies 检查每个厂商是否可探测、前缀是否互斥、系列名是否全局唯一、变体词是否合法，
// 以及剥离品牌词或厂商名后是否仍能匹配条目。
func (t *unitResourcesPresets) validateFamilies() error {
	families := sets.New[string]()

	for _, manufacturer := range slices.Sorted(maps.Keys(t.Families)) {
		if !_UnitResourcesPresetManufacturers.Has(manufacturer) {
			return fmt.Errorf("families[%s]: is not a manufacturer the operator can detect", manufacturer)
		}
		entries := t.Families[manufacturer]
		for _, e := range entries {
			if err := t.validateFamilyEntry(manufacturer, e, families); err != nil {
				return err
			}
		}
		if err := validatePresetPrefixesDisjoint(manufacturer, entries); err != nil {
			return err
		}
	}
	return nil
}

// validateFamilyEntry 校验一个条目，并登记它使用的系列名。
func (t *unitResourcesPresets) validateFamilyEntry(
	manufacturer string,
	e unitResourcesPresetEntry,
	families sets.Set[string],
) error {
	at := fmt.Sprintf("families[%s][%s]", manufacturer, e.Prefix)

	if err := validatePresetSequence(e.Prefix); err != nil {
		return fmt.Errorf("%s.prefix: %w", at, err)
	}
	prefix := splitPresetTokens(e.Prefix)
	if hasTokenPrefix(prefix, splitPresetTokens(manufacturer)) {
		return fmt.Errorf("%s.prefix: starts with its own manufacturer name, which normalization already "+
			"removes, so the entry can never match", at)
	}
	for _, s := range t.Strip {
		if hasTokenPrefix(prefix, splitPresetTokens(s)) {
			return fmt.Errorf("%s.prefix: starts with the stripped sequence %q, so the entry can never match", at, s)
		}
	}

	if err := t.claimPresetFamily(at, e.Family, e.Tier, families); err != nil {
		return err
	}

	tokens := sets.New[string]()
	for _, v := range e.By {
		if err := validatePresetToken(v.Token); err != nil {
			return fmt.Errorf("%s.by[%q].token: %w", at, v.Token, err)
		}
		if tokens.Has(v.Token) {
			return fmt.Errorf("%s.by[%q].token: is listed twice", at, v.Token)
		}
		if slices.Contains(prefix, v.Token) {
			return fmt.Errorf("%s.by[%q].token: is a token of the entry's own prefix, so it matches "+
				"unconditionally and makes the entry's own tier unreachable", at, v.Token)
		}
		tokens.Insert(v.Token)
		if err := t.claimPresetFamily(at+".by["+v.Token+"]", v.Family, v.Tier, families); err != nil {
			return err
		}
	}
	return nil
}

// claimPresetFamily 登记系列名以防重复，并确认该系列引用的档位已存在。
func (t *unitResourcesPresets) claimPresetFamily(at, family, tier string, families sets.Set[string]) error {
	if family == "" {
		return fmt.Errorf("%s.family: must not be empty", at)
	}
	if families.Has(family) {
		return fmt.Errorf("%s.family: %q is already claimed elsewhere in the table", at, family)
	}
	families.Insert(family)
	if _, ok := t.Tiers[tier]; !ok {
		return fmt.Errorf("%s.tier: %q is not a defined tier", at, tier)
	}
	return nil
}

// validatePresetPrefixesDisjoint 拒绝同一厂商下可能同时匹配同一产品键的前缀，
// 使每个产品最多命中一个条目。
func validatePresetPrefixesDisjoint(manufacturer string, entries []unitResourcesPresetEntry) error {
	for i, outer := range entries {
		for j, inner := range entries {
			if i == j {
				continue
			}
			if !hasTokenPrefix(splitPresetTokens(inner.Prefix), splitPresetTokens(outer.Prefix)) {
				continue
			}
			if outer.Prefix == inner.Prefix {
				return fmt.Errorf("families[%s]: prefix %q is used twice", manufacturer, outer.Prefix)
			}
			return fmt.Errorf("families[%s]: prefix %q is a token-prefix of %q, so a key could match both",
				manufacturer, outer.Prefix, inner.Prefix)
		}
	}
	return nil
}

// validatePresetSequence 检查产品前缀或剥离规则的词序列。至少要有一个词，
// 否则空序列会匹配所有产品键。
func validatePresetSequence(s string) error {
	for _, r := range s {
		if isPresetTokenRune(r) || strings.ContainsRune(_UnitResourcesPresetTokenSeparators, r) {
			continue
		}
		return fmt.Errorf("must be lowercase [a-z0-9] plus the token separators %q",
			_UnitResourcesPresetTokenSeparators)
	}
	if len(splitPresetTokens(s)) == 0 {
		return errors.New("must carry at least one token")
	}
	return nil
}

// validatePresetToken 检查一个完整的匹配词，词内不能含分隔符。
func validatePresetToken(s string) error {
	if s == "" {
		return errors.New("must not be empty")
	}
	for _, r := range s {
		if !isPresetTokenRune(r) {
			return errors.New("must be a single lowercase [a-z0-9] token")
		}
	}
	return nil
}

func isPresetTokenRune(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
}

// parsePresetUnitCPU 按 validateInstanceTypeUnitSpec 的准入规则解析每卡 CPU，
// 要求无单位、正整数且在 int32 范围内。webhook 包依赖本包，两处采用相同的数值解析约束，
// 保证预设表生成的规格可通过准入。
func parsePresetUnitCPU(v string) (int32, error) {
	n, err := strconvx.ParseInt[int32](v, 10, 32)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%q must be a positive integer with no unit suffix", v)
	}
	return n, nil
}

// parsePresetUnitRAM 按同一准入规则解析 RAM，要求正整数并带有大小写完全匹配的 Gi 后缀。
func parsePresetUnitRAM(v string) (int32, error) {
	b, ok := strings.CutSuffix(v, _UnitResourcesPresetRAMSuffix)
	if !ok {
		return 0, fmt.Errorf("%q must carry a case-sensitive %q suffix", v, _UnitResourcesPresetRAMSuffix)
	}
	return parsePresetUnitCPU(b)
}
