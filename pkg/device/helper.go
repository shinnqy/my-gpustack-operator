package device

import (
	"os"
	"regexp"
	"strings"

	"gpustack.ai/gpustack/pkg/utils/typex"
)

// Toggle of the functions.
var (
	constructGroupIDWithMemory bool
)

func init() {
	constructGroupIDWithMemory = os.Getenv("GPUSTACK_DEVICES_GROUP_ID_WITH_MEMORY") == "true"
}

var (
	// _CPUNameTrademarkMarkerReplacer drops the trademark markers carried by CPU product names.
	_CPUNameTrademarkMarkerReplacer = strings.NewReplacer("(R)", "", "(r)", "", "(TM)", "", "(tm)", "")

	// _CPUNamePrefixCruft matches the leading cruft carried by CPU product names,
	// e.g. "Genuine " and "11th Gen ".
	_CPUNamePrefixCruft = regexp.MustCompile(`^(Genuine |[0-9]+(st|nd|rd|th) Gen )+`)

	// _CPUNameCoreCountSuffix matches the trailing core-count carried by CPU product
	// names, e.g. " 64-Core" and " 16-Cores".
	_CPUNameCoreCountSuffix = regexp.MustCompile(`(?i) [0-9]+-cores?$`)
)

// NormalizeName sanitizes the given device or CPU product name into a Kubernetes
// label-safe slug: it lowercases letters, keeps only [a-z0-9-_.], converts
// whitespace to "-", collapses consecutive separators, trims the leading prefix
// when matched case-insensitively, and trims leading/trailing separators.
// When stripCruft is true, it first strips CPU-name cruft: the "@ <freq>" tail,
// trademark markers like "(R)"/"(TM)", the "Genuine"/"Nth Gen" leading words, the
// trailing "CPU"/"Processor" words, and the trailing "<n>-Core(s)" count.
// When maxLength is positive, the result is truncated to at most maxLength runes.
func NormalizeName(name, prefix string, maxLength int, stripCruft bool) string {
	if stripCruft {
		name, _, _ = strings.Cut(name, " @ ")
		name = _CPUNameTrademarkMarkerReplacer.Replace(name)
		name = _CPUNamePrefixCruft.ReplaceAllString(name, "")
		name = strings.TrimSpace(name)
		name = strings.TrimSuffix(name, " CPU")
		name = strings.TrimSuffix(name, " Processor")
		name = _CPUNameCoreCountSuffix.ReplaceAllString(name, "")
	}

	if maxLength <= 0 {
		maxLength = len(name)
	}

	buf := make([]rune, 0, min(len(name), maxLength))
	var pr rune
	for _, r := range name {
		switch {
		case r >= 'A' && r <= 'Z':
			r += 'a' - 'A'
		case r >= 'a' && r <= 'z':
		case r >= '0' && r <= '9':
		case r == ' ':
			r = '-'
			fallthrough
		case r == '-' || r == '_' || r == '.':
			if len(buf) == 0 {
				continue
			}
			if pr == '-' || pr == '_' || pr == '.' {
				continue
			}
		default:
			continue
		}
		buf = append(buf, r)
		pr = r

		// Trim prefix.
		if len(buf) == len(prefix) {
			if strings.EqualFold(string(buf), prefix) {
				buf = buf[:0]
				pr = 0
			}
		}

		// Stop processing if the buffer has reached the maximum length.
		if len(buf) >= maxLength {
			break
		}
	}

	if len(buf) == 0 {
		return ""
	}

	// Trim trailing non-alphanumeric character.
	if c := buf[len(buf)-1]; c == '-' || c == '_' || c == '.' {
		buf = buf[:len(buf)-1]
	}

	return string(buf)
}

// CalculateUtilization calculates the utilization percentage based on the given usage and total values.
func CalculateUtilization[U, T typex.Integer](usage U, total T) uint32 {
	if usage <= 0 || total <= 0 {
		return 0
	}
	return uint32(float64(usage) / float64(total) * 100)
}
