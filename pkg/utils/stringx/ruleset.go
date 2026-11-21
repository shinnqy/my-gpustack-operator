package stringx

import (
	"strings"
	"unicode"

	"github.com/go-openapi/inflect"
	"k8s.io/apimachinery/pkg/util/sets"
)

var (
	globalRuleset = inflect.NewDefaultRuleset()
	acronymSet    = sets.Set[string]{}
)

func init() {
	for _, w := range []string{
		"ACL", "API", "ACME", "ASCII", "AWS", "GPU", "CPU", "CSS", "DNS", "EOF", "GB", "GUID",
		"HTML", "HTTP", "HTTPS", "ID", "IP", "JSON", "KB", "LHS", "MAC", "MB",
		"QPS", "RAM", "RHS", "RPC", "SLA", "SMTP", "SQL", "SSH", "SSO", "TCP",
		"TLS", "TTL", "UDP", "UI", "UID", "URI", "URL", "UTF8", "UUID", "VM", "PV",
		"XML", "XMPP", "XSRF", "XSS",
	} {
		globalRuleset.AddAcronym(w)
		acronymSet.Insert(w)
	}

	for s, p := range map[string]string{
		"exec": "exec",
	} {
		globalRuleset.AddIrregular(s, p)
	}
}

// Pluralize returns the plural form of a word.
func Pluralize(word string) string {
	return globalRuleset.Pluralize(word)
}

// Singularize returns the singular form of a word.
func Singularize(word string) string {
	return globalRuleset.Singularize(word)
}

// CamelizeDownFirst is the same as Camelize but with first lowercase char,
// "IDRef" -> "idRef.
func CamelizeDownFirst(word string) string {
	words := strings.Split(globalRuleset.Underscore(word), "_")

	switch len(words) {
	case 0:
		return ""
	case 1:
		return words[0]
	}

	for i, w := range words[1:] {
		upper := strings.ToUpper(w)
		if acronymSet.Has(upper) {
			words[1:][i] = upper
		} else {
			words[1:][i] = globalRuleset.Capitalize(w)
		}
	}

	return Join("", words...)
}

// Decamelize converts camel-case words to space-split words, "DinoParty" -> "Dino Party".
// If lowercase is true, convert the first letter of each word to lowercase, "DinoParty" -> "dino party".
func Decamelize(s string, lowercase bool) string {
	var (
		b          strings.Builder
		splittable = false
	)

	for _, v := range s {
		if splittable && unicode.IsUpper(v) {
			b.WriteByte(' ')
		}

		if lowercase {
			b.WriteString(strings.ToLower(string(v)))
		} else {
			b.WriteRune(v)
		}

		splittable = unicode.IsLower(v) || unicode.IsNumber(v)
	}

	return b.String()
}

// Underscore returns the string in snake case, "BigBen" -> "big_ben".
func Underscore(word string) string {
	return globalRuleset.Underscore(word)
}

// UnderscoreUpper is the same as Underscore but with all uppercase char.
func UnderscoreUpper(word string) string {
	return strings.ToUpper(Underscore(word))
}

// Capitalize returns the string with first uppercase letter.
func Capitalize(word string) string {
	return globalRuleset.Capitalize(word)
}
