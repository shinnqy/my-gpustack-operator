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

// Pluralize 返回单词的复数形式。
func Pluralize(word string) string {
	return globalRuleset.Pluralize(word)
}

// Singularize 返回单词的单数形式。
func Singularize(word string) string {
	return globalRuleset.Singularize(word)
}

// CamelizeDownFirst 与 Camelize 相同，但首字母小写，
// "IDRef" -> "idRef"。
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

// Decamelize 将驼峰式单词转换为空格分隔的单词，如 "DinoParty" -> "Dino Party"。
// 如果 lowercase 为 true，则将每个单词的首字母转换为小写，如 "DinoParty" -> "dino party"。
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

// Underscore 返回蛇形命名（snake case）的字符串，如 "BigBen" -> "big_ben"。
func Underscore(word string) string {
	return globalRuleset.Underscore(word)
}

// UnderscoreUpper 与 Underscore 相同，但所有字符均为大写。
func UnderscoreUpper(word string) string {
	return strings.ToUpper(Underscore(word))
}

// Capitalize 返回首字母大写的字符串。
func Capitalize(word string) string {
	return globalRuleset.Capitalize(word)
}
