package modelartifact

import (
	"slices"
	"strings"
)

// FilterEntries 保留制品的 allow 和 ignore 模式所选中的条目，这是规范化前对标准清单格式应用的规则。
//
// 其语义与 huggingface_hub 的 filter_repo_objects 一致，用户可以直接复用传给该库的模式：
// 以 "/" 结尾的模式会变为 "<pattern>*"；空的 allow 列表会保留所有条目；
// 匹配任意 ignore 模式的条目即使匹配了 allow 模式也会被丢弃。匹配采用 Python 的 fnmatch.fnmatchcase，见 MatchPattern。
//
// 模式本身绝不会进入摘要计算，因此选中相同文件集合的两个制品共享同一个摘要，
// 未配置模式的制品则保留整个提交的摘要。
func FilterEntries(entries []ManifestEntry, allow, ignore []string) []ManifestEntry {
	if len(allow) == 0 && len(ignore) == 0 {
		return entries
	}

	allowed, ignored := compilePatterns(allow), compilePatterns(ignore)
	kept := make([]ManifestEntry, 0, len(entries))
	for _, e := range entries {
		if len(allowed) > 0 && !matchAny(allowed, e.Path) {
			continue
		}
		if matchAny(ignored, e.Path) {
			continue
		}
		kept = append(kept, e)
	}

	return kept
}

func compilePatterns(patterns []string) []globPattern {
	compiled := make([]globPattern, 0, len(patterns))
	for _, p := range patterns {
		if strings.HasSuffix(p, "/") {
			p += "*"
		}
		compiled = append(compiled, compilePattern(p))
	}

	return compiled
}

func matchAny(patterns []globPattern, name string) bool {
	rs := []rune(name)
	for _, p := range patterns {
		if p.match(rs) {
			return true
		}
	}

	return false
}

type globTokenKind int

const (
	globLiteral globTokenKind = iota
	globAny
	globStar
	globSet
	globNever
)

type globRange struct{ lo, hi rune }

type globToken struct {
	kind    globTokenKind
	literal rune
	negated bool
	ranges  []globRange
}

type globPattern []globToken

// compilePattern 严格遵循 CPython 的 fnmatch._translate 步骤：
// 采用相同的右括号扫描逻辑、相同的按连字符分割集合块的方式，
// 以及相同的移除上下界反转范围的处理（即同时丢弃两个边界）。
func compilePattern(pattern string) globPattern {
	pat := []rune(pattern)
	n := len(pat)

	var toks globPattern
	for i := 0; i < n; {
		c := pat[i]
		i++
		switch c {
		case '*':
			if len(toks) == 0 || toks[len(toks)-1].kind != globStar {
				toks = append(toks, globToken{kind: globStar})
			}
		case '?':
			toks = append(toks, globToken{kind: globAny})
		case '[':
			j := i
			if j < n && pat[j] == '!' {
				j++
			}
			if j < n && pat[j] == ']' {
				j++
			}
			for j < n && pat[j] != ']' {
				j++
			}
			if j >= n {
				toks = append(toks, globToken{kind: globLiteral, literal: '['})
				continue
			}
			toks = append(toks, compileSet(pat, i, j))
			i = j + 1
		default:
			toks = append(toks, globToken{kind: globLiteral, literal: c})
		}
	}

	return toks
}

// compileSet 编译 pat[i:j]，即 "[" 与其对应的 "]" 之间的文本。
func compileSet(pat []rune, i, j int) globToken {
	var chunks [][]rune
	if !slices.Contains(pat[i:j], '-') {
		chunks = [][]rune{pat[i:j]}
	} else {
		k := i + 1
		if pat[i] == '!' {
			k = i + 2
		}
		for {
			k = indexRune(pat, '-', k, j)
			if k < 0 {
				break
			}
			chunks = append(chunks, pat[i:k])
			i = k + 1
			k += 3
		}
		if chunk := pat[i:j]; len(chunk) > 0 {
			chunks = append(chunks, chunk)
		} else {
			last := len(chunks) - 1
			chunks[last] = append(append([]rune(nil), chunks[last]...), '-')
		}
		for k := len(chunks) - 1; k > 0; k-- {
			prev, cur := chunks[k-1], chunks[k]
			if prev[len(prev)-1] > cur[0] {
				merged := append(append([]rune(nil), prev[:len(prev)-1]...), cur[1:]...)
				chunks[k-1] = merged
				chunks = append(chunks[:k], chunks[k+1:]...)
			}
		}
	}

	// 按 CPython 连接各块的方式重建集合文本，每次连接都在前一块的最后一个字符与下一块的第一个字符之间构成一个范围。
	var (
		members []rune
		isRange []bool
	)
	for ci, chunk := range chunks {
		for ri, r := range chunk {
			members = append(members, r)
			isRange = append(isRange, ci > 0 && ri == 0)
		}
	}
	if len(members) == 0 {
		return globToken{kind: globNever}
	}

	tok := globToken{kind: globSet}
	start := 0
	if members[0] == '!' {
		if len(members) == 1 {
			return globToken{kind: globAny}
		}
		tok.negated = true
		start = 1
	}
	for m := start; m < len(members); m++ {
		if m+1 < len(members) && isRange[m+1] && m+1 > start {
			tok.ranges = append(tok.ranges, globRange{lo: members[m], hi: members[m+1]})
			m++
			continue
		}
		tok.ranges = append(tok.ranges, globRange{lo: members[m], hi: members[m]})
	}

	return tok
}

func (t globToken) matchRune(r rune) bool {
	switch t.kind {
	case globLiteral:
		return r == t.literal
	case globAny:
		return true
	case globSet:
		in := false
		for _, rg := range t.ranges {
			if rg.lo <= r && r <= rg.hi {
				in = true
				break
			}
		}
		return in != t.negated
	default:
		return false
	}
}

// match 对整个 name 进行匹配。每次失败时通配符星号会从后一个 rune 重试，
// 即常规的单步回溯 glob 遍历；由于连续的星号在编译时已合并，这已经足够。
func (p globPattern) match(name []rune) bool {
	pi, ni := 0, 0
	starPi, starNi := -1, 0
	for ni < len(name) {
		switch {
		case pi < len(p) && p[pi].kind == globStar:
			starPi, starNi = pi, ni
			pi++
		case pi < len(p) && p[pi].matchRune(name[ni]):
			pi++
			ni++
		case starPi >= 0:
			starNi++
			pi, ni = starPi+1, starNi
		default:
			return false
		}
	}
	for pi < len(p) && p[pi].kind == globStar {
		pi++
	}

	return pi == len(p)
}

func indexRune(rs []rune, r rune, from, to int) int {
	for k := from; k < to; k++ {
		if rs[k] == r {
			return k
		}
	}

	return -1
}
