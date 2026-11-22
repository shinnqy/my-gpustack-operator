package utils

import (
	"strings"
)

func ParseMarker(line string) map[string]string {
	rm := map[string]string{}

	h, r, ok := strings.Cut(line, "=")
	for ; ok; h, r, ok = strings.Cut(r, "=") {
		h = strings.TrimSpace(h)
		r = strings.TrimSpace(r)

		if r != "" {
			switch r[0] {
			case '[', '{', '"':
				if i := indexJSON(r); -1 < i && i < len(r)-1 {
					rm[h] = r[:i+1]
					r = r[i+2:]
					continue
				}
			default:
				if i := strings.Index(r, ","); -1 < i && i < len(r) {
					rm[h] = r[:i]
					r = r[i+1:]
					continue
				}
			}
		}

		// 上述两个分支均未认领剩余部分，因此这是该行的最后一个值，会被完整消费。\n// 在此处停止很关键：继续执行会对已经取出的值再次调用 Cut，而值本身可能合法\n// 地包含 "="，例如 jsonPath 过滤器的写法 `[?(@.name=='X')]`。如果不加 break，\n// 该尾部会被拆成额外两项（一个无效的非空键和一个空键），两者都无法与拼写\n// 错误的标记区分开。
		rm[h] = r
		break
	}

	return rm
}

func indexJSON(r string) int {
	if len(r) == 0 || r[0] == '\\' {
		return -1
	}

	var (
		c  = 1
		qs = r[0] == '"'
		bs bool
	)

	for i := 1; i < len(r); i++ {
		if bs {
			bs = false
			continue
		}

		switch r[i] {
		case '\\':
			bs = true
			continue
		case '"':
			qs = !qs
			if qs {
				continue
			}
			if r[0] == '"' {
				c--
			}
		case '{', '[':
			if !qs {
				c++
				continue
			}
		case '}', ']':
			if qs {
				continue
			}
			c--
		}

		if c == 0 {
			return i
		}
	}

	return -1
}
