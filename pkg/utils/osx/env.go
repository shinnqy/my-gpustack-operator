package osx

import (
	"os"
)

// LookupEnv 检索由 key 命名的环境变量的值。
// 如果该变量存在于环境中，则返回其值（可能为空）和布尔值 true。
// 否则，返回空字符串和布尔值 false。
func LookupEnv(key string) (string, bool) {
	return os.LookupEnv(key)
}

// Getenv 检索由 key 命名的环境变量的值。
// 如果变量不存在则返回默认值（未指定时为空）。
// 要区分空值与未设置的值，请使用 LookupEnv。
func Getenv(key string, def ...string) string {
	e, ok := LookupEnv(key)
	if !ok && len(def) != 0 {
		return def[0]
	}

	return e
}

// ExpandEnv 与 Getenv 类似，
// 但会替换结果中的 ${var} 或 $var。
func ExpandEnv(key string, def ...string) string {
	return os.ExpandEnv(Getenv(key, def...))
}
