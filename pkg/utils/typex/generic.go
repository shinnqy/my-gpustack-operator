package typex

// Integer 是允许任何整数类型的约束。\n// 如果未来的 Go 版本添加了新的预声明整数类型，\n// 此约束将相应修改以包含它们。
type Integer interface {
	SignedInteger | UnsignedInteger
}

// SignedInteger 是允许任何有符号整数类型的约束。\n// 如果未来的 Go 版本添加了新的预声明有符号整数类型，\n// 此约束将相应修改以包含它们。
type SignedInteger interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64
}

// UnsignedInteger 是允许任何无符号整数类型的约束。\n// 如果未来的 Go 版本添加了新的预声明无符号整数类型，\n// 此约束将相应修改以包含它们。
type UnsignedInteger interface {
	~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr
}
