package httpx

type FSOption struct {
	listable bool
	embedded bool
}

func FSOptions() *FSOption {
	return &FSOption{}
}

// WithEmbedded 启用嵌入模式，
// 该模式会提供固定的修改时间。
func (o *FSOption) WithEmbedded() *FSOption {
	o.embedded = true
	return o
}
