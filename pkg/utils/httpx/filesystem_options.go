package httpx

type FSOption struct {
	listable bool
	embedded bool
}

func FSOptions() *FSOption {
	return &FSOption{}
}

// WithEmbedded enables embedded mode,
// which provides a fixed modified time.
func (o *FSOption) WithEmbedded() *FSOption {
	o.embedded = true
	return o
}
