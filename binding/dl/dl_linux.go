package dl

// #cgo LDFLAGS: -ldl
// #define _GNU_SOURCE
// #include <dlfcn.h>
// #include <stdlib.h>
// #include <linux/limits.h>
import "C"

const (
	RTLD_DEEPBIND = C.RTLD_DEEPBIND
)
