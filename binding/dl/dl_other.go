//go:build !linux

package dl

// RTLD_DEEPBIND 是 GNU 扩展。在缺少该标志的平台将其定义为零，使调用方可以复用相同的打开参数。
const RTLD_DEEPBIND = 0
