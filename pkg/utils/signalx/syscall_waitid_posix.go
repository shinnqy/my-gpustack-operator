//go:build !windows

package signalx

import (
	"syscall"
	"unsafe"
)

// https://man7.org/linux/man-pages/man2/sigaction.2.html

const (
	P_ALL  = 0
	P_PID  = 1
	P_PGID = 2
)

const (
	cExited    = 1
	cKilled    = 2
	cDumped    = 3
	cTrapped   = 4
	cStopped   = 5
	cContinued = 6
)

type Siginfo struct {
	Signo  int32 /* 信号编号。 */
	Errno  int32 /* errno 值。 */
	Code   int32 /* 信号代码。 */
	Trapno int32
	Pid    int32
	Uid    int32
	Status int32 /* 退出值或信号。 */
	_      [100]byte
}

func (s Siginfo) Exited() bool {
	return s.Code == cExited
}

func syscallWaitid(idType, id int, info *Siginfo, options int, rusage *syscall.Rusage) error {
	_, _, e1 := syscall.Syscall6(
		syscall.SYS_WAITID,
		uintptr(idType),
		uintptr(id),
		uintptr(unsafe.Pointer(info)),
		uintptr(options),
		uintptr(unsafe.Pointer(rusage)),
		0,
	)
	if e1 == 0 {
		return nil
	}

	return e1
}
