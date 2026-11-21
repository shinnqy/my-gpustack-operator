package loggerx

import (
	"bufio"
	"bytes"
	"fmt"
	"runtime"
	"strings"
)

type (
	Scanner interface {
		Scan() bool
		Text() string
	}
	// RecoverStackScannerFunc 接收 panic 的错误和用于逐行读取堆栈的扫描器。
	RecoverStackScannerFunc = func(s Scanner, e error)
)

// RecoverWithGoroutineStackScanner 应直接用 defer 调用。捕获 panic 后，
// 把当前 goroutine 的堆栈和错误交给回调。
func RecoverWithGoroutineStackScanner(cb RecoverStackScannerFunc) {
	if r := recover(); r != nil {
		reportRecovered(false, r, cb)
	}
}

// maxStackBytes 限制采集堆栈的最大缓冲区。按需增大才能取得完整堆栈，上限则防止异常路径无限分配。
const maxStackBytes = 8 << 20

// reportRecovered 将 panic 值转换为错误并逐行提供堆栈。all 决定收集全部 goroutine 或仅当前 goroutine。
func reportRecovered(all bool, r any, cb RecoverStackScannerFunc) {
	var (
		b []byte
		n int
	)
	for size := 64 << 10; ; size *= 2 {
		b = make([]byte, size)
		n = runtime.Stack(b, all)
		if n < size || size >= maxStackBytes {
			break
		}
	}
	s := bufio.NewScanner(bytes.NewReader(b[:n]))
	var e error
	if err, ok := r.(error); ok {
		e = err
	} else {
		e = fmt.Errorf("panic: %v", r)
	}
	cb(_Scanner{s}, e)
}

type _Scanner struct {
	s *bufio.Scanner
}

func (s _Scanner) Scan() bool {
	return s.s.Scan()
}

func (s _Scanner) Text() string {
	str := s.s.Text()
	if !strings.Contains(str, "\t") {
		return str
	}

	var strBuilder strings.Builder
	for _, r := range str {
		if r == '\t' {
			strBuilder.WriteString("    ")
		} else {
			strBuilder.WriteRune(r)
		}
	}
	return strBuilder.String()
}

// RecoverWithStackScanner 应直接用 defer 调用，以便 recover 在正确的栈帧捕获 panic。
// 回调可逐行读取全部 goroutine 的堆栈。
func RecoverWithStackScanner(cb RecoverStackScannerFunc) {
	if r := recover(); r != nil {
		reportRecovered(true, r, cb)
	}
}
