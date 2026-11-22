package utils

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"time"
)

// Header 构建生成文件的头部。\n//\n// 构建约束位于最前，使编译器跳过该文件，其后为文件头模板。\n// 除 YEAR 占位符外，文件头模板按原样复制。gengo/v2 自带的模板辅助函数\n// 会向文件头模板文件额外追加一个换行符，从而在每个生成文件的文件头与\n// package 语句之间多出一个空行。
func Header(buildTag, headerFile string) ([]byte, error) {
	boilerplate, err := os.ReadFile(headerFile)
	if err != nil {
		return nil, fmt.Errorf("read boilerplate %q: %w", headerFile, err)
	}

	boilerplate = bytes.ReplaceAll(boilerplate, []byte("YEAR"), []byte(strconv.Itoa(time.Now().UTC().Year())))

	return append([]byte(fmt.Sprintf("//go:build !%s\n// +build !%s\n\n", buildTag, buildTag)), boilerplate...), nil
}
