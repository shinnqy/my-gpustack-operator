package args

// Args 保存 webhook-gen 生成器的选项。
type Args struct {
	// OutputFile 是生成文件的名称，包含 ".go" 后缀。
	OutputFile string

	// GoHeaderFile 是置于生成文件开头的文件头模板文件路径。
	GoHeaderFile string
}
