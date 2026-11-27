// chapter-source 从各章正文拼接练习，与参考源码和当前学习目录逐项核对。
package main

import (
	"bytes"
	"flag"
	"fmt"
	goscanner "go/scanner"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	cscanner "text/scanner"

	"github.com/BurntSushi/toml"
)

type edit struct {
	Chapter string
	Name    string
	File    string
	Lines   []int
	Parts   []string
}

var block = regexp.MustCompile("(?ms)^<!-- implement: ([^\n]+) -->\n\n```[^\n]*\n(.*?)^```$")

func chapterFragments(guide string) (map[string]map[string]string, map[string][]string, error) {
	pages, err := filepath.Glob(filepath.Join(guide, "[0-9][0-9]-*.md"))
	if err != nil {
		return nil, nil, err
	}
	pieces := make(map[string]map[string]string)
	order := make(map[string][]string)
	for _, page := range pages {
		chapter := filepath.Base(page)[:2]
		if _, exists := pieces[chapter]; exists {
			return nil, nil, fmt.Errorf("第 %s 章有多个正文文件", chapter)
		}
		pieces[chapter] = make(map[string]string)
		raw, err := os.ReadFile(page)
		if err != nil {
			return nil, nil, err
		}
		for _, m := range block.FindAllSubmatch(raw, -1) {
			name := string(m[1])
			if _, exists := pieces[chapter][name]; exists {
				return nil, nil, fmt.Errorf("第 %s 章重复出现 %s", chapter, name)
			}
			pieces[chapter][name] = string(m[2])
			order[chapter] = append(order[chapter], name)
		}
	}
	return pieces, order, nil
}

// Go 扫描器保留字符串、运算符和自动插入的分号。换行排版允许在最后一个实参后加逗号。
func tokens(source, file string) []string {
	var out []string
	if filepath.Ext(file) == ".c" {
		var s cscanner.Scanner
		s.Init(strings.NewReader(source))
		for t := s.Scan(); t != cscanner.EOF; t = s.Scan() {
			out = append(out, s.TokenText())
		}
		return out
	}
	f := token.NewFileSet().AddFile(file, -1, len(source))
	var s goscanner.Scanner
	s.Init(f, []byte(source), nil, 0)
	for {
		_, t, value := s.Scan()
		if t == token.EOF {
			return out
		}
		if t == token.RPAREN && len(out) > 0 && out[len(out)-1] == "," {
			out = out[:len(out)-1]
		}
		if value == "" || t == token.SEMICOLON {
			value = t.String()
		}
		out = append(out, value)
	}
}

func containsCode(file, fragment []string) bool {
	if len(fragment) == 0 {
		return false
	}
	for i := 0; i+len(fragment) <= len(file); i++ {
		if file[i] == fragment[0] && slices.Equal(file[i:i+len(fragment)], fragment) {
			return true
		}
	}
	return false
}

func compare(guide string) error {
	var plan struct {
		Revision string
		Edits    []edit
	}
	if _, err := toml.DecodeFile(filepath.Join(guide, "steps.toml"), &plan); err != nil {
		return err
	}
	if len(plan.Edits) == 0 || plan.Revision == "" {
		return fmt.Errorf("指南缺少固定版本或实现清单")
	}
	pieces, order, err := chapterFragments(guide)
	if err != nil {
		return err
	}
	matched, retained, lines := 0, 0, 0
	learner := make(map[string][]string)
	for _, e := range plan.Edits {
		parts := e.Parts
		if len(parts) == 0 {
			parts = []string{e.Name}
		}
		var b strings.Builder
		last := -1
		for _, name := range parts {
			body, ok := pieces[e.Chapter][name]
			pos := slices.Index(order[e.Chapter], name)
			if !ok || pos <= last {
				return fmt.Errorf("第 %s 章缺段、重复使用或顺序有误：%s", e.Chapter, name)
			}
			last = pos
			b.WriteString(body)
			delete(pieces[e.Chapter], name)
		}
		raw, err := os.ReadFile(filepath.Join(guide, "..", e.File))
		if err != nil {
			return err
		}
		rows := bytes.SplitAfter(raw, []byte("\n"))
		if len(e.Lines) != 2 || e.Lines[0] < 1 || e.Lines[1] < e.Lines[0] || e.Lines[1] > len(rows) {
			return fmt.Errorf("源码范围无效：%s", e.Name)
		}
		body := b.String()
		if body != string(bytes.Join(rows[e.Lines[0]-1:e.Lines[1]], nil)) {
			return fmt.Errorf("章节代码与参考源码不一致：%s %s", e.Name, e.File)
		}
		lines += e.Lines[1] - e.Lines[0] + 1
		if _, ok := learner[e.File]; !ok {
			raw, err := os.ReadFile(e.File)
			if err != nil {
				return err
			}
			learner[e.File] = tokens(string(raw), e.File)
		}
		if containsCode(learner[e.File], tokens(body, e.File)) {
			matched++
			continue
		}
		// 个人项目在 Watch 写出中断时主动关闭管道读端。仅认可这一句差异，其他语句继续逐个比对。
		if e.Name == "Service-streamResponse" {
			const anchor = "pr, pw := io.Pipe()"
			if strings.Count(body, anchor) == 1 && containsCode(learner[e.File], tokens(strings.Replace(body, anchor, anchor+"\n\tdefer pr.Close()", 1), e.File)) {
				fmt.Printf("保留学习项目的断连修复：%s（TestHTTPGatewayReleasesInterruptedEvent）\n", e.File)
				retained++
				continue
			}
		}
		return fmt.Errorf("第 %s 章的 %s 与学习目录不同：%s", e.Chapter, e.Name, e.File)
	}
	for chapter, rest := range pieces {
		for name := range rest {
			return fmt.Errorf("第 %s 章有未列入清单的代码段：%s", chapter, name)
		}
	}
	fmt.Printf("指南标记版本 %s，正文与本地参考源码吻合 %d 段、%d 行。学习目录匹配 %d 段，已核对的保留差异 %d 段。\n", plan.Revision, len(plan.Edits), lines, matched, retained)
	return nil
}

func main() {
	guide := flag.String("guide", "", "原项目 guide 目录")
	flag.Parse()
	if *guide == "" {
		flag.Usage()
		os.Exit(2)
	}
	if err := compare(filepath.Clean(*guide)); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
