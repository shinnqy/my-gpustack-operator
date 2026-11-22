package kubeappyaml

import (
	"strings"
	"text/template"

	sprig "github.com/go-task/slim-sprig/v3"
	"sigs.k8s.io/yaml"

	"gpustack.ai/gpustack/pkg/utils/bytex"
)

type Template string

// Render 使用给定的模板上下文和扩展函数映射渲染模板，并返回渲染后的字符串。
func (t Template) Render(context any, extendFuncMap template.FuncMap) (string, error) {
	tmpl, err := template.New("yaml-template").
		Funcs(templateFuncMap(extendFuncMap)).
		Parse(string(t))
	if err != nil {
		return "", err
	}

	buf := bytex.GetBuffer()
	defer bytex.Put(buf)

	if err = tmpl.Execute(buf, context); err != nil {
		return "", err
	}

	return buf.String(), nil
}

func templateFuncMap(extend template.FuncMap) template.FuncMap {
	fm := sprig.TxtFuncMap()
	fm["toYaml"] = toYAML
	if len(extend) > 0 {
		for k, v := range extend {
			fm[k] = v
		}
	}

	return fm
}

// toYAML 借鉴自 helm.sh/helm/pkg/engine/engine.go。
func toYAML(v any) string {
	data, err := yaml.Marshal(v)
	if err != nil {
		// 在模板内部忽略序列化错误。
		return ""
	}

	return strings.TrimSuffix(string(data), "\n")
}
