package extensionapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"slices"
	"strings"

	apiext "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiexttableconvertor "k8s.io/apiextensions-apiserver/pkg/registry/customresource/tableconvertor"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	kmeta "k8s.io/apimachinery/pkg/api/meta"
	metatable "k8s.io/apimachinery/pkg/api/meta/table"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/client-go/util/jsonpath"
)

var swaggerMetadataDescriptions = meta.ObjectMeta{}.SwaggerDoc()

type (
	// JSONPathTableColumnDefinition 定义表格中要填充的列。
	//
	// 必须提供 JSONPath 以从自定义资源中提取值。
	JSONPathTableColumnDefinition struct {
		meta.TableColumnDefinition

		// JSONPath 是一个简单的 JSON 路径（即支持数组下标语法），会对每个自定义资源求值，
		// 以生成该列的值。
		JSONPath string
	}
	_JSONPathTableColumnInternalDefinition = apiext.CustomResourceColumnDefinition
)

// JSONPathColumn 返回一个 string 类型的 JSONPathTableColumnDefinition，用于提取指定 JSONPath 处的值。
//
// 这是常见场景的简写形式，即不包含 format、description 或 priority 的列。
func JSONPathColumn(name, jsonPath string) JSONPathTableColumnDefinition {
	return JSONPathTableColumnDefinition{
		TableColumnDefinition: meta.TableColumnDefinition{Name: name, Type: "string"},
		JSONPath:              jsonPath,
	}
}

// NewJSONPathTableConvertor 创建一个使用 JSONPath 从自定义资源提取值的表格转换器。
//
// 它依赖 apiexttableconvertor.New 来创建表格转换器。
func NewJSONPathTableConvertor(definitions ...JSONPathTableColumnDefinition) (rest.TableConvertor, error) {
	defs := make([]_JSONPathTableColumnInternalDefinition, 0, len(definitions)+1)

	agePriority := int32(0)
	for i := range definitions {
		switch {
		case definitions[i].Name == "":
			return nil, fmt.Errorf("column definition must have a non-empty name")
		case definitions[i].JSONPath == "":
			return nil, fmt.Errorf("column definition %q must have a non-empty JSONPath", definitions[i].Name)
		case definitions[i].Priority < 0:
			return nil, fmt.Errorf("column definition %q must have a non-negative priority", definitions[i].Name)
		}

		desc := definitions[i].Description
		if desc == "" {
			// NB(thxCode): 避免使用 apiexttableconvertor.New 生成的默认描述。
			desc = "Description to " + definitions[i].Name
		}

		if strings.HasPrefix(definitions[i].JSONPath, "{") {
			if strings.HasSuffix(definitions[i].JSONPath, "}") {
				definitions[i].JSONPath = definitions[i].JSONPath[1 : len(definitions[i].JSONPath)-1]
			} else {
				definitions[i].JSONPath = definitions[i].JSONPath[1:]
			}
		} else if strings.HasSuffix(definitions[i].JSONPath, "}") {
			definitions[i].JSONPath = definitions[i].JSONPath[:len(definitions[i].JSONPath)-1]
		}

		defs = append(defs, _JSONPathTableColumnInternalDefinition{
			Name:        definitions[i].Name,
			Description: desc,
			Type:        definitions[i].Type,
			Format:      definitions[i].Format,
			JSONPath:    definitions[i].JSONPath,
			Priority:    definitions[i].Priority,
		})
		if definitions[i].Priority > agePriority {
			agePriority = definitions[i].Priority
		}
	}

	defs = append(defs, _JSONPathTableColumnInternalDefinition{
		Name:        "Age",
		Description: swaggerMetadataDescriptions["creationTimestamp"],
		Type:        "date",
		JSONPath:    ".metadata.creationTimestamp",
		Priority:    agePriority + 1,
	})

	defs = slices.CompactFunc(defs, func(a, b _JSONPathTableColumnInternalDefinition) bool {
		return a.Name == b.Name
	})

	return apiexttableconvertor.New(defs)
}

type (
	// JSONPathTemplateTableColumnDefinition 定义表格中要填充的列。
	//
	// 与 JSONPathTableColumnDefinition 不同，Template 可以包含多个与字面文本交错的 `{...}` 表达式，
	// 从而支持像 `"{.status.cpu.onceMaxRequest}/{.status.cpu.remaining}"` 这样的组合字段。
	//
	// 组合字段仅对 string 类型的列生效。对于非字符串列（integer/number/boolean/date），
	// 仅使用第一个表达式的第一个匹配结果。
	//
	// 字符串模板中的缺失值会渲染为 `-`。
	//
	// Template 和 Render 必须且只能设置其一。Render 是用于 jsonpath 无法表达的展示逻辑的逃生通道
	// （例如根据可辨识联合体中实际填充的子结构体进行条件渲染）。当设置了 Render 时，
	// 其返回的字符串将原样用作单元格值；列的 Type 会被忽略（该单元格按字符串处理）。
	JSONPathTemplateTableColumnDefinition struct {
		meta.TableColumnDefinition

		// Template 是一个 jsonpath 模板（kubectl `-o jsonpath` 语法），
		// 会对每个对象求值以生成该列的值。
		Template string

		// Render 是一个直接从类型化对象生成单元格值的回调。
		// 它接收与 OnList / OnGet 返回值相同的 runtime.Object。
		// 返回空字符串将生成空单元格。
		Render func(obj runtime.Object) string
	}
	_JSONPathTemplateTableColumn struct {
		// 对于基于 Template 的列，path 为非 nil。
		path *jsonpath.JSONPath
		// 如果解析后模板的第 i 个顶层节点是 action（`{...}`），则 isAction[i] 为 true；
		// 如果是字面文本，则为 false。与 jsonpath.JSONPath.FindResults 返回的切片对齐。
		// 仅当 path 非 nil 时有意义。
		isAction []bool
		// 对于基于 Render 的列，render 为非 nil。与 path 互斥。
		render func(obj runtime.Object) string
	}
	_JSONPathTemplateTableConvertor struct {
		headers []meta.TableColumnDefinition
		columns []_JSONPathTemplateTableColumn
		// 当至少有一列基于 Template，因而需要将对象转换为 unstructured
		// 以供 jsonpath 使用时，needsUnstructured 为 true。
		needsUnstructured bool
	}
)

// JSONPathTemplateColumn 返回一个 string 类型的 JSONPathTemplateTableColumnDefinition，
// 用于渲染指定的 jsonpath 模板。
//
// 这是常见场景的简写形式，即不包含 format、description 或 priority 的列。
func JSONPathTemplateColumn(name, template string) JSONPathTemplateTableColumnDefinition {
	return JSONPathTemplateTableColumnDefinition{
		TableColumnDefinition: meta.TableColumnDefinition{Name: name, Type: "string"},
		Template:              template,
	}
}

// RenderColumn 返回一个 string 类型的 JSONPathTemplateTableColumnDefinition，
// 使用指定的回调生成单元格值。
//
// T 必须是 OnList / OnGet 返回的具体类型，因为渲染前会将对象断言为该类型。
func RenderColumn[T runtime.Object](name string, render func(obj T) string) JSONPathTemplateTableColumnDefinition {
	return JSONPathTemplateTableColumnDefinition{
		TableColumnDefinition: meta.TableColumnDefinition{Name: name, Type: "string"},
		Render: func(obj runtime.Object) string {
			return render(obj.(T))
		},
	}
}

// NewJSONPathTemplateTableConvertor 创建一个从 jsonpath 模板（kubectl `-o jsonpath` 语法）
// 渲染各列的表格转换器。
//
// 它支持针对 string 类型列的多表达式模板（如 `"{.a}/{.b}"`），
// 并将解析到缺失键的表达式替换为 `-`。
func NewJSONPathTemplateTableConvertor(definitions ...JSONPathTemplateTableColumnDefinition) (rest.TableConvertor, error) {
	c := &_JSONPathTemplateTableConvertor{
		headers: []meta.TableColumnDefinition{
			{Name: "Name", Type: "string", Format: "name", Description: swaggerMetadataDescriptions["name"]},
		},
	}

	agePriority := int32(0)
	seen := make(map[string]struct{}, len(definitions))
	for i := range definitions {
		def := &definitions[i]
		hasTemplate := def.Template != ""
		hasRender := def.Render != nil
		switch {
		case def.Name == "":
			return nil, fmt.Errorf("column definition must have a non-empty name")
		case hasTemplate == hasRender:
			return nil, fmt.Errorf("column definition %q must set exactly one of Template or Render", def.Name)
		case def.Priority < 0:
			return nil, fmt.Errorf("column definition %q must have a non-negative priority", def.Name)
		}
		if _, dup := seen[def.Name]; dup {
			continue
		}
		seen[def.Name] = struct{}{}

		col := _JSONPathTemplateTableColumn{}
		if hasTemplate {
			// 解析两次：一次用于检查 AST 以区分文本与 action 槽位，
			// 另一次解析到 JSONPath 实例中用于执行。Parser 是导出的，但 JSONPath.parser 字段未导出，
			// 因此无法在两者之间共享 AST。
			parser := jsonpath.NewParser(def.Name)
			if err := parser.Parse(def.Template); err != nil {
				return nil, fmt.Errorf("invalid template %q for column %q: %w", def.Template, def.Name, err)
			}
			col.isAction = make([]bool, len(parser.Root.Nodes))
			for j, n := range parser.Root.Nodes {
				_, isText := n.(*jsonpath.TextNode)
				col.isAction[j] = !isText
			}
			col.path = jsonpath.New(def.Name).AllowMissingKeys(true)
			if err := col.path.Parse(def.Template); err != nil {
				return nil, fmt.Errorf("invalid template %q for column %q: %w", def.Template, def.Name, err)
			}
			c.needsUnstructured = true
		} else {
			col.render = def.Render
		}

		desc := def.Description
		if desc == "" {
			desc = "Description to " + def.Name
		}

		c.headers = append(c.headers, meta.TableColumnDefinition{
			Name:        def.Name,
			Description: desc,
			Type:        def.Type,
			Format:      def.Format,
			Priority:    def.Priority,
		})
		c.columns = append(c.columns, col)
		if def.Priority > agePriority {
			agePriority = def.Priority
		}
	}

	c.headers = append(c.headers, meta.TableColumnDefinition{
		Name:        "Age",
		Description: swaggerMetadataDescriptions["creationTimestamp"],
		Type:        "date",
		Priority:    agePriority + 1,
	})

	return c, nil
}

func (c *_JSONPathTemplateTableConvertor) ConvertToTable(_ context.Context, obj, tableOptions runtime.Object) (*meta.Table, error) {
	table := &meta.Table{}
	if opt, ok := tableOptions.(*meta.TableOptions); !ok || !opt.NoHeaders {
		table.ColumnDefinitions = c.headers
	}

	if m, err := kmeta.ListAccessor(obj); err == nil {
		table.ResourceVersion = m.GetResourceVersion()
		table.Continue = m.GetContinue()
		table.RemainingItemCount = m.GetRemainingItemCount()
	} else if m, err := kmeta.CommonAccessor(obj); err == nil {
		table.ResourceVersion = m.GetResourceVersion()
	}

	// headers[0] 是 Name，headers[len-1] 是 Age，中间是按 c.columns 顺序排列的用户自定义列。
	customHeaders := c.headers[1 : len(c.headers)-1]
	buf := &bytes.Buffer{}
	var err error
	table.Rows, err = metatable.MetaToTableRow(obj, func(obj runtime.Object, m meta.Object, name, age string) ([]any, error) {
		cells := make([]any, 0, 2+len(c.columns))
		cells = append(cells, name)

		// 仅当存在需要 jsonpath 的列时才转换为 unstructured。
		var data any
		if c.needsUnstructured {
			us, ok := obj.(runtime.Unstructured)
			if !ok {
				mm, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
				if err != nil {
					return nil, err
				}
				us = &unstructured.Unstructured{Object: mm}
			}
			data = us.UnstructuredContent()
		}

		for i := range c.columns {
			col := &c.columns[i]
			header := &customHeaders[i]

			if col.render != nil {
				cells = append(cells, col.render(obj))
				continue
			}

			results, ferr := col.path.FindResults(data)
			if ferr != nil {
				cells = append(cells, nil)
				continue
			}

			if header.Type == "string" {
				buf.Reset()
				if err := renderCustomFieldString(buf, col.path, col.isAction, results); err != nil {
					cells = append(cells, nil)
					continue
				}
				cells = append(cells, buf.String())
				continue
			}

			// 类型化单元格：取第一个 action 槽位的第一个匹配结果。
			if len(results) == 0 || len(results[0]) == 0 {
				cells = append(cells, nil)
				continue
			}
			cells = append(cells, cellForJSONValue(header.Type, results[0][0].Interface()))
		}

		cells = append(cells, age)
		return cells, nil
	})
	return table, err
}

// renderCustomFieldString 将 jsonpath.FindResults 返回的文本节点和 action 节点结果交错拼接。
func renderCustomFieldString(buf *bytes.Buffer, path *jsonpath.JSONPath, isAction []bool, results [][]reflect.Value) error {
	// 正常情况下 len(results) == len(isAction)。如果 range/end 配对导致数量变化，
	// 则对尾部槽位回退到默认的 Execute 风格行为。
	for i, slot := range results {
		switch {
		case i < len(isAction) && isAction[i] && len(slot) == 0:
			// 无需处理。
		default:
			if err := path.PrintResults(buf, slot); err != nil {
				return err
			}
		}
	}
	return nil
}

// cellForJSONValue 镜像了
// k8s.io/apiextensions-apiserver/pkg/registry/customresource/tableconvertor 中的未导出辅助函数。
func cellForJSONValue(headerType string, value any) any {
	if value == nil {
		return nil
	}
	switch headerType {
	case "integer":
		switch typed := value.(type) {
		case int64:
			return typed
		case float64:
			return int64(typed)
		case json.Number:
			if i64, err := typed.Int64(); err == nil {
				return i64
			}
		}
	case "number":
		switch typed := value.(type) {
		case int64:
			return float64(typed)
		case float64:
			return typed
		case json.Number:
			if f, err := typed.Float64(); err == nil {
				return f
			}
		}
	case "boolean":
		if b, ok := value.(bool); ok {
			return b
		}
	case "string":
		if s, ok := value.(string); ok {
			return s
		}
	case "date":
		if typed, ok := value.(string); ok {
			var timestamp meta.Time
			if err := timestamp.UnmarshalQueryParameter(typed); err != nil {
				return "<invalid>"
			}
			return metatable.ConvertToHumanReadableDateType(timestamp)
		}
	}
	return nil
}

type _DefaultTableColumnDefinition struct{}

// NewDefaultTableConvertor 创建一个默认转换器。
//
// 它借鉴了 k8s.io/apiserver/pkg/registry/rest.NewDefaultTableConvertor，
// 但修改了最后一列的名称。
func NewDefaultTableConvertor() rest.TableConvertor {
	return _DefaultTableColumnDefinition{}
}

func (c _DefaultTableColumnDefinition) ConvertToTable(ctx context.Context, object, tableOptions runtime.Object) (*meta.Table, error) {
	var table meta.Table
	fn := func(obj runtime.Object) error {
		m, err := kmeta.Accessor(obj)
		if err != nil {
			return &kerrors.StatusError{
				ErrStatus: meta.Status{
					Status:  meta.StatusFailure,
					Code:    http.StatusNotAcceptable,
					Reason:  meta.StatusReasonNotAcceptable,
					Message: fmt.Sprintf("do not support being converted to a Table: %v", err),
				},
			}
		}
		table.Rows = append(table.Rows, meta.TableRow{
			Cells: []any{
				m.GetName(),
				metatable.ConvertToHumanReadableDateType(m.GetCreationTimestamp()),
			},
			Object: runtime.RawExtension{
				Object: obj,
			},
		})
		return nil
	}

	switch {
	case kmeta.IsListType(object):
		if err := kmeta.EachListItem(object, fn); err != nil {
			return nil, err
		}
	default:
		if err := fn(object); err != nil {
			return nil, err
		}
	}

	if m, err := kmeta.ListAccessor(object); err == nil {
		table.ResourceVersion = m.GetResourceVersion()
		table.Continue = m.GetContinue()
		table.RemainingItemCount = m.GetRemainingItemCount()
	} else {
		if m, err := kmeta.CommonAccessor(object); err == nil {
			table.ResourceVersion = m.GetResourceVersion()
		}
	}
	if opt, ok := tableOptions.(*meta.TableOptions); !ok || !opt.NoHeaders {
		table.ColumnDefinitions = []meta.TableColumnDefinition{
			{
				Name:        "Name",
				Description: swaggerMetadataDescriptions["name"],
				Type:        "string",
				Format:      "name",
			},
			{
				Name:        "Age",
				Description: swaggerMetadataDescriptions["creationTimestamp"],
				Type:        "date",
			},
		}
	}

	return &table, nil
}
