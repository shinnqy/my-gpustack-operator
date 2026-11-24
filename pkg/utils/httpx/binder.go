package httpx

import (
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"

	"gpustack.ai/gpustack/pkg/utils/json"
)

var (
	ErrBindNilRequest = errors.New("http: nil request")
	ErrBindFuncNil    = errors.New("http: nil bind function")
)

// BindJSON 在请求携带 JSON 正文时将其解码到传入的结构体指针。
func BindJSON(r *http.Request, rc any) error {
	if r == nil {
		return ErrBindNilRequest
	}

	switch r.Method {
	default:
		return nil
	case http.MethodPost, http.MethodPut, http.MethodPatch:
	}

	switch strings.TrimSpace(strings.Split(r.Header.Get("Content-Type"), ";")[0]) {
	default:
		return nil
	case "application/json":
	}

	return json.NewDecoder(r.Body).Decode(rc)
}

// BindQuery 按 query 标签读取网关请求中的字符串、重复的字符串和布尔开关。
// 单值字段取第一次出现的值，切片保留各次出现的顺序。缺省字段沿用原值。
func BindQuery(r *http.Request, rc any) error {
	if r == nil {
		return ErrBindNilRequest
	}
	dst := reflect.ValueOf(rc)
	if dst.Kind() != reflect.Pointer || dst.IsNil() || dst.Elem().Kind() != reflect.Struct {
		return errors.New("http: expected pointer to query struct")
	}
	dst = dst.Elem()
	query := r.URL.Query()
	for i := range dst.NumField() {
		field := dst.Field(i)
		key, _, _ := strings.Cut(dst.Type().Field(i).Tag.Get("query"), ",")
		values := query[key]
		if key == "" || key == "-" || len(values) == 0 || !field.CanSet() {
			continue
		}
		switch field.Kind() {
		case reflect.String:
			field.SetString(values[0])
		case reflect.Bool:
			value := values[0]
			if value == "" {
				value = "false"
			}
			flag, err := strconv.ParseBool(value)
			if err != nil {
				return err
			}
			field.SetBool(flag)
		case reflect.Slice:
			if field.Type() != reflect.TypeFor[[]string]() {
				return fmt.Errorf("http: unsupported query field %q (%s)", key, field.Type())
			}
			field.Set(reflect.ValueOf(values))
		default:
			return fmt.Errorf("http: unsupported query field %q (%s)", key, field.Type())
		}
	}
	return nil
}

// BindFunc 描述从 HTTP 请求中读取参数并填入目标结构体的函数。
type BindFunc func(r *http.Request, rc any) error

// BindWith 按给定顺序绑定参数。某一步返回错误后停止，已成功读取的字段保留在目标中。
func BindWith(r *http.Request, rc any, b BindFunc, bs ...BindFunc) error {
	if b == nil {
		return ErrBindFuncNil
	}

	if err := b(r, rc); err != nil {
		return err
	}

	for _, b := range bs {
		if b == nil {
			return ErrBindFuncNil
		}

		if err := b(r, rc); err != nil {
			return err
		}
	}

	return nil
}
