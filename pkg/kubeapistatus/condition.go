package kubeapistatus

import (
	"fmt"
	"reflect"
	"time"

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	klog "k8s.io/klog/v2"
)

// ConditionType 是状态的类型。
type ConditionType string

// IsTrue 检查对象的状态值是否为 True，
// object 必须是指针。
func (c ConditionType) IsTrue(obj any) bool {
	return getStatus(obj, string(c)) == string(meta.ConditionTrue)
}

// IsFalse 检查对象的状态值是否为 False，
// object 必须是指针。
func (c ConditionType) IsFalse(obj any) bool {
	return getStatus(obj, string(c)) == string(meta.ConditionFalse)
}

// IsUnknown 检查对象的状态值是否为 Unknown，
// object 必须是指针。
func (c ConditionType) IsUnknown(obj any) bool {
	return getStatus(obj, string(c)) == string(meta.ConditionUnknown)
}

// Exists 检查 conditionType 是否存在于对象的 .Status.Conditions 字段中，
// object 必须是指针。
func (c ConditionType) Exists(obj any) bool {
	return findCond(obj, string(c)) != nil
}

// GetMessage 从对象的 .Status.Conditions 字段中获取指定 conditionType 的消息。
func (c ConditionType) GetMessage(obj any) string {
	cond := findCond(obj, string(c))
	if cond == nil {
		return ""
	}
	return getFieldValue(*cond, "Message").String()
}

// GetReason 从对象的 .Status.Conditions 字段中获取指定 conditionType 的原因。
func (c ConditionType) GetReason(obj any) string {
	cond := findCond(obj, string(c))
	if cond == nil {
		return ""
	}
	return getFieldValue(*cond, "Reason").String()
}

// True 将对象的 .Status.Conditions 字段中的状态值设为 True，
// object 必须是指针。
func (c ConditionType) True(obj any, reason, message string) {
	setStatus(obj, string(c), string(meta.ConditionTrue), reason, message)
}

// False 将对象的 .Status.Conditions 字段中的状态值设为 False，
// object 必须是指针。
func (c ConditionType) False(obj any, reason, message string) {
	setStatus(obj, string(c), string(meta.ConditionFalse), reason, message)
}

// Unknown 将对象的 .Status.Conditions 字段中的状态值设为 Unknown，
// object 必须是指针。
func (c ConditionType) Unknown(obj any, reason, message string) {
	setStatus(obj, string(c), string(meta.ConditionUnknown), reason, message)
}

// Status 将对象的 .Status.Conditions 字段中的状态值设为自定义值，
// object 必须是指针。
func (c ConditionType) Status(obj any, status, reason, message string) {
	setStatus(obj, string(c), status, reason, message)
}

// LastTransitionTime 为对象的 .Status.Conditions 字段中的指定 conditionType 设置最后转换时间，
// object 必须是指针。
func (c ConditionType) LastTransitionTime(obj any, ts string) {
	upsertTS(obj, string(c), ts)
}

// upsertTS 创建或更新状态条件并设置最后转换时间。
//
// 该字段类型为 meta.Time，因此必须构造值并通过 Set 而非 SetString 进行设置——
// 与下面 setTS 的做法相同。ts 采用 RFC3339 格式，这正是 meta.Time 的序列化格式，因此 getTS 返回的值可直接作为此处的输入。
//
// 无法解析的 ts 会触发 panic：所有调用方传入的都是格式化后的值，因此这属于编程错误，
// 且本文件对另一类可检测到的错误（非指针对象）也已采用 panic 处理。
func upsertTS(obj any, condName, ts string) {
	parsed, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		panic(fmt.Sprintf("kubeapistatus: last transition time must be RFC3339, got %q", ts))
	}

	cond := upsertCond(obj, condName)
	getFieldValue(cond, "LastTransitionTime").Set(reflect.ValueOf(meta.Time{Time: parsed.UTC()}))
}

// setTS 为状态条件设置最后转换时间。
func setTS(value reflect.Value) {
	now := meta.Time{
		Time: time.Now().UTC(),
	}

	getFieldValue(value, "LastTransitionTime").Set(reflect.ValueOf(now))
}

// getStatus 从状态条件中获取状态值。
func getStatus(obj any, condName string) string {
	cond := findCond(obj, condName)
	if cond == nil {
		return ""
	}
	return getFieldValue(*cond, "Status").String()
}

// setStatus 为状态条件设置状态值和消息。
func setStatus(obj any, condName, status, reason, message string) {
	if reflect.TypeOf(obj).Kind() != reflect.Pointer {
		panic("obj passed must be a pointer")
	}

	cond := upsertCond(obj, condName)

	// 在覆盖之前先读取先前的状态。LastTransitionTime 标记的是状态转换，因此
	// 只有在 Status 实际发生变化时才能更新——否则每次调谐都重写条件的控制器
	// 会每次都打上新时间戳，进而导致由相等性判断保护的状态写入不断触发，
	// 每次写入都会唤醒下一次调谐。
	//
	// 这里使用 getFieldValue 而非 getValue：cond 已经是 reflect.Value，而 getValue 接收的是 "any"
	// 并对传入值做反射——如果传入 reflect.Value，它会去反射该结构体本身，
	// 而该结构体并不存在 "Status" 字段。因此比较永远不会匹配，导致每次写入
	// （包括没有任何变更的写入）都会更新时间戳。
	transitioned := getFieldValue(cond, "Status").String() != status

	setValue(cond, "Status", status)
	setValue(cond, "Reason", reason)
	setValue(cond, "Message", message)

	if transitioned {
		setTS(cond)
	}
}

// getValue 根据字段名从对象中获取值。
func getValue(obj any, name string, snames ...string) reflect.Value {
	if obj == nil {
		return reflect.Value{}
	}
	v := reflect.ValueOf(obj)
	t := v.Type()
	if t.Kind() == reflect.Pointer {
		v = v.Elem()
	}

	field := v.FieldByName(name)
	if len(snames) == 0 {
		return field
	}
	return getFieldValue(field, snames[0], snames[1:]...)
}

// setValue 为状态条件设置字段值。
func setValue(cond reflect.Value, fieldName, newValue string) {
	value := getFieldValue(cond, fieldName)
	if value.String() != newValue {
		value.SetString(newValue)
	}
}

// findCond 从对象中查找状态条件。
func findCond(obj any, condName string) *reflect.Value {
	condSlice := getValue(obj, "Status", "Conditions")
	if !condSlice.IsValid() {
		condSlice = getValue(obj, "Conditions")
	}
	return queryCondsByName(obj, condSlice, condName)
}

// upsertCond 创建或更新状态条件。
func upsertCond(obj any, condName string) reflect.Value {
	conds := getValue(obj, "Status", "Conditions")
	cond := queryCondsByName(obj, conds, condName)
	if cond != nil {
		return *cond
	}

	newCond := reflect.New(conds.Type().Elem()).Elem()
	newCond.FieldByName("Type").SetString(condName)
	newCond.FieldByName("Status").SetString("Unknown")
	setTS(newCond)

	conds.Set(reflect.Append(conds, newCond))
	return *queryCondsByName(obj, conds, condName)
}

// queryCondsByName 按名称查询状态条件。
func queryCondsByName(obj any, val reflect.Value, condName string) *reflect.Value {
	defer func() {
		if recover() != nil {
			klog.Fatalf("failed to find .Status.Conditions field on %v", reflect.TypeOf(obj))
		}
	}()

	for i := 0; i < val.Len(); i++ {
		cond := val.Index(i)
		typeVal := getFieldValue(cond, "Type")
		if typeVal.String() == condName {
			return &cond
		}
	}

	return nil
}

// getFieldValue 根据字段名获取值。
func getFieldValue(v reflect.Value, name string, snames ...string) reflect.Value {
	if !v.IsValid() {
		return v
	}
	field := v.FieldByName(name)
	if len(snames) == 0 {
		return field
	}
	return getFieldValue(field, snames[0], snames[1:]...)
}
