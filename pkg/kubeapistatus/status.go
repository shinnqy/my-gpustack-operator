package kubeapistatus

import (
	"reflect"
	"strings"

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type (
	// StatusSummarizer 按配置的步骤读取 Conditions，生成状态摘要。
	StatusSummarizer interface {
		// GetSummary 遍历 Conditions，返回阶段及其说明。
		GetSummary(status any) (phase, phaseMessage string)
		// Summarize 遍历 Conditions，把阶段及说明写回状态对象。
		Summarize(status any)
	}
	// StatusScore 表示评估一个条件后得到的优先级。
	StatusScore int
	// StatusDecide 根据条件的状态和原因生成阶段、说明及评分。当前步骤完成后才继续下一步。
	StatusDecide func(st meta.ConditionStatus, reason string) (phase, message string, score StatusScore)
)

const (
	_ StatusScore = iota
	// StatusDone 表示当前步骤已完成，可以继续。
	StatusDone
	// StatusTransitioning 表示仍在过渡，停止遍历。
	StatusTransitioning
	// StatusInterrupted 表示步骤受阻，停止遍历。
	StatusInterrupted
)

// NewSummarizer 为各组步骤建立判定路径，并应用自定义规则。
//   - stepsGroup 按组列出顺序执行的步骤，默认在当前条件为 True 后进入下一步。
//   - arranges 可以改变单步判定，例如允许某个条件为 False 时继续，或自定义各状态的显示文字。
func NewSummarizer[T ~string](stepsGroup [][]T, arranges ...func(Decision[T])) StatusSummarizer {
	if len(stepsGroup) == 0 {
		panic("empty steps group")
	}

	fs := make(paths[T], 0, len(stepsGroup))
	for i := range stepsGroup {
		fs = append(fs, newPath(stepsGroup[i], arranges...))
	}

	return fs
}

// paths 汇总多条判定路径，选择 StatusScore 最高的结果。
type paths[T ~string] []path[T]

func (ps paths[T]) GetSummary(st any) (phase, phaseMessage string) {
	stValue := reflect.ValueOf(st)
	for stValue.Kind() == reflect.Interface || stValue.Kind() == reflect.Pointer {
		stValue = stValue.Elem()
	}

	phaseField := stValue.FieldByName("Phase")
	if phaseField.IsValid() && phaseField.Kind() == reflect.String {
		phase = phaseField.String()
	}
	phaseMessageField := stValue.FieldByName("PhaseMessage")
	if phaseMessageField.IsValid() && phaseMessageField.Kind() == reflect.String {
		phaseMessage = phaseMessageField.String()
	}

	conditionsField := stValue.FieldByName("Conditions")
	if !conditionsField.IsValid() || conditionsField.Kind() != reflect.Slice {
		return phase, formatMessage(phaseMessage)
	}

	var r *_ScoredStatusDescriptor

	for i := range ps {
		l := ps[i].Walk(conditionsField, phase, phaseMessage)
		if r == nil {
			r = l
			continue
		}

		// 保留评分较高的结果。
		ls, rs := l.Score, r.Score
		if ls <= rs {
			continue
		}
		r, rs = l, ls

		// 若结果已高于 StatusInterrupted，提前结束比较。
		if rs > StatusInterrupted {
			break
		}
	}

	if r != nil {
		phase, phaseMessage = r.Phase, r.PhaseMessage
	}

	return phase, formatMessage(phaseMessage)
}

func (ps paths[T]) Summarize(st any) {
	phase, phaseMessage := ps.GetSummary(st)

	stValue := reflect.ValueOf(st)
	for stValue.Kind() == reflect.Interface || stValue.Kind() == reflect.Pointer {
		stValue = stValue.Elem()
	}

	phaseField := stValue.FieldByName("Phase")
	if phaseField.IsValid() && phaseField.CanSet() && phaseField.Kind() == reflect.String {
		phaseField.SetString(phase)
	}
	phaseMessageField := stValue.FieldByName("PhaseMessage")
	if phaseMessageField.IsValid() && phaseMessageField.CanSet() && phaseMessageField.Kind() == reflect.String {
		phaseMessageField.SetString(phaseMessage)
	}
}

// newPath 根据给定步骤建立路径和索引。
func newPath[T ~string](steps []T, arranges ...func(Decision[T])) path[T] {
	if len(steps) == 0 {
		panic("empty steps")
	}

	p := path[T]{
		steps:       steps,
		stepsIndex:  make(map[T]int, len(steps)),
		stepsDecide: make([]StatusDecide, len(steps)),
	}
	for i := range steps {
		// 步骤重复意味着路径成环，立即报告配置错误。
		if _, exist := p.stepsIndex[steps[i]]; exist {
			panic("found loop")
		}
		p.stepsIndex[steps[i]] = i
		p.stepsDecide[i] = getGeneralDecide(steps[i])
	}

	// 用调用方提供的规则覆盖默认判定。
	for i := range arranges {
		arranges[i](Decision[T](p))
	}

	return p
}

// path 保存有序步骤，沿这条路径汇总 Conditions。
type path[T ~string] struct {
	steps       []T
	stepsIndex  map[T]int
	stepsDecide []StatusDecide
}

type (
	_ScoredStatusDescriptor struct {
		Phase        string
		PhaseMessage string
		Score        StatusScore
	}
)

func (f path[T]) Walk(conditionsField reflect.Value, phase, phaseMessage string) *_ScoredStatusDescriptor {
	s := &_ScoredStatusDescriptor{
		Phase:        phase,
		PhaseMessage: phaseMessage,
	}

	// 条件列表有值时，按步骤计算摘要。
	if conditionsField.Len() != 0 {
		// 为当前路径涉及的条件建立索引，后续可以直接定位。
		stepsConditionIndex := make([]int, len(f.steps))

		for i := 0; i < conditionsField.Len(); i++ {
			cValue := conditionsField.Index(i)
			for cValue.Kind() == reflect.Interface || cValue.Kind() == reflect.Pointer {
				cValue = cValue.Elem()
			}

			var cType T
			{
				typeField := cValue.FieldByName("Type")
				if typeField.IsValid() && typeField.Kind() == reflect.String {
					cType = T(typeField.String())
				}
			}

			// 索引加一，保留零表示未找到。
			if idx, exist := f.stepsIndex[cType]; exist {
				stepsConditionIndex[idx] = i + 1
			}
		}

		// 按路径中声明的顺序生成摘要。
		for i := range f.steps {
			if stepsConditionIndex[i] == 0 {
				if i == 0 {
					// 第一步对应的条件尚未出现，结束本路径的判定。
					return s
				}
				// 当前条件尚未出现，继续检查下一步。
				continue
			}
			cValue := conditionsField.Index(stepsConditionIndex[i] - 1)

			// 从条件的状态和原因计算显示结果。
			var (
				cStatus meta.ConditionStatus
				cReason string
			)
			{
				statusField := cValue.FieldByName("Status")
				if statusField.IsValid() && statusField.Kind() == reflect.String {
					cStatus = meta.ConditionStatus(statusField.String())
				} else {
					cStatus = meta.ConditionUnknown
				}
				reasonField := cValue.FieldByName("Reason")
				if reasonField.IsValid() && reasonField.Kind() == reflect.String {
					cReason = reasonField.String()
				}
			}

			s.Phase, s.PhaseMessage, s.Score = f.stepsDecide[i](cStatus, cReason)
			if s.PhaseMessage == "" {
				messageField := cValue.FieldByName("Message")
				if messageField.IsValid() && messageField.Kind() == reflect.String {
					s.PhaseMessage = messageField.String()
				}
			}

			// 步骤受阻或仍在过渡时，保留该步骤的结果并停止遍历。
			if s.Score == StatusInterrupted || s.Score == StatusTransitioning {
				break
			}
		}
	}

	// 尚未得到阶段时，用最后一步的默认判定生成摘要。
	if s.Phase == "" {
		s.Phase, s.PhaseMessage, s.Score = f.stepsDecide[len(f.steps)-1]("", "")
	}

	return s
}

// Decision 允许调用方自定义某一步骤的判定方式。
type Decision[T ~string] path[T]

// Make 为已有步骤设置专用的判断函数。步骤不存在或传入 nil 时保留原有规则。
func (d Decision[T]) Make(step T, with StatusDecide) Decision[T] {
	if with != nil {
		if idx, exist := d.stepsIndex[step]; exist {
			d.stepsDecide[idx] = with
		}
	}

	return d
}

// getGeneralDecide 生成默认判定函数。它根据步骤名称选择显示文字，遇到 False 标记为中断，
// 遇到 Unknown 标记为过渡，遇到 True 则进入下一步。
func getGeneralDecide[T ~string](step T) StatusDecide {
	s := string(step)

	// 根据词尾生成各阶段的显示文字，尤其要处理时态变化。
	displays := [3]string{s, s, s} // 依次表示过渡、出错和完成。

	for m, r := range replacements {
		if !strings.HasSuffix(s, m) {
			continue
		}
		p := s[:len(s)-len(m)]
		displays[0], displays[1], displays[2] = p+r.T, p+r.E, p+r.D
	}

	return func(st meta.ConditionStatus, _ string) (string, string, StatusScore) {
		switch st {
		case meta.ConditionUnknown:
			return displays[0], "", StatusTransitioning
		case meta.ConditionFalse:
			return displays[1], "", StatusInterrupted
		}

		return displays[2], "", StatusDone
	}
}

// replacements 定义步骤名称的词尾替换规则。T、E、D 分别对应过渡、出错和完成时的显示文字。
var replacements = map[string]struct {
	T, E, D string
}{
	"Running":     {"Running", "Failed", "Completed"},
	"Pending":     {"Pending", "Failed", "Pending"},
	"Progressing": {"Progressing", "Progressing", "Progressed"},
	"Imported":    {"Importing", "ImportedFailed", "Imported"},
	"Connected":   {"Connecting", "Disconnected", "Connected"},
	"Initialized": {"Initializing", "InitializeFailed", "Initialized"},
	"Scheduled":   {"Scheduling", "ScheduleFailed", "Scheduled"},
	"Accepted":    {"Accepting", "NotAccepted", "Accepted"},
	"Deployed":    {"Deploying", "DeployFailed", "Deployed"},
	"Stopped":     {"Stopping", "StopFailed", "Stopped"},
	"Synced":      {"Syncing", "SyncFailed", "Synced"},
	"Available":   {"Preparing", "Unavailable", "Available"},
	"Ready":       {"Starting", "NotReady", "Ready"},
	"Active":      {"Activating", "Inactive", "Active"},
	"Canceled":    {"Canceling", "CancelFailed", "Canceled"},
	"Planned":     {"Planning", "Failed", "Planned"},
	"Applied":     {"Applying", "Failed", "Succeeded"},
}

// formatMessage 将消息开头的小写英文字母改为大写。如果末尾缺少句末标点，则补上句号。
func formatMessage(str string) string {
	if str == "" {
		return str
	}

	runes := []rune(str)
	if runes[0] >= 'a' && runes[0] <= 'z' {
		runes[0] = runes[0] - 'a' + 'A'
	}
	if runes[len(runes)-1] != '.' && runes[len(runes)-1] != '!' && runes[len(runes)-1] != '?' {
		runes = append(runes, '.')
	}

	return string(runes)
}
