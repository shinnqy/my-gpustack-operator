package priors

import (
	"fmt"
	"strings"
)

// LaunchParameter 表示单个启动参数。
//
// 它包含参数名及其值，因为引擎既支持将值附加在标志中，也支持将值放在下一个参数中，这两种写法描述的是同一次启动配置。
type LaunchParameter struct {
	// Flag 是包含前导短横线的参数名称。
	Flag string

	// Value 是参数值；对于不带值的布尔标志，该字段为空。
	Value string
}

// ParseLaunchParameters 读取有序的参数列表。
//
// 它同时支持两种写法，因为引擎既接受值紧随其后的等号连接形式，也接受值位于下一参数的分离形式。
func ParseLaunchParameters(tokens []string) ([]LaunchParameter, error) {
	parameters := make([]LaunchParameter, 0, len(tokens))
	for i := 0; i < len(tokens); i++ {
		token := tokens[i]
		if token == "--" {
			return nil, fmt.Errorf("the arguments carry a %q separator", token)
		}
		flag, value, inline := strings.Cut(token, "=")
		if inline {
			if value == "" {
				return nil, fmt.Errorf("the argument %q carries no value", token)
			}
			parameters = append(parameters, LaunchParameter{Flag: flag, Value: value})
			continue
		}
		if i+1 < len(tokens) && !strings.HasPrefix(tokens[i+1], "-") {
			value = tokens[i+1]
			i++
		}
		parameters = append(parameters, LaunchParameter{Flag: flag, Value: value})
	}
	return parameters, nil
}

// CheckEffectiveLaunchParameters 检查生效的启动参数是否与给定的启动参数描述相同的启动配置。
//
// 若不一致则拒绝，因为包含其他参数的生效列表对应的是不同的启动配置。
//
// 若无生效参数列表则为空，因为直接按给定参数运行的启动无需额外记录。
func CheckEffectiveLaunchParameters(launch, effective []string) error {
	if len(effective) == 0 {
		return nil
	}

	launchParameters, err := ParseLaunchParameters(launch)
	if err != nil {
		return fmt.Errorf("the launch arguments cannot be read: %w", err)
	}
	effectiveParameters, err := ParseLaunchParameters(effective)
	if err != nil {
		return fmt.Errorf("the effective launch arguments cannot be read: %w", err)
	}
	if len(effectiveParameters) != len(launchParameters) {
		return fmt.Errorf(
			"the effective arguments name %d arguments and the launch arguments name %d",
			len(effectiveParameters),
			len(launchParameters),
		)
	}
	for i := range launchParameters {
		if launchParameters[i].Flag != effectiveParameters[i].Flag {
			return fmt.Errorf(
				"the effective argument %q is not the launch argument %q",
				effectiveParameters[i].Flag,
				launchParameters[i].Flag,
			)
		}
	}
	return nil
}
