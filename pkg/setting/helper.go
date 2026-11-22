package setting

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	conregname "github.com/google/go-containerregistry/pkg/name"

	"gpustack.ai/gpustack/pkg/utils/osx"
	"gpustack.ai/gpustack/pkg/utils/stringx"
)

// Initializer 定义用于初始化配置项的值及其环境变量元数据的函数类型。
// declared 表示 envName 是否存在（包括其值为空的情况）。
type Initializer func(name string) (value, envName string, declared bool)

// InitializeFromEnv 查找名为 `GPUSTACK_${UpperSnakeCase_of_SettingName}` 的环境变量，
// 未找到时返回默认值，并报告该变量在注册时是否存在。
func InitializeFromEnv(defValue ...string) Initializer {
	return func(name string) (string, string, bool) {
		envName := "GPUSTACK_" + stringx.UnderscoreUpper(name)
		value := osx.ExpandEnv(envName, defValue...)
		_, declared := osx.LookupEnv(envName)
		return value, envName, declared
	}
}

// Admission 定义用于校验配置写入的准入函数类型。
type Admission func(ctx context.Context, oldVal, newVal string) (err error)

// ErrAdmissionSkipped 是 Admission 返回的错误，用于表示跳过此次校验。
var ErrAdmissionSkipped = errors.New("admission skipped")

// AdmitWith 将多个 Admission 组合为一个；
// 若未提供任何 Admission，则等效于 Allow。
//
// 若其中任一 Admission 返回错误，
// 则立即返回该错误。
//
// 若其中任一 Admission 返回 ErrAdmissionSkipped，
// 则函数将立即放行返回。
func AdmitWith(admits ...Admission) Admission {
	if len(admits) == 0 {
		admits = []Admission{Allow()}
	}
	return func(ctx context.Context, oldValue, newValue string) error {
		var err error
		for i := range admits {
			err = admits[i](ctx, oldValue, newValue)
			if err != nil {
				break
			}
		}
		if errors.Is(err, ErrAdmissionSkipped) {
			return nil
		}
		return err
	}
}

// Allow 实现了 Admission 类型，
// 表示允许修改该配置值。
func Allow() Admission {
	return func(ctx context.Context, oldVal, newVal string) error {
		return nil
	}
}

// AllowBlank 实现了 Admission 类型，
// 表示当新值为空时允许修改。
//
// AllowBlank 通常与其他 Admission 组合使用；
// 若允许任意输入，请改用 Allow。
func AllowBlank() Admission {
	return func(ctx context.Context, oldVal, newVal string) error {
		if !isBlank(newVal) {
			return nil
		}
		return ErrAdmissionSkipped
	}
}

// DisallowBlank 实现了 Admission 类型，
// 表示当新值非空时才允许修改。
//
// DisallowBlank 通常与其他 Admission 组合使用；
// 若禁止任何输入，请改用 Disallow。
func DisallowBlank() Admission {
	return func(ctx context.Context, oldVal, newVal string) error {
		if !isBlank(newVal) {
			return nil
		}
		return errors.New("blank value")
	}
}

// AllowBool 实现了 Admission 类型，
// 表示当新值为布尔值时允许修改。
func AllowBool() Admission {
	return func(ctx context.Context, oldVal, newVal string) error {
		_, err := strconv.ParseBool(newVal)
		return err
	}
}

// AllowInt64InRange 实现了 Admission 类型，
// 表示当新值为 int64 且位于指定范围内时允许修改。
func AllowInt64InRange(minVal, maxVal int64) Admission {
	return func(ctx context.Context, oldVal, newVal string) error {
		v, err := strconv.ParseInt(newVal, 10, 64)
		if err != nil {
			return err
		}
		if v < minVal || v > maxVal {
			return fmt.Errorf("value %d is out of range [%d, %d]", v, minVal, maxVal)
		}
		return nil
	}
}

// AllowDurationInRange 实现了 Admission 类型，
// 表示当新值为 float64 且位于指定范围内时允许修改。
func AllowDurationInRange(minVal, maxVal time.Duration) Admission {
	return func(ctx context.Context, oldVal, newVal string) error {
		v, err := time.ParseDuration(newVal)
		if err != nil {
			return err
		}
		if v < minVal || v > maxVal {
			return fmt.Errorf("value %v is out of range [%v, %v]", v, minVal, maxVal)
		}
		return nil
	}
}

// AllowIPv4CIDRList 实现了 Admission 类型，
// 表示当新值为逗号分隔的 IPv4 CIDR 列表（含空值）时允许修改。IPv6 前缀会在写入阶段
// 被直接拒绝，而非在渲染阶段被静默丢弃，从而确保 ipBlock except 列表不会因静默过滤而丢失条目。
func AllowIPv4CIDRList() Admission {
	return func(ctx context.Context, oldVal, newVal string) error {
		for _, segment := range strings.Split(newVal, ",") {
			cidr := strings.TrimSpace(segment)
			if cidr == "" {
				continue
			}
			ip, _, err := net.ParseCIDR(cidr)
			if err != nil {
				return fmt.Errorf("invalid CIDR %q: %w", cidr, err)
			}
			if ip.To4() == nil {
				return fmt.Errorf("invalid CIDR %q: only IPv4 prefixes can ride an IPv4 rule's except list", cidr)
			}
		}
		return nil
	}
}

// AllowUrlWithSchema 实现了 Admission 类型，
// 表示当新值为具有指定协议头的 URL 时允许修改。
// 该 Admission 允许空的新值，
// 若不允许为空，请与 DisallowBlank 组合使用。
func AllowUrlWithSchema(schema string, otherSchemas ...string) Admission {
	return func(ctx context.Context, oldVal, newVal string) error {
		return checkUrl(newVal, func(u url.URL) error {
			if u.Scheme == schema {
				return nil
			}
			for _, s := range otherSchemas {
				if u.Scheme == s {
					return nil
				}
			}
			return fmt.Errorf("invalid schema: %q, allowed: %s",
				u.Scheme, strings.Join(append([]string{schema}, otherSchemas...), ", "))
		})
	}
}

// AllowContainerRegistry 实现了 Admission 类型，
// 表示当新值为合法的容器镜像仓库地址时允许修改。
func AllowContainerRegistry() Admission {
	return func(ctx context.Context, oldVal, newVal string) error {
		_, err := conregname.NewRepository(newVal)
		return err
	}
}

// AllowContainerImageReference 实现了 Admission 类型，
// 表示当新值为合法的容器镜像引用时允许修改。
func AllowContainerImageReference() Admission {
	return func(ctx context.Context, oldVal, newVal string) error {
		_, err := conregname.ParseReference(newVal)
		return err
	}
}

func isBlank(s string) bool {
	return slices.Contains([]string{"", "{}", "[]"}, strings.TrimSpace(s))
}

func checkUrl(str string, check func(url.URL) error) error {
	v, err := url.Parse(str)
	if err != nil {
		return fmt.Errorf("%s is illegal URL format: %w", str, err)
	}

	port := v.Port()
	if port != "" {
		p, err := strconv.ParseUint(port, 10, 32)
		if err != nil {
			return fmt.Errorf("parsing given port: %w", err)
		}

		if p > 65535 {
			return fmt.Errorf("given port %d: exceeded upper limit", p)
		}
	}

	return check(*v)
}
