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

// Initializer defines the stereotype for initializing a setting's value and environment metadata.
// declared reports whether envName was present, including when its value was empty.
type Initializer func(name string) (value, envName string, declared bool)

// InitializeFromEnv searches the variable of the `GPUSTACK_${UpperSnakeCase_of_SettingName}`,
// returns the default value if not found, and reports the variable's registration-time presence.
func InitializeFromEnv(defValue ...string) Initializer {
	return func(name string) (string, string, bool) {
		envName := "GPUSTACK_" + stringx.UnderscoreUpper(name)
		value := osx.ExpandEnv(envName, defValue...)
		_, declared := osx.LookupEnv(envName)
		return value, envName, declared
	}
}

// Admission defines the stereotype for validating writing.
type Admission func(ctx context.Context, oldVal, newVal string) (err error)

// ErrAdmissionSkipped is the error returned by Admission to indicate the validation is skipped.
var ErrAdmissionSkipped = errors.New("admission skipped")

// AdmitWith combines multiple Admission into one,
// if no Admission given, it will be Allow.
//
// If one Admission of the given Admissions returns an error,
// the error will be returned immediately.
//
// If one Admission of the given Admissions returns an ErrAdmissionSkipped,
// the function will return true immediately.
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

// Allow implements the Admission stereotype,
// which means the value can be modified.
func Allow() Admission {
	return func(ctx context.Context, oldVal, newVal string) error {
		return nil
	}
}

// AllowBlank implements the Admission stereotype,
// which means the value can be modified if blank.
//
// AllowBlank always combines with other Admission,
// if allow input anything, please use Allow instead.
func AllowBlank() Admission {
	return func(ctx context.Context, oldVal, newVal string) error {
		if !isBlank(newVal) {
			return nil
		}
		return ErrAdmissionSkipped
	}
}

// DisallowBlank implements the Admission stereotype,
// which means the value can be modified if not blank.
//
// DisallowBlank always combines with other Admission,
// if disallow input anything, please use Disallow instead.
func DisallowBlank() Admission {
	return func(ctx context.Context, oldVal, newVal string) error {
		if !isBlank(newVal) {
			return nil
		}
		return errors.New("blank value")
	}
}

// AllowBool implements the Admission stereotype,
// which means the value can be modified if it's boolean.
func AllowBool() Admission {
	return func(ctx context.Context, oldVal, newVal string) error {
		_, err := strconv.ParseBool(newVal)
		return err
	}
}

// AllowInt64InRange implements the Admission stereotype,
// which means the value can be modified if it's int64 and in the given range.
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

// AllowDurationInRange implements the Admission stereotype,
// which means the value can be modified if it's float64 and in the given range.
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

// AllowIPv4CIDRList implements the Admission stereotype,
// which means the value can be modified if it's a comma-separated list of IPv4 CIDRs, blank
// included. IPv6 prefixes are refused at the write rather than dropped at the render, so an
// ipBlock except list never loses an entry to a silent filter.
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

// AllowUrlWithSchema implements the Admission stereotype,
// which means the value can be modified if it is a URL with specified schemas.
// This Admission allows blank new value,
// if not allowed, combine with DisallowBlank.
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

// AllowContainerRegistry implements the Admission stereotype,
// which means the value can be modified if it's container registry.
func AllowContainerRegistry() Admission {
	return func(ctx context.Context, oldVal, newVal string) error {
		_, err := conregname.NewRepository(newVal)
		return err
	}
}

// AllowContainerImageReference implements the Admission stereotype,
// which means the value can be modified if it's container image reference.
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
