package netx

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"time"
)

// IsConnected 用于检查给定地址是否可访问。
//
// 该网络支持 tcp、udp、tls 和 unix 多种协议选择。
func IsConnected(ctx context.Context, network, address string, timeout time.Duration) error {
	var (
		dialer = &net.Dialer{Timeout: timeout}
		dial   = dialer.DialContext
	)

	if network == "tls" {
		network = "tcp"
		tlsDialer := &tls.Dialer{
			NetDialer: dialer,
			Config: &tls.Config{
				InsecureSkipVerify: true, // nolint:gosec
			},
		}
		dial = tlsDialer.DialContext
	}

	c, err := dial(ctx, network, address)
	if err != nil {
		return fmt.Errorf("%s is unreachable", address)
	}

	err = c.Close()
	if err != nil {
		return fmt.Errorf("%s is unreachable: closing connection: %w", address, err)
	}

	return nil
}
