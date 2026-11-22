package netx

import (
	"net"
)

type IPv4 net.IPNet

func MustIPv4FromCIDR(cidr string) IPv4 {
	ip, err := IPv4FromCIDR(cidr)
	if err != nil {
		panic(err)
	}

	return ip
}

func IPv4FromCIDR(cidr string) (IPv4, error) {
	_, n, err := net.ParseCIDR(cidr)
	if err != nil {
		return IPv4{}, err
	}

	// NB(thxCode): 返回 RFC 4632 和 RFC 4291 中定义的结果。
	return IPv4FromIPNet(*n), nil
}

func IPv4FromIPNet(n net.IPNet) IPv4 {
	return IPv4(net.IPNet{
		IP:   n.IP.To4().Mask(n.Mask),
		Mask: n.Mask,
	})
}

// Contains 在给定的 net.IP 包含于该 IPv4 网段中时返回 true。
func (l IPv4) Contains(ip net.IP) bool {
	return (*net.IPNet)(&l).Contains(ip)
}

// String 返回该 IPv4 的字符串表示形式。
func (l IPv4) String() string {
	return (*net.IPNet)(&l).String()
}

// Next 返回具有相同掩码的下一个 IPv4 网段。
//
// 例如，
// 给定 172.16.128.0/18 时返回 172.16.192.0/18，
// 给定 172.16.192.0/18 时返回 172.17.0.0/18。
func (l IPv4) Next() IPv4 {
	var (
		o, s = l.Mask.Size()
		i, j int
	)
	{
		for i, j = range []int{24, 16, 8, 0} {
			if o > j {
				break
			}
		}
		i = 3 - i
		j = 8 + j - o
	}

	r := IPv4{
		IP:   []byte{l.IP[0], l.IP[1], l.IP[2], l.IP[3]},
		Mask: net.CIDRMask(o, s),
	}
	r.IP[i] += 1 << j

	if r.IP[i] == 0 && i > 0 {
		r.IP[i-1] += 1
	}

	return r
}
