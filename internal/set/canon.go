package set

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"net/netip"
	"strconv"
	"strings"
)

const ValueMax = 255

func MD5Hex(v string) string {
	sum := md5.Sum([]byte(v))

	return hex.EncodeToString(sum[:])
}

var (
	ErrEmpty   = errors.New("empty value")
	ErrTooLong = errors.New("value too long")
	ErrType    = errors.New("value does not match set type")
)

func Canon(t Type, raw string) (string, error) {
	v, _, err := CanonPrefix(t, raw)

	return v, err
}

func CanonPrefix(t Type, raw string) (string, netip.Prefix, error) {
	v := strings.TrimSpace(raw)
	if v == "" {
		return "", netip.Prefix{}, ErrEmpty
	}

	if len(v) > ValueMax {
		return "", netip.Prefix{}, ErrTooLong
	}

	switch t {
	case TypeCIDR:
		return canonCIDR(v)
	case TypeNumeric:
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return "", netip.Prefix{}, ErrType
		}

		return strconv.FormatInt(n, 10), netip.Prefix{}, nil
	default:
		return v, netip.Prefix{}, nil
	}
}

func canonCIDR(v string) (string, netip.Prefix, error) {
	if p, err := netip.ParsePrefix(v); err == nil {
		p = p.Masked()

		return p.String(), materialPrefix(p), nil
	}

	a, err := netip.ParseAddr(v)
	if err != nil {
		return "", netip.Prefix{}, ErrType
	}

	if a.Is4In6() {
		a = a.Unmap()
	}

	p := netip.PrefixFrom(a, a.BitLen())

	return p.String(), p, nil
}

func TypeOf(catalog string) Type {
	switch catalog {
	case "ip", "ipv4", "cidr":
		return TypeCIDR
	case "numeric":
		return TypeNumeric
	default:
		return TypeString
	}
}
