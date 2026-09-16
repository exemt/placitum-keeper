package set

import "net/netip"

func Material(t Type, value string) []byte {
	if t == TypeCIDR {
		if p, ok := ParsePrefix(value); ok {
			return PrefixMaterial(p)
		}
	}

	return []byte(value)
}

func ParsePrefix(value string) (netip.Prefix, bool) {
	if p, err := netip.ParsePrefix(value); err == nil {
		return materialPrefix(p), true
	}

	if a, err := netip.ParseAddr(value); err == nil {
		a = a.Unmap()

		return netip.PrefixFrom(a, a.BitLen()), true
	}

	return netip.Prefix{}, false
}

func materialPrefix(p netip.Prefix) netip.Prefix {
	a, bits := p.Addr(), p.Bits()

	if a.Is4In6() && bits >= 96 {
		a, bits = a.Unmap(), bits-96
	}

	return netip.PrefixFrom(a, bits).Masked()
}

func PrefixMaterial(p netip.Prefix) []byte {
	if p.Addr().Is4() {
		b := p.Addr().As4()

		return append([]byte{4, byte(p.Bits())}, b[:]...)
	}

	b := p.Addr().As16()

	return append([]byte{6, byte(p.Bits())}, b[:]...)
}

func HashValue(k Key, t Type, value string) uint64 {
	return SipHashBytes(k, Material(t, value))
}

func HashValues(k Key, t Type, values []string) uint64 {
	var h uint64
	for _, v := range values {
		h ^= HashValue(k, t, v)
	}

	return h
}
