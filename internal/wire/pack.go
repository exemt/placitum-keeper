package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net/netip"
	"sort"

	"github.com/exemt/placitum-keeper/internal/set"
)

const (
	PackVersion = 1

	KindSnapshot = 1
	KindPackage  = 2

	PackTypeCIDR   = 1
	PackTypeString = 2

	FlagReasons = 1

	RecAdd    = 1
	RecRemove = 2

	packHeader = 52
	packMagic  = "WAFS"
)

var (
	ErrPackMagic   = errors.New("pack: not a WAFS object")
	ErrPackVersion = errors.New("pack: unsupported version")
	ErrPackShort   = errors.New("pack: truncated")
)

type PackHead struct {
	Kind  uint8
	Type  uint8
	Flags uint8
	Epoch uint64
	Seq   uint64
	Hash  uint64
	Key   set.Key
	Count uint32
}

type PackRecord struct {
	Op     uint8
	Prefix netip.Prefix
	Value  string
	Exp    int64
	Reason string
}

func PackType(t set.Type) uint8 {
	if t == set.TypeCIDR {
		return PackTypeCIDR
	}

	return PackTypeString
}

func PackFlags(t set.Type) uint8 {
	if t == set.TypeCIDR {
		return 0
	}

	return FlagReasons
}

func RecordOf(t set.Type, op uint8, r set.Record) PackRecord {
	out := PackRecord{Op: op, Exp: r.Exp, Reason: r.Reason, Value: r.Value}

	if t == set.TypeCIDR {
		if r.Prefix.IsValid() {
			out.Prefix = r.Prefix
		} else {
			out.Prefix, _ = set.ParsePrefix(r.Value)
		}
	}

	return out
}

func Pack(head PackHead, recs []PackRecord) []byte {
	if head.Kind == KindSnapshot {
		sort.Slice(recs, func(i, j int) bool { return less(head.Type, recs[i], recs[j]) })
	}

	head.Count = uint32(len(recs))

	size := packHeader
	for i := range recs {
		size += recordSize(head.Type, head.Flags, &recs[i])
	}

	out := make([]byte, 0, size)
	out = append(out, packMagic...)
	out = append(out, PackVersion, head.Kind, head.Type, head.Flags)
	out = binary.LittleEndian.AppendUint64(out, head.Epoch)
	out = binary.LittleEndian.AppendUint64(out, head.Seq)
	out = binary.LittleEndian.AppendUint64(out, head.Hash)
	out = append(out, head.Key[:]...)
	out = binary.LittleEndian.AppendUint32(out, head.Count)

	for i := range recs {
		out = appendRecord(out, head.Type, head.Flags, &recs[i])
	}

	return out
}

func less(t uint8, a, b PackRecord) bool {
	if t == PackTypeCIDR {
		fa, fb := family(a.Prefix), family(b.Prefix)
		if fa != fb {
			return fa < fb
		}

		if a.Prefix.Bits() != b.Prefix.Bits() {
			return a.Prefix.Bits() < b.Prefix.Bits()
		}

		return a.Prefix.Addr().Compare(b.Prefix.Addr()) < 0
	}

	return a.Value < b.Value
}

func family(p netip.Prefix) int {
	if p.Addr().Is4() {
		return 4
	}

	return 6
}

func recordSize(t uint8, flags uint8, r *PackRecord) int {
	if t == PackTypeCIDR {
		if family(r.Prefix) == 4 {
			return 1 + 1 + 1 + 4 + 8
		}

		return 1 + 1 + 1 + 16 + 8
	}

	n := 1 + 2 + len(r.Value) + 8
	if flags&FlagReasons != 0 {
		n += 2 + len(r.Reason)
	}

	return n
}

func appendRecord(out []byte, t uint8, flags uint8, r *PackRecord) []byte {
	out = append(out, r.Op)

	if t == PackTypeCIDR {
		out = append(out, set.PrefixMaterial(r.Prefix)...)

		return binary.LittleEndian.AppendUint64(out, uint64(r.Exp))
	}

	out = binary.LittleEndian.AppendUint16(out, uint16(len(r.Value)))
	out = append(out, r.Value...)
	out = binary.LittleEndian.AppendUint64(out, uint64(r.Exp))

	if flags&FlagReasons != 0 {
		out = binary.LittleEndian.AppendUint16(out, uint16(len(r.Reason)))
		out = append(out, r.Reason...)
	}

	return out
}

func Unpack(data []byte) (PackHead, []PackRecord, error) {
	var head PackHead

	if len(data) < packHeader {
		return head, nil, ErrPackShort
	}

	if string(data[:4]) != packMagic {
		return head, nil, ErrPackMagic
	}

	if data[4] != PackVersion {
		return head, nil, ErrPackVersion
	}

	head.Kind = data[5]
	head.Type = data[6]
	head.Flags = data[7]
	head.Epoch = binary.LittleEndian.Uint64(data[8:])
	head.Seq = binary.LittleEndian.Uint64(data[16:])
	head.Hash = binary.LittleEndian.Uint64(data[24:])
	copy(head.Key[:], data[32:48])
	head.Count = binary.LittleEndian.Uint32(data[48:])

	recs := make([]PackRecord, 0, head.Count)
	pos := packHeader

	for i := uint32(0); i < head.Count; i++ {
		var r PackRecord

		if pos+1 > len(data) {
			return head, nil, ErrPackShort
		}

		r.Op = data[pos]
		pos++

		switch head.Type {
		case PackTypeCIDR:
			if pos+2 > len(data) {
				return head, nil, ErrPackShort
			}

			fam, bits := data[pos], int(data[pos+1])
			pos += 2

			switch fam {
			case 4:
				if pos+4+8 > len(data) {
					return head, nil, ErrPackShort
				}

				var b [4]byte
				copy(b[:], data[pos:pos+4])
				r.Prefix = netip.PrefixFrom(netip.AddrFrom4(b), bits)
				pos += 4

			case 6:
				if pos+16+8 > len(data) {
					return head, nil, ErrPackShort
				}

				var b [16]byte
				copy(b[:], data[pos:pos+16])
				r.Prefix = netip.PrefixFrom(netip.AddrFrom16(b), bits)
				pos += 16

			default:
				return head, nil, fmt.Errorf("pack: address family %d", fam)
			}

		case PackTypeString:
			if pos+2 > len(data) {
				return head, nil, ErrPackShort
			}

			n := int(binary.LittleEndian.Uint16(data[pos:]))
			pos += 2

			if pos+n+8 > len(data) {
				return head, nil, ErrPackShort
			}

			r.Value = string(data[pos : pos+n])
			pos += n

		default:
			return head, nil, fmt.Errorf("pack: set type %d", head.Type)
		}

		r.Exp = int64(binary.LittleEndian.Uint64(data[pos:]))
		pos += 8

		if head.Type == PackTypeString && head.Flags&FlagReasons != 0 {
			if pos+2 > len(data) {
				return head, nil, ErrPackShort
			}

			n := int(binary.LittleEndian.Uint16(data[pos:]))
			pos += 2

			if pos+n > len(data) {
				return head, nil, ErrPackShort
			}

			r.Reason = string(data[pos : pos+n])
			pos += n
		}

		recs = append(recs, r)
	}

	return head, recs, nil
}

func (r PackRecord) Material(t uint8) []byte {
	if t == PackTypeCIDR {
		return set.PrefixMaterial(r.Prefix)
	}

	return []byte(r.Value)
}
