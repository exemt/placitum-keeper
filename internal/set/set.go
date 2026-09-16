package set

import (
	"container/heap"
	"crypto/rand"
	"encoding/binary"
	"net/netip"
	"sync"
	"time"
)

type Type int

const (
	TypeString Type = iota
	TypeCIDR
	TypeNumeric
)

type Definition struct {
	ID     string
	Name   string
	Type   Type
	Limit  int
	TTL    int64
	Active bool
	Hash   bool
}

type Entry struct {
	Exp    int64
	Origin string
	Reason string
	hash   uint64
}

type Record struct {
	Value  string
	Exp    int64
	Origin string
	Reason string
	Prefix netip.Prefix
}

const (
	OpAdd    = "add"
	OpRemove = "remove"
)

type Delta struct {
	Op   string
	Rec  Record
	hash uint64
}

type Change struct {
	Seq    uint64
	Hash   uint64
	At     time.Time
	Deltas []Delta
}

type Set struct {
	Def Definition

	mu      sync.Mutex
	epoch   uint64
	key     Key
	seq     uint64
	hash    uint64
	entries map[string]Entry
	exp     expHeap

	pend     map[string]pendEntry
	pendSeq  uint64
	pendHash uint64
	pendSize int
	gen      uint64
}

type pendEntry struct {
	e       Entry
	present bool
	seq     uint64
}

func New(def Definition, seq0 uint64) *Set {
	s := &Set{
		Def:     def,
		seq:     seq0,
		pendSeq: seq0,
		entries: map[string]Entry{},
		pend:    map[string]pendEntry{},
	}

	s.epoch = randomU64()
	_, _ = rand.Read(s.key[:])

	return s
}

func randomU64() uint64 {
	var b [8]byte
	_, _ = rand.Read(b[:])

	v := binary.LittleEndian.Uint64(b[:])
	if v == 0 {
		v = 1
	}

	return v
}

func (s *Set) hashOf(r Record) uint64 {
	if s.Def.Type == TypeCIDR && r.Prefix.IsValid() {
		return SipHashBytes(s.key, PrefixMaterial(r.Prefix))
	}

	return HashValue(s.key, s.Def.Type, r.Value)
}

func (s *Set) Load(recs []Record) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.load(recs)
}

func (s *Set) Resume(epoch uint64, key Key, seq uint64, recs []Record) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.epoch, s.key, s.seq = epoch, key, seq
	s.load(recs)
}

func (s *Set) load(recs []Record) {
	s.hash = 0
	s.entries = make(map[string]Entry, len(recs))
	s.exp = s.exp[:0]

	for _, r := range recs {
		e, ok := s.entries[r.Value]
		if !ok {
			e.hash = s.hashOf(r)
			s.hash ^= e.hash
		}

		e.Exp, e.Origin, e.Reason = r.Exp, r.Origin, r.Reason
		s.entries[r.Value] = e

		if r.Exp > 0 {
			s.exp = append(s.exp, expItem{value: r.Value, exp: r.Exp})
		}
	}

	heap.Init(&s.exp)
	s.settle()
}

func (s *Set) settle() {
	s.pend = map[string]pendEntry{}
	s.pendSeq, s.pendHash, s.pendSize = s.seq, s.hash, len(s.entries)
}

func (s *Set) State() (epoch uint64, seq uint64, hash uint64, key Key) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.epoch, s.seq, s.hash, s.key
}

func (s *Set) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.entries)
}

func (s *Set) Size() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.pendSize
}

func (s *Set) Gen() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.gen
}

func (s *Set) lookup(v string) (e Entry, exists bool, planned bool) {
	if p, ok := s.pend[v]; ok {
		return p.e, p.present, true
	}

	e, ok := s.entries[v]

	return e, ok, false
}

func (s *Set) Fresh(values []string, seen map[string]bool) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	n := 0

	for _, v := range values {
		if seen[v] {
			continue
		}

		if _, exists, _ := s.lookup(v); !exists {
			n++
		}
	}

	return n
}

type Plan struct {
	Deltas   []Delta
	Rejected []Record
	Seq      uint64
	Hash     uint64
	Gen      uint64
}

func (s *Set) Plan(deltas []Delta) Plan {
	s.mu.Lock()
	defer s.mu.Unlock()

	const (
		fromCommitted = iota
		fromBatch
		fromFlight
	)

	over := map[string]pendEntry{}
	size := s.pendSize
	hash := s.pendHash
	seq := s.pendSeq + 1

	lookup := func(v string) (Entry, bool, int) {
		if p, ok := over[v]; ok {
			return p.e, p.present, fromBatch
		}

		e, exists, planned := s.lookup(v)
		if planned {
			return e, exists, fromFlight
		}

		return e, exists, fromCommitted
	}

	applied := make([]Delta, 0, len(deltas))
	var rejected []Record

	for _, d := range deltas {
		r := d.Rec
		cur, exists, from := lookup(r.Value)

		switch d.Op {
		case OpAdd:
			var h uint64

			if !exists {
				if s.Def.Limit > 0 && size >= s.Def.Limit {
					rejected = append(rejected, r)
					continue
				}

				h = s.hashOf(r)
				hash ^= h
				size++
			} else {
				if from != fromFlight && cur.Exp == r.Exp && cur.Origin == r.Origin && cur.Reason == r.Reason {
					continue
				}

				h = cur.hash
			}

			over[r.Value] = pendEntry{e: Entry{Exp: r.Exp, Origin: r.Origin, Reason: r.Reason, hash: h}, present: true}
			applied = append(applied, Delta{Op: OpAdd, Rec: r, hash: h})

		case OpRemove:
			if !exists {
				continue
			}

			over[r.Value] = pendEntry{}
			hash ^= cur.hash
			size--
			applied = append(applied, Delta{
				Op:   OpRemove,
				Rec:  Record{Value: r.Value, Origin: r.Origin, Reason: r.Reason, Prefix: r.Prefix},
				hash: cur.hash,
			})
		}
	}

	if len(applied) == 0 {
		return Plan{Rejected: rejected, Seq: s.seq, Hash: s.hash, Gen: s.gen}
	}

	for v, p := range over {
		p.seq = seq
		s.pend[v] = p
	}

	s.pendSeq, s.pendHash, s.pendSize = seq, hash, size

	return Plan{Deltas: applied, Rejected: rejected, Seq: seq, Hash: hash, Gen: s.gen}
}

func (s *Set) Commit(p Plan, now time.Time) *Change {
	if len(p.Deltas) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, d := range p.Deltas {
		r := d.Rec
		cur, exists := s.entries[r.Value]

		switch d.Op {
		case OpAdd:
			if !exists {
				s.hash ^= d.hash
			}

			s.entries[r.Value] = Entry{Exp: r.Exp, Origin: r.Origin, Reason: r.Reason, hash: d.hash}

			if r.Exp > 0 && (!exists || cur.Exp == 0 || r.Exp < cur.Exp) {
				heap.Push(&s.exp, expItem{value: r.Value, exp: r.Exp})
			}

		case OpRemove:
			if exists {
				delete(s.entries, r.Value)
				s.hash ^= cur.hash
			}
		}

		if q, ok := s.pend[r.Value]; ok && q.seq == p.Seq {
			delete(s.pend, r.Value)
		}
	}

	s.seq = p.Seq

	if len(s.pend) == 0 {
		s.pendSeq, s.pendHash, s.pendSize = s.seq, s.hash, len(s.entries)
	}

	return &Change{Seq: s.seq, Hash: s.hash, At: now, Deltas: p.Deltas}
}

func (s *Set) Abort() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.settle()
	s.gen++
}

func (s *Set) Expired(now time.Time) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	nowMs := now.UnixMilli()

	var (
		out  []string
		back []expItem
	)

	for s.exp.Len() > 0 {
		top := s.exp[0]
		if top.exp > nowMs {
			break
		}

		heap.Pop(&s.exp)

		cur, ok := s.entries[top.value]
		if !ok {
			continue
		}

		if cur.Exp != top.exp {
			if cur.Exp > top.exp {
				back = append(back, expItem{value: top.value, exp: cur.Exp})
			}

			continue
		}

		back = append(back, top)

		if _, planned := s.pend[top.value]; planned {
			continue
		}

		out = append(out, top.value)
	}

	for _, it := range back {
		heap.Push(&s.exp, it)
	}

	return out
}

func (s *Set) Get(value string) (Entry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.entries[value]

	return e, ok
}

func (s *Set) Contains(value string, now time.Time) bool {
	e, ok := s.Get(value)
	if !ok {
		return false
	}

	return e.Exp == 0 || e.Exp > now.UnixMilli()
}

func (s *Set) Snapshot() (epoch uint64, seq uint64, hash uint64, key Key, recs []Record) {
	s.mu.Lock()
	defer s.mu.Unlock()

	recs = make([]Record, 0, len(s.entries))
	for v, e := range s.entries {
		recs = append(recs, Record{Value: v, Exp: e.Exp, Origin: e.Origin, Reason: e.Reason})
	}

	return s.epoch, s.seq, s.hash, s.key, recs
}

type expItem struct {
	value string
	exp   int64
}

type expHeap []expItem

func (h expHeap) Len() int           { return len(h) }
func (h expHeap) Less(i, j int) bool { return h[i].exp < h[j].exp }
func (h expHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *expHeap) Push(x any)        { *h = append(*h, x.(expItem)) }
func (h *expHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	*h = old[:n-1]

	return it
}
