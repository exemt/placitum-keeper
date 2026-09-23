package keeper

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/exemt/placitum-keeper/internal/set"
	"github.com/exemt/placitum-keeper/internal/state"
	"github.com/exemt/placitum-keeper/internal/store"
	"github.com/exemt/placitum-keeper/internal/wire"
)

type Options struct {
	Name        string
	Tick        time.Duration
	Sweep       time.Duration
	BatchMax    int
	Coalesce    time.Duration
	QueueMax    int
	Pipeline    int
	DiffTTL     time.Duration
	SnapshotTTL time.Duration
	// InlineMax is the largest package, in bytes, that travels inside the
	// diff frame as well as through Redis; zero sends every package by
	// reference only.
	InlineMax int
}

type Bus interface {
	Publish(setName string, body []byte)
}

type job struct {
	op       string
	recs     []set.Record
	reply    chan jobResult
	deadline time.Time
}

type jobResult struct {
	code  string
	seq   uint64
	epoch uint64
}

const codeStale = "\x00stale"

type Stats struct {
	Writes     atomic.Int64
	Rejects    atomic.Int64
	Diffs      atomic.Int64
	Inline     atomic.Int64
	Expired    atomic.Int64
	Snapshots  atomic.Int64
	Packs      atomic.Int64
	StoreErr   atomic.Int64
	StoreMs    atomic.Int64
	Overloaded atomic.Int64
	Stale      atomic.Int64
}

type batch struct {
	at      time.Time
	jobs    []job
	results []jobResult
	plan    set.Plan
	pkgKey  string
	pkg     []byte
}

type holder struct {
	s       *set.Set
	jobs    chan job
	queued  atomic.Int64
	batches chan *batch
	stop    chan struct{}
	done    sync.WaitGroup
	stats   Stats

	snapMu sync.Mutex
	snap   *snapObj
}

type Keeper struct {
	defs  store.Store
	state state.Store
	bus   Bus
	log   *slog.Logger
	opts  Options

	mu   sync.RWMutex
	sets map[string]*holder

	ready atomic.Bool
	now   func() time.Time
}

func New(defs store.Store, st state.Store, bus Bus, log *slog.Logger, opts Options) *Keeper {
	if opts.BatchMax <= 0 {
		opts.BatchMax = 10_000
	}

	if opts.QueueMax <= 0 {
		opts.QueueMax = 50_000
	}

	if opts.Pipeline <= 0 {
		opts.Pipeline = 4
	}

	if opts.Sweep <= 0 {
		opts.Sweep = time.Second
	}

	if opts.SnapshotTTL <= 0 {
		opts.SnapshotTTL = 30 * time.Second
	}

	if opts.DiffTTL < 2*opts.SnapshotTTL {
		opts.DiffTTL = 2 * opts.SnapshotTTL
	}

	return &Keeper{
		defs:  defs,
		state: st,
		bus:   bus,
		log:   log,
		opts:  opts,
		sets:  map[string]*holder{},
		now:   time.Now,
	}
}

func seq0() uint64 {
	return uint64(time.Now().UnixMilli()) << 4
}

func (k *Keeper) Ready() bool { return k.ready.Load() }

func (k *Keeper) Start(ctx context.Context) error {
	defs, err := k.defs.LoadDefinitions(ctx)
	if err != nil {
		return fmt.Errorf("load definitions: %w", err)
	}

	for _, d := range defs {
		if err := k.mount(ctx, d, false); err != nil {
			return err
		}
	}

	k.ready.Store(true)
	k.log.Info("keeper ready", "sets", len(defs))

	go k.tickLoop()

	return nil
}

func (k *Keeper) mount(ctx context.Context, d set.Definition, fresh bool) error {
	if old := k.get(d.Name); old != nil && !fresh && old.s.Def == d {
		return nil
	}

	meta, recs, err := k.state.Load(ctx, d.Name)

	badMeta := errors.Is(err, state.ErrBadMeta)
	if err != nil && !badMeta {
		return fmt.Errorf("load %s: %w", d.Name, err)
	}

	if badMeta {
		k.log.Warn("set meta is unreadable, opening a new epoch", "set", d.Name, "error", err.Error())
	}

	keep := badMeta || (meta != nil && meta.ID == d.ID && meta.Type == d.Type)
	resume := keep && !fresh && !badMeta
	s := set.New(d, seq0())

	switch {
	case resume:
		s.Resume(meta.Epoch, meta.Key, meta.Seq, recs)

		if _, _, hash, _ := s.State(); hash != meta.Hash {
			k.log.Warn("set state disagrees with its meta, opening a new epoch",
				"set", d.Name, "meta", wire.Hex(meta.Hash), "content", wire.Hex(hash))

			s = set.New(d, seq0())
			s.Load(recs)
		}

	case keep:
		s.Load(recs)

	default:
		if meta != nil {
			k.log.Info("set state belongs to another definition, starting empty",
				"set", d.Name, "was", meta.ID, "now", d.ID)
		}
	}

	epoch, seq, hash, key := s.State()

	if err := k.state.Reset(ctx, d.Name, state.Meta{
		ID: d.ID, Type: d.Type, Epoch: epoch, Key: key, Seq: seq, Hash: hash,
	}, keep); err != nil {
		return fmt.Errorf("reset %s: %w", d.Name, err)
	}

	h := &holder{
		s:       s,
		jobs:    make(chan job, k.opts.QueueMax),
		batches: make(chan *batch, k.opts.Pipeline),
		stop:    make(chan struct{}),
	}

	k.mu.Lock()
	old := k.sets[d.Name]
	k.sets[d.Name] = h

	var renamed []*holder
	for name, other := range k.sets {
		if name != d.Name && other.s.Def.ID == d.ID {
			renamed = append(renamed, other)
			delete(k.sets, name)
			k.log.Info("set renamed", "from", name, "to", d.Name)
		}
	}
	k.mu.Unlock()

	if old != nil {
		old.shutdown()
	}

	for _, other := range renamed {
		other.shutdown()

		if err := k.state.Drop(ctx, other.s.Def.Name); err != nil {
			k.log.Warn("drop renamed set state", "set", other.s.Def.Name, "error", err.Error())
		}
	}

	h.done.Add(2)
	go k.planner(h)
	go k.committer(h)

	k.log.Info("set mounted", "set", d.Name, "entries", s.Len(), "resumed", resume,
		"epoch", wire.Hex(epoch), "seq", seq, "hash", wire.Hex(hash))

	k.publishTick(h)

	return nil
}

func (h *holder) shutdown() {
	close(h.stop)
	h.done.Wait()
}

func (k *Keeper) Define(ctx context.Context, name string) wire.DefineReply {
	d, err := k.defs.LoadDefinition(ctx, name)
	if err != nil {
		k.log.Error("define: definitions", "set", name, "error", err.Error())

		return wire.DefineReply{Error: wire.ErrStoreUnavailable}
	}

	if d == nil || !d.Active {
		k.drop(ctx, name)

		return wire.DefineReply{OK: true}
	}

	if err := k.mount(ctx, *d, true); err != nil {
		k.log.Error("define: mount", "set", name, "error", err.Error())

		return wire.DefineReply{Error: wire.ErrStoreUnavailable}
	}

	h := k.get(name)
	epoch, _, _, _ := h.s.State()

	return wire.DefineReply{OK: true, Epoch: wire.Hex(epoch)}
}

func (k *Keeper) Reload(ctx context.Context) error {
	defs, err := k.defs.LoadDefinitions(ctx)
	if err != nil {
		return err
	}

	seen := map[string]bool{}

	for _, d := range defs {
		seen[d.Name] = true

		if err := k.mount(ctx, d, false); err != nil {
			return err
		}
	}

	k.mu.RLock()
	var gone []string
	for name := range k.sets {
		if !seen[name] {
			gone = append(gone, name)
		}
	}
	k.mu.RUnlock()

	for _, name := range gone {
		k.drop(ctx, name)
	}

	return nil
}

func (k *Keeper) drop(ctx context.Context, name string) {
	k.mu.Lock()
	h := k.sets[name]
	delete(k.sets, name)
	k.mu.Unlock()

	if h == nil {
		return
	}

	h.shutdown()

	if err := k.state.Drop(ctx, name); err != nil {
		k.log.Warn("drop set state", "set", name, "error", err.Error())
	}

	k.log.Info("set dropped", "set", name)
}

func (k *Keeper) get(name string) *holder {
	k.mu.RLock()
	defer k.mu.RUnlock()

	return k.sets[name]
}

func (k *Keeper) Submit(ctx context.Context, name string, ev wire.Event) wire.EventReply {
	if !k.ready.Load() {
		return wire.EventReply{Error: wire.ErrNotReady}
	}

	h := k.get(name)
	if h == nil {
		return wire.EventReply{Error: wire.ErrUnknownSet}
	}

	op := ev.Op
	if op == "" {
		op = set.OpAdd
	}

	if op != set.OpAdd && op != set.OpRemove {
		return wire.EventReply{Error: wire.ErrBadOp}
	}

	if strings.TrimSpace(ev.Origin) == "" {
		return wire.EventReply{Error: wire.ErrNoOrigin}
	}

	values := ev.Values
	if ev.Value != "" {
		values = append([]string{ev.Value}, values...)
	}

	if len(values) == 0 {
		return wire.EventReply{Error: wire.ErrWrongType}
	}

	recs := make([]set.Record, 0, len(values))
	seen := make(map[string]struct{}, len(values))

	for _, raw := range values {
		value, prefix, err := set.CanonPrefix(h.s.Def.Type, raw)
		if err != nil {
			code := wire.ErrWrongType
			if errors.Is(err, set.ErrTooLong) {
				code = wire.ErrTooLong
			}

			return wire.EventReply{Error: code}
		}

		if h.s.Def.Hash && !ev.Hashed {
			value = set.MD5Hex(value)
		}

		if _, dup := seen[value]; dup {
			continue
		}

		seen[value] = struct{}{}
		recs = append(recs, set.Record{Value: value, Origin: ev.Origin, Reason: ev.Reason, Prefix: prefix})
	}

	if op == set.OpAdd {
		ttl := ev.TTL
		if ttl <= 0 {
			ttl = h.s.Def.TTL
		}

		if ev.Forever {
			return wire.EventReply{Error: wire.ErrForever}
		}

		if ttl <= 0 {
			return wire.EventReply{Error: wire.ErrNoTTL}
		}

		exp := k.now().Add(time.Duration(ttl) * time.Second).UnixMilli()

		for i := range recs {
			recs[i].Exp = exp
		}
	}

	n := int64(len(recs))

	if h.queued.Load()+n > int64(k.opts.QueueMax) {
		h.stats.Overloaded.Add(1)

		return wire.EventReply{Error: wire.ErrOverloaded}
	}

	h.queued.Add(n)

	deadline, _ := ctx.Deadline()
	j := job{op: op, recs: recs, reply: make(chan jobResult, 1), deadline: deadline}

	select {
	case h.jobs <- j:
	case <-ctx.Done():
		h.queued.Add(-n)

		return wire.EventReply{Error: wire.ErrStoreUnavailable}
	case <-h.stop:
		h.queued.Add(-n)

		return wire.EventReply{Error: wire.ErrUnknownSet}
	}

	select {
	case r := <-j.reply:
		if r.code != "" {
			reply := wire.EventReply{Error: r.code}
			if r.code == wire.ErrFull {
				reply.Limit = h.s.Def.Limit
			}

			return reply
		}

		return wire.EventReply{OK: true, Seq: r.seq, Epoch: wire.Hex(r.epoch)}
	case <-ctx.Done():
		return wire.EventReply{Error: wire.ErrStoreUnavailable}
	}
}

func (k *Keeper) planner(h *holder) {
	defer h.done.Done()

	sweep := time.NewTicker(k.opts.Sweep)
	defer sweep.Stop()

	for {
		select {
		case <-h.stop:
			return
		case j := <-h.jobs:
			k.plan(h, k.gather(h, j))
		case <-sweep.C:
			vals := h.s.Expired(k.now())
			for len(vals) > 0 {
				cut := len(vals)
				if cut > k.opts.BatchMax {
					cut = k.opts.BatchMax
				}

				recs := make([]set.Record, cut)
				for i, v := range vals[:cut] {
					recs[i] = set.Record{Value: v, Origin: k.opts.Name, Reason: "EXPIRED"}
				}

				h.stats.Expired.Add(int64(cut))
				k.plan(h, []job{{op: set.OpRemove, recs: recs}})
				vals = vals[cut:]
			}
		}
	}
}

func (h *holder) take(j job) job {
	h.queued.Add(-int64(len(j.recs)))

	return j
}

func (k *Keeper) gather(h *holder, first job) []job {
	batch := []job{h.take(first)}
	n := len(first.recs)

	var window <-chan time.Time
	if k.opts.Coalesce > 0 {
		t := time.NewTimer(k.opts.Coalesce)
		defer t.Stop()
		window = t.C
	}

	for n < k.opts.BatchMax {
		select {
		case j := <-h.jobs:
			batch = append(batch, h.take(j))
			n += len(j.recs)

			continue
		default:
		}

		if window == nil {
			break
		}

		select {
		case j := <-h.jobs:
			batch = append(batch, h.take(j))
			n += len(j.recs)
		case <-window:
			return batch
		case <-h.stop:
			return batch
		}
	}

	return batch
}

func (k *Keeper) plan(h *holder, jobs []job) {
	now := k.now()
	epoch, _, _, _ := h.s.State()
	results := make([]jobResult, len(jobs))

	var (
		deltas   []set.Delta
		accepted []int
	)

	seen := map[string]bool{}
	size := h.s.Size()

	for i, j := range jobs {
		if !j.deadline.IsZero() && now.After(j.deadline) {
			results[i].code = codeStale
			h.stats.Stale.Add(1)

			continue
		}

		if len(j.recs) == 0 {
			results[i].code = wire.ErrBadRequest

			continue
		}

		if j.op == set.OpAdd && h.s.Def.Limit > 0 {
			values := make([]string, len(j.recs))
			for n, r := range j.recs {
				values[n] = r.Value
			}

			fresh := h.s.Fresh(values, seen)
			if size+fresh > h.s.Def.Limit {
				results[i].code = wire.ErrFull

				continue
			}

			for _, v := range values {
				seen[v] = true
			}

			size += fresh
		}

		for _, r := range j.recs {
			deltas = append(deltas, set.Delta{Op: j.op, Rec: r})
		}

		accepted = append(accepted, i)
	}

	var p set.Plan

	if len(deltas) > 0 {
		p = h.s.Plan(deltas)
		rejected := map[string]bool{}

		for _, r := range p.Rejected {
			rejected[r.Value] = true
		}

		for _, i := range accepted {
			if len(jobs[i].recs) == 1 && rejected[jobs[i].recs[0].Value] {
				results[i].code = wire.ErrFull

				continue
			}

			results[i].seq = p.Seq
		}
	}

	if len(p.Deltas) == 0 {
		for i, j := range jobs {
			k.answer(h, j, results[i], epoch)
		}

		return
	}

	b := &batch{at: now, plan: p}

	for i, j := range jobs {
		if results[i].code != "" {
			k.answer(h, j, results[i], epoch)

			continue
		}

		b.jobs = append(b.jobs, j)
		b.results = append(b.results, results[i])
	}

	b.pkgKey, b.pkg = k.pack(h, p)

	select {
	case h.batches <- b:
	case <-h.stop:
	}
}

func (k *Keeper) answer(h *holder, j job, r jobResult, epoch uint64) {
	if r.code == codeStale || j.reply == nil {
		return
	}

	if r.code == "" {
		h.stats.Writes.Add(1)
	} else {
		h.stats.Rejects.Add(1)
	}

	r.epoch = epoch
	j.reply <- r
}

func (k *Keeper) pack(h *holder, p set.Plan) (string, []byte) {
	epoch, _, _, key := h.s.State()

	recs := make([]wire.PackRecord, len(p.Deltas))
	for i, d := range p.Deltas {
		op := uint8(wire.RecAdd)
		if d.Op == set.OpRemove {
			op = wire.RecRemove
		}

		recs[i] = wire.RecordOf(h.s.Def.Type, op, d.Rec)
	}

	pkg := wire.Pack(wire.PackHead{
		Kind:  wire.KindPackage,
		Type:  wire.PackType(h.s.Def.Type),
		Flags: wire.PackFlags(h.s.Def.Type),
		Epoch: epoch,
		Seq:   p.Seq,
		Hash:  p.Hash,
		Key:   key,
	}, recs)

	return state.DiffKey(h.s.Def.Name, p.Seq), pkg
}

func (k *Keeper) committer(h *holder) {
	defer h.done.Done()

	for {
		select {
		case <-h.stop:
			return
		case b := <-h.batches:
			k.commit(h, b)
		}
	}
}

func (k *Keeper) commit(h *holder, b *batch) {
	epoch, _, _, _ := h.s.State()
	name := h.s.Def.Name

	fail := func(code string) {
		for i, j := range b.jobs {
			b.results[i].code = code
			k.answer(h, j, b.results[i], epoch)
		}
	}

	if b.plan.Gen != h.s.Gen() {
		fail(wire.ErrStoreUnavailable)

		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	started := time.Now()
	err := k.state.Commit(ctx, name, b.plan.Deltas, b.plan.Seq, b.plan.Hash, b.pkgKey, b.pkg, k.opts.DiffTTL)
	h.stats.StoreMs.Store(time.Since(started).Milliseconds())

	if err != nil {
		h.stats.StoreErr.Add(1)
		k.log.Error("state commit failed", "set", name, "seq", b.plan.Seq, "error", err.Error())
		h.s.Abort()
		fail(wire.ErrStoreUnavailable)

		return
	}

	ch := h.s.Commit(b.plan, b.at)

	if ch.Hash != b.plan.Hash {
		k.log.Error("committed state differs from the plan", "set", name,
			"plan", wire.Hex(b.plan.Hash), "state", wire.Hex(ch.Hash))
	}

	h.stats.Diffs.Add(1)

	f := wire.Frame{
		V:       wire.V3,
		Set:     name,
		Epoch:   wire.Hex(epoch),
		Seq:     ch.Seq,
		Op:      wire.OpDiff,
		Hash:    wire.Hex(ch.Hash),
		Package: b.pkgKey,
		Count:   len(ch.Deltas),
	}

	/*
	 * A small package rides in the frame itself: a mirror that is exactly one
	 * step behind applies it without a round trip to Redis. The copy in Redis
	 * stays for mirrors that are further behind or come from a snapshot.
	 */
	if k.opts.InlineMax > 0 && len(b.pkg) <= k.opts.InlineMax {
		f.Inline = base64.StdEncoding.EncodeToString(b.pkg)
		h.stats.Inline.Add(1)
	}

	k.bus.Publish(name, wire.Encode(f))

	for i, j := range b.jobs {
		b.results[i].seq = ch.Seq
		k.answer(h, j, b.results[i], epoch)
	}
}

func (k *Keeper) publishTick(h *holder) {
	epoch, seq, hash, _ := h.s.State()

	k.bus.Publish(h.s.Def.Name, wire.Encode(wire.Frame{
		V:     wire.V3,
		Set:   h.s.Def.Name,
		Epoch: wire.Hex(epoch),
		Seq:   seq,
		Op:    wire.OpTick,
		Hash:  wire.Hex(hash),
	}))
}

func (k *Keeper) tickLoop() {
	if k.opts.Tick <= 0 {
		return
	}

	t := time.NewTicker(k.opts.Tick)
	defer t.Stop()

	for range t.C {
		k.mu.RLock()
		hs := make([]*holder, 0, len(k.sets))
		for _, h := range k.sets {
			hs = append(hs, h)
		}
		k.mu.RUnlock()

		for _, h := range hs {
			k.publishTick(h)
		}
	}
}

type snapObj struct {
	key   string
	epoch uint64
	seq   uint64
	hash  uint64
	count int
	bytes int
	at    time.Time
}

const snapMargin = 5 * time.Second

func (k *Keeper) Snapshot(name string, req wire.SnapshotRequest) wire.Frame {
	if !k.ready.Load() {
		return wire.Frame{V: wire.V3, Set: name, Op: wire.OpNotReady}
	}

	h := k.get(name)
	if h == nil {
		return wire.Frame{V: wire.V3, Set: name, Op: wire.OpUnknownSet}
	}

	h.snapMu.Lock()
	defer h.snapMu.Unlock()

	now := k.now()
	epoch, _, _, key := h.s.State()

	o := h.snap
	stale := o == nil || o.epoch != epoch || now.Sub(o.at) > k.opts.SnapshotTTL-snapMargin

	if stale {
		packed, err := k.packSnapshot(h)
		if err != nil {
			k.log.Error("snapshot pack failed", "set", name, "from", req.From, "error", err.Error())

			return wire.Frame{V: wire.V3, Set: name, Op: wire.OpStoreUnavailable}
		}

		h.snap = packed
		o = packed
		h.stats.Packs.Add(1)
	}

	h.stats.Snapshots.Add(1)
	k.log.Info("snapshot", "set", name, "from", req.From, "entries", o.count,
		"bytes", o.bytes, "seq", o.seq, "packed", stale)

	left := k.opts.SnapshotTTL - now.Sub(o.at)

	return wire.Frame{
		V:      wire.V3,
		Set:    name,
		Epoch:  wire.Hex(o.epoch),
		Seq:    o.seq,
		Op:     wire.OpSnapshot,
		Hash:   wire.Hex(o.hash),
		Key:    wire.KeyHex(key),
		Object: o.key,
		Count:  o.count,
		Bytes:  o.bytes,
		TTL:    int(left / time.Second),
	}
}

func (k *Keeper) packSnapshot(h *holder) (*snapObj, error) {
	epoch, seq, hash, key, recs := h.s.Snapshot()
	t := h.s.Def.Type

	packed := make([]wire.PackRecord, len(recs))
	for i, r := range recs {
		packed[i] = wire.RecordOf(t, wire.RecAdd, r)
	}

	data := wire.Pack(wire.PackHead{
		Kind:  wire.KindSnapshot,
		Type:  wire.PackType(t),
		Flags: wire.PackFlags(t),
		Epoch: epoch,
		Seq:   seq,
		Hash:  hash,
		Key:   key,
	}, packed)

	objKey := state.SnapKey(h.s.Def.Name, epoch, seq)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := k.state.Put(ctx, objKey, data, k.opts.SnapshotTTL); err != nil {
		return nil, err
	}

	return &snapObj{
		key:   objKey,
		epoch: epoch,
		seq:   seq,
		hash:  hash,
		count: len(recs),
		bytes: len(data),
		at:    k.now(),
	}, nil
}

func (k *Keeper) Lookup(name, value string) wire.LookupReply {
	h := k.get(name)
	if h == nil {
		return wire.LookupReply{Error: wire.ErrUnknownSet}
	}

	v, err := set.Canon(h.s.Def.Type, value)
	if err != nil {
		return wire.LookupReply{Error: wire.ErrWrongType}
	}

	if h.s.Def.Hash {
		v = set.MD5Hex(v)
	}

	e, ok := h.s.Get(v)
	if !ok || (e.Exp > 0 && e.Exp <= k.now().UnixMilli()) {
		return wire.LookupReply{OK: true}
	}

	return wire.LookupReply{OK: true, Found: true, Exp: e.Exp}
}

type SetStat struct {
	Name       string `json:"name"`
	Entries    int    `json:"entries"`
	Epoch      string `json:"epoch"`
	Seq        uint64 `json:"seq"`
	Hash       string `json:"hash"`
	Limit      int    `json:"limit"`
	Writes     int64  `json:"writes"`
	Rejects    int64  `json:"rejects"`
	Diffs      int64  `json:"diffs"`
	Inline     int64  `json:"inline"`
	Expired    int64  `json:"expired"`
	Snapshots  int64  `json:"snapshots"`
	Packs      int64  `json:"packs"`
	StoreErr   int64  `json:"store_err"`
	StoreMs    int64  `json:"store_ms"`
	Queued     int64  `json:"queued"`
	Overloaded int64  `json:"overloaded"`
	Stale      int64  `json:"stale"`
}

func (k *Keeper) Stats() []SetStat {
	k.mu.RLock()
	defer k.mu.RUnlock()

	out := make([]SetStat, 0, len(k.sets))

	for name, h := range k.sets {
		epoch, seq, hash, _ := h.s.State()
		out = append(out, SetStat{
			Name:       name,
			Entries:    h.s.Len(),
			Epoch:      wire.Hex(epoch),
			Seq:        seq,
			Hash:       wire.Hex(hash),
			Limit:      h.s.Def.Limit,
			Writes:     h.stats.Writes.Load(),
			Rejects:    h.stats.Rejects.Load(),
			Diffs:      h.stats.Diffs.Load(),
			Inline:     h.stats.Inline.Load(),
			Expired:    h.stats.Expired.Load(),
			Snapshots:  h.stats.Snapshots.Load(),
			Packs:      h.stats.Packs.Load(),
			StoreErr:   h.stats.StoreErr.Load(),
			StoreMs:    h.stats.StoreMs.Load(),
			Queued:     h.queued.Load(),
			Overloaded: h.stats.Overloaded.Load(),
			Stale:      h.stats.Stale.Load(),
		})
	}

	return out
}
