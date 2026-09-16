package state

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/exemt/placitum-keeper/internal/set"
)

const whySep = "\x1f"

type Meta struct {
	ID    string
	Type  set.Type
	Epoch uint64
	Key   set.Key
	Seq   uint64
	Hash  uint64
}

type Store interface {
	Load(ctx context.Context, name string) (*Meta, []set.Record, error)
	Reset(ctx context.Context, name string, m Meta, keep bool) error
	Commit(ctx context.Context, name string, deltas []set.Delta, seq, hash uint64,
		pkgKey string, pkg []byte, ttl time.Duration) error
	Put(ctx context.Context, key string, data []byte, ttl time.Duration) error
	Drop(ctx context.Context, name string) error
	Close() error
}

func SetKey(name string) string  { return "waf:set:" + name }
func WhyKey(name string) string  { return "waf:set:" + name + ":why" }
func MetaKey(name string) string { return "waf:set:" + name + ":meta" }

func DiffKey(name string, seq uint64) string {
	return "waf:diff:" + name + ":" + strconv.FormatUint(seq, 10)
}

func SnapKey(name string, epoch, seq uint64) string {
	return "waf:snap:" + name + ":" + fmt.Sprintf("%016x", epoch) + ":" + strconv.FormatUint(seq, 10)
}

type Redis struct {
	client *redis.Client
}

func Open(url string, timeout time.Duration) (*Redis, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}

	opt.DialTimeout = timeout
	opt.ReadTimeout = timeout
	opt.WriteTimeout = timeout

	return &Redis{client: redis.NewClient(opt)}, nil
}

func (r *Redis) Ping(ctx context.Context) error { return r.client.Ping(ctx).Err() }

func (r *Redis) Close() error { return r.client.Close() }

func (r *Redis) Load(ctx context.Context, name string) (*Meta, []set.Record, error) {
	fields, err := r.client.HGetAll(ctx, MetaKey(name)).Result()
	if err != nil {
		return nil, nil, err
	}

	if len(fields) == 0 {
		return nil, nil, nil
	}

	m, err := parseMeta(fields)
	if err != nil {
		return nil, nil, fmt.Errorf("meta of %s: %w", name, err)
	}

	zs, err := r.client.ZRangeWithScores(ctx, SetKey(name), 0, -1).Result()
	if err != nil {
		return nil, nil, err
	}

	whys, err := r.client.HGetAll(ctx, WhyKey(name)).Result()
	if err != nil {
		return nil, nil, err
	}

	recs := make([]set.Record, 0, len(zs))

	for _, z := range zs {
		v, _ := z.Member.(string)
		rec := set.Record{Value: v, Exp: int64(z.Score)}

		if w, ok := whys[v]; ok {
			rec.Origin, rec.Reason, _ = strings.Cut(w, whySep)
		}

		recs = append(recs, rec)
	}

	return &m, recs, nil
}

func (r *Redis) Reset(ctx context.Context, name string, m Meta, keep bool) error {
	_, err := r.client.TxPipelined(ctx, func(p redis.Pipeliner) error {
		if !keep {
			p.Del(ctx, SetKey(name), WhyKey(name))
		}

		p.HSet(ctx, MetaKey(name), metaFields(m)...)

		return nil
	})

	return err
}

func (r *Redis) Commit(ctx context.Context, name string, deltas []set.Delta, seq, hash uint64,
	pkgKey string, pkg []byte, ttl time.Duration) error {
	_, err := r.client.TxPipelined(ctx, func(p redis.Pipeliner) error {
		for i := 0; i < len(deltas); {
			j := i
			for j < len(deltas) && deltas[j].Op == deltas[i].Op {
				j++
			}

			run := deltas[i:j]
			i = j

			if run[0].Op == set.OpAdd {
				zs := make([]redis.Z, len(run))
				whys := make([]any, 0, 2*len(run))

				for n, d := range run {
					zs[n] = redis.Z{Score: float64(d.Rec.Exp), Member: d.Rec.Value}
					whys = append(whys, d.Rec.Value, d.Rec.Origin+whySep+d.Rec.Reason)
				}

				p.ZAdd(ctx, SetKey(name), zs...)
				p.HSet(ctx, WhyKey(name), whys...)

				continue
			}

			vals := make([]any, len(run))
			names := make([]string, len(run))

			for n, d := range run {
				vals[n] = d.Rec.Value
				names[n] = d.Rec.Value
			}

			p.ZRem(ctx, SetKey(name), vals...)
			p.HDel(ctx, WhyKey(name), names...)
		}

		p.Set(ctx, pkgKey, pkg, ttl)
		p.HSet(ctx, MetaKey(name), "seq", strconv.FormatUint(seq, 10), "hash", fmt.Sprintf("%016x", hash))

		return nil
	})

	return err
}

func (r *Redis) Put(ctx context.Context, key string, data []byte, ttl time.Duration) error {
	return r.client.Set(ctx, key, data, ttl).Err()
}

func (r *Redis) Drop(ctx context.Context, name string) error {
	return r.client.Del(ctx, SetKey(name), WhyKey(name), MetaKey(name)).Err()
}

func metaFields(m Meta) []any {
	return []any{
		"id", m.ID,
		"type", strconv.Itoa(int(m.Type)),
		"epoch", fmt.Sprintf("%016x", m.Epoch),
		"key", hex.EncodeToString(m.Key[:]),
		"seq", strconv.FormatUint(m.Seq, 10),
		"hash", fmt.Sprintf("%016x", m.Hash),
	}
}

func parseMeta(f map[string]string) (Meta, error) {
	var (
		m   Meta
		err error
	)

	m.ID = f["id"]

	t, err := strconv.Atoi(f["type"])
	if err != nil {
		return m, fmt.Errorf("type: %w", err)
	}

	m.Type = set.Type(t)

	if m.Epoch, err = strconv.ParseUint(f["epoch"], 16, 64); err != nil {
		return m, fmt.Errorf("epoch: %w", err)
	}

	key, err := hex.DecodeString(f["key"])
	if err != nil || len(key) != len(m.Key) {
		return m, fmt.Errorf("key: %q", f["key"])
	}

	copy(m.Key[:], key)

	if m.Seq, err = strconv.ParseUint(f["seq"], 10, 64); err != nil {
		return m, fmt.Errorf("seq: %w", err)
	}

	if m.Hash, err = strconv.ParseUint(f["hash"], 16, 64); err != nil {
		return m, fmt.Errorf("hash: %w", err)
	}

	return m, nil
}
