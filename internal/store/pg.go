package store

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/exemt/placitum-keeper/internal/nginxtime"
	"github.com/exemt/placitum-keeper/internal/set"
)

type Store interface {
	LoadDefinitions(ctx context.Context) ([]set.Definition, error)
	LoadDefinition(ctx context.Context, name string) (*set.Definition, error)
	Close()
}

type PG struct {
	pool *pgxpool.Pool
}

func Open(ctx context.Context, url string) (*PG, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}

	cfg.MaxConns = 2

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()

		return nil, err
	}

	return &PG{pool: pool}, nil
}

func (p *PG) Close() { p.pool.Close() }

const defCols = `id, name, type, max_entries, coalesce(ttl, ''), active, coalesce(hash, false)`

func scanDef(rows pgx.Rows) (set.Definition, error) {
	var (
		d   set.Definition
		typ string
		ttl string
	)

	if err := rows.Scan(&d.ID, &d.Name, &typ, &d.Limit, &ttl, &d.Active, &d.Hash); err != nil {
		return d, err
	}

	d.Type = set.TypeOf(typ)
	d.TTL = nginxtime.ParseSeconds(ttl)

	return d, nil
}

func (p *PG) LoadDefinitions(ctx context.Context) ([]set.Definition, error) {
	rows, err := p.pool.Query(ctx,
		`select `+defCols+` from datasets where kind = 'list' and active order by name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []set.Definition

	for rows.Next() {
		d, err := scanDef(rows)
		if err != nil {
			return nil, err
		}

		out = append(out, d)
	}

	return out, rows.Err()
}

func (p *PG) LoadDefinition(ctx context.Context, name string) (*set.Definition, error) {
	rows, err := p.pool.Query(ctx,
		`select `+defCols+` from datasets where kind = 'list' and name = $1 limit 1`, name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, rows.Err()
	}

	d, err := scanDef(rows)
	if err != nil {
		return nil, err
	}

	return &d, nil
}
