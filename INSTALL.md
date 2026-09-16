# Installation

English · [Русский](INSTALL.ru.md)

Keeper is needed wherever active datasets are used: address autobans, lists, rate limit keys.
Without it inspectors keep checking against the last known contents, but nobody can add a new
record. Usually `placitum-core` installs it.

## What it needs

| Component | Required | Why |
| --- | --- | --- |
| NATS | yes | write requests arrive over the bus, deltas go back there |
| Internal Redis | yes | dataset contents, packages and snapshots |
| Controller PostgreSQL | yes | records: values, expiry, reasons, search from the panel |

**Exactly one copy.** Keeper is the sequencer: it defines the order of records, and a second copy
would mean two truths. It scales with an internal pipeline (`WAF_KEEPER_PIPELINE`), not with
replicas.

## Settings

| Variable | Default | Purpose |
| --- | --- | --- |
| `WAF_NATS_URL` | `NATS_URL`, then `nats://nats:4222` | bus |
| `WAF_KEEPER_REDIS_URL` | `REDIS_INTERNAL_URL` | internal Redis for state and packages; required, state does not belong in the exchange |
| `WAF_KEEPER_DATABASE_URL` | `postgres://waf:waf@postgres:5432/waf` | controller database; set it explicitly |
| `WAF_KEEPER_HTTP` | `:8094` | `/healthz` and dataset state |
| `WAF_KEEPER_NAME` | `keeper` | name in the presence frame |
| `WAF_KEEPER_LOG` | `info` | log level |
| `WAF_KEEPER_QUEUE_MAX` | `50000` | requests waiting for a place in the queue |
| `WAF_KEEPER_BATCH_MAX` | `10000` | records in one batch |
| `WAF_KEEPER_PIPELINE` | `4` | writers working in parallel |
| `WAF_KEEPER_COALESCE` | `10ms` | window that groups events into one package |
| `WAF_KEEPER_TICK` | `2s` | tick per dataset: liveness and hash check for mirrors |
| `WAF_KEEPER_SWEEP` | `1s` | removal of expired records |
| `WAF_KEEPER_PULSE` | `5s` | presence frame interval |
| `WAF_KEEPER_SNAPSHOT_TTL` | `30s` | snapshot lifetime in Redis |
| `WAF_KEEPER_DIFF_TTL` | `90s` | package lifetime in Redis; at least twice the snapshot lifetime |

The defaults are sized for up to about ten thousand records per second.

## Docker Compose

```yaml
services:
  keeper:
    image: placitum/keeper
    environment:
      WAF_NATS_URL: nats://nats:4222
      REDIS_INTERNAL_URL: redis://redis-internal:6379
      WAF_KEEPER_DATABASE_URL: postgres://waf:password@postgres:5432/waf
      WAF_KEEPER_LOG: info
      GOMEMLIMIT: 1536MiB
    mem_limit: 2g
    cpus: "4.0"
    depends_on: [nats, postgres, redis-internal]
```

Do not publish `:8094`: the panel sees dataset state through the controller.

## Checking

```sh
curl -fsS http://127.0.0.1:8094/healthz
curl -fsS http://127.0.0.1:8094/sets
```

A healthy start logs connections to the bus, the database and the internal Redis, then loads the
active datasets with their record counts. The first write request must get an answer with a
sequence number; if it does not, look at the queue and the limits.

## Pitfalls

- **Credentials in the default.** `postgres://waf:waf@postgres:5432/waf` matches the bundled
  installation. Set `WAF_KEEPER_DATABASE_URL` explicitly, otherwise the process may quietly connect
  to the wrong database.
- **State goes to the internal Redis, not the exchange.** Mixed-up addresses give a dataset that
  exists but looks empty: packages are written to one instance and read from another.
- **Overload shows up as an answer.** When the queue is full keeper rejects instead of piling up;
  the sender must log that, not treat the record as written.
