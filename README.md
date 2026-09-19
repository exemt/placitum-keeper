# Placitum keeper

English · [Русский](README.ru.md)

Source of truth for Placitum active datasets: bans, address lists and rate limit keys, everything
that changes at runtime and must be the same on every node.

An inspector signals to add an address to a dataset, and the request comes here. Keeper applies it in
memory, stores the record, publishes a delta and answers. Mirrors in inspectors and on nodes pick
up the delta and keep their own copy.

```
inspector ──► waf.sets.<set>.event ──► keeper ──► record in PostgreSQL
                                          │        package in the internal Redis
                                          └──────► delta to every mirror
```

Why a separate process and not the controller: dataset contents change by thousands of records per
second, a different rhythm from configuration. The controller is just another client here, like an
inspector.

## Build and run

```sh
docker build -t placitum/keeper .
```

What it needs, settings and limits are in [INSTALL.md](INSTALL.md).

## Bus and HTTP

| Subject | Kind | What |
| --- | --- | --- |
| `waf.sets.<set>` | publish | `diff` with a reference to a change package in Redis, and a `tick` every 2 s |
| `waf.sets.<set>.snapshot` | request | a mirror asks for a snapshot reference |
| `waf.sets.<set>.event` | request or publish | a writer adds or removes values; the answer has `seq` or a rejection |
| `waf.keeper.define` | request | the controller asks to reread a dataset definition |
| `waf.keeper.reload` | request | reconcile every definition |
| `waf.keeper.lookup` | request | `{set, value}` → `{found, exp}`, not for the hot path |

Contents travel through the internal Redis, not the bus: change packages live under
`waf:diff:<set>:<seq>` and snapshots under `waf:snap:<set>:<epoch>:<seq>`.

A write is accepted in full or rejected in full. Rejections: `unknown_set`, `full`, `wrong_type`,
`too_long`, `no_origin`, `no_ttl`, `forever`, `bad_op`, `store_unavailable`, `not_ready`,
`overloaded`.

HTTP on `:8094`: `GET /healthz` (`503` while datasets are loading), `GET /sets` with dataset stats,
`GET /sets/<name>` with a snapshot reference.

## Good to know

- **Storage is the controller database.** Keeper has none of its own: records, sizes and search
  come from there, contents live in memory and in internal Redis packages.
- **An answer is mandatory.** The sender waits for it: a rejection (`full`, `store_unavailable`) is
  a line in the inspector log, not silence.
- **One process per installation.** Keeper is the sequencer: it defines the order of records, and a
  second copy would mean two truths. It scales with an internal pipeline, not with replicas.

## License

[Apache License 2.0](LICENSE); the attribution notice is in [NOTICE](NOTICE). This repository is
part of the Placitum open core. The inspectors are licensed separately: each inspector repository
carries the Placitum License Agreement. Releases made before this change came under the Placitum
License Agreement 1.1.
