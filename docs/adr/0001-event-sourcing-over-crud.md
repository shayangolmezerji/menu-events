# ADR 0001: Event sourcing and CQRS on PostgreSQL, over CRUD and over Kafka

## Status

Accepted. 2026-09-22.

What has been executed is not all of what is argued here. The version rule, the
idempotency rule and the fold run in the unit tier, against the in-memory adapter, and
they run again over HTTP in the api tier, in process.
The PostgreSQL claims, meaning the trigger, the grants, the advisory lock and the
one-transaction atomicity of a write, are read out of `migrations/` and the two
adapters against psycopg's documented behaviour. No server has run them: the tier
that would, `tests/integration/`, skips wherever `MENU_EVENTS_TEST_DSN` is unset.

## Context

The service stores a restaurant menu that several terminals edit at the same
time: a manager repricing, a line cook marking an item sold out, front-of-house
restoring it. Three properties matter and a table of current rows gives up all
three:

- An operator must be able to see who changed an item, to what, when, and why.
- A correction must not rewrite history. A mis-tap is answered by a later fact,
  not by editing the row that recorded the mistake.
- Two writers who read the same menu must not overwrite each other by accident.

This record covers two decisions that were taken together, because the second
follows from the first. The first is the shape of the data: an append-only event
log plus a derived read model (event sourcing and CQRS) instead of a CRUD table.
The second is the stack: Python and PostgreSQL instead of the Go and Apache
Kafka that the original project brief named.

The scale is a menu. A venue has a small number of menus, each with tens of
items, each edited a handful of times a day. Nothing here pushes the volume a
partitioned log exists to absorb.

## Decision

Represent the menu as a log of immutable facts and derive the current menu by
folding that log. One command produces exactly one event (`store/base.py`).
Every command carries the version it was decided against, and a write is
refused if the log has moved (`handlers.py`, `domain/commands.py`). The current
menu is a projection over the log, never a primary store.

Run the whole thing on PostgreSQL. The log is one table, `events`. Append-only
is enforced by a trigger that refuses `UPDATE`, `DELETE` and `TRUNCATE`, backed
up by grants (`migrations/0001_event_store.sql`). Optimistic concurrency is a
per-stream advisory transaction lock around a head read and an insert; the
primary key on `(stream_id, version)` is the backstop for a writer that reaches
the table without the lock (`store/postgres.py`). The read model,
`menu_projection` plus `menu_item`, is derived from the log and disposable
(`migrations/0002_projection.sql`, `projections/postgres.py`).

The fold itself is pure Python and reads no clock (`projections/menu.py`).
`recorded_at` arrives on the envelope the store assigned, so a replay reproduces
the menu a customer saw at a point in the log rather than at the moment of
replay. That is what lets the same `project()` serve both an in-memory test and
the PostgreSQL projector without a second implementation to keep honest.

## Consequences

What this buys over CRUD:

- The audit trail is the storage model, not a feature bolted onto it. `actor`,
  `reason` and the `corrects` pointer live on every event, and the log already
  orders them, so "who set this price and was it a correction" is a read, not a
  reconstruction.
- A correction is a normal later event. `corrects` points at the event it amends;
  both stay in the log and the fold ends at the later one. Nothing is rewritten.
- Losing a race is explicit. A stale command raises `ConcurrencyConflict` with
  the version the log now holds (`domain/errors.py`), so a caller re-reads and
  decides instead of silently overwriting. A retried command is matched on
  `command_id` before the version check, so a timeout becomes the original
  result, not a second event.

What this gives up, honestly:

- The current menu is derived, so it can lag the log by one projection run. A
  read served before the projector has caught up shows an old price. This is the
  eventual-consistency window CQRS names rather than hides; the checkpoint column
  in `menu_projection` exists so a caller can tell a stale read from a fresh one.
- Folding the whole stream is O(events). It is fine at menu size and is not tuned
  for streams that grow without bound. `advance` folds only the new tail to keep
  steady-state reads cheap, but a `rebuild` is a full fold.

What was given up by choosing PostgreSQL over Kafka:

- No partitioned-log throughput. One Postgres stream is serialised through its
  advisory lock, so a single very hot menu is a bottleneck Kafka would spread
  across partitions. At menu scale that bottleneck is not reachable.
- No consumer groups or offsets. Independent consumers that track their own
  position in the log, and the rebalancing that manages them, are not here. This
  service has exactly one projection, and its position is a row in
  `menu_projection` guarded by the same transaction that writes the derived rows.
- No replay tooling from the Kafka ecosystem. The compaction, the connect
  sources and sinks, and the CLI replay commands do not exist; the fold and the
  checkpoint are code in this repository that has to be correct on its own.

What was gained by that same choice:

- One system, so the write is atomic without cross-service coordination. A single
  command appends its event and releases the lock in one transaction. Advancing
  the projection writes the derived rows and the checkpoint together, in one
  transaction, in the same database that holds the log. With Kafka the events sit
  in the broker and the read model in a separate store, so recording a consumer
  offset and publishing a projection span two systems; exactly-once there means
  broker transactions and a transactional read store. Here the checkpoint and the
  rows commit as one.
- One fewer service to run. No broker, no cluster manager, no consumer process.
  A deployment is the app plus a PostgreSQL it already needed.
- The in-memory adapter is not a toy. It persists the same canonical row
  (`store/serialization.py`) the database stores in a `jsonb` column, so the unit
  tests exercise the real row shape, the checksum, and the version rules without
  a server.

What this does not claim: the event append and the projection are two separate
transactions, deliberately. A command commits on its own; the projector runs
after. They share one database and one consistency boundary, which is the point,
but they do not commit in a single transaction, and the read model is allowed to
lag.

## Redis, and what would move the answer

No Redis, and no cache tier of any kind. The stack this repo was briefed under named
Redis; nothing in the build uses it. `dependencies` is psycopg and pydantic, the `api`
extra adds fastapi and uvicorn, and no module imports a Redis client.

A cache in front of the read side reintroduces, one hop earlier, precisely the problem
the write side exists to refuse. Every read answers with the version it folded from and
a writer is expected to claim that version. Serve the read from something that can be
behind the log and two things break at once: a writer holding the current version is
refused against a stale one it never saw, and a writer holding a genuinely stale version
is refused for the wrong reason, so a 409 stops being a reliable signal that a race was
lost. Optimistic concurrency is only worth having if the version a reader was handed is
the version the store held at that moment.

`menu_projection` is the cache this design does keep, and it is honest about being one:
it stores the version it folded to, so a consumer can tell a stale menu from a fresh one
instead of guessing. A Redis layer would need the same discipline, would still not be
the authority, and would add a third place the menu can disagree with the log.

What would have to be measured before that answer changes:

- The cost of the fold at the longest real stream. `GET /menu/{menu_id}` folds every
  event of the stream in process, so it is O(events) per read. Measure p99 against the
  stream a venue actually accumulates over a year, not a synthetic one.
- Reads per menu against writes per menu. A cache pays only where reads outnumber writes
  by enough that the folds it saves outweigh the false conflicts it creates.
- Whether a cached answer can be validated at all for less than it costs to compute one.
  A version-tagged entry is safe to return only once you know the head version, and the
  read that fetches the head is the read the cache was meant to avoid.

None of these are measured here, and no number for them is claimed anywhere in this
repository: nothing has run against a server, so a figure quoted for a cache tier would
be a guess dressed as an argument.

## Rejected alternatives

- CRUD, one mutable row per item, plus `updated_by`/`updated_at`. Rejected: it
  cannot answer the correction requirement without rewriting history, and the
  audit columns capture only the last change. The multi-writer case still needs a
  version token, which is the event-sourcing machinery kept but the log thrown
  away.
- CRUD plus a separate append-only audit table. Rejected: the current menu and
  the audit log become two sources of truth that can disagree, and there is no
  principled way to rebuild one from the other. It pays the cost of keeping a log
  while treating the log as second-class.
- Go plus Apache Kafka, as originally briefed. Rejected for this milestone: it
  adds a broker and a cluster manager to run, a consumer to operate, and a second
  store for the read model, all to serve a write volume a single Postgres handles
  while holding the log and the projection in one place. Kafka's replay and
  ordering guarantees would also have to be reproduced against Postgres anyway to
  test them here, since no broker is reachable.
- Read the log and fold on every request, with no stored read model. Rejected:
  correct but wasteful for a menu read constantly, and it couples read latency to
  stream length. A stored read model is cheap to maintain from the fold and is
  what a menu endpoint wants.
- Redis, both as the store and as the cache, from the portfolio stack adaptation.
  Neither is used: PostgreSQL holds the immutable log and the derived read model in one
  system, and a cache would open a third place the menu can be inconsistent. The
  reasoning and the measurements that would change it are in the section above.
