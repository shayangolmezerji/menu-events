# menu-events

Append-only event store for restaurant menus, with optimistic concurrency and replayable read models. Python, FastAPI and PostgreSQL.

[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](pyproject.toml)
[![Python](https://img.shields.io/badge/python-3.12%20%7C%203.13-blue.svg)](https://www.python.org/)

## Overview

A menu is edited by several people at once. A manager reprices an item, a line cook marks it sold out, front-of-house restores it a minute later. A plain table with one row per item handles that badly: the last write wins and the earlier one is gone, an edit erases the price that was there before, and a mis-tap can only be undone by changing a stored row.

This service keeps the history instead of the current row. Every accepted change is an immutable event. The menu you read is a fold over those events. Two competing writes do not silently overwrite each other, because a command names the version it was decided against and the loser is refused. A mis-tap is answered by a later event, not by rewriting an earlier one.

The write path and the read path are separate (CQRS). A command is validated and appended. A projection folds the log into a menu state, in Python for replay and in PostgreSQL for the read tables. The read model can lag the log by one projection run. The reasoning, and the switch from the originally planned Go and Kafka, is in `docs/adr/0001-event-sourcing-over-crud.md`.

Three things sit behind those rules: the library (`menu_events`), the HTTP app over it (`menu_events.api`), and the PostgreSQL adapters. The web framework is an optional extra, so `import menu_events` reaches for no FastAPI for the same reason it reaches for no database driver. The app is a factory taking a store rather than an object built at import time, which is what lets the HTTP tier run against the in-memory adapter.

## Architecture

```
    POST /commands   {"event_type": ..., "menu_id": ..., "expected_menu_version": ...}
       |     menu_events.api, an optional extra. Maps ConcurrencyConflict to 409,
       |     a domain rejection to 400, an empty stream to 404, a body the
       |     command models refuse to 422. No auth, no metrics, no cache.
       v
    command  (menu_id, item_id, command_id, expected_menu_version)
       |
       v
    MenuCommandHandler.handle
       |     validate against the fold up to expected_menu_version,
       |     then store.append: advisory lock -> duplicate check ->
       |     version check -> INSERT. A rejected command writes nothing.
       v
    events            the only immutable table. One row per command.
       |
       |     fold = apply_event / project  (pure Python, no clock read)
       |     GET /menu/{id} and GET /menu/{id}/events fold here, in the request
       |     advance or rebuild writes the read model under its own lock
       v
    menu_projection (checkpoint) + menu_item (one row per item, derived)
       |
       v
    MenuState.on_menu        items sorted by category, then name
```

The two reads answer from the log's head rather than from `menu_projection`, so a client cannot be told a version and then refused for writing against it. The PostgreSQL read tables are the other half of the picture: what `projections/postgres.py` maintains for a deployment that wants the derived rows queryable in SQL.

## Requirements

- Python 3.12 or newer. `.github/workflows/ci.yml` names 3.12 and 3.13.
- `psycopg[binary]` and `pydantic`, installed with the package.
- `fastapi` and `uvicorn`, from the `api` extra, only to serve or test the HTTP layer.
- A PostgreSQL server only for the integration tier. The unit tests, the HTTP tier and every example on this page need none.

## Quickstart

From a clean checkout:

```bash
python3 -m venv .venv  # Windows: python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev,api]"
pytest
```

On a machine with no database the last line reports `92 passed, 10 skipped`. The 10 skips are the PostgreSQL tier; see [Testing](#testing). Without the `api` extra the HTTP tier skips too and the line reads `60 passed, 42 skipped`. To lint:

```bash
ruff check .
```

## Running the Service

```bash
uvicorn --factory menu_events.api:create_app --host 127.0.0.1 --port 8000
```

```
INFO:     Started server process [100756]
INFO:     Waiting for application startup.
INFO:     Application startup complete.
INFO:     Uvicorn running on http://127.0.0.1:8077 (Press CTRL+C to quit)
```

That call was run, on 3.13, and the transcript in [Usage](#usage) came from the process it started. The port differs because the run used `--port 8077` to stay out of the way of anything already listening on 8000.

`create_app()` with no arguments serves an `InMemoryEventStore`: a log per process, gone on restart. It is the right default for the tests and for trying the routes, and the wrong one for anything that has to keep a menu. To serve the PostgreSQL adapter, build the app in your own entry point:

```python
from menu_events.api import create_app
from menu_events.store.postgres import PostgresEventStore

app = create_app(PostgresEventStore("host=localhost dbname=menu_events_scratch"))
```

Point `uvicorn` at that module instead of using `--factory`. This snippet has not been run: it needs a server, and the DSN is the shape `tests/integration/README.md` describes rather than one that answers here. Interactive docs are at `/docs` once the app is serving.


## Usage

A command carries the version it read before deciding. The handler validates against that version, not against the head of the log, so a caller cannot get a write accepted by claiming a version it never looked at.

```python
import uuid
from menu_events import (
    AddMenuItem,
    ChangePrice,
    ConcurrencyConflict,
    InMemoryEventStore,
    MarkSoldOut,
    MenuCommandHandler,
    menu_stream_id,
    project,
)

menu_id = uuid.UUID("6f1c9f2e-0a1b-4c2d-9e3f-1a2b3c4d5e6f")
fries = uuid.UUID("11111111-1111-4111-8111-111111111111")
bread = uuid.UUID("22222222-2222-4222-8222-222222222222")

store = InMemoryEventStore()
handler = MenuCommandHandler(store)
stream_id = menu_stream_id(menu_id)

handler.handle(
    AddMenuItem(menu_id=menu_id, item_id=fries, command_id=uuid.uuid4(),
                expected_menu_version=0, actor="line-1",
                name="Duck fat fries", price_cents=900, category="sides")
)
handler.handle(
    AddMenuItem(menu_id=menu_id, item_id=bread, command_id=uuid.uuid4(),
                expected_menu_version=1, actor="line-1",
                name="Sourdough", price_cents=400, category="breads")
)
handler.handle(
    ChangePrice(menu_id=menu_id, item_id=fries, command_id=uuid.uuid4(),
                expected_menu_version=2, actor="manager",
                reason="potato cost up", price_cents=1150)
)
handler.handle(
    MarkSoldOut(menu_id=menu_id, item_id=bread, command_id=uuid.uuid4(),
                expected_menu_version=3, actor="line-1")
)

state = project(stream_id, store.read(stream_id))
print(f"menu at version {state.version}:")
for item in state.on_menu:
    flag = "sold out" if item.sold_out else "available"
    print(f"  {item.category:<7} {item.name:<16} {item.price_cents / 100:>6.2f}  {flag}")

try:
    handler.handle(
        ChangePrice(menu_id=menu_id, item_id=fries, command_id=uuid.uuid4(),
                    expected_menu_version=1, actor="late-terminal", price_cents=500)
    )
except ConcurrencyConflict as exc:
    print(f"refused: {exc}")
```

Output:

```
menu at version 4:
  breads  Sourdough          4.00  sold out
  sides   Duck fat fries    11.50  available
refused: stream menu/6f1c9f2e-0a1b-4c2d-9e3f-1a2b3c4d5e6f is at version 4, command expected 1
```

The replay reads the log back through `store.read` and folds it with `project`. Nothing in that fold reads the clock: `recorded_at` arrives on the event envelope, so a replay reproduces the state a customer saw at the time, not the state at the moment of replay.

The last command was decided against version 1. The log is at version 4, so `store.append` refuses it with `ConcurrencyConflict` and writes no event. The caller re-reads and decides whether its intent still holds.

Retrying a command that already landed is not a conflict. The handler matches on `command_id` before the version check, so a client that timed out and resent gets the original result back rather than a second event.

### Over HTTP

The same rules through the front door. Each block is a request to the process `uvicorn` started and the body it answered with, in the order they were sent, against a fresh log. `HTTP` is the status the connection returned.

```
$ curl -s -w "\n%{http_code}" -X POST http://127.0.0.1:8077/commands \
    -H "content-type: application/json" \
    -d '{"event_type": "menu_item_added", "menu_id": "6f1c9f2e-0a1b-4c2d-9e3f-1a2b3c4d5e6f", "item_id": "11111111-1111-4111-8111-111111111111",
 "command_id": "aaaaaaaa-0000-4000-8000-000000000001", "expected_menu_version": 0,
 "actor": "line-1", "name": "Duck fat fries", "price_cents": 900, "category": "sides"}'
HTTP 200
{"menu_id":"6f1c9f2e-0a1b-4c2d-9e3f-1a2b3c4d5e6f","menu_version":1,"event_id":"0b2ba64d-1ef7-4749-a44c-bc3b302fc2e3","applied":true}
```

Three more writes, each claiming the version the previous one returned: add `2222…` at 0's successor, reprice the fries at 2 with a `reason`, mark the bread sold out at 3.

```
$ curl -s -w "\n%{http_code}" -X POST http://127.0.0.1:8077/commands \
    -H "content-type: application/json" \
    -d '{"event_type": "menu_item_added", "menu_id": "6f1c9f2e-0a1b-4c2d-9e3f-1a2b3c4d5e6f", "item_id": "22222222-2222-4222-8222-222222222222",
 "command_id": "aaaaaaaa-0000-4000-8000-000000000002", "expected_menu_version": 1,
 "actor": "line-1", "name": "Sourdough", "price_cents": 400, "category": "breads"}'
HTTP 200
{"menu_id":"6f1c9f2e-0a1b-4c2d-9e3f-1a2b3c4d5e6f","menu_version":2,"event_id":"aa0d23a0-3755-4cae-9a4a-c8d567b2bae8","applied":true}

$ curl -s -w "\n%{http_code}" -X POST http://127.0.0.1:8077/commands \
    -H "content-type: application/json" \
    -d '{"event_type": "price_changed", "menu_id": "6f1c9f2e-0a1b-4c2d-9e3f-1a2b3c4d5e6f", "item_id": "11111111-1111-4111-8111-111111111111",
 "command_id": "aaaaaaaa-0000-4000-8000-000000000003", "expected_menu_version": 2,
 "actor": "manager", "reason": "potato cost up", "price_cents": 1150}'
HTTP 200
{"menu_id":"6f1c9f2e-0a1b-4c2d-9e3f-1a2b3c4d5e6f","menu_version":3,"event_id":"9da5dd20-790e-4e24-84e1-c41ded56007c","applied":true}

$ curl -s -w "\n%{http_code}" -X POST http://127.0.0.1:8077/commands \
    -H "content-type: application/json" \
    -d '{"event_type": "item_sold_out", "menu_id": "6f1c9f2e-0a1b-4c2d-9e3f-1a2b3c4d5e6f", "item_id": "22222222-2222-4222-8222-222222222222",
 "command_id": "aaaaaaaa-0000-4000-8000-000000000004", "expected_menu_version": 3,
 "actor": "line-1"}'
HTTP 200
{"menu_id":"6f1c9f2e-0a1b-4c2d-9e3f-1a2b3c4d5e6f","menu_version":4,"event_id":"4cff1781-ef85-46be-be53-389bffea44d9","applied":true}
```

The read answers with the fold and the head to write against. `menu_version` on a write and `version` on a read are the same counter, which is the whole contract:

```
$ curl -s -w "\n%{http_code}" "http://127.0.0.1:8077/menu/6f1c9f2e-0a1b-4c2d-9e3f-1a2b3c4d5e6f" | python3 -m json.tool
HTTP 200
{
    "menu_id": "6f1c9f2e-0a1b-4c2d-9e3f-1a2b3c4d5e6f",
    "stream_id": "menu/6f1c9f2e-0a1b-4c2d-9e3f-1a2b3c4d5e6f",
    "version": 4,
    "items": [
        {
            "item_id": "22222222-2222-4222-8222-222222222222",
            "name": "Sourdough",
            "price_cents": 400,
            "description": "",
            "category": "breads",
            "sold_out": true,
            "updated_at": "2026-09-23T17:13:43.394242Z",
            "updated_by": "line-1"
        },
        {
            "item_id": "11111111-1111-4111-8111-111111111111",
            "name": "Duck fat fries",
            "price_cents": 1150,
            "description": "",
            "category": "sides",
            "sold_out": false,
            "updated_at": "2026-09-23T17:13:43.376392Z",
            "updated_by": "manager"
        }
    ]
}
```

The log, oldest first, with `from_version` naming the last entry the caller already has. Each entry is the stored envelope: `version` is the position the fold orders by, `command_id` the key a retry is recognised by, and `event` the payload the checksum was taken over.

```
$ curl -s -w "\n%{http_code}" "http://127.0.0.1:8077/menu/6f1c9f2e-0a1b-4c2d-9e3f-1a2b3c4d5e6f/events?from_version=2" | python3 -m json.tool
HTTP 200
[
    {
        "stream_id": "menu/6f1c9f2e-0a1b-4c2d-9e3f-1a2b3c4d5e6f",
        "version": 3,
        "event_id": "9da5dd20-790e-4e24-84e1-c41ded56007c",
        "command_id": "aaaaaaaa-0000-4000-8000-000000000003",
        "recorded_at": "2026-09-23T17:13:43.376392Z",
        "event": {
            "item_id": "11111111-1111-4111-8111-111111111111",
            "actor": "manager",
            "reason": "potato cost up",
            "corrects": null,
            "event_type": "price_changed",
            "price_cents": 1150
        }
    },
    {
        "stream_id": "menu/6f1c9f2e-0a1b-4c2d-9e3f-1a2b3c4d5e6f",
        "version": 4,
        "event_id": "4cff1781-ef85-46be-be53-389bffea44d9",
        "command_id": "aaaaaaaa-0000-4000-8000-000000000004",
        "recorded_at": "2026-09-23T17:13:43.394242Z",
        "event": {
            "item_id": "22222222-2222-4222-8222-222222222222",
            "actor": "line-1",
            "reason": null,
            "corrects": null,
            "event_type": "item_sold_out"
        }
    }
]
```

A terminal that read version 1 and is still holding it loses, and the answer tells it what it lost to. Nothing was written by the refused command; the next read still says 4.

```
$ curl -s -w "\n%{http_code}" -X POST http://127.0.0.1:8077/commands \
    -H "content-type: application/json" \
    -d '{"event_type": "price_changed", "menu_id": "6f1c9f2e-0a1b-4c2d-9e3f-1a2b3c4d5e6f", "item_id": "11111111-1111-4111-8111-111111111111",
 "command_id": "aaaaaaaa-0000-4000-8000-000000000005", "expected_menu_version": 1,
 "actor": "late-terminal", "price_cents": 500}'
HTTP 409
{"reason":"stream menu/6f1c9f2e-0a1b-4c2d-9e3f-1a2b3c4d5e6f is at version 4, command expected 1","stream_id":"menu/6f1c9f2e-0a1b-4c2d-9e3f-1a2b3c4d5e6f","expected":1,"actual":4}
```

The third write resent with its original `command_id` is not a second event. It returns the version and the `event_id` from the first answer, with `applied` false:

```
$ curl -s -w "\n%{http_code}" -X POST http://127.0.0.1:8077/commands \
    -H "content-type: application/json" \
    -d '{"event_type": "price_changed", "menu_id": "6f1c9f2e-0a1b-4c2d-9e3f-1a2b3c4d5e6f", "item_id": "11111111-1111-4111-8111-111111111111",
 "command_id": "aaaaaaaa-0000-4000-8000-000000000003", "expected_menu_version": 2,
 "actor": "manager", "reason": "potato cost up", "price_cents": 1150}'
HTTP 200
{"menu_id":"6f1c9f2e-0a1b-4c2d-9e3f-1a2b3c4d5e6f","menu_version":3,"event_id":"9da5dd20-790e-4e24-84e1-c41ded56007c","applied":false}
```

A menu nobody has written is a 404 on either side of the boundary, carrying the sentence the domain raised:

```
$ curl -s -w "\n%{http_code}" "http://127.0.0.1:8077/menu/9a8b7c6d-5e4f-4a1b-9c8d-7e6f5a4b3c2d" | python3 -m json.tool
HTTP 404
{
    "reason": "stream menu/9a8b7c6d-5e4f-4a1b-9c8d-7e6f5a4b3c2d has no events"
}

$ curl -s -w "\n%{http_code}" -X POST http://127.0.0.1:8077/commands \
    -H "content-type: application/json" \
    -d '{"event_type": "item_sold_out", "menu_id": "9a8b7c6d-5e4f-4a1b-9c8d-7e6f5a4b3c2d", "item_id": "11111111-1111-4111-8111-111111111111",
 "command_id": "aaaaaaaa-0000-4000-8000-000000000006", "expected_menu_version": 0,
 "actor": "line-1"}'
HTTP 404
{"reason":"stream menu/9a8b7c6d-5e4f-4a1b-9c8d-7e6f5a4b3c2d has no events"}
```

A body naming a command that does not exist is a 422, and the message lists the ones that do:

```
$ curl -s -w "\n%{http_code}" -X POST http://127.0.0.1:8077/commands \
    -H "content-type: application/json" \
    -d '{"event_type": "sold_a_thing", "menu_id": "6f1c9f2e-0a1b-4c2d-9e3f-1a2b3c4d5e6f", "item_id": "11111111-1111-4111-8111-111111111111",
 "command_id": "aaaaaaaa-0000-4000-8000-000000000007", "expected_menu_version": 4,
 "actor": "line-1"}'
HTTP 422
{"detail":[{"type":"value_error","loc":["body"],"msg":"Value error, event_type 'sold_a_thing' is not a command, expected one of: description_edited, item_back_in_stock, item_sold_out, menu_item_added, price_changed","input":{"event_type":"sold_a_thing","menu_id":"6f1c9f2e-0a1b-4c2d-9e3f-1a2b3c4d5e6f","item_id":"11111111-1111-4111-8111-111111111111","command_id":"aaaaaaaa-0000-4000-8000-000000000007","expected_menu_version":4,"actor":"line-1"},"ctx":{"error":{}}}]}
```

## API Reference

Available at `/docs` (Swagger UI) or `/openapi.json`. The request schema shown there lists the fields every command shares; the fields a given command adds, and the `event_type` that selects it, are in the description on the same page rather than in a `oneOf` the base model cannot express.

| Method | Path | What it answers | Status |
|--------|------|-----------------|--------|
| `POST` | `/commands` | the version the log reached, and whether this command wrote it | 200 applied, or already applied. 400 the menu says no. 404 no such stream. 409 lost the race. 422 not a command |
| `GET` | `/menu/{menu_id}` | the fold of the whole log, plus the version to claim | 200. 404 no such stream. 422 the path is not a UUID |
| `GET` | `/menu/{menu_id}/events` | the log, oldest first. `from_version` excludes what the caller has seen | 200, possibly an empty list. 404 no such stream. 422 |

`{menu_id}` is the UUID, not the `menu/<uuid>` string the store calls a stream id: a path segment cannot hold a slash. `menu_stream_id()` in the library turns one into the other and the 404 body above shows the name the stream actually has.

Nothing here authenticates. A deployment that needs it puts a gateway in front, because an operator identity belongs on the `actor` field of the command and not in a session this service does not keep.

## Testing

Three tiers, and two of them need no server. The unit tier runs the whole portable core (commands, handler, idempotency, concurrency, the fold) against `InMemoryEventStore`. That store is not a bag of live model objects: rows go through the same `to_row` and `from_row` the PostgreSQL adapter uses, so a serialization bug surfaces here as it would against a real database.

The HTTP tier drives `menu_events.api` in process over that same store. No port is bound and no host is resolved. It covers each endpoint, each status the error mapping can answer with, and a stale write losing over the wire rather than only in process. Where the `api` extra is not installed the tier skips instead of erroring: the package imports no web framework, so a test run should not claim a machine has one.

```bash
pytest                      # 92 passed, 10 skipped with no database
pytest tests/unit           # the portable core
pytest tests/api            # the HTTP tier
pytest -m "not integration" # everything a machine without a server can run
```

## What runs here, and what does not

The two tiers that need no server are executed on any machine, and every Python example on this page runs against them. That is the source of the `passed` counts above.

The HTTP transcripts came from a process, not a test client. `uvicorn --factory menu_events.api:create_app` was started on a localhost port, every request and response quoted above is what that exchange carried, and the server's own access log shows the 200s, the 409, the two 404s and the 422. What has not been served is the app over `PostgresEventStore`: the factory was only ever handed the in-memory adapter here.

The PostgreSQL tier is not executed here. There is no server in the development environment, so `pytest` skips all 10 integration tests, and a green run without a database proves nothing about `store/postgres.py` or `projections/postgres.py`. The SQL, the append-only trigger, and the advisory-lock concurrency control in those files are unverified until someone runs the tier against a real server.

The workflow is defined, not executed. Nothing has been pushed, so no job has produced a result, and the file starts no database: a green there would carry exactly the weight described above, and no more. Its three commands are the ones this page shows, and all three were run locally, on 3.13. No 3.12 interpreter was available here, so the lower bound of the version range comes from `requires-python` and not from a run.

To exercise it, start a local PostgreSQL and point the DSN at a scratch database:

```bash
createdb menu_events_scratch
psql menu_events_scratch -f migrations/0001_event_store.sql
psql menu_events_scratch -f migrations/0002_projection.sql
export MENU_EVENTS_TEST_DSN="host=localhost dbname=menu_events_scratch user=postgres"
pytest tests/integration
```

These match `tests/integration/README.md`, and `.env.example` templates the same variable. The log is append-only, so nothing here cleans up after itself. Use a database you are willing to drop.

## License

MIT, as declared in `pyproject.toml`.
