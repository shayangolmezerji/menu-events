# menu-events

Append-only event store for restaurant menus, with optimistic concurrency and replayable read models. Python and PostgreSQL.

[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](pyproject.toml)
[![Python](https://img.shields.io/badge/python-3.12%20%7C%203.13-blue.svg)](https://www.python.org/)

## Overview

A menu is edited by several people at once. A manager reprices an item, a line cook marks it sold out, front-of-house restores it a minute later. A plain table with one row per item handles that badly: the last write wins and the earlier one is gone, an edit erases the price that was there before, and a mis-tap can only be undone by changing a stored row.

This service keeps the history instead of the current row. Every accepted change is an immutable event. The menu you read is a fold over those events. Two competing writes do not silently overwrite each other, because a command names the version it was decided against and the loser is refused. A mis-tap is answered by a later event, not by rewriting an earlier one.

The write path and the read path are separate (CQRS). A command is validated and appended. A projection folds the log into a menu state, in Python for replay and in PostgreSQL for the read tables. The read model can lag the log by one projection run. The reasoning, and the switch from the originally planned Go and Kafka, is in `docs/adr/0001-event-sourcing-over-crud.md`.

## Architecture

```
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
       |     advance or rebuild writes the read model under its own lock
       v
    menu_projection (checkpoint) + menu_item (one row per item, derived)
       |
       v
    MenuState.on_menu        items sorted by category, then name
```

## Requirements

- Python 3.12 or newer. `.github/workflows/ci.yml` names 3.12 and 3.13.
- `psycopg[binary]` and `pydantic`, installed with the package.
- A PostgreSQL server only for the integration tier. The unit tests and every example on this page need none.

## Quickstart

From a clean checkout:

```bash
python3 -m venv .venv  # Windows: python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest
```

On a machine with no database the last line reports `60 passed, 10 skipped`. The 10 skips are the PostgreSQL tier; see [Testing](#testing). To lint:

```bash
ruff check .
```

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

## Testing

The unit tier runs the whole portable core (commands, handler, idempotency, concurrency, the fold) against `InMemoryEventStore`. That store is not a bag of live model objects: rows go through the same `to_row` and `from_row` the PostgreSQL adapter uses, so a serialization bug surfaces here as it would against a real database.

```bash
pytest                 # everything reachable without a database
pytest tests/unit      # just the portable core
```

## What runs here, and what does not

The in-memory tier is executed on any machine. Every example on this page runs against it. That is the source of the `passed` counts above.

The PostgreSQL tier is not executed here. There is no server in the development environment, so `pytest` skips all 10 integration tests, and a green run without a database proves nothing about `store/postgres.py` or `projections/postgres.py`. The SQL, the append-only trigger, and the advisory-lock concurrency control in those files are unverified until someone runs the tier against a real server.

The workflow is defined, not executed. Nothing has been pushed, so no job has produced a result, and the file starts no database: a green there would carry exactly the weight described above, and no more. Its two commands are the ones this page shows, and both were run locally, on 3.13. No 3.12 interpreter was available here, so the lower bound of the version range comes from `requires-python` and not from a run.

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
