"""PostgreSQL adapter for the projection tables. The fold lives in
:mod:`menu_events.projections.menu`; this file only moves rows.

Both writers take one advisory lock per stream, in a namespace of its own, and
read the log while holding it. That ordering is what makes them safe to run at
the same time as each other and as a command: neither can publish a snapshot of
the log older than the one already stored, because the snapshot is taken after
the lock is held and the lock is released only once the rows and the checkpoint
are written.

The fold behind both writers is tested event by event in
:mod:`menu_events.projections.menu`. Moving the rows is tested against a server in
``tests/integration/test_projection_postgres.py``, which skips while
``MENU_EVENTS_TEST_DSN`` is unset, and the locking argument above has met one too:
``test_projection_concurrency_postgres.py`` races two writers over one stream and
each of them leaves a projection equal to a fold of the log. So the writers do
exclude each other, on one server, which is the whole of what that measures. A
projection running while a command appends is the other half of the paragraph
above, and no test has put those two locks in each other's way.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

import psycopg
from psycopg.rows import dict_row

from ..store.base import EventStore
from .menu import MenuItem, MenuState, apply_event, project

__all__ = ["PostgresMenuReadModel"]

_PROJECTION_LOCK_PREFIX = "projection/"

_CHECKPOINT_SQL = "SELECT version FROM menu_projection WHERE stream_id = %s FOR UPDATE"

_TOUCH_CHECKPOINT_SQL = """
    INSERT INTO menu_projection (stream_id, version)
    VALUES (%s, %s)
    ON CONFLICT (stream_id) DO UPDATE
    SET version = EXCLUDED.version, refreshed_at = now()
"""

_DELETE_ITEMS_SQL = "DELETE FROM menu_item WHERE stream_id = %s"

_UPSERT_ITEM_SQL = """
    INSERT INTO menu_item
        (stream_id, item_id, name, price_cents, description, category, sold_out, updated_at,
         updated_by)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (stream_id, item_id) DO UPDATE SET
        name = EXCLUDED.name,
        price_cents = EXCLUDED.price_cents,
        description = EXCLUDED.description,
        category = EXCLUDED.category,
        sold_out = EXCLUDED.sold_out,
        updated_at = EXCLUDED.updated_at,
        updated_by = EXCLUDED.updated_by
"""

_SELECT_ITEMS_SQL = """
    SELECT item_id, name, price_cents, description, category, sold_out, updated_at, updated_by
    FROM menu_item
    WHERE stream_id = %s
    ORDER BY category, name
"""


class PostgresMenuReadModel:
    """The menu table a read endpoint would query, rebuilt from the log.

    These tables are disposable by design: :meth:`rebuild` derives the whole
    contents from the event log, so a bad projection is dropped and recomputed
    rather than patched by hand. Only ``events`` carries the rule that a written
    row cannot be rewritten.
    """

    def __init__(self, dsn: str, store: EventStore) -> None:
        self._dsn = dsn
        self._store = store

    def advance(self, stream_id: str) -> int:
        """Apply whatever the log gained since the last run. Returns the new version."""
        with self._locked(stream_id) as conn:
            checkpoint = self._checkpoint(conn, stream_id)
            # menu_item.stream_id references menu_projection (0002), so the
            # checkpoint row goes first: a stream that has never been projected
            # has no parent for its items yet.
            self._touch_checkpoint(conn, stream_id, checkpoint)
            state = self._load(conn, stream_id, version=checkpoint)
            for envelope in self._store.read(stream_id, from_version=checkpoint):
                state = apply_event(state, envelope)
                self._write_item(conn, stream_id, state.items[envelope.event.item_id])
            self._touch_checkpoint(conn, stream_id, state.version)
            return state.version

    def rebuild(self, stream_id: str) -> int:
        """Discard the rows and fold the entire stream again. Returns the version."""
        with self._locked(stream_id) as conn:
            checkpoint = self._checkpoint(conn, stream_id)
            self._touch_checkpoint(conn, stream_id, checkpoint)
            state = project(stream_id, self._store.read(stream_id))
            conn.execute(_DELETE_ITEMS_SQL, (stream_id,))
            for item in state.items.values():
                self._write_item(conn, stream_id, item)
            self._touch_checkpoint(conn, stream_id, state.version)
            return state.version

    def state(self, stream_id: str) -> MenuState:
        """What the projection tables say right now, without touching the log."""
        with psycopg.connect(self._dsn, row_factory=dict_row) as conn:
            return self._load(conn, stream_id, version=self._checkpoint(conn, stream_id))

    @contextmanager
    def _locked(self, stream_id: str) -> Iterator[psycopg.Connection[dict]]:
        with psycopg.connect(self._dsn, row_factory=dict_row) as conn:
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (_PROJECTION_LOCK_PREFIX + stream_id,),
            )
            yield conn

    @staticmethod
    def _checkpoint(conn: psycopg.Connection[dict], stream_id: str) -> int:
        row = conn.execute(_CHECKPOINT_SQL, (stream_id,)).fetchone()
        return 0 if row is None else int(row["version"])

    @staticmethod
    def _touch_checkpoint(conn: psycopg.Connection[dict], stream_id: str, version: int) -> None:
        conn.execute(_TOUCH_CHECKPOINT_SQL, (stream_id, version))

    @staticmethod
    def _write_item(conn: psycopg.Connection[dict], stream_id: str, item: MenuItem) -> None:
        conn.execute(
            _UPSERT_ITEM_SQL,
            (
                stream_id,
                item.item_id,
                item.name,
                item.price_cents,
                item.description,
                item.category,
                item.sold_out,
                item.updated_at,
                item.updated_by,
            ),
        )

    @staticmethod
    def _load(conn: psycopg.Connection[dict], stream_id: str, *, version: int) -> MenuState:
        rows = conn.execute(_SELECT_ITEMS_SQL, (stream_id,)).fetchall()
        return MenuState(
            stream_id=stream_id,
            version=version,
            items={
                row["item_id"]: MenuItem(
                    item_id=row["item_id"],
                    name=row["name"],
                    price_cents=row["price_cents"],
                    description=row["description"],
                    category=row["category"],
                    sold_out=row["sold_out"],
                    updated_at=row["updated_at"],
                    updated_by=row["updated_by"],
                )
                for row in rows
            },
        )
