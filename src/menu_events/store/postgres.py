"""PostgreSQL adapter. Every statement that touches the event log lives here.

The log is one table. Append-only is enforced twice, in different layers: a
trigger that refuses UPDATE, DELETE and TRUNCATE, and grants that give the
application role neither. The trigger is the one that matters, because it still
fires for the table owner and for a superuser, while ``REVOKE`` does not. Both
are defined in ``migrations/0001_event_store.sql``.

Concurrency control is an advisory transaction lock per stream, then a head
read, then the insert. Serialising on the lock means two racing writers find
out which one of them is late instead of both discovering it after a failed
insert; the primary key on ``(stream_id, version)`` is the backstop for any
writer that reached the table without the lock.

Most of that is measured now. ``tests/integration/test_event_store_postgres.py``
sends these statements to a PostgreSQL 16.15 server and skips only while
``MENU_EVENTS_TEST_DSN`` is unset, which is this machine's state without a
container up: the trigger has refused an ``UPDATE`` and a ``DELETE`` from a
superuser whom the grants could not have bound, and the log has been seen to hold
a price no projection column downstream can store. The race above is measured by
``tests/integration/test_append_concurrency_postgres.py``, run on 2026-09-24: two
writers released at the version both of them read, and one event per round in
every one of them, with the loser's ``command_id`` absent from the table. Which
guard refused it was settled by patching each one out, and they are not
interchangeable. Without the version check both writers land, each at the head it
read for itself, so they take consecutive versions and the key on
``(stream_id, version)`` has nothing to object to: the state the backstop was
never for, red in six patched runs of six. Without the advisory lock the
two-writer race survives, a writer that reaches the table second being refused by
the head read or caught by the key with the same answer either way. The retried
command is what the lock is for: without it that command's duplicate lookup can
run before the first copy commits, and the retry comes back refused as stale
instead of handed the original result. With both guards in place the key on
``(stream_id, version)`` is still a backstop nothing in the tier has reached.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator, Sequence
from contextlib import contextmanager

import psycopg
from psycopg.rows import dict_row

from ..domain.errors import ConcurrencyConflict
from ..domain.events import MenuEvent
from .base import AppendResult, EventStore
from .serialization import EventEnvelope, from_row, payload_checksum, payload_json

__all__ = ["PostgresEventStore"]

_COLUMNS = (
    "stream_id, version, event_id, command_id, event_type, recorded_at, payload, checksum"
)

# hashtextextended rather than hashtext because the former returns bigint, which
# is what pg_advisory_xact_lock takes; the 32-bit form would collide on the
# first few thousand streams and then block unrelated writers on each other.
_LOCK_SQL = "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))"

_HEAD_SQL = "SELECT COALESCE(max(version), 0)::bigint AS head FROM events WHERE stream_id = %s"

_FIND_SQL = f"""
    SELECT {_COLUMNS} FROM events
    WHERE stream_id = %s AND command_id = %s
"""

_READ_SQL = f"""
    SELECT {_COLUMNS} FROM events
    WHERE stream_id = %s AND version > %s
    ORDER BY version
"""

_INSERT_SQL = """
    INSERT INTO events
        (stream_id, version, event_id, command_id, event_type, payload, checksum)
    VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s)
"""


class PostgresEventStore(EventStore):
    """The real thing. One connection per call, one transaction per write.

    Reads take no lock. A stream is append-only and a version, once written,
    never changes, so a plain SELECT is a consistent snapshot of everything up
    to some point in the log. It also means a projector can hold a lock of its
    own while reading through this adapter without waiting on itself.

    No pool. A service answering kitchen terminals would hold a pool open for
    the request rather than reconnect per command, which belongs to the HTTP
    milestone and is listed there rather than half-done here.
    """

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    def append(
        self,
        *,
        stream_id: str,
        expected_version: int,
        event: MenuEvent,
        command_id: uuid.UUID,
        event_id: uuid.UUID | None = None,
    ) -> AppendResult:
        new_id = event_id or uuid.uuid4()
        payload = payload_json(event)
        checksum = payload_checksum(event)
        try:
            with self._transaction(stream_id) as conn:
                # The duplicate check runs before the version check on purpose.
                # A retry of a command that already landed carries the version
                # from before that write, and calling that stale would turn a
                # network timeout into a failed command.
                row = conn.execute(_FIND_SQL, (stream_id, command_id)).fetchone()
                if row is not None:
                    return AppendResult(
                        version=int(row["version"]),
                        event_id=row["event_id"],
                        duplicated=True,
                    )

                head = self._head(conn, stream_id)
                if expected_version != head:
                    raise ConcurrencyConflict(stream_id, expected_version, head)

                conn.execute(
                    _INSERT_SQL,
                    (
                        stream_id,
                        head + 1,
                        new_id,
                        command_id,
                        event.event_type,
                        payload,
                        checksum,
                    ),
                )
                # Committing is what releases the advisory lock, so nothing
                # between here and the end of the block may raise.
                return AppendResult(head + 1, new_id)
        except psycopg.errors.UniqueViolation:
            # Either a writer that reached the table without the lock took this
            # version, or the same command is committing twice. The second is
            # not a conflict, so it is settled before the first is reported.
            recorded = self.find_by_command(stream_id, command_id)
            if recorded is not None:
                return AppendResult(recorded.version, recorded.event_id, duplicated=True)
            raise ConcurrencyConflict(
                stream_id, expected_version, self.head_of(stream_id)
            ) from None

    def read(self, stream_id: str, *, from_version: int = 0) -> Sequence[EventEnvelope]:
        with psycopg.connect(self._dsn, row_factory=dict_row) as conn:
            rows = conn.execute(_READ_SQL, (stream_id, from_version)).fetchall()
        return [from_row(row) for row in rows]

    def find_by_command(
        self, stream_id: str, command_id: uuid.UUID
    ) -> EventEnvelope | None:
        with psycopg.connect(self._dsn, row_factory=dict_row) as conn:
            row = conn.execute(_FIND_SQL, (stream_id, command_id)).fetchone()
        return from_row(row) if row is not None else None

    def head_of(self, stream_id: str) -> int:
        """The current head, for callers that need the version without the history."""
        with psycopg.connect(self._dsn, row_factory=dict_row) as conn:
            return self._head(conn, stream_id)

    @contextmanager
    def _transaction(self, stream_id: str) -> Iterator[psycopg.Connection[dict]]:
        # `with conn` commits on the way out, rolls back when an exception is in
        # flight, and closes the connection. Releasing the advisory lock is the
        # commit, so a writer that dies mid-transaction unblocks the next one.
        with psycopg.connect(self._dsn, row_factory=dict_row) as conn:
            conn.execute(_LOCK_SQL, (stream_id,))
            yield conn

    @staticmethod
    def _head(conn: psycopg.Connection[dict], stream_id: str) -> int:
        row = conn.execute(_HEAD_SQL, (stream_id,)).fetchone()
        return int(row["head"])
