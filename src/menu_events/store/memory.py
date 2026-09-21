"""In-memory adapter: the fake the unit tests run against.

It is deliberately not a dict of live model objects. Rows go through
:func:`menu_events.store.serialization.to_row` and come back through
:func:`~menu_events.store.serialization.from_row`, so a bug that only shows up
once an event has been serialised and re-read (a payload type the union does
not accept, a mutated frozen model, an event_type that no longer decodes)
fails here the same way it would fail against PostgreSQL.
"""

from __future__ import annotations

import threading
import uuid
from collections.abc import Callable, Sequence
from datetime import UTC, datetime

from ..domain.errors import ConcurrencyConflict
from ..domain.events import MenuEvent
from .base import AppendResult, EventStore
from .serialization import EventEnvelope, from_row, to_row

__all__ = ["InMemoryEventStore"]


def _utc_now() -> datetime:
    return datetime.now(UTC)


class InMemoryEventStore(EventStore):
    """Thread-safe enough to lose a race on purpose.

    A lock wraps the read-head-then-append step because that whole step has to
    be one decision, exactly as it is in the PostgreSQL adapter where an
    advisory transaction lock plays the same role. Without it the concurrency
    tests would pass for the wrong reason: two threads reading the same head
    and both writing.
    """

    def __init__(self, clock: Callable[[], datetime] | None = None) -> None:
        self._clock = clock or _utc_now
        self._rows: dict[str, list[dict]] = {}
        self._lock = threading.Lock()

    def append(
        self,
        *,
        stream_id: str,
        expected_version: int,
        event: MenuEvent,
        command_id: uuid.UUID,
        event_id: uuid.UUID | None = None,
    ) -> AppendResult:
        with self._lock:
            rows = self._rows.setdefault(stream_id, [])
            recorded = self._row_for_command(rows, command_id)
            if recorded is not None:
                return AppendResult(
                    version=int(recorded["version"]),
                    event_id=uuid.UUID(str(recorded["event_id"])),
                    duplicated=True,
                )

            head = int(rows[-1]["version"]) if rows else 0
            if expected_version != head:
                raise ConcurrencyConflict(stream_id, expected_version, head)

            envelope = EventEnvelope(
                stream_id=stream_id,
                version=head + 1,
                event_id=event_id or uuid.uuid4(),
                command_id=command_id,
                recorded_at=self._clock(),
                event=event,
            )
            rows.append(to_row(envelope))
            return AppendResult(envelope.version, envelope.event_id)

    def read(self, stream_id: str, *, from_version: int = 0) -> Sequence[EventEnvelope]:
        with self._lock:
            rows = list(self._rows.get(stream_id, ()))
        rows.sort(key=lambda row: int(row["version"]))
        return [from_row(row) for row in rows if int(row["version"]) > from_version]

    def find_by_command(
        self, stream_id: str, command_id: uuid.UUID
    ) -> EventEnvelope | None:
        with self._lock:
            row = self._row_for_command(self._rows.get(stream_id, []), command_id)
        return from_row(row) if row is not None else None

    @staticmethod
    def _row_for_command(rows: Sequence[dict], command_id: uuid.UUID) -> dict | None:
        needle = str(command_id)
        for row in rows:
            if str(row["command_id"]) == needle:
                return row
        return None
