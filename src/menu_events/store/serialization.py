"""Canonical serialization for the log.

Both adapters build and read rows through :func:`to_row` and :func:`from_row`.
That is what keeps the in-memory fake from lying about the shape of a
PostgreSQL row: it stores the same dict the database stores in a jsonb column,
and decodes it back through the same validation.

The payload is serialised with sorted keys and no whitespace, so the checksum
of an event does not depend on the order the producing language happened to
walk its fields in. The checksum covers the payload only, never ``recorded_at``
or ``version``: those are assigned by the store, and a row restored from a
backup can be verified without having to agree with the database about
timezone handling.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from ..domain.events import MenuEvent, parse_menu_event

__all__ = [
    "EventEnvelope",
    "from_row",
    "menu_id_from_stream",
    "menu_stream_id",
    "payload_checksum",
    "payload_json",
    "row_matches_checksum",
    "to_row",
]

_STREAM_PREFIX = "menu/"


def menu_stream_id(menu_id: uuid.UUID) -> str:
    """The stream holding one menu's history. One stream per menu, see ADR 0001."""
    return f"{_STREAM_PREFIX}{menu_id}"


def menu_id_from_stream(stream_id: str) -> uuid.UUID:
    if not stream_id.startswith(_STREAM_PREFIX):
        raise ValueError(f"{stream_id!r} is not a menu stream")
    return uuid.UUID(stream_id[len(_STREAM_PREFIX) :])


class EventEnvelope(BaseModel):
    """An event plus the coordinates the store gave it.

    ``version`` is the position in the stream, starting at 1, and is the value
    ``menu_version`` refers to in commands. ``command_id`` is the idempotency
    key the writer supplied, stored so a retry can be recognised as the same
    write rather than a second one.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    stream_id: str
    version: int = Field(gt=0)
    event_id: uuid.UUID
    command_id: uuid.UUID
    recorded_at: datetime
    event: MenuEvent

    @property
    def event_type(self) -> str:
        return self.event.event_type

    @property
    def checksum(self) -> str:
        return payload_checksum(self.event)


def payload_json(event: MenuEvent) -> str:
    return json.dumps(
        event.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def payload_checksum(event: MenuEvent) -> str:
    return hashlib.sha256(payload_json(event).encode()).hexdigest()


def to_row(envelope: EventEnvelope) -> dict[str, Any]:
    """The one-row form both stores persist.

    ``payload`` comes back as a parsed dict because that is what a jsonb column
    hands over on read: jsonb keeps its own key order and collapses duplicate
    keys, so nothing downstream should assume it got the original text back.
    """
    return {
        "stream_id": envelope.stream_id,
        "version": envelope.version,
        "event_id": str(envelope.event_id),
        "command_id": str(envelope.command_id),
        "event_type": envelope.event_type,
        "recorded_at": envelope.recorded_at,
        "payload": json.loads(payload_json(envelope.event)),
        "checksum": envelope.checksum,
    }


def from_row(row: Mapping[str, Any]) -> EventEnvelope:
    """Rebuild an envelope from a stored row.

    Accepts both driver shapes: ``payload`` as text (the in-memory store) or as
    a mapping (psycopg decoding jsonb), ``recorded_at`` as text or datetime.
    """
    payload = row["payload"]
    if isinstance(payload, (str, bytes)):
        payload = json.loads(payload)
    return EventEnvelope(
        stream_id=str(row["stream_id"]),
        version=int(row["version"]),
        event_id=_as_uuid(row["event_id"]),
        command_id=_as_uuid(row["command_id"]),
        recorded_at=_as_datetime(row["recorded_at"]),
        event=parse_menu_event(payload),
    )


def row_matches_checksum(row: Mapping[str, Any]) -> bool:
    """True when a row's payload still hashes to the checksum stored with it."""
    payload = row["payload"]
    if isinstance(payload, (str, bytes)):
        payload = json.loads(payload)
    return payload_checksum(parse_menu_event(payload)) == str(row["checksum"])


def _as_uuid(value: Any) -> uuid.UUID:
    return value if isinstance(value, uuid.UUID) else uuid.UUID(str(value))


def _as_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))
