"""The event store port.

Three methods: write one event, read a stream, look up the event a command
already produced. The command side and the projections depend on this
interface only, which is what lets the unit tests run against the fake in
:mod:`menu_events.store.memory` and the PostgreSQL adapter stay a thin
translation of the same contract.
"""

from __future__ import annotations

import abc
import uuid
from collections.abc import Sequence
from dataclasses import dataclass

from ..domain.events import MenuEvent
from .serialization import EventEnvelope

__all__ = ["AppendResult", "EventStore"]


@dataclass(frozen=True, slots=True)
class AppendResult:
    """What the store decided about one write.

    ``duplicated`` is set when the command had already been recorded, so the
    caller can tell a retried request from a new one without comparing
    versions.
    """

    version: int
    event_id: uuid.UUID
    duplicated: bool = False


class EventStore(abc.ABC):
    """Append-only, ordered, one event per command.

    A command produces exactly one event. Batching several events behind one
    command would need either a multi-row insert guarded by one version check
    or a sequence of checks, and the second breaks the guarantee that a
    rejected command left nothing behind. Nothing in this domain needs that
    yet, so the port does not offer it. See ADR 0001.
    """

    @abc.abstractmethod
    def append(
        self,
        *,
        stream_id: str,
        expected_version: int,
        event: MenuEvent,
        command_id: uuid.UUID,
        event_id: uuid.UUID | None = None,
    ) -> AppendResult:
        """Add one event at the head of a stream.

        ``expected_version`` is the version the writer read before deciding.
        Zero means "I believe this stream does not exist yet". Any other value
        that is not the current head raises ``ConcurrencyConflict`` and writes
        nothing.

        A command whose ``command_id`` is already on the stream returns the
        recorded result instead of appending a second event, and that check
        happens before the version check: the retry of a successful write
        carries the version from before that write, so treating it as stale
        would turn a network timeout into a failed command.

        ``event_id`` exists so a test can pin identity; production callers
        leave it out and get a uuid4.
        """

    @abc.abstractmethod
    def read(self, stream_id: str, *, from_version: int = 0) -> Sequence[EventEnvelope]:
        """Events with a version above ``from_version``, oldest first.

        Ordering is by version, never by timestamp. Two events in the same
        committed batch share a timestamp; their versions are the order.
        """

    @abc.abstractmethod
    def find_by_command(
        self, stream_id: str, command_id: uuid.UUID
    ) -> EventEnvelope | None:
        """The event this command already produced, if there is one."""
