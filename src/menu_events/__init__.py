"""Append-only menu event store with optimistic concurrency and replayable projections.

The portable parts are re-exported here: the domain types, the store port, the
in-memory adapter and the projection fold. The PostgreSQL adapters are not, so
that ``import menu_events`` never reaches for a database driver. Get them from
``menu_events.store.postgres`` and ``menu_events.projections.postgres``.
"""

from __future__ import annotations

from .domain.commands import (
    AddMenuItem,
    ChangePrice,
    Command,
    EditDescription,
    MarkSoldOut,
    PutBackInStock,
)
from .domain.errors import CommandRejected, ConcurrencyConflict
from .handlers import CommandResult, MenuCommandHandler
from .projections.menu import MenuItem, MenuState, ReplayError, project
from .store.base import AppendResult, EventStore
from .store.memory import InMemoryEventStore
from .store.serialization import EventEnvelope, menu_stream_id

__version__ = "0.1.0"

__all__ = [
    "AddMenuItem",
    "AppendResult",
    "ChangePrice",
    "Command",
    "CommandRejected",
    "CommandResult",
    "ConcurrencyConflict",
    "EditDescription",
    "EventEnvelope",
    "EventStore",
    "InMemoryEventStore",
    "MarkSoldOut",
    "MenuItem",
    "MenuState",
    "MenuCommandHandler",
    "PutBackInStock",
    "ReplayError",
    "menu_stream_id",
    "project",
    "__version__",
]
