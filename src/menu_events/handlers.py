"""The write side: the only path from a command to the log.

Two checks run on the way in, and they answer different questions. The
projection decides whether the command makes sense against the menu as the
caller described it. The store decides whether the menu still looks like that,
which is the question a kitchen terminal two metres away can change the answer
to. The second is the concurrency control; the first is a courtesy, and losing
it to a race is exactly what the version check is there to catch.

The handler projects the stream only up to ``expected_menu_version`` rather than
to the current head. That keeps the state it validated and the version the
append compares against the same state, so a caller cannot get a write accepted
by claiming a version it never looked at.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from .domain.commands import AddMenuItem, Command, MarkSoldOut, PutBackInStock
from .domain.errors import (
    ItemAlreadyOnMenu,
    ItemAlreadySoldOut,
    ItemNotOnMenu,
    ItemNotSoldOut,
    MenuStreamNotFound,
)
from .projections.menu import MenuState, project
from .store.base import EventStore
from .store.serialization import menu_stream_id

__all__ = ["CommandResult", "MenuCommandHandler"]


@dataclass(frozen=True, slots=True)
class CommandResult:
    """The outcome of an accepted command.

    ``applied`` is False when the log already held this command, so the caller
    can tell a retry from a new write. The version is the same either way,
    which is the whole point of an idempotency key.
    """

    menu_id: uuid.UUID
    menu_version: int
    event_id: uuid.UUID
    applied: bool


class MenuCommandHandler:
    def __init__(self, store: EventStore) -> None:
        self._store = store

    def handle(self, command: Command) -> CommandResult:
        stream_id = menu_stream_id(command.menu_id)

        recorded = self._store.find_by_command(stream_id, command.command_id)
        if recorded is not None:
            return CommandResult(
                menu_id=command.menu_id,
                menu_version=recorded.version,
                event_id=recorded.event_id,
                applied=False,
            )

        state = project(
            stream_id,
            self._store.read(stream_id),
            through_version=command.expected_menu_version,
        )
        _validate(state, command, stream_id=stream_id)

        result = self._store.append(
            stream_id=stream_id,
            expected_version=command.expected_menu_version,
            event=command.to_event(),
            command_id=command.command_id,
        )
        return CommandResult(
            menu_id=command.menu_id,
            menu_version=result.version,
            event_id=result.event_id,
            applied=not result.duplicated,
        )


def _validate(state: MenuState, command: Command, *, stream_id: str) -> None:
    """Reject a command that contradicts the menu, before it reaches the log.

    Anything can be appended to a log; that is why the projection has to be
    worth reading. A rejected command writes nothing, so the stream stays
    contiguous and a replay never has to step over a hole.
    """
    if isinstance(command, AddMenuItem):
        if state.get(command.item_id) is not None:
            raise ItemAlreadyOnMenu(command.item_id)
        return

    if state.version == 0:
        raise MenuStreamNotFound(stream_id)

    item = state.get(command.item_id)
    if item is None:
        raise ItemNotOnMenu(command.item_id)

    if isinstance(command, MarkSoldOut) and item.sold_out:
        raise ItemAlreadySoldOut(command.item_id)
    if isinstance(command, PutBackInStock) and not item.sold_out:
        raise ItemNotSoldOut(command.item_id)
    # A price change or a description edit that lands on the value the item
    # already has is still recorded: the menu did not move, but the fact that
    # an operator set it at this moment on this terminal did.
