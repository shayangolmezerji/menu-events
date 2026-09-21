"""The menu read model, as a fold over the log.

No SQL and no store in here: :func:`apply_event` takes a state and one event
and returns a new state. That is what makes replay testable without a
database, and what lets one implementation serve both a full rebuild and an
incremental catch-up. A projector that reached its result by reading a table
could not promise those two agree.

Nothing here reads the clock. ``recorded_at`` arrives on the envelope, so a
replay at midnight reproduces the state a customer saw at 19:42, which is the
only reason keeping the log around is worth the space.

``actor``, ``reason`` and ``corrects`` on an event are audit fields. The fold
uses ``actor`` to label the last change and ignores the other two: a correction
of a price is simply a later price, and the ordering of the log already decides
which of them wins.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterable
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from ..domain.events import (
    DescriptionEdited,
    ItemBackInStock,
    ItemSoldOut,
    MenuEvent,
    MenuItemAdded,
    PriceChanged,
)
from ..store.serialization import EventEnvelope

__all__ = [
    "MenuItem",
    "MenuState",
    "ReplayError",
    "apply_event",
    "empty_state",
    "project",
]


class ReplayError(Exception):
    """The events handed to the fold are not a contiguous piece of one stream.

    A projector that tolerated a gap would publish a menu that looks fine and is
    quietly wrong, so it stops here instead.
    """


class MenuItem(BaseModel):
    model_config = ConfigDict(frozen=True)

    item_id: uuid.UUID
    name: str
    price_cents: int
    description: str
    category: str
    sold_out: bool = False
    updated_at: datetime
    updated_by: str = ""


class MenuState(BaseModel):
    """The menu as of ``version``: the position of the last event folded in."""

    model_config = ConfigDict(frozen=True)

    stream_id: str
    version: int = 0
    items: dict[uuid.UUID, MenuItem] = Field(default_factory=dict)

    def get(self, item_id: uuid.UUID) -> MenuItem | None:
        return self.items.get(item_id)

    @property
    def on_menu(self) -> list[MenuItem]:
        """Menu items in the order a printed menu would use."""
        return sorted(self.items.values(), key=lambda i: (i.category, i.name))


def empty_state(stream_id: str) -> MenuState:
    return MenuState(stream_id=stream_id)


def apply_event(state: MenuState, envelope: EventEnvelope) -> MenuState:
    """One step of the fold. Returns a new state, never edits the one passed in."""
    if envelope.stream_id != state.stream_id:
        raise ReplayError(
            f"event for {envelope.stream_id} folded into a projection of {state.stream_id}"
        )
    if envelope.version != state.version + 1:
        raise ReplayError(
            f"stream {state.stream_id}: expected version {state.version + 1}, "
            f"got {envelope.version}"
        )

    event = envelope.event
    existing = state.items.get(event.item_id)

    if isinstance(event, MenuItemAdded):
        if existing is not None:
            raise ReplayError(f"stream {state.stream_id}: item {event.item_id} added twice")
        item = MenuItem(
            item_id=event.item_id,
            name=event.name,
            price_cents=event.price_cents,
            description=event.description,
            category=event.category,
            updated_at=envelope.recorded_at,
            updated_by=event.actor,
        )
    else:
        if existing is None:
            raise ReplayError(
                f"stream {state.stream_id}: {event.event_type} for item {event.item_id} "
                "that was never added"
            )
        item = _amend(existing, event).model_copy(
            update={"updated_at": envelope.recorded_at, "updated_by": event.actor}
        )

    items = dict(state.items)
    items[event.item_id] = item
    return MenuState(stream_id=state.stream_id, version=envelope.version, items=items)


def project(
    stream_id: str,
    envelopes: Iterable[EventEnvelope],
    *,
    through_version: int | None = None,
) -> MenuState:
    """Fold a whole stream, or the part of it up to ``through_version``."""
    state = empty_state(stream_id)
    for envelope in envelopes:
        if through_version is not None and envelope.version > through_version:
            break
        state = apply_event(state, envelope)
    return state


def _amend(item: MenuItem, event: MenuEvent) -> MenuItem:
    if isinstance(event, PriceChanged):
        return item.model_copy(update={"price_cents": event.price_cents})
    if isinstance(event, DescriptionEdited):
        return item.model_copy(update={"description": event.description})
    if isinstance(event, ItemSoldOut):
        return item.model_copy(update={"sold_out": True})
    if isinstance(event, ItemBackInStock):
        return item.model_copy(update={"sold_out": False})
    raise ReplayError(f"no projection rule for {event.event_type}")
