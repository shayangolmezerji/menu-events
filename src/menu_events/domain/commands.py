"""Commands: the write side's only entry point.

Every command carries ``expected_menu_version``. That is the optimistic token:
the caller says which state of the menu it looked at before deciding, and the
store rejects the command if the log has moved since. There is no path to the
log that does not go through it.
"""

from __future__ import annotations

import uuid
from typing import ClassVar

from pydantic import BaseModel, ConfigDict, Field

from .events import (
    DescriptionEdited,
    ItemBackInStock,
    ItemSoldOut,
    MenuEvent,
    MenuItemAdded,
    PriceChanged,
)

__all__ = [
    "AddMenuItem",
    "ChangePrice",
    "Command",
    "EditDescription",
    "MAX_PRICE_CENTS",
    "MarkSoldOut",
    "PutBackInStock",
]


# 2**53 - 1: the largest safe integer, the last one every IEEE-754 double holds
# exactly. A client that parses JSON numbers as doubles rounds past it, so this
# keeps the log, the BIGINT column and that client on the same digits. Well below
# the ``menu_item.price_cents`` maximum and well above any price a menu has
# carried. The reasoning, and the replay rule, are in README's "A price is
# bounded".
MAX_PRICE_CENTS = 2**53 - 1


class Command(BaseModel):
    """Base shape. ``command_id`` doubles as the idempotency key: the store
    remembers it next to the event, so a client that timed out and retried
    lands once."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    menu_id: uuid.UUID
    item_id: uuid.UUID
    command_id: uuid.UUID
    expected_menu_version: int = Field(ge=0)
    actor: str = Field(min_length=1, max_length=64)
    reason: str | None = Field(default=None, max_length=500)
    corrects: uuid.UUID | None = None

    event_type: ClassVar[str]

    def to_event(self) -> MenuEvent:
        raise NotImplementedError


class AddMenuItem(Command):
    name: str = Field(min_length=1, max_length=120)
    price_cents: int = Field(ge=0, le=MAX_PRICE_CENTS)
    description: str = Field(default="", max_length=2000)
    category: str = Field(default="menu", min_length=1, max_length=60)

    event_type: ClassVar[str] = "menu_item_added"

    def to_event(self) -> MenuItemAdded:
        return MenuItemAdded(
            item_id=self.item_id,
            actor=self.actor,
            reason=self.reason,
            corrects=self.corrects,
            name=self.name,
            price_cents=self.price_cents,
            description=self.description,
            category=self.category,
        )


class ChangePrice(Command):
    price_cents: int = Field(ge=0, le=MAX_PRICE_CENTS)

    event_type: ClassVar[str] = "price_changed"

    def to_event(self) -> PriceChanged:
        return PriceChanged(
            item_id=self.item_id,
            actor=self.actor,
            reason=self.reason,
            corrects=self.corrects,
            price_cents=self.price_cents,
        )


class EditDescription(Command):
    description: str = Field(max_length=2000)

    event_type: ClassVar[str] = "description_edited"

    def to_event(self) -> DescriptionEdited:
        return DescriptionEdited(
            item_id=self.item_id,
            actor=self.actor,
            reason=self.reason,
            corrects=self.corrects,
            description=self.description,
        )


class MarkSoldOut(Command):
    event_type: ClassVar[str] = "item_sold_out"

    def to_event(self) -> ItemSoldOut:
        return ItemSoldOut(
            item_id=self.item_id,
            actor=self.actor,
            reason=self.reason,
            corrects=self.corrects,
        )


class PutBackInStock(Command):
    event_type: ClassVar[str] = "item_back_in_stock"

    def to_event(self) -> ItemBackInStock:
        return ItemBackInStock(
            item_id=self.item_id,
            actor=self.actor,
            reason=self.reason,
            corrects=self.corrects,
        )
