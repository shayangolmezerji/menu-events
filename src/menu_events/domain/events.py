"""Menu facts. Instances of these types are what the event log stores.

Every event is a statement about something that already happened, so every
field is a fact rather than an instruction: there are no setters here and no
"old value" fields, because the previous value is whatever the projection held
at that point in the log, and reading it back from the log is the only way to
get it right.
"""

from __future__ import annotations

import uuid
from typing import Annotated, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

__all__ = [
    "DescriptionEdited",
    "ItemBackInStock",
    "MenuItemAdded",
    "ItemSoldOut",
    "MenuEvent",
    "PriceChanged",
    "parse_menu_event",
]


class EventBase(BaseModel):
    """Identity fields shared by every fact on the menu stream.

    ``corrects`` is how a repair is marked. The event it points at stays in the
    log untouched; the two of them together are the audit trail, and the
    projection ends at whichever of them came later. See ADR 0001.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    item_id: uuid.UUID
    actor: str = Field(min_length=1, max_length=64)
    reason: str | None = Field(default=None, max_length=500)
    corrects: uuid.UUID | None = None


class MenuItemAdded(EventBase):
    event_type: Literal["menu_item_added"] = "menu_item_added"
    name: str = Field(min_length=1, max_length=120)
    price_cents: int = Field(ge=0)
    description: str = Field(default="", max_length=2000)
    category: str = Field(default="menu", min_length=1, max_length=60)


class PriceChanged(EventBase):
    """Minor units, always. A float price would make replay order-dependent
    once rounding enters the projection."""

    event_type: Literal["price_changed"] = "price_changed"
    price_cents: int = Field(ge=0)


class DescriptionEdited(EventBase):
    """Full replacement text, not a diff. A diff would have to be applied in
    exactly the right order to be re-read, which makes every consumer of the
    log order-sensitive for no benefit at this size."""

    event_type: Literal["description_edited"] = "description_edited"
    description: str = Field(max_length=2000)


class ItemSoldOut(EventBase):
    event_type: Literal["item_sold_out"] = "item_sold_out"


class ItemBackInStock(EventBase):
    """The inverse of ``ItemSoldOut``. Without it a mis-tap on a kitchen
    terminal could only be answered by editing the log, which is the one thing
    this store refuses to do."""

    event_type: Literal["item_back_in_stock"] = "item_back_in_stock"


MenuEvent = Annotated[
    Union[
        MenuItemAdded,
        PriceChanged,
        DescriptionEdited,
        ItemSoldOut,
        ItemBackInStock,
    ],
    Field(discriminator="event_type"),
]

_MENU_EVENT: TypeAdapter[MenuEvent] = TypeAdapter(MenuEvent)


def parse_menu_event(data: object) -> MenuEvent:
    """Rebuild a typed event from decoded JSON.

    Raises ``pydantic.ValidationError`` for an unknown event_type, so a log
    entry written by a newer version of this package fails loudly on read
    instead of being skipped.
    """
    return _MENU_EVENT.validate_python(data)
