"""Fixtures for the portable core: the memory store, the handler, command builders.

The store clock is pinned so ``recorded_at`` and the ``updated_at`` the
projection copies from it can be asserted instead of approximated.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

import pytest

from menu_events import (
    AddMenuItem,
    ChangePrice,
    EditDescription,
    InMemoryEventStore,
    MarkSoldOut,
    MenuCommandHandler,
    PutBackInStock,
    menu_stream_id,
)

RECORDED_AT = datetime(2026, 3, 14, 19, 30, tzinfo=UTC)


@pytest.fixture
def store() -> InMemoryEventStore:
    return InMemoryEventStore(clock=lambda: RECORDED_AT)


@pytest.fixture
def menu_id() -> uuid.UUID:
    return uuid.uuid4()


@pytest.fixture
def handler(store: InMemoryEventStore) -> MenuCommandHandler:
    return MenuCommandHandler(store)


@pytest.fixture
def commands(menu_id: uuid.UUID) -> MenuCommands:
    return MenuCommands(menu_id=menu_id)


@pytest.fixture
def stream_id(commands: MenuCommands) -> str:
    """Derived from the commands a test writes, so the stream it reads back
    cannot silently belong to a different menu than the one it wrote to.
    """
    return menu_stream_id(commands.menu_id)


@dataclass(frozen=True, slots=True)
class MenuCommands:
    """Builders so a test states only the fields it asserts on.

    ``version`` is a positional argument on purpose: it is the optimistic
    token, and a test that could omit it could not express a stale one.
    """

    menu_id: uuid.UUID
    actor: str = "line-1"

    def add(
        self,
        item_id: uuid.UUID,
        version: int,
        *,
        name: str = "Duck fat fries",
        price_cents: int = 900,
        description: str = "",
        category: str = "sides",
        command_id: uuid.UUID | None = None,
    ) -> AddMenuItem:
        return AddMenuItem(
            menu_id=self.menu_id,
            item_id=item_id,
            command_id=command_id or uuid.uuid4(),
            expected_menu_version=version,
            actor=self.actor,
            name=name,
            price_cents=price_cents,
            description=description,
            category=category,
        )

    def price(
        self,
        item_id: uuid.UUID,
        version: int,
        price_cents: int,
        *,
        reason: str | None = None,
        command_id: uuid.UUID | None = None,
        actor: str | None = None,
    ) -> ChangePrice:
        return ChangePrice(
            menu_id=self.menu_id,
            item_id=item_id,
            command_id=command_id or uuid.uuid4(),
            expected_menu_version=version,
            actor=actor or self.actor,
            reason=reason,
            price_cents=price_cents,
        )

    def describe(
        self,
        item_id: uuid.UUID,
        version: int,
        description: str,
        *,
        command_id: uuid.UUID | None = None,
    ) -> EditDescription:
        return EditDescription(
            menu_id=self.menu_id,
            item_id=item_id,
            command_id=command_id or uuid.uuid4(),
            expected_menu_version=version,
            actor=self.actor,
            description=description,
        )

    def sold_out(
        self,
        item_id: uuid.UUID,
        version: int,
        *,
        reason: str | None = None,
        command_id: uuid.UUID | None = None,
    ) -> MarkSoldOut:
        return MarkSoldOut(
            menu_id=self.menu_id,
            item_id=item_id,
            command_id=command_id or uuid.uuid4(),
            expected_menu_version=version,
            actor=self.actor,
            reason=reason,
        )

    def back_in_stock(
        self,
        item_id: uuid.UUID,
        version: int,
        *,
        command_id: uuid.UUID | None = None,
    ) -> PutBackInStock:
        return PutBackInStock(
            menu_id=self.menu_id,
            item_id=item_id,
            command_id=command_id or uuid.uuid4(),
            expected_menu_version=version,
            actor=self.actor,
        )


def mixed_history(commands: MenuCommands, fries: uuid.UUID, bread: uuid.UUID) -> list[object]:
    """One of every move a shift makes: add, reprice, sell out, restock, edit."""
    return [
        commands.add(fries, 0, name="Duck fat fries", price_cents=900, category="sides"),
        commands.add(bread, 1, name="Sourdough", price_cents=400, category="breads"),
        commands.price(fries, 2, 1150, reason="potato cost up"),
        commands.sold_out(fries, 3),
        commands.describe(bread, 4, description="Baked at 06:00, milled in state."),
        commands.back_in_stock(fries, 5),
        commands.sold_out(bread, 6),
        commands.price(bread, 7, 450),
        commands.back_in_stock(bread, 8),
    ]
