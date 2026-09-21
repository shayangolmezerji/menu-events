"""The PostgreSQL tier: a real server, the real tables, no fake in sight.

Everything here is skipped while ``MENU_EVENTS_TEST_DSN`` is unset. That is the
honest state on a machine with no database, and it is why the SQL in
``store/postgres.py`` and ``projections/postgres.py`` carries a second tier:
neither file can be reached by the unit tests without lying about what it does.
See README.md for the scratch database these tests expect.
"""

from __future__ import annotations

import os
import uuid

import pytest

from menu_events import MenuCommandHandler, menu_stream_id
from menu_events.domain.commands import AddMenuItem, ChangePrice
from menu_events.store.postgres import PostgresEventStore

DSN = os.environ.get("MENU_EVENTS_TEST_DSN", "")

requires_postgres = pytest.mark.skipif(
    not DSN, reason="MENU_EVENTS_TEST_DSN is unset, so no server is reachable"
)


@pytest.fixture
def menu_id() -> uuid.UUID:
    return uuid.uuid4()


@pytest.fixture
def stream_id(menu_id: uuid.UUID) -> str:
    return menu_stream_id(menu_id)


@pytest.fixture
def store() -> PostgresEventStore:
    return PostgresEventStore(DSN)


@pytest.fixture
def handler(store: PostgresEventStore) -> MenuCommandHandler:
    return MenuCommandHandler(store)


def add_item(
    menu_id: uuid.UUID, item_id: uuid.UUID, *, expected_version: int = 0
) -> AddMenuItem:
    return AddMenuItem(
        menu_id=menu_id,
        item_id=item_id,
        command_id=uuid.uuid4(),
        expected_menu_version=expected_version,
        actor="line-1",
        name="Duck fat fries",
        price_cents=900,
        category="sides",
    )


def change_price(
    menu_id: uuid.UUID, item_id: uuid.UUID, *, expected_version: int, price_cents: int = 1150
) -> ChangePrice:
    return ChangePrice(
        menu_id=menu_id,
        item_id=item_id,
        command_id=uuid.uuid4(),
        expected_menu_version=expected_version,
        actor="manager",
        reason="potato cost up",
        price_cents=price_cents,
    )
