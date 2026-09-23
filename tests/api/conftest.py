"""Fixtures for the HTTP tier: a real app, a real request cycle, and no network.

Requests go through ``fastapi.testclient``, which drives the ASGI app inside the
test process: no port is bound and no host is resolved, so the tier needs no
network and cannot leak into one.

Nothing here imports the web framework at module level. The whole tier skips
when the ``api`` extra is not installed, and a bare ``pip install -e ".[dev]"``
has no fastapi to import: an error at collection would be a lie about the
package, which imports no web framework, while a skip is the honest state of a
machine that did not ask for one. Same bargain ``tests/integration/`` strikes
with a missing server.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

import pytest

from menu_events import InMemoryEventStore

RECORDED_AT = datetime(2026, 3, 14, 19, 30, tzinfo=UTC)


@pytest.fixture
def create_app():
    """The factory itself, so a test can build a second app and compare logs."""
    api = pytest.importorskip("menu_events.api", reason="the api extra is not installed")
    return api.create_app


@pytest.fixture
def app(create_app):
    """A fresh app over a fresh log, so one test cannot see another's writes.

    The clock is pinned for the same reason the unit tier pins it: the
    timestamps a read returns become assertable instead of approximate.
    """
    return create_app(InMemoryEventStore(clock=lambda: RECORDED_AT))


@pytest.fixture
def client_for():
    """A client bound to whichever app a test names. In process, no port."""
    testclient = pytest.importorskip(
        "fastapi.testclient", reason="the api extra is not installed"
    )
    return lambda app: testclient.TestClient(app)


@pytest.fixture
def client(app, client_for) -> object:
    return client_for(app)


@pytest.fixture
def menu_id() -> uuid.UUID:
    return uuid.uuid4()


@pytest.fixture
def bodies(menu_id: uuid.UUID) -> CommandBodies:
    return CommandBodies(menu_id=menu_id)


@dataclass(frozen=True, slots=True)
class CommandBodies:
    """JSON bodies for one menu, in the shape ``POST /commands`` expects.

    ``version`` is positional because it is the optimistic token: a test that
    could omit it could not express a stale one either.
    """

    menu_id: uuid.UUID
    actor: str = "line-1"

    @property
    def stream_id(self) -> str:
        return f"menu/{self.menu_id}"

    def add(
        self,
        item_id: uuid.UUID,
        version: int,
        *,
        name: str = "Duck fat fries",
        price_cents: int = 900,
        category: str = "sides",
        command_id: uuid.UUID | None = None,
    ) -> dict[str, object]:
        return self._base(
            "menu_item_added",
            item_id,
            version,
            command_id=command_id,
            name=name,
            price_cents=price_cents,
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
    ) -> dict[str, object]:
        return self._base(
            "price_changed",
            item_id,
            version,
            command_id=command_id,
            price_cents=price_cents,
            reason=reason,
        )

    def describe(
        self,
        item_id: uuid.UUID,
        version: int,
        description: str,
        *,
        command_id: uuid.UUID | None = None,
    ) -> dict[str, object]:
        return self._base(
            "description_edited", item_id, version, command_id=command_id, description=description
        )

    def sold_out(
        self,
        item_id: uuid.UUID,
        version: int,
        *,
        command_id: uuid.UUID | None = None,
    ) -> dict[str, object]:
        return self._base("item_sold_out", item_id, version, command_id=command_id)

    def back_in_stock(
        self,
        item_id: uuid.UUID,
        version: int,
        *,
        command_id: uuid.UUID | None = None,
    ) -> dict[str, object]:
        return self._base("item_back_in_stock", item_id, version, command_id=command_id)

    def _base(
        self,
        event_type: str,
        item_id: uuid.UUID,
        version: int,
        *,
        command_id: uuid.UUID | None,
        **fields: object,
    ) -> dict[str, object]:
        body: dict[str, object] = {
            "event_type": event_type,
            "menu_id": str(self.menu_id),
            "item_id": str(item_id),
            "command_id": str(command_id or uuid.uuid4()),
            "expected_menu_version": version,
            "actor": self.actor,
        }
        body.update({key: value for key, value in fields.items() if value is not None})
        return body
