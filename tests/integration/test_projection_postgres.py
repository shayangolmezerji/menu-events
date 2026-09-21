"""The projection tables: the checkpoint row, the item rows, and the log behind them.

``menu_item.stream_id`` references ``menu_projection``, so a stream that has
never been projected has no parent for its items yet. Nothing but a real
foreign key can show whether the projector writes its rows in the order that
implies, which is what the first two tests here are for.
"""

from __future__ import annotations

import uuid

import psycopg
import pytest
from conftest import DSN, add_item, change_price, requires_postgres
from psycopg.rows import dict_row

from menu_events import project
from menu_events.projections.postgres import PostgresMenuReadModel
from menu_events.store.postgres import PostgresEventStore

pytestmark = [requires_postgres, pytest.mark.integration]


@pytest.fixture
def read_model(store: PostgresEventStore) -> PostgresMenuReadModel:
    return PostgresMenuReadModel(DSN, store)


def test_the_first_advance_writes_the_parent_row_before_any_item(
    read_model, handler, stream_id, menu_id
):
    """The bug this pins: ``advance`` on a stream with no checkpoint row reaches
    the item insert first, and the transaction dies on the foreign key.
    """
    handler.handle(add_item(menu_id, uuid.uuid4()))

    assert read_model.advance(stream_id) == 1

    with psycopg.connect(DSN, row_factory=dict_row) as conn:
        checkpoint = conn.execute(
            "SELECT version FROM menu_projection WHERE stream_id = %s", (stream_id,)
        ).fetchone()
    assert checkpoint["version"] == 1


def test_rebuild_on_a_never_projected_stream_satisfies_the_foreign_key(
    read_model, handler, store, stream_id, menu_id
):
    item = uuid.uuid4()
    handler.handle(add_item(menu_id, item))
    handler.handle(change_price(menu_id, item, expected_version=1))

    assert read_model.rebuild(stream_id) == 2
    assert read_model.state(stream_id) == project(stream_id, store.read(stream_id))


def test_catching_up_twice_lands_where_a_fold_of_the_log_lands(
    read_model, handler, store, stream_id, menu_id
):
    item = uuid.uuid4()
    handler.handle(add_item(menu_id, item))
    handler.handle(change_price(menu_id, item, expected_version=1, price_cents=1000))

    assert read_model.advance(stream_id) == 2

    handler.handle(change_price(menu_id, item, expected_version=2, price_cents=700))
    handler.handle(add_item(menu_id, uuid.uuid4(), expected_version=3))

    assert read_model.advance(stream_id) == 4
    assert read_model.state(stream_id) == project(stream_id, store.read(stream_id))


def test_rebuild_replaces_a_row_edited_behind_the_projector(
    read_model, handler, store, stream_id, menu_id
):
    """The tables are derived state: 0002 says a wrong row is evidence of a
    projector bug and is answered by rebuilding rather than by patching, so
    ``rebuild`` has to overwrite a value edited by hand and not skip it.
    """
    item = uuid.uuid4()
    handler.handle(add_item(menu_id, item))
    read_model.advance(stream_id)

    with psycopg.connect(DSN) as conn:
        conn.execute(
            "UPDATE menu_item SET price_cents = 1 WHERE stream_id = %s AND item_id = %s",
            (stream_id, item),
        )

    assert read_model.rebuild(stream_id) == 1
    assert read_model.state(stream_id) == project(stream_id, store.read(stream_id))
