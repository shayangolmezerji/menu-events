"""A price too large for `bigint`, against a server that has the column.

The ceiling on a command keeps such a price out of any log written from now
on, but it cannot reach into one written before it: ``events.payload`` is
jsonb and holds any magnitude, and the read side deliberately does not
re-check it, so the fold still meets prices the projection column cannot
store. What each writer then does is an argument about ``projections``
(``advance`` writes a row per version, ``rebuild`` one per item) that only a
server can settle, because the refusal comes from PostgreSQL's type, not from
any Python code.

Measured here on PostgreSQL 16.15. See ``README.md``'s "A price is bounded"
for the rule these tests are the evidence for.
"""

from __future__ import annotations

import uuid

import psycopg
import pytest
from conftest import DSN, add_item, requires_postgres
from psycopg.rows import dict_row

from menu_events.domain.events import PriceChanged
from menu_events.projections.postgres import PostgresMenuReadModel
from menu_events.store.postgres import PostgresEventStore

pytestmark = [requires_postgres, pytest.mark.integration]

# Past BIGINT's maximum (2**63 - 1), so the refusal that follows comes from the
# column and not from MAX_PRICE_CENTS: a value inside the ceiling still fits
# this column, and a value past it never reaches one.
OVER_BIGINT = 2**64


@pytest.fixture
def read_model(store: PostgresEventStore) -> PostgresMenuReadModel:
    return PostgresMenuReadModel(DSN, store)


def append_price(
    store: PostgresEventStore,
    stream_id: str,
    item_id: uuid.UUID,
    *,
    expected_version: int,
    price_cents: int,
    corrects: uuid.UUID | None = None,
):
    """A ``PriceChanged`` straight into the log, past the command layer entirely.

    This is the shape a pre-ceiling entry has, or one a hand-built event leaves
    behind: the guard in ``ChangePrice`` is never constructed, so nothing between
    it and the table refuses the value.
    """
    return store.append(
        stream_id=stream_id,
        expected_version=expected_version,
        event=PriceChanged(
            item_id=item_id, actor="typo", price_cents=price_cents, corrects=corrects
        ),
        command_id=uuid.uuid4(),
    )


def checkpoint_version(stream_id: str) -> int:
    """The projector's position, read raw. A stream never projected has no row,
    which is the same position ``_checkpoint`` reports as 0.
    """
    with psycopg.connect(DSN, row_factory=dict_row) as conn:
        row = conn.execute(
            "SELECT version FROM menu_projection WHERE stream_id = %s", (stream_id,)
        ).fetchone()
    return 0 if row is None else int(row["version"])


def projected_prices(stream_id: str) -> list[int]:
    with psycopg.connect(DSN, row_factory=dict_row) as conn:
        rows = conn.execute(
            "SELECT price_cents FROM menu_item WHERE stream_id = %s ORDER BY item_id",
            (stream_id,),
        ).fetchall()
    return [int(row["price_cents"]) for row in rows]


def test_the_log_holds_a_price_the_projection_cannot_write(
    read_model, handler, store, stream_id, menu_id
):
    """2**64 is appended, read back, and then stops ``advance`` at the column.

    Both halves matter: the log accepting it is why the projector has to be the
    one to answer, and the answer being an error from PostgreSQL rather than a
    truncation is what keeps a wrong price off the menu card.
    """
    item = uuid.uuid4()
    handler.handle(add_item(menu_id, item))

    appended = append_price(
        store, stream_id, item, expected_version=1, price_cents=OVER_BIGINT
    )

    assert appended.version == 2
    assert store.read(stream_id)[-1].event.price_cents == OVER_BIGINT
    assert checkpoint_version(stream_id) == 0

    with pytest.raises(psycopg.errors.NumericValueOutOfRange, match="bigint out of range"):
        read_model.advance(stream_id)

    # The failed run had already written the checkpoint row for this stream and
    # rolled it back with the item, so nothing partial is left behind: the
    # projector is exactly where it was, with the whole tail still to do.
    assert checkpoint_version(stream_id) == 0
    assert projected_prices(stream_id) == []
    assert [envelope.version for envelope in store.read(stream_id)] == [1, 2]


def test_a_refused_advance_undoes_only_its_own_run(
    read_model, handler, store, stream_id, menu_id
):
    """A stream already projected once keeps its checkpoint and its row when a
    later version overflows the column.

    The row left standing holds the older price while the log holds the larger
    one: the lag ``migrations/0002_projection.sql`` calls eventual consistency,
    made visible rather than argued about.
    """
    item = uuid.uuid4()
    handler.handle(add_item(menu_id, item))
    assert read_model.advance(stream_id) == 1
    assert projected_prices(stream_id) == [900]

    append_price(store, stream_id, item, expected_version=1, price_cents=OVER_BIGINT)

    with pytest.raises(psycopg.errors.NumericValueOutOfRange):
        read_model.advance(stream_id)

    assert checkpoint_version(stream_id) == 1
    assert projected_prices(stream_id) == [900]


def test_a_correction_carries_rebuild_past_a_price_advance_keeps_reading(
    read_model, handler, store, stream_id, menu_id
):
    """``advance`` re-reads from its own checkpoint, so the overflowing version is
    in the tail of every run after it, correction included.

    ``rebuild`` folds the whole stream and writes where each item ended, so the
    same log lets it through to version 3. The difference is the reason a stuck
    projection is answered by a rebuild and not by retrying the advance.
    """
    item = uuid.uuid4()
    handler.handle(add_item(menu_id, item))

    bad = append_price(store, stream_id, item, expected_version=1, price_cents=OVER_BIGINT)
    append_price(
        store,
        stream_id,
        item,
        expected_version=bad.version,
        price_cents=500,
        corrects=bad.event_id,
    )

    with pytest.raises(psycopg.errors.NumericValueOutOfRange):
        read_model.advance(stream_id)
    assert checkpoint_version(stream_id) == 0

    assert read_model.rebuild(stream_id) == 3
    assert checkpoint_version(stream_id) == 3
    assert projected_prices(stream_id) == [500]

    # Unstuck: advance from the checkpoint rebuild published has nothing left to
    # fold, so the overflowing version is behind it for good.
    assert read_model.advance(stream_id) == 3
