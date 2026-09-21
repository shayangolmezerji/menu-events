"""Rows in and rows out: the shape both adapters persist, and the checksum over it."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime

import pytest
from conftest import RECORDED_AT, mixed_history
from pydantic import ValidationError

from menu_events import EventEnvelope, menu_stream_id
from menu_events.domain.events import (
    DescriptionEdited,
    ItemBackInStock,
    ItemSoldOut,
    MenuItemAdded,
    PriceChanged,
    parse_menu_event,
)
from menu_events.store.serialization import (
    from_row,
    menu_id_from_stream,
    payload_checksum,
    payload_json,
    row_matches_checksum,
    to_row,
)

OTHER_STREAM = "menu/00000000-0000-0000-0000-000000000000"
MENU_ID = uuid.UUID("00000000-0000-0000-0000-000000000000")
ITEM = uuid.UUID("11111111-1111-1111-1111-111111111111")
EVENT_ID = uuid.UUID("22222222-2222-2222-2222-222222222222")
COMMAND_ID = uuid.UUID("33333333-3333-3333-3333-333333333333")


def every_event_type() -> list[object]:
    return [
        MenuItemAdded(
            item_id=ITEM,
            actor="line-1",
            name="Duck fat fries",
            price_cents=900,
            description="with rosemary salt",
            category="sides",
        ),
        PriceChanged(item_id=ITEM, actor="manager", reason="potato cost up", price_cents=1150),
        DescriptionEdited(item_id=ITEM, actor="manager", description="Hand-cut, twice fried."),
        ItemSoldOut(item_id=ITEM, actor="kitchen", reason="last one"),
        ItemBackInStock(item_id=ITEM, actor="kitchen", corrects=EVENT_ID),
    ]


def envelope_for(event: object) -> EventEnvelope:
    return EventEnvelope(
        stream_id=OTHER_STREAM,
        version=7,
        event_id=EVENT_ID,
        command_id=COMMAND_ID,
        recorded_at=RECORDED_AT,
        event=event,
    )


@pytest.mark.parametrize("event", every_event_type(), ids=lambda e: e.event_type)
def test_an_event_survives_a_row_round_trip(event):
    written = envelope_for(event)

    restored = from_row(to_row(written))

    assert restored == written
    assert restored.event is not written.event


@pytest.mark.parametrize("event", every_event_type(), ids=lambda e: e.event_type)
def test_a_field_this_version_does_not_know_is_refused_on_read(event):
    """``extra="forbid"`` is why a row written by a newer release fails the read
    it should fail instead of coming back missing a fact nobody notices is gone.
    """
    row = to_row(envelope_for(event))
    row["payload"] = {**row["payload"], "gluten_free": True}

    with pytest.raises(ValidationError, match="gluten_free"):
        from_row(row)


def test_from_row_accepts_what_psycopg_hands_over():
    """jsonb arrives as a mapping and uuid columns arrive as UUIDs, while a
    restore-from-backup path can hand over text instead.
    """
    written = envelope_for(every_event_type()[0])
    row = to_row(written)

    driver_row = {
        **row,
        "payload": payload_json(written.event),
        "recorded_at": RECORDED_AT.isoformat(),
        "event_id": written.event_id,
        "command_id": written.command_id,
        "version": "7",
    }

    assert from_row(driver_row) == written


def test_the_row_shape_is_fixed_and_holds_only_json_scalars():
    row = to_row(envelope_for(every_event_type()[0]))

    assert set(row) == {
        "stream_id",
        "version",
        "event_id",
        "command_id",
        "event_type",
        "recorded_at",
        "payload",
        "checksum",
    }
    assert row["event_type"] == row["payload"]["event_type"]
    assert row["payload"] == json.loads(payload_json(parse_menu_event(row["payload"])))
    assert all(
        value is None or isinstance(value, (str, int, bool)) for value in row["payload"].values()
    )


def test_the_checksum_ignores_the_order_the_fields_were_written_in():
    event = every_event_type()[0]
    dumped = event.model_dump(mode="json")
    shuffled = {key: dumped[key] for key in sorted(dumped, reverse=True)}

    assert payload_checksum(parse_menu_event(shuffled)) == payload_checksum(event)
    assert row_matches_checksum(
        {"payload": json.dumps(shuffled), "checksum": payload_checksum(event)}
    ) is True


def test_a_changed_payload_no_longer_matches_its_stored_checksum():
    written = envelope_for(every_event_type()[1])
    row = to_row(written)

    assert row_matches_checksum(row) is True
    tampered = {**row, "payload": {**row["payload"], "price_cents": 1}}
    assert row_matches_checksum(tampered) is False


def test_the_checksum_covers_the_payload_only():
    """``version`` and ``recorded_at`` are assigned by the store, so a row
    restored from a backup must still verify against them moving.
    """
    event = every_event_type()[2]

    first = EventEnvelope(
        stream_id=OTHER_STREAM,
        version=1,
        event_id=EVENT_ID,
        command_id=COMMAND_ID,
        recorded_at=RECORDED_AT,
        event=event,
    )
    restored = EventEnvelope(
        stream_id=OTHER_STREAM,
        version=942,
        event_id=EVENT_ID,
        command_id=COMMAND_ID,
        recorded_at=datetime(2020, 1, 1, tzinfo=UTC),
        event=event,
    )

    assert first.checksum == restored.checksum == payload_checksum(event)


def test_every_row_the_memory_store_wrote_still_verifies(handler, store, stream_id, commands):
    fries, bread = uuid.uuid4(), uuid.uuid4()
    for command in mixed_history(commands, fries, bread):
        handler.handle(command)

    rows = [to_row(envelope) for envelope in store.read(stream_id)]

    assert len(rows) == 9
    assert all(row_matches_checksum(row) for row in rows)
    assert len({row["checksum"] for row in rows}) == len(rows)


def test_an_unknown_event_type_is_refused_on_read():
    with pytest.raises(ValidationError, match="event_type"):
        parse_menu_event({"item_id": str(ITEM), "actor": "x", "event_type": "menu_item_retired"})


def test_the_stream_id_carries_the_menu_it_belongs_to():
    assert menu_stream_id(MENU_ID) == OTHER_STREAM
    assert menu_id_from_stream(OTHER_STREAM) == MENU_ID

    with pytest.raises(ValueError, match="is not a menu stream"):
        menu_id_from_stream("orders/12")
