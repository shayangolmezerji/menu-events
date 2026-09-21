"""The log is the source of truth, so it must not move underneath a reader."""

from __future__ import annotations

import uuid

import pytest
from conftest import MenuCommands, mixed_history
from pydantic import ValidationError

from menu_events import menu_stream_id
from menu_events.store.serialization import from_row, row_matches_checksum, to_row


def test_events_already_written_are_unchanged_by_events_written_after(
    store,
    handler,
    stream_id,
    commands,
):
    fries, bread = uuid.uuid4(), uuid.uuid4()
    for command in (
        commands.add(fries, 0),
        commands.add(bread, 1),
        commands.price(fries, 2, 1150),
    ):
        handler.handle(command)

    read_midway = list(store.read(stream_id))

    handler.handle(commands.sold_out(fries, 3))
    handler.handle(commands.back_in_stock(fries, 4))

    assert list(store.read(stream_id)[:3]) == read_midway


def test_read_returns_the_stream_in_version_order_and_from_a_point(
    handler, store, stream_id, commands
):
    fries, bread = uuid.uuid4(), uuid.uuid4()
    for command in mixed_history(commands, fries, bread):
        handler.handle(command)

    assert [envelope.version for envelope in store.read(stream_id)] == list(range(1, 10))
    assert [envelope.version for envelope in store.read(stream_id, from_version=7)] == [8, 9]
    assert store.read(menu_stream_id(uuid.uuid4())) == []


def test_a_stream_holds_one_menu_only(handler, store, stream_id, commands):
    fries = uuid.uuid4()
    other_menu, other_fries = uuid.uuid4(), uuid.uuid4()
    other_stream = menu_stream_id(other_menu)
    other = MenuCommands(menu_id=other_menu)

    handler.handle(commands.add(fries, 0))
    handler.handle(commands.price(fries, 1, 1000))
    handler.handle(other.add(other_fries, 0))

    written = store.read(stream_id)
    assert [envelope.event.item_id for envelope in written] == [fries, fries]
    assert {envelope.stream_id for envelope in written} == {stream_id}
    assert [envelope.event.item_id for envelope in store.read(other_stream)] == [other_fries]


def test_an_envelope_cannot_be_edited_in_place(handler, store, stream_id, commands):
    fries = uuid.uuid4()
    handler.handle(commands.add(fries, 0))
    [envelope] = store.read(stream_id)

    with pytest.raises(ValidationError):
        envelope.version = 99


def test_a_reader_cannot_wreck_the_log_through_the_list_it_was_handed(
    handler, store, stream_id, commands
):
    fries = uuid.uuid4()
    handler.handle(commands.add(fries, 0))
    handler.handle(commands.price(fries, 1, 1000))
    untouched = list(store.read(stream_id))

    handed_out = store.read(stream_id)
    handed_out.pop()
    handed_out.append(untouched[0])

    assert list(store.read(stream_id)) == untouched


def test_the_row_the_store_wrote_is_the_canonical_row(handler, store, stream_id, commands):
    """The shape 0001 persists, including its two table checks: the type column
    agreeing with the document, and the checksum belonging to that document.
    """
    fries = uuid.uuid4()
    handler.handle(commands.add(fries, 0))
    [envelope] = store.read(stream_id)
    row = to_row(envelope)

    assert (row["event_type"], row["version"]) == ("menu_item_added", 1)
    assert row["payload"]["event_type"] == row["event_type"]
    assert row["payload"]["item_id"] == str(fries)
    assert row_matches_checksum(row) is True
    assert from_row(row) == envelope
