"""Rejections and idempotency: what the write side refuses, and what it absorbs."""

from __future__ import annotations

import uuid

import pytest
from pydantic import ValidationError

from menu_events import AddMenuItem, menu_stream_id, project
from menu_events.domain.commands import MAX_PRICE_CENTS
from menu_events.domain.errors import (
    CommandRejected,
    ItemAlreadyOnMenu,
    ItemAlreadySoldOut,
    ItemNotOnMenu,
    ItemNotSoldOut,
    MenuStreamNotFound,
)
from menu_events.domain.events import PriceChanged


def test_a_second_add_for_an_item_id_already_on_the_menu_is_refused(
    handler, store, stream_id, commands
):
    fries = uuid.uuid4()
    handler.handle(commands.add(fries, 0))

    with pytest.raises(ItemAlreadyOnMenu) as excinfo:
        handler.handle(commands.add(fries, 1, name="Better fries", price_cents=1200))

    assert excinfo.value.item_id == fries
    assert [envelope.event_type for envelope in store.read(stream_id)] == ["menu_item_added"]


def test_a_command_naming_an_item_that_is_not_on_the_menu_is_refused(
    handler, store, stream_id, commands
):
    fries = uuid.uuid4()
    handler.handle(commands.add(fries, 0))

    with pytest.raises(ItemNotOnMenu):
        handler.handle(commands.price(uuid.uuid4(), 1, 700))

    assert len(store.read(stream_id)) == 1


def test_the_first_event_on_a_stream_has_to_add_an_item(handler, store, stream_id, commands):
    with pytest.raises(MenuStreamNotFound):
        handler.handle(commands.price(uuid.uuid4(), 0, 700))

    assert store.read(stream_id) == []


def test_selling_an_item_out_twice_is_refused(handler, store, stream_id, commands):
    fries = uuid.uuid4()
    handler.handle(commands.add(fries, 0))
    handler.handle(commands.sold_out(fries, 1))

    with pytest.raises(ItemAlreadySoldOut):
        handler.handle(commands.sold_out(fries, 2))

    assert [envelope.event_type for envelope in store.read(stream_id)] == [
        "menu_item_added",
        "item_sold_out",
    ]


def test_restocking_an_item_that_was_never_sold_out_is_refused(
    handler, store, stream_id, commands
):
    fries = uuid.uuid4()
    handler.handle(commands.add(fries, 0))

    with pytest.raises(ItemNotSoldOut):
        handler.handle(commands.back_in_stock(fries, 1))

    assert len(store.read(stream_id)) == 1


def test_a_rejected_command_leaves_the_stream_contiguous(handler, store, stream_id, commands):
    fries, bread = uuid.uuid4(), uuid.uuid4()
    handler.handle(commands.add(fries, 0))
    handler.handle(commands.sold_out(fries, 1))
    handler.handle(commands.add(bread, 2))

    for command in (
        commands.add(bread, 3, name="Second sourdough"),
        commands.price(uuid.uuid4(), 3, 700),
        commands.back_in_stock(bread, 3),
        commands.sold_out(fries, 3),
        commands.describe(uuid.uuid4(), 3, description="nothing"),
    ):
        with pytest.raises(CommandRejected):
            handler.handle(command)

    assert [envelope.version for envelope in store.read(stream_id)] == [1, 2, 3]


def test_repricing_a_sold_out_item_is_recorded(handler, store, stream_id, commands):
    """The write side has no rule tying price to availability: selling out is a
    fact about stock, a price is a fact about the menu.
    """
    fries = uuid.uuid4()
    handler.handle(commands.add(fries, 0))
    handler.handle(commands.sold_out(fries, 1))

    result = handler.handle(commands.price(fries, 2, 1150))

    assert (result.applied, result.menu_version) == (True, 3)
    assert [envelope.event_type for envelope in store.read(stream_id)] == [
        "menu_item_added",
        "item_sold_out",
        "price_changed",
    ]


def test_writing_the_value_an_item_already_holds_is_still_recorded(
    handler, store, stream_id, commands
):
    fries = uuid.uuid4()
    handler.handle(commands.add(fries, 0, price_cents=900))

    result = handler.handle(commands.price(fries, 1, 900, reason="checked against the till"))

    assert result.menu_version == 2
    assert store.read(stream_id)[1].event.reason == "checked against the till"


def test_a_price_above_the_ceiling_is_refused_by_both_price_commands(commands):
    """The ceiling has to be on the command because nothing downstream can be:
    ``events.payload`` is jsonb and holds any magnitude, so a price with no upper
    bound is caught nowhere until a projection column refuses it.
    """
    fries = uuid.uuid4()

    with pytest.raises(ValidationError, match="less than or equal to"):
        commands.add(fries, 0, price_cents=MAX_PRICE_CENTS + 1)
    with pytest.raises(ValidationError, match="less than or equal to"):
        commands.price(fries, 1, MAX_PRICE_CENTS + 1)


def test_a_price_at_the_ceiling_and_one_below_it_are_written_and_read_back(
    handler, store, stream_id, commands
):
    """The bound is inclusive, and the value comes back out of the canonical row
    both adapters persist as the number that was written.
    """
    fries = uuid.uuid4()
    handler.handle(commands.add(fries, 0, price_cents=MAX_PRICE_CENTS))
    handler.handle(commands.price(fries, 1, MAX_PRICE_CENTS - 1))

    assert [envelope.event.price_cents for envelope in store.read(stream_id)] == [
        MAX_PRICE_CENTS,
        MAX_PRICE_CENTS - 1,
    ]


def test_a_price_above_the_ceiling_already_in_the_log_is_still_read(
    handler, store, stream_id, commands
):
    """A log written before the ceiling existed can hold a price no command would
    accept now, above BIGINT besides. It stays readable: the event cannot be
    edited or deleted, and a parser that refused it would leave the stream
    unreadable to the command that was going to correct it.
    """
    fries = uuid.uuid4()
    handler.handle(commands.add(fries, 0))
    above_bigint = 2**64
    store.append(
        stream_id=stream_id,
        expected_version=1,
        event=PriceChanged(item_id=fries, actor="line-1", price_cents=above_bigint),
        command_id=uuid.uuid4(),
    )

    assert store.read(stream_id)[1].event.price_cents == above_bigint
    assert project(stream_id, store.read(stream_id)).get(fries).price_cents == above_bigint


def test_a_retry_of_a_landed_command_lands_once(handler, store, stream_id, commands):
    """The retry carries the version from before the write it repeats, and the
    store answers it with the recorded event instead of a conflict.
    """
    fries = uuid.uuid4()
    command = commands.add(fries, 0)

    first = handler.handle(command)
    retry = handler.handle(command)

    assert retry.applied is False
    assert (retry.menu_version, retry.event_id) == (first.menu_version, first.event_id)
    assert len(store.read(stream_id)) == 1


def test_a_retry_after_the_menu_moved_on_still_reports_the_original_write(
    handler, store, stream_id, commands
):
    fries = uuid.uuid4()
    command = commands.add(fries, 0)
    handler.handle(command)
    handler.handle(commands.price(fries, 1, 1000))

    retry = handler.handle(command)

    assert (retry.applied, retry.menu_version) == (False, 1)
    assert len(store.read(stream_id)) == 2


def test_a_spent_command_id_is_answered_with_the_event_it_produced(
    handler, store, stream_id, commands
):
    """The key is the command id alone. New words under a spent id count as the
    same request, so a client that generated a new payload and reused the id
    learns about the first write rather than making a second one.
    """
    fries, bread = uuid.uuid4(), uuid.uuid4()
    command_id = uuid.uuid4()

    first = handler.handle(commands.add(fries, 0, command_id=command_id))
    second = handler.handle(commands.add(bread, 1, command_id=command_id))

    assert second.applied is False
    assert (second.menu_version, second.event_id) == (first.menu_version, first.event_id)
    assert [envelope.event.item_id for envelope in store.read(stream_id)] == [fries]


def test_a_command_id_is_spent_per_stream_only(handler, store, commands, menu_id):
    fries = uuid.uuid4()
    command_id = uuid.uuid4()
    handler.handle(commands.add(fries, 0, command_id=command_id))

    other_menu = uuid.uuid4()
    result = handler.handle(
        AddMenuItem(
            menu_id=other_menu,
            item_id=fries,
            command_id=command_id,
            expected_menu_version=0,
            actor="line-1",
            name="Duck fat fries",
            price_cents=900,
            category="sides",
        )
    )

    assert (result.applied, result.menu_version) == (True, 1)
    assert len(store.read(menu_stream_id(other_menu))) == 1
    assert len(store.read(menu_stream_id(menu_id))) == 1
