"""Optimistic concurrency: the second writer is told it is late, and by how much."""

from __future__ import annotations

import uuid

import pytest

from menu_events import ConcurrencyConflict
from menu_events.domain.errors import CommandRejected, ItemNotOnMenu


def test_a_stale_expected_version_writes_nothing(store, handler, stream_id, commands):
    fries, bread = uuid.uuid4(), uuid.uuid4()
    handler.handle(commands.add(fries, 0))

    with pytest.raises(ConcurrencyConflict):
        handler.handle(commands.add(bread, 0))

    assert [envelope.event_type for envelope in store.read(stream_id)] == ["menu_item_added"]


def test_the_conflict_names_the_version_the_log_holds_and_the_one_claimed(handler, commands):
    fries, bread = uuid.uuid4(), uuid.uuid4()
    handler.handle(commands.add(fries, 0))
    handler.handle(commands.add(bread, 1))

    with pytest.raises(ConcurrencyConflict) as excinfo:
        handler.handle(commands.price(fries, 1, 1000))

    conflict = excinfo.value
    assert (conflict.expected, conflict.actual) == (1, 2)
    message = str(conflict)
    assert "is at version 2" in message
    assert "expected 1" in message


def test_claiming_a_first_event_against_a_non_empty_stream_conflicts(
    handler, store, stream_id, commands
):
    fries, ghost = uuid.uuid4(), uuid.uuid4()
    handler.handle(commands.add(fries, 0))

    with pytest.raises(ConcurrencyConflict) as excinfo:
        handler.handle(commands.add(ghost, 5))

    assert excinfo.value.actual == 1
    assert len(store.read(stream_id)) == 1


def test_a_writer_that_rereads_the_head_gets_in_next(handler, store, stream_id, commands):
    fries, bread = uuid.uuid4(), uuid.uuid4()
    handler.handle(commands.add(fries, 0))

    with pytest.raises(ConcurrencyConflict) as excinfo:
        handler.handle(commands.add(bread, 0))

    head = excinfo.value.actual
    result = handler.handle(commands.add(bread, head))
    assert (result.applied, result.menu_version) == (True, 2)
    assert store.read(stream_id)[-1].event.item_id == bread


def test_a_conflict_is_a_command_rejection(handler, commands):
    fries = uuid.uuid4()
    handler.handle(commands.add(fries, 0))

    with pytest.raises(CommandRejected) as excinfo:
        handler.handle(commands.add(uuid.uuid4(), 0))

    assert isinstance(excinfo.value, ConcurrencyConflict)


def test_a_stale_writer_is_answered_against_the_state_it_claimed(
    handler, store, stream_id, commands
):
    """The handler folds up to ``expected_menu_version``, not to the head, so a
    caller cannot get a write accepted against state it never looked at. The
    cost is that the state check speaks before the version check does.
    """
    fries, bread = uuid.uuid4(), uuid.uuid4()
    handler.handle(commands.add(fries, 0))
    handler.handle(commands.add(bread, 1))

    with pytest.raises(ItemNotOnMenu):
        handler.handle(commands.sold_out(bread, 1))

    assert len(store.read(stream_id)) == 2
