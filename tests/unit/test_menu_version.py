"""``menu_version`` is the token every reader and every writer compares against."""

from __future__ import annotations

import uuid

from conftest import mixed_history

from menu_events import MenuCommandHandler, project


def test_each_applied_command_moves_the_version_by_exactly_one(
    handler, store, stream_id, commands
):
    fries, bread = uuid.uuid4(), uuid.uuid4()

    results = [handler.handle(command) for command in mixed_history(commands, fries, bread)]

    assert [result.menu_version for result in results] == list(range(1, 10))
    assert all(result.applied for result in results)
    assert [envelope.version for envelope in store.read(stream_id)] == [
        result.menu_version for result in results
    ]


def test_the_version_never_goes_backwards_when_retries_are_interleaved(
    handler, store, stream_id, commands
):
    fries, bread = uuid.uuid4(), uuid.uuid4()
    history = mixed_history(commands, fries, bread)
    already_landed = history[2]

    results = []
    for index, command in enumerate(history):
        results.append(handler.handle(command))
        if index == 5:
            results.append(handler.handle(already_landed))

    assert [(r.menu_version, r.applied) for r in results] == [
        (1, True),
        (2, True),
        (3, True),
        (4, True),
        (5, True),
        (6, True),
        (3, False),
        (7, True),
        (8, True),
        (9, True),
    ]
    assert [r.menu_version for r in results if r.applied] == list(range(1, 10))
    assert len(store.read(stream_id)) == 9


def test_a_second_handler_on_the_same_store_counts_from_the_same_head(
    store, stream_id, commands
):
    first = MenuCommandHandler(store)
    second = MenuCommandHandler(store)
    fries = uuid.uuid4()

    first.handle(commands.add(fries, 0))
    result = second.handle(commands.price(fries, 1, 1000))

    assert result.menu_version == 2
    assert project(stream_id, store.read(stream_id)).version == 2


def test_the_result_points_at_the_event_the_log_got(handler, store, stream_id, commands, menu_id):
    fries = uuid.uuid4()
    command = commands.add(fries, 0)

    result = handler.handle(command)
    envelope = store.read(stream_id)[0]

    assert (result.menu_id, result.event_id) == (menu_id, envelope.event_id)
    assert envelope.command_id == command.command_id
    assert envelope.version == result.menu_version
