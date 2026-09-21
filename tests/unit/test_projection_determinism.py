"""The fold: one implementation, so a rebuild and a catch-up cannot disagree."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest
from conftest import RECORDED_AT, mixed_history

from menu_events import EventEnvelope, ReplayError, menu_stream_id, project
from menu_events.domain.events import ItemSoldOut, MenuItemAdded, PriceChanged
from menu_events.projections.menu import apply_event, empty_state

LATER = datetime(2026, 3, 15, 8, 5, tzinfo=UTC)


def test_an_added_item_is_stamped_from_its_envelope_not_from_the_clock(
    store, handler, stream_id, commands
):
    fries = uuid.uuid4()
    handler.handle(commands.add(fries, 0, name="Duck fat fries", category="sides"))
    [envelope] = store.read(stream_id)

    state = project(stream_id, [envelope])
    item = state.get(fries)

    assert item is not None
    assert item.updated_at == envelope.recorded_at == RECORDED_AT
    assert item.updated_by == "line-1"
    assert item.sold_out is False
    assert state.version == 1


def test_a_hand_built_add_envelope_folds_without_a_validation_error(stream_id):
    """The first version of this fold omitted ``updated_at``, which pydantic
    rejects on a required field, so no stream holding an item could be read.
    """
    bread = uuid.uuid4()
    envelope = EventEnvelope(
        stream_id=stream_id,
        version=1,
        event_id=uuid.uuid4(),
        command_id=uuid.uuid4(),
        recorded_at=LATER,
        event=MenuItemAdded(
            item_id=bread,
            actor="morning-prep",
            name="Sourdough",
            price_cents=400,
            category="breads",
        ),
    )

    state = apply_event(empty_state(stream_id), envelope)
    item = state.get(bread)

    assert item.updated_at == LATER
    assert item.updated_by == "morning-prep"
    assert item.name == "Sourdough"
    assert item.price_cents == 400
    assert item.description == ""
    assert item.sold_out is False
    assert state.version == 1


def test_every_event_kind_restamps_the_item_it_touches(stream_id):
    """``updated_at`` is the only sign a projection row is stale (0002 copies it
    out of the log), so the amend path has to restamp it on every kind of event
    and not only on the add that introduces the item.
    """
    bread = uuid.uuid4()
    timeline = [
        (
            datetime(2026, 3, 15, 8, 5, tzinfo=UTC),
            MenuItemAdded(item_id=bread, actor="morning-prep", name="Sourdough", price_cents=400),
        ),
        (
            datetime(2026, 3, 16, 12, 0, tzinfo=UTC),
            PriceChanged(item_id=bread, actor="manager", price_cents=450),
        ),
        (
            datetime(2026, 3, 17, 18, 30, tzinfo=UTC),
            ItemSoldOut(item_id=bread, actor="kitchen", reason="last one"),
        ),
    ]

    state = empty_state(stream_id)
    for version, (recorded_at, event) in enumerate(timeline, start=1):
        state = apply_event(
            state,
            EventEnvelope(
                stream_id=stream_id,
                version=version,
                event_id=uuid.uuid4(),
                command_id=uuid.uuid4(),
                recorded_at=recorded_at,
                event=event,
            ),
        )
        assert state.get(bread).updated_at == recorded_at
        assert state.get(bread).updated_by == event.actor

    item = state.get(bread)
    assert (item.name, item.price_cents, item.sold_out) == ("Sourdough", 450, True)


def test_a_full_replay_matches_the_state_built_one_event_at_a_time(
    handler, store, stream_id, commands
):
    fries, bread = uuid.uuid4(), uuid.uuid4()
    for command in mixed_history(commands, fries, bread):
        handler.handle(command)

    envelopes = store.read(stream_id)
    replayed = project(stream_id, envelopes)

    stepped = empty_state(stream_id)
    for envelope in envelopes:
        stepped = apply_event(stepped, envelope)

    assert stepped == replayed
    assert replayed.version == len(envelopes)
    assert replayed.get(fries).price_cents == 1150
    assert replayed.get(fries).sold_out is False
    assert replayed.get(bread).price_cents == 450
    assert replayed.get(bread).description == "Baked at 06:00, milled in state."


def test_catching_up_from_a_checkpoint_lands_on_the_same_state_as_a_rebuild(
    handler, store, stream_id, commands
):
    fries, bread = uuid.uuid4(), uuid.uuid4()
    for command in mixed_history(commands, fries, bread):
        handler.handle(command)

    whole = project(stream_id, store.read(stream_id))
    caught_up = project(stream_id, store.read(stream_id), through_version=6)
    assert caught_up.version == 6

    for envelope in store.read(stream_id, from_version=6):
        caught_up = apply_event(caught_up, envelope)

    assert caught_up == whole
    assert caught_up.get(fries).price_cents == 1150


def test_replaying_through_a_version_returns_the_menu_of_that_moment(
    handler, store, stream_id, commands
):
    fries, bread = uuid.uuid4(), uuid.uuid4()
    for command in mixed_history(commands, fries, bread):
        handler.handle(command)

    at_sold_out = project(stream_id, store.read(stream_id), through_version=4)

    assert at_sold_out.version == 4
    assert at_sold_out.get(fries).sold_out is True
    assert at_sold_out.get(fries).price_cents == 1150
    assert at_sold_out.get(bread).description == ""
    assert at_sold_out == project(stream_id, store.read(stream_id)[:4])


def test_the_fold_leaves_the_state_it_was_given_alone(handler, store, stream_id, commands):
    fries, bread = uuid.uuid4(), uuid.uuid4()
    for command in mixed_history(commands, fries, bread):
        handler.handle(command)

    envelopes = store.read(stream_id)
    early = project(stream_id, envelopes, through_version=3)
    late = apply_event(apply_event(early, envelopes[3]), envelopes[4])

    assert (early.version, late.version) == (3, 5)
    assert early.get(bread).description == ""
    assert late.get(bread).description == "Baked at 06:00, milled in state."
    assert late.items is not early.items


def test_the_read_model_orders_items_as_a_printed_menu_would(handler, store, stream_id, commands):
    ids = [uuid.uuid4() for _ in range(4)]
    handler.handle(commands.add(ids[0], 0, name="Fries", category="sides"))
    handler.handle(commands.add(ids[1], 1, name="Anchovy toast", category="small plates"))
    handler.handle(commands.add(ids[2], 2, name="Ras el hanout", category="spices"))
    handler.handle(commands.add(ids[3], 3, name="Ale", category="small plates"))

    menu = project(stream_id, store.read(stream_id))

    assert [(item.category, item.name) for item in menu.on_menu] == [
        ("sides", "Fries"),
        ("small plates", "Ale"),
        ("small plates", "Anchovy toast"),
        ("spices", "Ras el hanout"),
    ]


def test_a_gap_in_the_log_stops_the_replay(handler, store, stream_id, commands):
    fries, bread = uuid.uuid4(), uuid.uuid4()
    handler.handle(commands.add(fries, 0))
    handler.handle(commands.add(bread, 1))
    handler.handle(commands.price(fries, 2, 1000))
    handler.handle(commands.sold_out(bread, 3))

    truncated = [envelope for envelope in store.read(stream_id) if envelope.version != 3]

    with pytest.raises(ReplayError, match=r"expected version 3, got 4"):
        project(stream_id, truncated)


def test_folding_a_foreign_stream_stops(store, handler, stream_id, commands):
    fries = uuid.uuid4()
    handler.handle(commands.add(fries, 0))
    [envelope] = store.read(stream_id)

    with pytest.raises(ReplayError, match="folded into a projection of"):
        apply_event(empty_state(menu_stream_id(uuid.uuid4())), envelope)


def test_an_event_for_an_item_that_was_never_added_stops_the_replay(stream_id):
    envelope = EventEnvelope(
        stream_id=stream_id,
        version=1,
        event_id=uuid.uuid4(),
        command_id=uuid.uuid4(),
        recorded_at=RECORDED_AT,
        event=PriceChanged(item_id=uuid.uuid4(), actor="line-1", price_cents=500),
    )

    with pytest.raises(ReplayError, match="never added"):
        apply_event(empty_state(stream_id), envelope)


def test_the_same_item_added_on_two_versions_stops_the_replay(stream_id):
    bread = uuid.uuid4()

    def added(version: int) -> EventEnvelope:
        return EventEnvelope(
            stream_id=stream_id,
            version=version,
            event_id=uuid.uuid4(),
            command_id=uuid.uuid4(),
            recorded_at=RECORDED_AT,
            event=MenuItemAdded(
                item_id=bread,
                actor="line-1",
                name="Sourdough",
                price_cents=400,
            ),
        )

    state = apply_event(empty_state(stream_id), added(1))

    with pytest.raises(ReplayError, match="added twice"):
        apply_event(state, added(2))


def test_the_sold_out_flag_and_its_reason_survive_the_log(handler, store, stream_id, commands):
    fries = uuid.uuid4()
    handler.handle(commands.add(fries, 0))
    handler.handle(commands.sold_out(fries, 1, reason="last one"))

    envelopes = store.read(stream_id)

    assert project(stream_id, envelopes).get(fries).sold_out is True
    assert isinstance(envelopes[1].event, ItemSoldOut)
    assert envelopes[1].event.reason == "last one"
