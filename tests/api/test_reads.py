"""The read side: ``GET /menu/{menu_id}`` and its ``/events`` tail."""

from __future__ import annotations

import uuid

import pytest

pytestmark = pytest.mark.api


def test_a_menu_read_folds_the_log_and_names_its_head(client, bodies):
    fries = uuid.uuid4()
    client.post("/commands", json=bodies.add(fries, 0))
    client.post("/commands", json=bodies.price(fries, 1, 1150, reason="potato cost up"))

    response = client.get(f"/menu/{bodies.menu_id}")

    assert response.status_code == 200
    menu = response.json()
    assert (menu["menu_id"], menu["stream_id"], menu["version"]) == (
        str(bodies.menu_id),
        bodies.stream_id,
        2,
    )
    [item] = menu["items"]
    assert (item["item_id"], item["price_cents"], item["updated_by"]) == (
        str(fries),
        1150,
        "line-1",
    )


def test_a_menu_read_orders_items_the_way_a_printed_menu_would(client, bodies):
    fries, bread, wine = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    client.post("/commands", json=bodies.add(fries, 0, name="Duck fat fries", category="sides"))
    client.post("/commands", json=bodies.add(wine, 1, name="Ribolla", category="wine"))
    client.post("/commands", json=bodies.add(bread, 2, name="Sourdough", category="breads"))

    menu = client.get(f"/menu/{bodies.menu_id}").json()

    assert [(item["category"], item["name"]) for item in menu["items"]] == [
        ("breads", "Sourdough"),
        ("sides", "Duck fat fries"),
        ("wine", "Ribolla"),
    ]


def test_a_menu_read_reports_the_sold_out_flag_the_last_event_decided(client, bodies):
    fries = uuid.uuid4()
    client.post("/commands", json=bodies.add(fries, 0))
    client.post("/commands", json=bodies.sold_out(fries, 1))

    menu = client.get(f"/menu/{bodies.menu_id}").json()

    assert [item["sold_out"] for item in menu["items"]] == [True]


def test_the_events_endpoint_returns_the_log_oldest_first(client, bodies):
    fries, bread = uuid.uuid4(), uuid.uuid4()
    client.post("/commands", json=bodies.add(fries, 0))
    client.post("/commands", json=bodies.add(bread, 1))
    client.post("/commands", json=bodies.sold_out(fries, 2))

    events = client.get(f"/menu/{bodies.menu_id}/events").json()

    assert [event["version"] for event in events] == [1, 2, 3]
    assert [event["event"]["event_type"] for event in events] == [
        "menu_item_added",
        "menu_item_added",
        "item_sold_out",
    ]
    assert all(event["stream_id"] == bodies.stream_id for event in events)


def test_the_stored_timestamp_survives_the_trip_untouched(client, bodies):
    """The fold reads no clock, so a read after a replay still answers with the
    moment the write was recorded.
    """
    fries = uuid.uuid4()
    client.post("/commands", json=bodies.add(fries, 0))

    events = client.get(f"/menu/{bodies.menu_id}/events").json()
    menu = client.get(f"/menu/{bodies.menu_id}").json()

    assert events[0]["recorded_at"] == "2026-03-14T19:30:00Z"
    assert menu["items"][0]["updated_at"] == "2026-03-14T19:30:00Z"


def test_from_version_hands_over_the_tail_the_caller_has_not_seen(client, bodies):
    for version, item in enumerate([uuid.uuid4(), uuid.uuid4(), uuid.uuid4()]):
        client.post("/commands", json=bodies.add(item, version))

    caught_up = client.get(f"/menu/{bodies.menu_id}/events?from_version=2").json()

    assert [event["version"] for event in caught_up] == [3]


def test_a_window_above_the_head_is_an_empty_list_and_not_a_missing_stream(client, bodies):
    fries = uuid.uuid4()
    client.post("/commands", json=bodies.add(fries, 0))

    response = client.get(f"/menu/{bodies.menu_id}/events?from_version=1")

    assert response.status_code == 200
    assert response.json() == []
