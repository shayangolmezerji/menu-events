"""``POST /commands``: the write side's rules, answered over the wire."""

from __future__ import annotations

import uuid

import pytest

pytestmark = pytest.mark.api


def test_an_accepted_command_returns_the_version_the_log_reached(client, bodies, menu_id):
    fries = uuid.uuid4()

    response = client.post("/commands", json=bodies.add(fries, 0))

    assert response.status_code == 200
    body = response.json()
    assert (body["menu_id"], body["menu_version"], body["applied"]) == (str(menu_id), 1, True)
    assert uuid.UUID(body["event_id"])


def test_the_event_id_a_write_returns_is_the_row_the_log_holds(client, bodies):
    fries, command_id = uuid.uuid4(), uuid.uuid4()

    written = client.post("/commands", json=bodies.add(fries, 0, command_id=command_id)).json()
    stored = client.get(f"/menu/{bodies.menu_id}/events").json()

    assert [event["event_id"] for event in stored] == [written["event_id"]]
    assert stored[0]["command_id"] == str(command_id)


def test_the_version_a_read_returns_is_the_one_the_next_write_claims(client, bodies):
    """The optimistic token has to survive the round trip through HTTP: a client
    that read the menu must be able to write against what it saw.
    """
    fries = uuid.uuid4()
    client.post("/commands", json=bodies.add(fries, 0))

    head = client.get(f"/menu/{bodies.menu_id}").json()
    response = client.post("/commands", json=bodies.price(fries, head["version"], 1150))

    assert response.status_code == 200
    assert response.json()["menu_version"] == head["version"] + 1


def test_a_stale_write_loses_over_http_and_writes_nothing(client, bodies):
    fries, bread = uuid.uuid4(), uuid.uuid4()
    client.post("/commands", json=bodies.add(fries, 0))
    client.post("/commands", json=bodies.add(bread, 1))

    response = client.post("/commands", json=bodies.price(fries, 1, 1000))

    assert response.status_code == 409
    assert response.json() == {
        "reason": f"stream {bodies.stream_id} is at version 2, command expected 1",
        "stream_id": bodies.stream_id,
        "expected": 1,
        "actual": 2,
    }
    assert client.get(f"/menu/{bodies.menu_id}").json()["version"] == 2
    assert len(client.get(f"/menu/{bodies.menu_id}/events").json()) == 2


def test_a_writer_that_rereads_the_conflict_gets_in_next(client, bodies):
    """409 carries the version to write against, which is the whole point of the
    body shape: the caller does not have to guess how late it was.
    """
    fries, bread = uuid.uuid4(), uuid.uuid4()
    client.post("/commands", json=bodies.add(fries, 0))

    conflict = client.post("/commands", json=bodies.add(bread, 0)).json()
    response = client.post("/commands", json=bodies.add(bread, conflict["actual"]))

    assert (response.status_code, response.json()["menu_version"]) == (200, 2)
    assert response.json()["applied"] is True


def test_a_retried_command_id_is_the_original_write_and_nothing_more(client, bodies):
    """``command_id`` is the idempotency key, so a client that timed out and sent
    again is told what landed rather than being refused or, worse, doubled.
    """
    fries = uuid.uuid4()
    body = bodies.add(fries, 0)
    first = client.post("/commands", json=body).json()

    again = client.post("/commands", json=body)

    assert again.status_code == 200
    assert again.json() == {**first, "applied": False}
    assert len(client.get(f"/menu/{bodies.menu_id}/events").json()) == 1


def test_a_second_app_starts_from_an_empty_log(client, client_for, create_app, bodies):
    """Two apps built by the factory hold two logs. A module-level app would
    share one, and a test could pass by reading another test's writes.
    """
    client.post("/commands", json=bodies.add(uuid.uuid4(), 0))

    other = client_for(create_app())

    assert other.get(f"/menu/{bodies.menu_id}").status_code == 404
    assert client.get(f"/menu/{bodies.menu_id}").status_code == 200
