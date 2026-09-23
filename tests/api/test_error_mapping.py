"""Which status each refusal answers with, and whether it still says why.

The interesting half of the boundary: a refusal that arrives as a bare 400
saying "bad request" throws away the reason the domain worked out. Each case
here asserts the domain's own sentence came through.
"""

from __future__ import annotations

import uuid

import pytest

from menu_events.domain.commands import MAX_PRICE_CENTS

pytestmark = pytest.mark.api


def test_a_command_naming_an_item_that_was_never_added_is_refused(client, bodies):
    ghost = uuid.uuid4()
    client.post("/commands", json=bodies.add(uuid.uuid4(), 0))

    response = client.post("/commands", json=bodies.price(ghost, 1, 500))

    assert response.status_code == 400
    assert response.json() == {"reason": f"item {ghost} is not on the menu"}


def test_a_second_add_of_the_same_item_is_refused(client, bodies):
    fries = uuid.uuid4()
    client.post("/commands", json=bodies.add(fries, 0))

    response = client.post("/commands", json=bodies.add(fries, 1))

    assert response.status_code == 400
    assert response.json() == {"reason": f"item {fries} is already on the menu"}


def test_marking_an_already_sold_out_item_sold_out_is_refused(client, bodies):
    fries = uuid.uuid4()
    client.post("/commands", json=bodies.add(fries, 0))
    client.post("/commands", json=bodies.sold_out(fries, 1))

    response = client.post("/commands", json=bodies.sold_out(fries, 2))

    assert response.status_code == 400
    assert response.json() == {"reason": f"item {fries} is already sold out"}


def test_a_refused_command_wrote_nothing(client, bodies):
    """A refusal leaves the stream contiguous: the version a reader held before
    asking is the version it holds afterwards.
    """
    fries = uuid.uuid4()
    client.post("/commands", json=bodies.add(fries, 0))

    refused = client.post("/commands", json=bodies.price(uuid.uuid4(), 1, 500))

    assert refused.status_code == 400
    assert client.get(f"/menu/{bodies.menu_id}").json()["version"] == 1


def test_a_write_against_a_menu_that_does_not_exist_is_not_found(client, bodies):
    response = client.post("/commands", json=bodies.sold_out(uuid.uuid4(), 0))

    assert response.status_code == 404
    assert response.json() == {"reason": f"stream {bodies.stream_id} has no events"}


def test_reading_a_menu_that_does_not_exist_is_not_found(client, menu_id):
    response = client.get(f"/menu/{menu_id}")

    assert response.status_code == 404
    assert response.json() == {"reason": f"stream menu/{menu_id} has no events"}


def test_reading_the_log_of_a_menu_that_does_not_exist_is_not_found(client, menu_id):
    response = client.get(f"/menu/{menu_id}/events")

    assert response.status_code == 404
    assert response.json() == {"reason": f"stream menu/{menu_id} has no events"}


@pytest.mark.parametrize(
    ("field", "value", "why"),
    [
        ("event_type", "sold_a_thing", "is not a command"),
        ("price_cents", "ten", "should be a valid integer"),
        ("price_cents", MAX_PRICE_CENTS + 1, "less than or equal to"),
        ("expected_menu_version", -1, "greater than or equal to 0"),
        ("actor", "", "at least 1 character"),
        ("wrong_field", True, "Extra inputs are not permitted"),
    ],
)
def test_a_body_the_command_models_refuse_is_a_422(client, bodies, field, value, why):
    body = dict(bodies.add(uuid.uuid4(), 0))
    body[field] = value

    response = client.post("/commands", json=body)

    assert response.status_code == 422
    assert why in str(response.json())


def test_a_price_above_the_ceiling_writes_nothing(client, bodies):
    """One past the BIGINT maximum of ``menu_item.price_cents``, which is the
    value no layer used to refuse. The answer is a 422 and the log is untouched,
    because a price the domain refuses never reaches an adapter at all.
    """
    fries = uuid.uuid4()
    client.post("/commands", json=bodies.add(fries, 0))

    response = client.post("/commands", json=bodies.price(fries, 1, 2**63 + 1))

    assert response.status_code == 422
    assert "less than or equal to" in str(response.json())
    assert client.get(f"/menu/{bodies.menu_id}").json()["version"] == 1


def test_a_body_without_the_tag_is_a_422(client, bodies):
    body = {k: v for k, v in bodies.add(uuid.uuid4(), 0).items() if k != "event_type"}

    response = client.post("/commands", json=body)

    assert response.status_code == 422
    assert "is not a command" in str(response.json())


def test_a_body_missing_a_required_field_is_a_422(client, bodies):
    body = {k: v for k, v in bodies.add(uuid.uuid4(), 0).items() if k != "actor"}

    response = client.post("/commands", json=body)

    assert response.status_code == 422
    detail = str(response.json())
    assert "actor" in detail
    assert "Field required" in detail


def test_a_body_that_is_not_an_object_is_a_422(client, bodies):
    response = client.post("/commands", json=[bodies.add(uuid.uuid4(), 0)])

    assert response.status_code == 422
    assert "must be a JSON object" in str(response.json())


def test_a_body_that_is_not_json_is_a_422(client):
    response = client.post(
        "/commands",
        content=b'{"event_type": menu_item_added,}',
        headers={"content-type": "application/json"},
    )

    assert response.status_code == 422
    assert "json_invalid" in str(response.json())


def test_a_menu_id_that_is_not_a_uuid_is_a_422(client):
    response = client.get("/menu/not-a-menu-id")

    assert response.status_code == 422
    assert "uuid_parsing" in str(response.json())


def test_a_version_window_starting_below_zero_is_a_422(client, bodies):
    fries = uuid.uuid4()
    client.post("/commands", json=bodies.add(fries, 0))

    response = client.get(f"/menu/{bodies.menu_id}/events?from_version=-1")

    assert response.status_code == 422
    assert "greater than or equal to 0" in str(response.json())
