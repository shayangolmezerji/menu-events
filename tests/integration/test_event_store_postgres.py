"""The event log's SQL, against a server that has the table and its trigger.

The advisory lock, the head read, the insert and the idempotency lookup are one
transaction each: an argument count that does not match its statement's
placeholders is an error here and invisible to a unit test, which never sends
SQL. So is the append-only rule, because it lives in a trigger.
"""

from __future__ import annotations

import uuid

import psycopg
import pytest
from conftest import DSN, add_item, change_price, requires_postgres
from psycopg.rows import dict_row

from menu_events import ConcurrencyConflict
from menu_events.store.serialization import row_matches_checksum

pytestmark = [requires_postgres, pytest.mark.integration]


def test_the_first_append_to_a_stream_writes_one_row(handler, store, stream_id, menu_id):
    item = uuid.uuid4()
    command = add_item(menu_id, item)

    result = handler.handle(command)

    assert (result.menu_version, result.applied) == (1, True)
    [envelope] = store.read(stream_id)
    assert (envelope.event_id, envelope.command_id) == (result.event_id, command.command_id)
    assert envelope.version == 1
    # The insert names no recorded_at: now() fills the column, so the timestamp a
    # reader gets back was assigned by the server and not echoed by the writer.
    assert envelope.recorded_at.tzinfo is not None


def test_the_stored_rows_verify_against_the_checksum_written_with_them(
    handler, stream_id, menu_id
):
    """``payload`` and ``checksum`` are two arguments to one insert, taken from
    the same event. A raw read is the only way to see they were paired, and the
    only way to see jsonb came back as a mapping the way ``from_row`` assumes.
    """
    item = uuid.uuid4()
    handler.handle(add_item(menu_id, item))
    handler.handle(change_price(menu_id, item, expected_version=1))

    with psycopg.connect(DSN, row_factory=dict_row) as conn:
        rows = conn.execute(
            "SELECT * FROM events WHERE stream_id = %s ORDER BY version", (stream_id,)
        ).fetchall()

    assert [row["event_type"] for row in rows] == ["menu_item_added", "price_changed"]
    assert [row["version"] for row in rows] == [1, 2]
    assert [row["payload"]["event_type"] for row in rows] == [row["event_type"] for row in rows]
    assert all(isinstance(row["payload"], dict) for row in rows)
    assert all(row_matches_checksum(row) for row in rows)


def test_a_reader_gets_everything_above_the_version_it_named(handler, store, stream_id, menu_id):
    item = uuid.uuid4()
    handler.handle(add_item(menu_id, item))
    handler.handle(change_price(menu_id, item, expected_version=1))
    handler.handle(change_price(menu_id, item, expected_version=2, price_cents=1000))

    caught_up = store.read(stream_id, from_version=2)

    assert [envelope.version for envelope in caught_up] == [3]
    assert caught_up[0].event.price_cents == 1000


def test_a_writer_claiming_a_version_it_never_read_writes_nothing(
    handler, store, stream_id, menu_id
):
    item = uuid.uuid4()
    handler.handle(add_item(menu_id, item))

    with pytest.raises(ConcurrencyConflict) as excinfo:
        store.append(
            stream_id=stream_id,
            expected_version=7,
            event=change_price(menu_id, item, expected_version=7).to_event(),
            command_id=uuid.uuid4(),
        )

    assert (excinfo.value.expected, excinfo.value.actual) == (7, 1)
    assert store.read(stream_id)[0].version == 1


def test_a_landed_command_id_is_found_before_the_version_is_checked(store, stream_id, menu_id):
    """The retry of a write carries the version from before that write, so the
    duplicate lookup has to answer first. Reorder the two statements and this
    turns a timed-out response into a refused command. The handler short-circuits
    a retry through ``find_by_command``, so this ordering is the adapter's own
    promise and nothing else checks it.
    """
    command = add_item(menu_id, uuid.uuid4())

    first = store.append(
        stream_id=stream_id,
        expected_version=0,
        event=command.to_event(),
        command_id=command.command_id,
    )
    again = store.append(
        stream_id=stream_id,
        expected_version=0,
        event=command.to_event(),
        command_id=command.command_id,
    )

    assert (again.duplicated, again.version, again.event_id) == (
        True,
        first.version,
        first.event_id,
    )
    assert len(store.read(stream_id)) == 1


def test_the_log_refuses_to_be_edited_or_emptied(handler, store, stream_id, menu_id):
    """The trigger, not the grants: it fires for the table owner and for a
    superuser, which is the reason 0001 calls it the load-bearing half. Both
    statements name it in the error, so a tier that only had the grants to lean
    on would fail here for the right reason.
    """
    handler.handle(add_item(menu_id, uuid.uuid4()))

    with psycopg.connect(DSN, autocommit=True) as conn:
        with pytest.raises(psycopg.errors.InsufficientPrivilege, match="append-only"):
            conn.execute(
                "UPDATE events SET payload = payload WHERE stream_id = %s", (stream_id,)
            )
        with pytest.raises(psycopg.errors.InsufficientPrivilege, match="append-only"):
            conn.execute("DELETE FROM events WHERE stream_id = %s", (stream_id,))

    [envelope] = store.read(stream_id)
    assert envelope.version == 1
