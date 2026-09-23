"""Two writers on one projection, against a server that holds the lock.

``projections/postgres.py`` argues that both writers take one advisory lock per
stream and read the log only once they hold it, and that this ordering is what
makes them safe to run at the same time as each other. No other test here checks
that: the only other concurrent writers in the tier race commands on the log, and
that lock is keyed on a different value, so the projection lock is taken and
released by a single transaction here and never waited on behind a second.

Measured here on PostgreSQL 16.15.

What is asserted survives any interleaving on purpose. Which writer goes first is
not a promise the adapter makes, so an assertion about it would be a coin flip:
the two are only required to serialise, and the serialised end state is the one a
fold of the log produces. ``menu_events.project`` is the oracle, as it is
everywhere else in this tier. A run in which the threads miss each other entirely
passes for the same reason a run in which they genuinely collide does, which is
what makes this a check of the lock's consequence rather than of the scheduler's
mood.
"""

from __future__ import annotations

import threading
import uuid
from collections.abc import Callable

import psycopg
import pytest
from conftest import DSN, add_item, change_price, requires_postgres
from psycopg.rows import dict_row

from menu_events import project
from menu_events.projections.postgres import PostgresMenuReadModel
from menu_events.store.postgres import PostgresEventStore

pytestmark = [requires_postgres, pytest.mark.integration]

# Two writers released together, once per round, over a stream that gains an
# event between the rounds. Enough rounds that a server which interleaves the two
# only sometimes has to do it to be caught, few enough that the whole file stays
# two seconds: a race is not something a test can force, only repeat at.
ROUNDS = 8


@pytest.fixture
def read_model(store: PostgresEventStore) -> PostgresMenuReadModel:
    return PostgresMenuReadModel(DSN, store)


def race(calls: list[Callable[[], int]]) -> list[object]:
    """Hand every callable to its own thread and release them from one barrier.

    Returns each thread's value, or the exception it raised, in the order the
    callables were given. A writer that dies is a result like any other, so the
    assertion reading these is the one that reports it: an exception escaping a
    thread would otherwise surface as nothing at all.
    """
    gate = threading.Barrier(len(calls))
    outcomes: list[object] = [None] * len(calls)

    def run(index: int, call: Callable[[], int]) -> None:
        gate.wait()
        try:
            outcomes[index] = call()
        except Exception as exc:
            outcomes[index] = exc

    threads = [threading.Thread(target=run, args=(i, call)) for i, call in enumerate(calls)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return outcomes


def checkpoint_of(stream_id: str) -> int:
    """The projector's position, read from the column rather than through the
    adapter, so that the claim about it is about a row the server wrote.
    """
    with psycopg.connect(DSN, row_factory=dict_row) as conn:
        row = conn.execute(
            "SELECT version FROM menu_projection WHERE stream_id = %s", (stream_id,)
        ).fetchone()
    return 0 if row is None else int(row["version"])


def head_of_log(stream_id: str) -> int:
    """The highest version the log holds, from the table and not from a writer's
    return value, so the two can be compared rather than assumed equal.
    """
    with psycopg.connect(DSN, row_factory=dict_row) as conn:
        row = conn.execute(
            "SELECT COALESCE(max(version), 0)::bigint AS head FROM events WHERE stream_id = %s",
            (stream_id,),
        ).fetchone()
    return int(row["head"])


def projected_rows(stream_id: str) -> tuple[int, int]:
    """``(rows, distinct items)`` as the table has them.

    ``menu_item``'s primary key makes the two equal, so this is the no-duplicate
    rule stated against the table rather than against the dict ``state()`` builds,
    which would silently collapse a second row for one item into the first.
    """
    with psycopg.connect(DSN, row_factory=dict_row) as conn:
        row = conn.execute(
            "SELECT count(*)::bigint AS rows, count(DISTINCT item_id)::bigint AS items"
            " FROM menu_item WHERE stream_id = %s",
            (stream_id,),
        ).fetchone()
    return int(row["rows"]), int(row["items"])


def test_two_advancing_writers_leave_the_projection_at_the_head_of_the_log(
    read_model, handler, store, stream_id, menu_id
):
    """``advance`` called from two threads on one stream at the same time.

    Both have to come back with the log's head rather than refuse, and neither may
    publish a snapshot older than the one already stored: the second to hold the
    lock finds the checkpoint the first committed and folds nothing. The first
    round starts with no checkpoint row at all, which is where a lock that did not
    hold would leave two writers racing to insert the same parent row and the same
    three items behind it.
    """
    item = uuid.uuid4()
    handler.handle(add_item(menu_id, item))
    handler.handle(add_item(menu_id, uuid.uuid4(), expected_version=1))
    handler.handle(change_price(menu_id, item, expected_version=2))
    handler.handle(add_item(menu_id, uuid.uuid4(), expected_version=3))
    handler.handle(change_price(menu_id, item, expected_version=4))

    for round_index in range(ROUNDS):
        if round_index:
            handler.handle(change_price(menu_id, item, expected_version=head_of_log(stream_id)))

        head = head_of_log(stream_id)
        outcomes = race(
            [
                lambda: read_model.advance(stream_id),
                lambda: read_model.advance(stream_id),
            ]
        )

        assert [o for o in outcomes if isinstance(o, BaseException)] == []
        assert outcomes == [head, head]
        assert checkpoint_of(stream_id) == head == head_of_log(stream_id)
        assert projected_rows(stream_id) == (3, 3)
        assert read_model.state(stream_id) == project(stream_id, store.read(stream_id))


def test_advance_and_rebuild_raced_leave_what_either_alone_would(
    read_model, handler, store, stream_id, menu_id
):
    """The other half of that argument: the two writers share one lock namespace,
    so a catch-up and a full rebuild cannot interleave their row writes.

    Which holds the lock first decides only who finds work to do. A rebuild that
    goes first clears the table and refolds it, and the advance behind it reads
    from the checkpoint that rebuild published and writes nothing; the order
    reversed, the advance catches up and the rebuild then discards and reproduces
    exactly what it wrote. Both orders end on the same rows, which is the only
    thing about it that a test can state without naming a winner.
    """
    item = uuid.uuid4()
    handler.handle(add_item(menu_id, item))
    handler.handle(add_item(menu_id, uuid.uuid4(), expected_version=1))
    handler.handle(change_price(menu_id, item, expected_version=2))

    for round_index in range(ROUNDS):
        if round_index:
            handler.handle(change_price(menu_id, item, expected_version=head_of_log(stream_id)))

        head = head_of_log(stream_id)
        outcomes = race(
            [
                lambda: read_model.advance(stream_id),
                lambda: read_model.rebuild(stream_id),
            ]
        )

        assert [o for o in outcomes if isinstance(o, BaseException)] == []
        assert outcomes == [head, head]
        assert checkpoint_of(stream_id) == head == head_of_log(stream_id)
        assert projected_rows(stream_id) == (2, 2)
        assert read_model.state(stream_id) == project(stream_id, store.read(stream_id))
