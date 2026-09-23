"""Two writers at one version, against a server that holds the lock.

``store/postgres.py`` argues that an append takes a per-stream advisory
transaction lock, reads the head while holding it, and refuses a command that
claims any other version. No other test here checks that: the other four files in
this tier write one command at a time, so the lock is taken and released by a
single transaction and the head read has never been stale underneath a second
writer. The unit tier races the same rule, but only against
``InMemoryEventStore``, where a ``threading.Lock`` plays the lock's part.

Measured here on PostgreSQL 16.15.

Which writer reaches the lock first is not a promise the adapter makes, so no
assertion names one: each round states that exactly one command wrote a row, that
the versions run to one past where both writers aimed, and that the refused
command left nothing in the table. A round in which the two threads miss each
other entirely passes for the same reason one in which they genuinely collide
does, which is what makes this a check of the rule's consequence rather than of
the scheduler's mood.

What each guard is doing was settled by taking it out. Remove the version check
and both writers land, at two consecutive versions the primary key on
``(stream_id, version)`` finds nothing wrong with, in six patched runs of six:
that is the state the key cannot backstop. Remove only the advisory lock and the
two-writer race stays green, because the loser is refused either way, by the head
it reads after the winner committed or by that key, and no assertion here can tell
the two apart. The retried command is the one that needs the lock. Without it the
second copy's duplicate lookup can run before the first copy commits, so the
retry reads a head that has moved and is refused as stale, which is the exact
failure ``base.py`` says the lookup exists to prevent: red in eighteen of the
twenty-six runs made that way.

That the two writers really queue rather than merely alternate was checked out of
band, against ``pg_locks``, on the same server: with a poller watching for an
ungranted advisory lock, one was seen in 60 of 60 rounds of the two-writer race.
No assertion here depends on it. Nor does any round put a command's lock in a
projector's way: the two namespaces are still untested against each other.
"""

from __future__ import annotations

import threading
import uuid
from collections.abc import Callable
from functools import partial

import psycopg
import pytest
from conftest import DSN, add_item, change_price, requires_postgres
from psycopg.rows import dict_row

from menu_events import AppendResult, CommandResult, ConcurrencyConflict
from menu_events.domain.commands import Command
from menu_events.store.postgres import PostgresEventStore

pytestmark = [requires_postgres, pytest.mark.integration]

# Two writers released together, once per round, over a stream that gains an
# event between the rounds. Enough rounds that a server which interleaves the two
# only sometimes has to do it to be caught, few enough that the whole file stays
# a couple of seconds: a race is not something a test can force, only repeat at.
ROUNDS = 8


def append_command(
    store: PostgresEventStore, stream_id: str, command: Command
) -> AppendResult:
    """Write one command to the log the way the handler writes it.

    ``store.append`` is the call the concurrency argument is about, so the
    command layer is used for its event and its id and left out of the race
    itself.
    """
    return store.append(
        stream_id=stream_id,
        expected_version=command.expected_menu_version,
        event=command.to_event(),
        command_id=command.command_id,
    )


def race(calls: list[Callable[[], object]]) -> list[object]:
    """Hand every callable to its own thread and release them from one barrier.

    Returns each thread's value, or the exception it raised, in the order the
    callables were given. A writer that dies is a result like any other, so the
    assertion reading these is the one that reports it: an exception escaping a
    thread would otherwise surface as nothing at all.
    """
    gate = threading.Barrier(len(calls))
    outcomes: list[object] = [None] * len(calls)

    def run(index: int, call: Callable[[], object]) -> None:
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


def logged_rows(stream_id: str) -> list[tuple[int, uuid.UUID, uuid.UUID]]:
    """``(version, command_id, event_id)`` for every row the stream holds.

    Read from the table rather than from a writer's return value, so that the
    claim the rounds make is about a row the server wrote and about the absence
    of the one it refused to write.
    """
    with psycopg.connect(DSN, row_factory=dict_row) as conn:
        rows = conn.execute(
            "SELECT version, command_id, event_id FROM events WHERE stream_id = %s"
            " ORDER BY version",
            (stream_id,),
        ).fetchall()
    return [(int(row["version"]), row["command_id"], row["event_id"]) for row in rows]


def test_two_writers_at_one_version_cannot_both_land(store, handler, stream_id, menu_id):
    """``store.append`` called from two threads at the version both of them read.

    This is the shape two terminals on one menu produce: each claims the head as
    of its own read, and the log has to decide between them. The loser is refused
    with the version the winner took, and the versions in the table stay one
    contiguous run, which is the no-hole rule ``handlers.py`` states as the
    reason a rejected command must write nothing.
    """
    item = uuid.uuid4()
    handler.handle(add_item(menu_id, item))

    for round_index in range(ROUNDS):
        if round_index:
            handler.handle(change_price(menu_id, item, expected_version=head_of_log(stream_id)))

        head = head_of_log(stream_id)
        first = change_price(menu_id, item, expected_version=head, price_cents=1000)
        second = change_price(menu_id, item, expected_version=head, price_cents=2000)
        outcomes = race(
            [
                partial(append_command, store, stream_id, first),
                partial(append_command, store, stream_id, second),
            ]
        )

        winners = [
            (command, outcome)
            for command, outcome in zip((first, second), outcomes, strict=True)
            if isinstance(outcome, AppendResult)
        ]
        refused = [outcome for outcome in outcomes if isinstance(outcome, ConcurrencyConflict)]
        assert len(winners) == 1, outcomes
        assert len(refused) == 1, outcomes

        landed, result = winners[0]
        loser = second if landed is first else first
        rows = logged_rows(stream_id)
        assert [version for version, _, _ in rows] == list(range(1, head + 2))
        assert len(rows) == head + 1
        assert rows[-1] == (head + 1, landed.command_id, result.event_id)
        assert loser.command_id not in [command_id for _, command_id, _ in rows]
        assert (refused[0].expected, refused[0].actual) == (head, head + 1)


def test_a_retried_command_raced_against_itself_lands_once(store, handler, stream_id, menu_id):
    """The same ``command_id`` written by two threads at the same version.

    A client that timed out and resent is the case ``base.py`` promises an answer
    for rather than a refusal, so the second copy has to come back
    ``duplicated`` with the version and the ``event_id`` the first wrote, and the
    table has to hold one row for that id. Which thread is the retry is not
    decidable from here, so both answers are only required to disagree about
    ``duplicated`` and agree about everything else.
    """
    item = uuid.uuid4()
    handler.handle(add_item(menu_id, item))

    for round_index in range(ROUNDS):
        if round_index:
            handler.handle(change_price(menu_id, item, expected_version=head_of_log(stream_id)))

        head = head_of_log(stream_id)
        command = change_price(menu_id, item, expected_version=head)
        retry = partial(append_command, store, stream_id, command)
        outcomes = race([retry, retry])

        answers = [outcome for outcome in outcomes if isinstance(outcome, AppendResult)]
        assert len(answers) == 2, outcomes
        assert [answer.duplicated for answer in answers] in ([False, True], [True, False])
        assert answers[0].version == answers[1].version == head + 1
        assert answers[0].event_id == answers[1].event_id

        rows = logged_rows(stream_id)
        assert [command_id for _, command_id, _ in rows].count(command.command_id) == 1
        assert len(rows) == head + 1
        assert rows[-1] == (head + 1, command.command_id, answers[0].event_id)


def test_two_commands_raced_through_the_handler_write_one_event(
    handler, store, stream_id, menu_id
):
    """The whole write path from two callers at once, not just the append.

    The handler folds the stream and validates outside the lock, so this is the
    version of the race a kitchen terminal runs: two ``AddMenuItem`` commands for
    two different items, both claiming the same version, both valid against the
    state each of them read. Only the append can notice the other one, and the
    loser has to leave the log the length the winner's answer reports.
    """
    item = uuid.uuid4()
    handler.handle(add_item(menu_id, item))

    for round_index in range(ROUNDS):
        if round_index:
            handler.handle(change_price(menu_id, item, expected_version=head_of_log(stream_id)))

        head = head_of_log(stream_id)
        first = add_item(menu_id, uuid.uuid4(), expected_version=head)
        second = add_item(menu_id, uuid.uuid4(), expected_version=head)
        outcomes = race(
            [
                partial(handler.handle, first),
                partial(handler.handle, second),
            ]
        )

        winners = [
            (command, outcome)
            for command, outcome in zip((first, second), outcomes, strict=True)
            if isinstance(outcome, CommandResult)
        ]
        refused = [outcome for outcome in outcomes if isinstance(outcome, ConcurrencyConflict)]
        assert len(winners) == 1, outcomes
        assert len(refused) == 1, outcomes

        landed, result = winners[0]
        loser = second if landed is first else first
        assert (result.menu_version, result.applied) == (head + 1, True)
        rows = logged_rows(stream_id)
        assert [version for version, _, _ in rows] == list(range(1, head + 2))
        assert rows[-1] == (head + 1, landed.command_id, result.event_id)
        assert loser.command_id not in [command_id for _, command_id, _ in rows]
        assert (refused[0].expected, refused[0].actual) == (head, head + 1)
