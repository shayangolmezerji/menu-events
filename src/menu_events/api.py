"""The HTTP front door: the same two rules the library has, over a socket.

Why a factory and not a module-level ``app``: the store is the dependency the
routes exist to serve, and an app built at import time can only ever be wired to
one adapter. ``create_app()`` defaults to :class:`InMemoryEventStore`, so
``tests/api/`` exercises every route with no database in sight, and a deployment
hands it the adapter it actually runs on. The store and the handler hang off
``app.state`` for the same reason: two apps in one process must not end up
sharing one log.

The endpoint functions are synchronous on purpose. ``psycopg`` blocks, and a
coroutine that called it would hold the event loop open for every other request.
FastAPI runs a ``def`` endpoint on the threadpool, which is what a blocking
adapter wants.

Nothing here is reachable from ``import menu_events``, so the web framework
stays an optional extra (``pip install -e ".[api]"``) exactly as the database
driver stays out of the portable core.

Unlike the two PostgreSQL adapters, this module has been executed: every request
and response quoted in README.md came from a live uvicorn process on a localhost
port, and each status code below is asserted through ``fastapi.testclient``. The
one path that has not been run is ``create_app(PostgresEventStore(dsn))``,
because no server was reachable here.
"""

from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, FastAPI, Path, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, BeforeValidator, RootModel

from . import __version__
from .domain.commands import (
    AddMenuItem,
    ChangePrice,
    Command,
    EditDescription,
    MarkSoldOut,
    PutBackInStock,
)
from .domain.errors import CommandRejected, ConcurrencyConflict, MenuStreamNotFound
from .handlers import CommandResult, MenuCommandHandler
from .projections.menu import MenuItem, project
from .store.base import EventStore
from .store.memory import InMemoryEventStore
from .store.serialization import EventEnvelope, menu_stream_id

__all__ = ["CommandRequest", "MenuReadModel", "create_app"]

_COMMAND_TYPES: tuple[type[Command], ...] = (
    AddMenuItem,
    ChangePrice,
    EditDescription,
    MarkSoldOut,
    PutBackInStock,
)

# The tag on the wire is the command's own ``event_type``, the name it already
# uses in the log, so a client reads and writes one vocabulary. It has to be a
# tag rather than a union pydantic infers: MarkSoldOut and PutBackInStock carry
# no fields beyond the base, so each other's bodies validate against them.
_COMMAND_BY_EVENT_TYPE: dict[str, type[Command]] = {
    command_type.event_type: command_type for command_type in _COMMAND_TYPES
}

_KNOWN_EVENT_TYPES = ", ".join(sorted(_COMMAND_BY_EVENT_TYPE))


def _parse_command(value: object) -> Command:
    """Turn a request body into the command class its tag names.

    The domain models are the only definition of what a command is, so this
    strips the tag and hands the rest over untouched. Their ``extra="forbid"``
    carries through: an unknown field is a 422 rather than a typo that silently
    took a default.
    """
    if not isinstance(value, dict):
        raise ValueError("a command body must be a JSON object")
    fields = dict(value)
    event_type = fields.pop("event_type", None)
    command_type = _COMMAND_BY_EVENT_TYPE.get(event_type) if isinstance(event_type, str) else None
    if command_type is None:
        raise ValueError(
            f"event_type {event_type!r} is not a command, expected one of: {_KNOWN_EVENT_TYPES}"
        )
    return command_type.model_validate(fields)


class CommandRequest(RootModel[Command]):
    """The request body, unwrapped into the command class its tag names.

    A root model rather than a bare annotated ``Command`` parameter: FastAPI
    rebuilds a body parameter's field from the model alone and drops a
    ``BeforeValidator`` hanging off the annotation, which leaves the base
    ``Command`` answering the request and every command-specific field reported
    as an extra. Inside a root model's own field the validator survives, and a
    body pydantic dislikes still comes back as one 422 per field.
    """

    root: Annotated[Command, BeforeValidator(_parse_command)]


class MenuReadModel(BaseModel):
    """The menu folded from the head of the log, with the version to write against."""

    menu_id: uuid.UUID
    stream_id: str
    version: int
    items: list[MenuItem]


router = APIRouter()


@router.post("/commands", response_model=CommandResult)
def submit_command(request: Request, body: CommandRequest) -> CommandResult:
    """Append one command, or refuse it. The answer is the log's version either way.

    A 200 with ``applied`` false is not an error: the ``command_id`` was already
    on the stream, so this is the result of the write that landed, and retrying
    changes nothing.
    """
    handler: MenuCommandHandler = request.app.state.handler
    return handler.handle(body.root)


@router.get("/menu/{menu_id}", response_model=MenuReadModel)
def read_menu(
    request: Request,
    menu_id: Annotated[uuid.UUID, Path(description="The menu whose stream to fold.")],
) -> MenuReadModel:
    """The current menu: the whole log folded, never a cached copy of it.

    Folding to the head on every read is O(events) and always agrees with
    ``/events`` below. It is also why no cache tier sits in front of this: see
    ADR 0001.
    """
    store: EventStore = request.app.state.store
    stream_id = menu_stream_id(menu_id)
    envelopes = store.read(stream_id)
    if not envelopes:
        raise MenuStreamNotFound(stream_id)
    state = project(stream_id, envelopes)
    return MenuReadModel(
        menu_id=menu_id,
        stream_id=state.stream_id,
        version=state.version,
        items=state.on_menu,
    )


@router.get("/menu/{menu_id}/events", response_model=list[EventEnvelope])
def read_events(
    request: Request,
    menu_id: Annotated[uuid.UUID, Path(description="The menu whose log to read.")],
    from_version: Annotated[
        int,
        Query(ge=0, description="Lowest version to exclude; 0 returns the whole stream."),
    ] = 0,
) -> list[EventEnvelope]:
    """The log itself, oldest first, as stored.

    ``from_version`` is the port's own catch-up argument, so a projector that
    stopped at version 4 asks for 4 and gets what it has not seen. An empty
    window above a stream that exists is a 200 with no events; only a stream
    with no events at all is a 404.
    """
    store: EventStore = request.app.state.store
    stream_id = menu_stream_id(menu_id)
    if not store.read(stream_id):
        raise MenuStreamNotFound(stream_id)
    return list(store.read(stream_id, from_version=from_version))


async def _conflict(_: Request, exc: ConcurrencyConflict) -> JSONResponse:
    """The loser of a race, told which version it read and which the log holds."""
    return JSONResponse(
        status_code=409,
        content={
            "reason": str(exc),
            "stream_id": exc.stream_id,
            "expected": exc.expected,
            "actual": exc.actual,
        },
    )


async def _unknown_stream(_: Request, exc: MenuStreamNotFound) -> JSONResponse:
    return JSONResponse(status_code=404, content={"reason": str(exc)})


async def _rejected(_: Request, exc: CommandRejected) -> JSONResponse:
    return JSONResponse(status_code=400, content={"reason": str(exc)})


def create_app(store: EventStore | None = None) -> FastAPI:
    """An app serving one event store, with the handler built over the same object."""
    event_store = store if store is not None else InMemoryEventStore()
    app = FastAPI(
        title="menu-events",
        version=__version__,
        summary="Append-only menu event store with optimistic concurrency",
        description=(
            "A command names the menu version it was decided against, and the "
            "writer that lost the race is refused rather than allowed to overwrite."
        ),
    )
    app.state.store = event_store
    app.state.handler = MenuCommandHandler(event_store)
    app.include_router(router)
    app.add_exception_handler(ConcurrencyConflict, _conflict)
    app.add_exception_handler(MenuStreamNotFound, _unknown_stream)
    # ConcurrencyConflict and MenuStreamNotFound are CommandRejected subclasses,
    # and Starlette resolves a handler by walking the exception's MRO, so the two
    # above answer their own kinds first and everything the domain rejects for a
    # reason other than a race or a missing stream lands here.
    app.add_exception_handler(CommandRejected, _rejected)
    return app
