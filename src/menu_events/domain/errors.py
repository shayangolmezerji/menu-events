"""Rejections raised by the write side.

They split into two kinds. The state errors mean the command made no sense
against the menu as it was read. The concurrency error means it did make sense
but someone else wrote first, and it carries the version the caller needs to
re-read against before retrying.
"""

from __future__ import annotations

import uuid

__all__ = [
    "CommandRejected",
    "ConcurrencyConflict",
    "ItemAlreadyOnMenu",
    "ItemAlreadySoldOut",
    "ItemNotOnMenu",
    "ItemNotSoldOut",
    "MenuStreamNotFound",
]


class CommandRejected(Exception):
    """Base for every rejected command."""


class ConcurrencyConflict(CommandRejected):
    """The stream moved between the read and the write.

    ``actual`` is the version the log holds now, so a caller can refetch and
    decide whether its intent still applies instead of retrying blind.
    """

    def __init__(self, stream_id: str, expected: int, actual: int) -> None:
        super().__init__(
            f"stream {stream_id} is at version {actual}, command expected {expected}"
        )
        self.stream_id = stream_id
        self.expected = expected
        self.actual = actual


class MenuStreamNotFound(CommandRejected):
    """No events at all for that menu, so there is nothing to change."""

    def __init__(self, stream_id: str) -> None:
        super().__init__(f"stream {stream_id} has no events")
        self.stream_id = stream_id


class ItemNotOnMenu(CommandRejected):
    def __init__(self, item_id: uuid.UUID) -> None:
        super().__init__(f"item {item_id} is not on the menu")
        self.item_id = item_id


class ItemAlreadyOnMenu(CommandRejected):
    def __init__(self, item_id: uuid.UUID) -> None:
        super().__init__(f"item {item_id} is already on the menu")
        self.item_id = item_id


class ItemAlreadySoldOut(CommandRejected):
    def __init__(self, item_id: uuid.UUID) -> None:
        super().__init__(f"item {item_id} is already sold out")
        self.item_id = item_id


class ItemNotSoldOut(CommandRejected):
    def __init__(self, item_id: uuid.UUID) -> None:
        super().__init__(f"item {item_id} is not sold out")
        self.item_id = item_id
