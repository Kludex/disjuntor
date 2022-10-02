from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import rich.repr


class Storage:
    def increment_success_counter(self, name: str) -> int:
        raise NotImplementedError()

    def increment_failure_counter(self, name: str) -> int:
        raise NotImplementedError()

    def start_timeout_timer(self, name: str) -> None:
        raise NotImplementedError()

    def failure_counter(self, name: str) -> int:
        raise NotImplementedError()

    def success_counter(self, name: str) -> int:
        raise NotImplementedError()

    def timer(self, name: str) -> datetime | None:
        raise NotImplementedError()


class MemoryStorage(Storage):
    def __init__(self) -> None:
        self._failure_counter: defaultdict[str, int] = defaultdict(int)
        self._success_counter: defaultdict[str, int] = defaultdict(int)
        self._start_time: dict[str, datetime] = {}

    def increment_failure_counter(self, name: str) -> int:
        self._failure_counter[name] += 1
        return self._failure_counter[name]

    def increment_success_counter(self, name: str) -> int:
        self._success_counter[name] += 1
        return self._success_counter[name]

    def start_timeout_timer(self, name: str) -> None:
        self._start_time[name] = datetime.now()

    def failure_counter(self, name: str) -> int:
        return self._failure_counter[name]

    def success_counter(self, name: str) -> int:
        return self._success_counter[name]

    def timer(self, name: str) -> datetime | None:
        if name not in self._start_time:
            return None
        return self._start_time[name]

    def __rich_repr__(self) -> rich.repr.Result:
        yield self.__class__.__name__
        yield "failure_counter", self._failure_counter
        yield "success_counter", self._success_counter
        yield "start_time", self._start_time
