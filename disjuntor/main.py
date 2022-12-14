from __future__ import annotations

import asyncio
from abc import abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from types import TracebackType
from typing import Awaitable, Callable, ParamSpec, Type, TypeVar, overload

from disjuntor.storage import MemoryStorage, Storage

P = ParamSpec("P")
T = TypeVar("T")


class State(str, Enum):
    OPEN = "open"
    CLOSED = "closed"
    HALF_OPEN = "half-open"


class CircuitBreakerException(Exception):
    ...


class BaseState:
    state: State
    name: str
    storage: Storage
    timeout: timedelta
    threshold: int

    def __init_subclass__(cls, state: State) -> None:
        cls.state = state
        return super().__init_subclass__()

    def __init__(
        self, name: str, storage: Storage, timeout: timedelta, threshold: int
    ) -> None:
        raise NotImplementedError()

    def next_state(self) -> BaseState:
        raise NotImplementedError()

    def success(self) -> None:
        raise NotImplementedError()

    @abstractmethod
    def failure(self) -> None:
        raise NotImplementedError()

    def is_open(self) -> bool:
        return self.state == State.OPEN

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(timeout={self.timeout}, threshold={self.threshold})"

    def __rich_repr__(self):
        yield self.__class__.__name__
        yield "name", self.name
        yield "storage", self.storage

    def __eq__(self, __o: object) -> bool:
        if isinstance(__o, State):
            return self.state == __o
        elif isinstance(__o, BaseState):
            return self.state == __o.state
        return False


class OpenState(BaseState, state=State.OPEN):
    def __init__(
        self, name: str, storage: Storage, timeout: timedelta, threshold: int
    ) -> None:
        self.name = name
        self.storage = storage
        self.timeout = timeout
        self.threshold = threshold
        self.storage.start_timeout_timer(name)

    def next_state(self) -> BaseState:
        timer = self.storage.timer(self.name)
        if timer and timer - datetime.now() > self.timeout:
            return self
        return HalfOpenState(
            name=self.name,
            storage=self.storage,
            timeout=self.timeout,
            threshold=self.threshold,
        )

    def success(self) -> None:
        ...

    def failure(self) -> None:
        ...


class ClosedState(BaseState, state=State.CLOSED):
    _failure_counter: int

    def __init__(
        self, name: str, storage: Storage, timeout: timedelta, threshold: int
    ) -> None:
        self.name = name
        self.storage = storage
        self.timeout = timeout
        self.threshold = threshold

    def next_state(self) -> BaseState:
        failure_counter = self.storage.failure_counter(self.name)
        if failure_counter >= self.threshold:
            return OpenState(
                name=self.name,
                storage=self.storage,
                timeout=self.timeout,
                threshold=self.threshold,
            )
        return self

    def success(self) -> None:
        ...

    def failure(self) -> None:
        self.storage.increment_failure_counter(self.name)


class HalfOpenState(BaseState, state=State.HALF_OPEN):
    def __init__(
        self, name: str, storage: Storage, timeout: timedelta, threshold: int
    ) -> None:
        self.name = name
        self.storage = storage
        self.timeout = timeout
        self.threshold = threshold

    def next_state(self) -> BaseState:
        if self.storage.success_counter(self.name) >= self.threshold:
            return ClosedState(
                name=self.name,
                storage=self.storage,
                timeout=self.timeout,
                threshold=self.threshold,
            )
        return self

    def success(self) -> None:
        self.storage.increment_success_counter(self.name)

    def failure(self) -> None:
        self = OpenState(
            name=self.name,
            storage=self.storage,
            timeout=self.timeout,
            threshold=self.threshold,
        )


def _get_state(
    state: State, name: str, storage: Storage, timeout: timedelta, threshold: int
) -> BaseState:
    for cls in BaseState.__subclasses__():
        if cls.state == state:
            return cls(name=name, storage=storage, timeout=timeout, threshold=threshold)


class CircuitBreaker:
    def __init__(
        self,
        *,
        name: str,
        storage: Storage,
        state: State = State.CLOSED,
        threshold: int = 5,
        timeout: timedelta = timedelta(seconds=60),
    ) -> None:
        self.storage = storage
        self.state = _get_state(
            name=name,
            state=state,
            storage=storage,
            timeout=timeout,
            threshold=threshold,
        )

    @overload
    def __call__(self, func: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]:
        ...

    @overload
    def __call__(self, func: Callable[P, T]) -> Callable[P, T]:
        ...

    def __call__(
        self, func: Callable[P, T | Awaitable[T]]
    ) -> Callable[P, T | Awaitable[T]]:
        self.state = self.state.next_state()
        if self.state.is_open():
            raise CircuitBreakerException()

        if asyncio.iscoroutine(func):

            async def async_decorator(*args: P.args, **kwargs: P.kwargs) -> T:
                try:
                    result = await func(*args, **kwargs)
                    self.state.success()
                except Exception:
                    self.state.failure()
                    raise
                return result

            return async_decorator
        else:

            def decorator(*args: P.args, **kwargs: P.kwargs) -> T:
                try:
                    result = func(*args, **kwargs)
                    self.state.success()
                except Exception:
                    self.state.failure()
                    raise
                return result

            return decorator

    def __enter__(self):
        self.state = self.state.next_state()
        if self.state.is_open():
            raise CircuitBreakerException()
        return self

    def __exit__(
        self,
        exc_type: Type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        if exc is None:
            self.state.success()
        else:
            self.state.failure()

    async def __aenter__(self):
        self.state = self.state.next_state()
        if self.state.is_open():
            raise CircuitBreakerException()
        return self

    async def __aexit__(
        self,
        exc_type: Type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        if exc is None:
            self.state.success()
        else:
            self.state.failure()


@dataclass
class CircuitBreakerFactory:
    storage: Storage = MemoryStorage()
    state: State = State.CLOSED
    threshold: int = 5
    timeout: timedelta = timedelta(seconds=60)

    def __call__(self, name: str) -> CircuitBreaker:
        return CircuitBreaker(
            name=name,
            storage=self.storage,
            state=self.state,
            threshold=self.threshold,
            timeout=self.timeout,
        )


if __name__ == "__main__":
    from rich.console import Console

    cb_factory = CircuitBreakerFactory()
    circuit_breaker = cb_factory("foo")
    console = Console()

    for i in range(10):
        try:
            with circuit_breaker:
                console.print(circuit_breaker.state)
                print("success")
                raise Exception("inside circuit breaker")
        except CircuitBreakerException:
            print("failure")
        except Exception as exc:
            ...
