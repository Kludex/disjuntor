from datetime import timedelta
from typing import Awaitable, Callable

import pytest
from rich import print

from disjuntor import CircuitBreaker, CircuitBreakerFactory
from disjuntor.main import CircuitBreakerException, State
from disjuntor.storage import MemoryStorage


def success_context_manager(cb: CircuitBreaker):
    with cb:
        ...


async def async_success_context_manager(cb: CircuitBreaker):
    async with cb:
        ...


def failure_context_manager(cb: CircuitBreaker):
    class Potato(Exception):
        ...

    try:
        with cb:
            raise Potato("inside circuit breaker")
    except Potato:
        ...


async def async_failure_context_manager(cb: CircuitBreaker):
    class Potato(Exception):
        ...

    try:
        async with cb:
            raise Potato("inside circuit breaker")
    except Potato:
        ...


def success_decorator(cb: CircuitBreaker):
    @cb
    def foo():
        ...

    foo()


async def async_success_decorator(cb: CircuitBreaker):
    @cb
    async def foo():
        ...

    foo()


def failure_decorator(cb: CircuitBreaker):
    class Potato(Exception):
        ...

    @cb
    def foo():
        raise Potato("inside circuit breaker")

    try:
        foo()
    except Potato:
        ...


async def async_failure_decorator(cb: CircuitBreaker):
    class Potato(Exception):
        ...

    @cb
    async def foo():
        raise Potato("inside circuit breaker")

    try:
        await foo()
    except Potato:
        ...


@pytest.fixture(params=[success_context_manager, success_decorator])
def success(request):
    yield request.param


@pytest.fixture(params=[async_success_context_manager, async_success_decorator])
def async_success(request):
    yield request.param


@pytest.fixture(params=[failure_context_manager, failure_decorator])
def failure(request):
    yield request.param


@pytest.fixture(params=[async_failure_context_manager, async_failure_decorator])
def async_failure(request):
    yield request.param


def test_circuit_breaker_closed(success: Callable[[CircuitBreaker], None]):
    cb = CircuitBreaker(name="foo", storage=MemoryStorage())

    success(cb)

    assert cb.state == State.CLOSED
    assert cb.state.storage.failure_counter("foo") == 0
    assert cb.state.storage.success_counter("foo") == 0
    assert cb.state.storage.timer("foo") is None


def test_circuit_breaker_opened(failure: Callable[[CircuitBreaker], None]):
    threshold = 5
    cb = CircuitBreaker(name="foo", storage=MemoryStorage(), threshold=threshold)

    for _ in range(threshold):
        failure(cb)

    with pytest.raises(CircuitBreakerException):
        failure(cb)

    assert cb.state == State.OPEN
    assert cb.state.storage.failure_counter("foo") == threshold
    assert cb.state.storage.success_counter("foo") == 0
    assert cb.state.storage.timer("foo") is not None


def test_circuit_breaker_half_opened(success: Callable[[CircuitBreaker], None]):
    cb = CircuitBreaker(
        name="foo",
        storage=MemoryStorage(),
        state=State.OPEN,
        timeout=timedelta(seconds=0),
    )

    success(cb)

    assert cb.state == State.HALF_OPEN
    assert cb.state.storage.failure_counter("foo") == 0
    assert cb.state.storage.success_counter("foo") == 1
    assert cb.state.storage.timer("foo") is not None


def test_circuit_half_opened_to_open(failure: Callable[[CircuitBreaker], None]):
    cb = CircuitBreaker(name="foo", storage=MemoryStorage(), state=State.HALF_OPEN)

    failure(cb)

    assert cb.state == State.OPEN
    assert cb.state.storage.failure_counter("foo") == 0
    assert cb.state.storage.success_counter("foo") == 0
    assert cb.state.storage.timer("foo") is not None


async def test_circuit_half_opened_to_open_async(
    async_failure: Callable[[CircuitBreaker], Awaitable[None]]
):
    cb = CircuitBreaker(name="foo", storage=MemoryStorage(), state=State.HALF_OPEN)

    await async_failure(cb)

    assert cb.state == State.OPEN, print(cb.state)
    assert cb.state.storage.failure_counter("foo") == 0
    assert cb.state.storage.success_counter("foo") == 0
    assert cb.state.storage.timer("foo") is not None


def test_circuit_breaker_factory() -> None:
    cb_factory = CircuitBreakerFactory()
    circuit_breaker = cb_factory("foo")

    assert circuit_breaker.name == "foo"
    assert circuit_breaker.state == State.CLOSED
