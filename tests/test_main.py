from datetime import timedelta
from typing import Callable

import pytest
from disjuntor import CircuitBreaker
from disjuntor.main import CircuitBreakerException, State
from disjuntor.storage import MemoryStorage


def success_context_manager(cb: CircuitBreaker):
    with cb:
        ...


def failure_context_manager(cb: CircuitBreaker):
    class Potato(Exception):
        ...

    try:
        with cb:
            raise Potato("inside circuit breaker")
    except Potato:
        ...


def success_decorator(cb: CircuitBreaker):
    @cb
    def foo():
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


@pytest.fixture(params=[success_context_manager, success_decorator])
def success(request):
    yield request.param


@pytest.fixture(params=[failure_context_manager, failure_decorator])
def failure(request):
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
