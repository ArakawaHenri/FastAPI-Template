from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest

from app.service.openai_client.main import AsyncOpenAIClientService


class _FakeCompletions:
    def __init__(self) -> None:
        self.active = 0
        self.max_active = 0

    async def create(self, **kwargs):
        self.active += 1
        try:
            self.max_active = max(self.max_active, self.active)
            await asyncio.sleep(0.01)
            return {"kwargs": kwargs}
        finally:
            self.active -= 1


class _StreamTracker:
    def __init__(self) -> None:
        self.active = 0
        self.max_active = 0


class _FakeAsyncStream:
    def __init__(self, tracker: _StreamTracker) -> None:
        self._tracker = tracker
        self._remaining = 2

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._remaining < 1:
            raise StopAsyncIteration
        self._remaining -= 1
        self._tracker.active += 1
        try:
            self._tracker.max_active = max(self._tracker.max_active, self._tracker.active)
            await asyncio.sleep(0.01)
            return {"chunk": self._remaining}
        finally:
            self._tracker.active -= 1


class _FakeStreamingCompletions:
    def __init__(self, tracker: _StreamTracker) -> None:
        self._tracker = tracker

    async def create(self, **kwargs):
        if kwargs.get("stream"):
            return _FakeAsyncStream(self._tracker)
        return {"kwargs": kwargs}


@pytest.mark.asyncio
async def test_openai_client_proxies_nested_methods_under_internal_semaphore() -> None:
    completions = _FakeCompletions()
    raw_client = SimpleNamespace(
        chat=SimpleNamespace(completions=completions),
    )
    service = AsyncOpenAIClientService(
        api_key="sk-test",
        concurrency_limit=1,
        client=raw_client,
    )

    results = await asyncio.gather(
        service.chat.completions.create(model="a"),
        service.chat.completions.create(model="b"),
    )

    assert service.concurrency_limit == 1
    assert completions.max_active == 1
    assert [result["kwargs"]["model"] for result in results] == ["a", "b"]


@pytest.mark.asyncio
async def test_openai_client_helper_methods_use_same_proxy() -> None:
    completions = _FakeCompletions()
    service = AsyncOpenAIClientService(
        api_key="sk-test",
        concurrency_limit=1,
        client=SimpleNamespace(chat=SimpleNamespace(completions=completions)),
    )

    await asyncio.gather(
        service.chat_completions_create(model="a"),
        service.chat_completions_create(model="b"),
    )

    assert completions.max_active == 1


@pytest.mark.asyncio
async def test_openai_client_holds_semaphore_for_async_stream_consumption() -> None:
    tracker = _StreamTracker()
    service = AsyncOpenAIClientService(
        api_key="sk-test",
        concurrency_limit=1,
        client=SimpleNamespace(
            chat=SimpleNamespace(completions=_FakeStreamingCompletions(tracker))
        ),
    )

    async def consume(model: str) -> list[dict[str, int]]:
        stream = await service.chat.completions.create(model=model, stream=True)
        chunks = []
        async for chunk in stream:
            chunks.append(chunk)
        return chunks

    results = await asyncio.gather(consume("a"), consume("b"))

    assert tracker.max_active == 1
    assert [len(chunks) for chunks in results] == [2, 2]


@pytest.mark.asyncio
async def test_openai_client_stream_context_manager_releases_semaphore() -> None:
    class _FakeStreamContext:
        def __init__(self) -> None:
            self.entered = False
            self.exited = False

        async def __aenter__(self) -> _FakeStreamContext:
            self.entered = True
            return self

        async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
            self.exited = True

        def __aiter__(self) -> _FakeStreamContext:
            return self

        async def __anext__(self) -> str:
            raise StopAsyncIteration

    class _FakeStreamingCompletionsContext:
        async def create(self, **kwargs: Any) -> Any:
            return _FakeStreamContext()

    service = AsyncOpenAIClientService(
        api_key="sk-test",
        concurrency_limit=1,
        client=SimpleNamespace(
            chat=SimpleNamespace(completions=_FakeStreamingCompletionsContext())
        ),
    )

    # Use as async context manager
    async with await service.chat.completions.create(model="test", stream=True) as stream:
        assert stream._acquired is True
        # Iterate inside context
        async for chunk in stream:
            pass

    assert service._semaphore._value == 1


@pytest.mark.asyncio
async def test_openai_client_context_manager_inner_resource_does_not_over_release() -> None:
    import gc

    inner = SimpleNamespace(name="inner")

    class _FakeOuterContext:
        async def __aenter__(self) -> Any:
            return inner

        async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
            return None

    class _FakeStreamingCompletionsContext:
        async def create(self, **kwargs: Any) -> Any:
            return _FakeOuterContext()

    service = AsyncOpenAIClientService(
        api_key="sk-test",
        concurrency_limit=1,
        client=SimpleNamespace(
            chat=SimpleNamespace(completions=_FakeStreamingCompletionsContext())
        ),
    )

    async with await service.chat.completions.create(model="test", stream=True) as resource:
        assert resource is inner
        assert service._semaphore._value == 0

    del resource
    gc.collect()

    assert service._semaphore._value == 1


@pytest.mark.asyncio
async def test_openai_client_stream_closes_iterator_returned_by_aiter() -> None:
    class _Iterator:
        def __init__(self) -> None:
            self.closed = False
            self._remaining = 1

        def __aiter__(self) -> _Iterator:
            return self

        async def __anext__(self) -> str:
            if self._remaining < 1:
                raise StopAsyncIteration
            self._remaining -= 1
            return "chunk"

        async def aclose(self) -> None:
            self.closed = True

    class _Iterable:
        def __init__(self) -> None:
            self.iterator: _Iterator | None = None

        def __aiter__(self) -> _Iterator:
            self.iterator = _Iterator()
            return self.iterator

        async def __anext__(self) -> str:
            raise AssertionError("wrapper should iterate through __aiter__ result")

    class _FakeStreamingCompletions:
        def __init__(self, iterable: _Iterable) -> None:
            self.iterable = iterable

        async def create(self, **kwargs: Any) -> Any:
            return self.iterable

    iterable = _Iterable()
    service = AsyncOpenAIClientService(
        api_key="sk-test",
        concurrency_limit=1,
        client=SimpleNamespace(
            chat=SimpleNamespace(completions=_FakeStreamingCompletions(iterable))
        ),
    )

    stream = await service.chat.completions.create(model="test", stream=True)
    async for _chunk in stream:
        break
    await stream.aclose()

    assert iterable.iterator is not None
    assert iterable.iterator.closed is True
    assert service._semaphore._value == 1


@pytest.mark.asyncio
async def test_openai_client_releases_semaphore_when_aiter_raises() -> None:
    class _BrokenIterable:
        def __aiter__(self) -> Any:
            raise RuntimeError("stream iterator failed")

        async def __anext__(self) -> str:
            raise AssertionError("unreachable")

    class _FakeStreamingCompletions:
        async def create(self, **kwargs: Any) -> Any:
            return _BrokenIterable()

    service = AsyncOpenAIClientService(
        api_key="sk-test",
        concurrency_limit=1,
        client=SimpleNamespace(
            chat=SimpleNamespace(completions=_FakeStreamingCompletions())
        ),
    )

    stream = await service.chat.completions.create(model="test", stream=True)
    with pytest.raises(RuntimeError, match="stream iterator failed"):
        async for _chunk in stream:
            pass

    assert service._semaphore._value == 1


@pytest.mark.asyncio
async def test_openai_client_releases_semaphore_when_context_enter_raises() -> None:
    class _BrokenContext:
        async def __aenter__(self) -> Any:
            raise RuntimeError("stream enter failed")

        async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
            return None

    class _FakeStreamingCompletions:
        async def create(self, **kwargs: Any) -> Any:
            return _BrokenContext()

    service = AsyncOpenAIClientService(
        api_key="sk-test",
        concurrency_limit=1,
        client=SimpleNamespace(
            chat=SimpleNamespace(completions=_FakeStreamingCompletions())
        ),
    )

    with pytest.raises(RuntimeError, match="stream enter failed"):
        async with await service.chat.completions.create(model="test", stream=True):
            pass

    assert service._semaphore._value == 1


@pytest.mark.asyncio
async def test_openai_client_early_break_gc_releases_semaphore() -> None:
    import gc

    tracker = _StreamTracker()
    service = AsyncOpenAIClientService(
        api_key="sk-test",
        concurrency_limit=1,
        client=SimpleNamespace(
            chat=SimpleNamespace(completions=_FakeStreamingCompletions(tracker))
        ),
    )

    stream = await service.chat.completions.create(model="test", stream=True)
    async for chunk in stream:
        break

    assert service._semaphore._value == 0  # Still acquired before GC/deletion

    del stream
    gc.collect()

    assert service._semaphore._value == 1  # Released after garbage collection
