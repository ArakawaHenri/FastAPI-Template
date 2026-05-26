from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Awaitable, Callable
from typing import Any, Generic, TypeVar

from fastapiex.di import BaseService, ServiceMap
from fastapiex.settings import BaseSettings, GetSettingsMap, SettingsMap
from openai import AsyncOpenAI
from pydantic import Field

logger = logging.getLogger(__name__)
T = TypeVar("T")


def _normalize_base_url(base_url: str | None) -> str | None:
    if not base_url:
        return None
    return base_url.rstrip("/") + "/"


@SettingsMap("openai_clients")
class OpenAIClientSettings(BaseSettings):
    api_key: str = ""
    base_url: str | None = None
    timeout: float = Field(default=60.0, gt=0)
    organization: str | None = None
    model: str = "gpt-4o-mini"
    concurrency_limit: int = Field(default=20, ge=1)


@ServiceMap("{}_openai_client_service", mapping=lambda: GetSettingsMap("openai_clients"))
class AsyncOpenAIClientService(BaseService):
    """Async OpenAI client service with an internal per-client semaphore."""

    @classmethod
    async def create(
        cls,
        api_key: str = "",
        base_url: str | None = None,
        timeout: float = 60.0,
        organization: str | None = None,
        model: str = "gpt-4o-mini",
        concurrency_limit: int = 20,
    ) -> AsyncOpenAIClientService:
        return cls(
            api_key=api_key,
            base_url=base_url,
            timeout=timeout,
            organization=organization,
            model=model,
            concurrency_limit=concurrency_limit,
        )

    @classmethod
    async def destroy(cls, instance: AsyncOpenAIClientService) -> None:
        await instance.close()

    def __init__(
        self,
        api_key: str = "",
        base_url: str | None = None,
        timeout: float = 60.0,
        organization: str | None = None,
        model: str = "gpt-4o-mini",
        concurrency_limit: int = 20,
        client: Any | None = None,
    ) -> None:
        if timeout <= 0:
            raise ValueError("OpenAI client timeout must be > 0")
        if concurrency_limit < 1:
            raise ValueError("OpenAI client concurrency_limit must be >= 1")
        self.model = model
        self.base_url = _normalize_base_url(base_url)
        self.timeout = timeout
        self.concurrency_limit = concurrency_limit
        self._semaphore = asyncio.Semaphore(concurrency_limit)
        self._client = client or AsyncOpenAI(
            api_key=api_key or "sk-placeholder",
            base_url=self.base_url,
            timeout=timeout,
            organization=organization,
        )
        self.client = _SemaphoreProxy(self._client, self._semaphore)
        logger.debug(
            "AsyncOpenAI client initialized base_url=%s model=%s concurrency_limit=%s",
            self.base_url,
            model,
            concurrency_limit,
        )

    async def close(self) -> None:
        close = getattr(self._client, "close", None)
        if close is None:
            return
        result = close()
        if inspect.isawaitable(result):
            await result
        logger.debug("AsyncOpenAI client closed")

    @property
    def semaphore(self) -> asyncio.Semaphore:
        return self._semaphore

    async def chat_completions_create(self, **kwargs: Any) -> Any:
        return await self.chat.completions.create(**kwargs)

    async def responses_create(self, **kwargs: Any) -> Any:
        return await self.responses.create(**kwargs)

    async def embeddings_create(self, **kwargs: Any) -> Any:
        return await self.embeddings.create(**kwargs)

    async def run(self, call: Callable[..., Awaitable[T]], *args: Any, **kwargs: Any) -> T:
        """Run a custom async OpenAI call under this client's semaphore."""
        async with self._semaphore:
            return await call(*args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.client, name)


class _SemaphoreProxy:
    """Recursive proxy that guards async OpenAI SDK calls with a semaphore."""

    def __init__(self, target: Any, semaphore: asyncio.Semaphore) -> None:
        self._target = target
        self._semaphore = semaphore

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._target, name)
        return _wrap_attr(attr, self._semaphore)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._target!r})"


class _GuardedAwaitable(Generic[T]):
    def __init__(self, awaitable: Awaitable[T], semaphore: asyncio.Semaphore) -> None:
        self._awaitable = awaitable
        self._semaphore = semaphore

    def __await__(self):
        return self._run().__await__()

    async def _run(self) -> T:
        await self._semaphore.acquire()
        release = True
        try:
            result = await self._awaitable
            wrapped, transferred = _wrap_result_after_acquire(result, self._semaphore)
            release = not transferred
            return wrapped
        finally:
            if release:
                self._semaphore.release()


class _GuardedAsyncResource:
    """Guards an async resource (context manager and/or async iterator) with a semaphore."""

    def __init__(self, target: Any, semaphore: asyncio.Semaphore, *, acquired: bool = False) -> None:
        self._target = target
        self._semaphore = semaphore
        self._acquired = acquired
        self._closed = False
        self._iterator = None

    async def __aenter__(self) -> Any:
        if not self._acquired:
            await self._semaphore.acquire()
            self._acquired = True
        try:
            inner_res = await self._target.__aenter__()
            if inner_res is self._target:
                return self
            return inner_res
        except Exception:
            self._release()
            raise

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> Any:
        try:
            return await self._target.__aexit__(exc_type, exc, tb)
        finally:
            self._release()

    def __aiter__(self) -> _GuardedAsyncResource:
        if self._iterator is None:
            try:
                self._iterator = self._target.__aiter__()
            except Exception:
                self._release()
                raise
        return self

    async def __anext__(self) -> Any:
        if self._closed:
            raise StopAsyncIteration
        if not self._acquired:
            await self._semaphore.acquire()
            self._acquired = True
        if self._iterator is None:
            self._iterator = self._target.__aiter__()
        try:
            return await self._iterator.__anext__()
        except StopAsyncIteration:
            await self.aclose()
            raise
        except Exception:
            await self.aclose()
            raise

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        close_target = self._iterator if self._iterator is not None else self._target
        close_method = getattr(close_target, "aclose", None)
        if close_method is None and close_target is not self._target:
            close_method = getattr(self._target, "aclose", None)
        try:
            if close_method is not None:
                res = close_method()
                if inspect.isawaitable(res):
                    await res
        finally:
            self._release()

    def _release(self) -> None:
        if self._acquired:
            self._semaphore.release()
            self._acquired = False

    def __getattr__(self, name: str) -> Any:
        return getattr(self._target, name)

    def __del__(self) -> None:
        try:
            self._release()
        except Exception:
            pass


def _wrap_attr(attr: Any, semaphore: asyncio.Semaphore) -> Any:
    if callable(attr):
        return _guard_callable(attr, semaphore)
    if _should_proxy(attr):
        return _SemaphoreProxy(attr, semaphore)
    return attr


def _guard_callable(func: Callable[..., Any], semaphore: asyncio.Semaphore) -> Callable[..., Any]:
    def guarded(*args: Any, **kwargs: Any) -> Any:
        result = func(*args, **kwargs)
        return _wrap_result(result, semaphore)

    return guarded


def _wrap_result(result: Any, semaphore: asyncio.Semaphore) -> Any:
    if inspect.isawaitable(result):
        return _GuardedAwaitable(result, semaphore)
    if _is_async_context_manager(result) or _is_async_iterable(result):
        return _GuardedAsyncResource(result, semaphore)
    return result


def _wrap_result_after_acquire(result: Any, semaphore: asyncio.Semaphore) -> tuple[Any, bool]:
    if _is_async_context_manager(result) or _is_async_iterable(result):
        return _GuardedAsyncResource(result, semaphore, acquired=True), True
    return result, False


def _is_async_context_manager(value: Any) -> bool:
    return hasattr(value, "__aenter__") and hasattr(value, "__aexit__")


def _is_async_iterable(value: Any) -> bool:
    return hasattr(value, "__aiter__") and hasattr(value, "__anext__")


def _should_proxy(value: Any) -> bool:
    if value is None or isinstance(value, str | bytes | int | float | bool | tuple | list | dict | set):
        return False
    return hasattr(value, "__dict__")
