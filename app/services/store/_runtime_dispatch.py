from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Callable, Coroutine
from concurrent.futures import Future as ConcurrentFuture
from functools import partial
from typing import ParamSpec, TypeVar

from loguru import logger

_RUNTIME_THREAD_START_TIMEOUT_SECONDS = 5.0
_RUNTIME_THREAD_STOP_TIMEOUT_SECONDS = 5.0
_RUNTIME_SUBMIT_TIMEOUT_SECONDS = 30.0
_RUNTIME_MAX_PENDING_CALLS = 1024
_RUNTIME_BLOCKING_WAIT_POLL_SECONDS = 0.1
_P = ParamSpec("_P")
_T = TypeVar("_T")


class StoreRuntimeDispatchMixin:
    _closed: bool

    def _init_runtime_dispatch_state(self) -> None:
        self._runtime_loop: asyncio.AbstractEventLoop | None = None
        self._runtime_thread: threading.Thread | None = None
        self._runtime_thread_id: int | None = None
        self._runtime_ready = threading.Event()
        self._runtime_submit_semaphore = threading.BoundedSemaphore(
            _RUNTIME_MAX_PENDING_CALLS
        )

    async def _run_in_executor(
        self,
        fn: Callable[..., _T],
        *args: object,
        **kwargs: object,
    ) -> _T:
        # Store operations execute on the service runtime thread.
        # This helper keeps the call signature aligned with other services.
        if kwargs:
            fn = partial(fn, **kwargs)
        return fn(*args)

    async def _await_cancellable_thread_call(
        self,
        fn: Callable[[threading.Event], _T],
    ) -> _T:
        cancel_event = threading.Event()
        waiter = asyncio.create_task(asyncio.to_thread(fn, cancel_event))
        try:
            return await waiter
        except asyncio.CancelledError:
            cancel_event.set()
            try:
                await waiter
            except Exception:
                logger.exception("[STORE] Cancellation cleanup for thread wait failed")
            raise

    @staticmethod
    def _acquire_thread_lock_cancellable(
        lock: threading.Lock,
        cancel_event: threading.Event,
    ) -> bool:
        while not cancel_event.is_set():
            acquired = lock.acquire(timeout=_RUNTIME_BLOCKING_WAIT_POLL_SECONDS)
            if not acquired:
                continue
            if cancel_event.is_set():
                lock.release()
                return False
            return True
        return False

    def _acquire_runtime_submit_slot(
        self,
        cancel_event: threading.Event,
    ) -> bool:
        deadline = time.monotonic() + _RUNTIME_SUBMIT_TIMEOUT_SECONDS
        while not cancel_event.is_set():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            wait_seconds = min(_RUNTIME_BLOCKING_WAIT_POLL_SECONDS, remaining)
            acquired = self._runtime_submit_semaphore.acquire(timeout=wait_seconds)
            if not acquired:
                continue
            if cancel_event.is_set():
                self._runtime_submit_semaphore.release()
                return False
            return True
        return False

    def _make_runtime_submit_releaser(self) -> Callable[..., None]:
        released = False
        released_lock = threading.Lock()

        def _release_slot(_fut: object | None = None) -> None:
            nonlocal released
            with released_lock:
                if released:
                    return
                released = True
            self._runtime_submit_semaphore.release()

        return _release_slot

    def _is_runtime_thread(self) -> bool:
        return self._runtime_thread_id == threading.get_ident()

    def _start_runtime_thread(self) -> None:
        if self._runtime_thread is not None:
            return
        self._runtime_ready.clear()
        self._runtime_thread = threading.Thread(
            target=self._runtime_thread_main,
            name="store-runtime",
            daemon=True,
        )
        self._runtime_thread.start()
        if not self._runtime_ready.wait(timeout=_RUNTIME_THREAD_START_TIMEOUT_SECONDS):
            loop = self._runtime_loop
            if loop is not None and loop.is_running():
                loop.call_soon_threadsafe(loop.stop)
            self._runtime_thread.join(timeout=_RUNTIME_THREAD_STOP_TIMEOUT_SECONDS)
            if self._runtime_thread.is_alive():
                raise RuntimeError(
                    "StoreService runtime thread failed to start and could not be stopped"
                )
            self._runtime_thread = None
            self._runtime_loop = None
            self._runtime_thread_id = None
            raise RuntimeError(
                "StoreService runtime thread did not start within timeout"
            )
        if self._runtime_loop is None:
            raise RuntimeError("StoreService runtime thread failed to start")

    def _runtime_thread_main(self) -> None:
        loop = asyncio.new_event_loop()
        self._runtime_loop = loop
        self._runtime_thread_id = threading.get_ident()
        asyncio.set_event_loop(loop)
        self._runtime_ready.set()
        try:
            loop.run_forever()
        finally:
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True)
                )
            loop.run_until_complete(loop.shutdown_asyncgens())
            asyncio.set_event_loop(None)
            loop.close()
            self._runtime_loop = None
            self._runtime_thread_id = None

    async def _run_on_runtime_thread(
        self,
        fn: Callable[_P, Coroutine[object, object, _T]],
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> _T:
        if self._is_runtime_thread():
            return await fn(*args, **kwargs)
        loop = self._runtime_loop
        if loop is None:
            if self._closed:
                raise RuntimeError("StoreService is closed")
            raise RuntimeError("StoreService runtime loop is unavailable")
        acquired = await self._await_cancellable_thread_call(
            self._acquire_runtime_submit_slot
        )
        if not acquired:
            raise TimeoutError("StoreService runtime queue is saturated")

        release_slot = self._make_runtime_submit_releaser()

        try:
            coro = fn(*args, **kwargs)
        except Exception:
            release_slot()
            raise
        try:
            future: ConcurrentFuture[_T] = asyncio.run_coroutine_threadsafe(coro, loop)
        except Exception:
            coro.close()
            release_slot()
            raise
        future.add_done_callback(release_slot)
        return await asyncio.wrap_future(future)

    async def _stop_runtime_thread(self) -> None:
        thread = self._runtime_thread
        loop = self._runtime_loop
        if thread is None:
            return
        if loop is not None and loop.is_running():
            loop.call_soon_threadsafe(loop.stop)
        await asyncio.to_thread(thread.join, _RUNTIME_THREAD_STOP_TIMEOUT_SECONDS)
        if thread.is_alive():
            raise RuntimeError("StoreService runtime thread did not stop in time")
        self._runtime_thread = None
