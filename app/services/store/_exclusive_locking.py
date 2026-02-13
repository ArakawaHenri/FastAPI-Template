from __future__ import annotations

import asyncio
import threading
from collections.abc import Awaitable, Callable
from functools import partial
from types import TracebackType
from typing import ParamSpec, Protocol, TypeVar
from uuid import uuid4

from loguru import logger

_P = ParamSpec("_P")
_T = TypeVar("_T")


class _StoreExclusiveOwner(Protocol):
    async def _acquire_exclusive(self, namespace: str) -> tuple[str, bool]:
        ...

    def _release_exclusive(self, namespace: str) -> None:
        ...


class StoreExclusiveNamespaceLock:
    def __init__(self, store: _StoreExclusiveOwner, namespace: str) -> None:
        self._store = store
        self._namespace = namespace
        self._lease_id: str | None = None
        self._task_group: asyncio.TaskGroup | None = None
        self._enter_depth = 0

    async def __aenter__(self) -> StoreExclusiveNamespaceLock:
        lease_id, owns_lease = await self._store._acquire_exclusive(
            self._namespace
        )
        task_group: asyncio.TaskGroup | None = None
        try:
            if owns_lease:
                task_group = asyncio.TaskGroup()
                await task_group.__aenter__()
        except Exception:
            self._store._release_exclusive(self._namespace)
            raise

        if self._enter_depth == 0:
            self._lease_id = lease_id
            if task_group is not None:
                self._task_group = task_group
        # _acquire_exclusive handles stale contextvar state by re-acquiring.
        self._enter_depth += 1
        return self

    def create_task(
        self,
        coro: Awaitable[_T],
        *,
        name: str | None = None,
    ) -> asyncio.Task[_T]:
        if self._task_group is None:
            raise RuntimeError(
                "This lock instance does not own the active lease task group. "
                "Create child tasks from the lock object that acquired the lease."
            )
        return self._task_group.create_task(coro, name=name)

    def start_soon(
        self,
        fn: Callable[_P, Awaitable[_T]],
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> asyncio.Task[_T]:
        return self.create_task(fn(*args, **kwargs))

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._enter_depth <= 0:
            logger.warning(
                "[STORE] Exclusive namespace lock exited without enter",
                namespace=self._namespace,
            )
            return

        self._enter_depth -= 1

        task_group_error: BaseException | None = None
        if self._enter_depth == 0 and self._task_group is not None:
            try:
                await self._task_group.__aexit__(exc_type, exc, tb)
            except BaseException as error:
                task_group_error = error
            finally:
                self._task_group = None

        self._store._release_exclusive(self._namespace)
        if self._enter_depth == 0:
            self._lease_id = None

        if task_group_error is not None:
            raise task_group_error


class StoreExclusiveLockingMixin:
    def create_namespace_lock(self, namespace: str) -> StoreExclusiveNamespaceLock:
        self._encode_namespace(namespace)
        with self._exclusive_registry_lock:
            if namespace not in self._exclusive_locks:
                self._exclusive_locks[namespace] = threading.Lock()
        return StoreExclusiveNamespaceLock(self, namespace)

    def _assert_namespace_access(self, namespace: str) -> None:
        with self._exclusive_registry_lock:
            lock = self._exclusive_locks.get(namespace)
            active_lease_id = self._exclusive_active_leases.get(namespace)
        if lock is None:
            return
        state = self._exclusive_guard.get() or {}
        lease_entry = state.get(namespace)
        if lease_entry is None:
            raise RuntimeError(
                f"Namespace '{namespace}' is exclusive; "
                f"use store.create_namespace_lock('{namespace}') to access it."
            )
        lease_id, _depth = lease_entry
        if active_lease_id != lease_id:
            raise RuntimeError(
                f"Namespace '{namespace}' exclusive lease is no longer active. "
                f"Reacquire via store.create_namespace_lock('{namespace}')."
            )

    async def _acquire_exclusive(self, namespace: str) -> tuple[str, bool]:
        with self._exclusive_registry_lock:
            lock = self._exclusive_locks.get(namespace)
            if lock is None:
                lock = threading.Lock()
                self._exclusive_locks[namespace] = lock

        state = self._exclusive_guard.get() or {}
        lease_entry = state.get(namespace)
        with self._exclusive_registry_lock:
            active_lease_id = self._exclusive_active_leases.get(namespace)
        if lease_entry is not None:
            lease_id, depth = lease_entry
            if depth > 0 and active_lease_id == lease_id:
                new_state = dict(state)
                new_state[namespace] = (lease_id, depth + 1)
                self._exclusive_guard.set(new_state)
                return lease_id, False
            # Drop stale inherited state and reacquire a fresh lease.
            new_state = dict(state)
            new_state.pop(namespace, None)
            self._exclusive_guard.set(new_state)
            state = new_state

        acquired = await self._await_cancellable_thread_call(
            partial(self._acquire_thread_lock_cancellable, lock)
        )
        if not acquired:
            raise asyncio.CancelledError()
        lease_id = uuid4().hex
        with self._exclusive_registry_lock:
            self._exclusive_active_leases[namespace] = lease_id
        new_state = dict(state)
        new_state[namespace] = (lease_id, 1)
        self._exclusive_guard.set(new_state)
        return lease_id, True

    def _release_exclusive(self, namespace: str) -> None:
        state = self._exclusive_guard.get() or {}
        lease_entry = state.get(namespace)
        if lease_entry is None:
            logger.warning(
                "[STORE] Exclusive namespace release without acquisition",
                namespace=namespace,
            )
            return
        lease_id, depth = lease_entry

        if depth > 1:
            new_state = dict(state)
            new_state[namespace] = (lease_id, depth - 1)
            self._exclusive_guard.set(new_state)
            return

        new_state = dict(state)
        new_state.pop(namespace, None)
        self._exclusive_guard.set(new_state)

        with self._exclusive_registry_lock:
            active_lease_id = self._exclusive_active_leases.get(namespace)
        if active_lease_id != lease_id:
            logger.warning(
                "[STORE] Exclusive namespace lease mismatch during release",
                namespace=namespace,
                expected_lease=active_lease_id,
                local_lease=lease_id,
            )
            return

        with self._exclusive_registry_lock:
            lock = self._exclusive_locks.get(namespace)
            self._exclusive_active_leases.pop(namespace, None)
        if lock is None:
            logger.warning(
                "[STORE] Exclusive namespace lock missing during release",
                namespace=namespace,
            )
            return
        if lock.locked():
            lock.release()
            return
        logger.warning(
            "[STORE] Exclusive namespace lock already released",
            namespace=namespace,
        )
