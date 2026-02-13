from __future__ import annotations

import asyncio
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeVar

from loguru import logger

type _Metadata = tuple[bool, str]
_T = TypeVar("_T")


class TempFileMetadataMixin:
    _base_dir: Path
    _namespace_lock: asyncio.Lock
    _namespace: str
    _callback_name: str
    _retention_minutes: int
    _retention_seconds: float
    _store: Any

    if TYPE_CHECKING:
        def _encode_metadata(self, is_text: bool, revision: str) -> object:
            ...

        def _decode_metadata(self, value: object) -> _Metadata | None:
            ...

        async def _run_in_executor(
            self,
            fn: Callable[..., _T],
            *args: object,
            **kwargs: object,
        ) -> _T:
            ...

        @staticmethod
        def _get_mtime(path: Path) -> float:
            ...

        def _delete_file(self, name: str) -> int:
            ...

        @staticmethod
        def _is_utf8(data: bytes) -> bool:
            ...

        def _new_revision(self) -> str:
            ...

    async def _write_metadata(
        self,
        name: str,
        is_text: bool,
        retention_minutes: int,
        revision: str,
    ) -> None:
        payload = self._encode_metadata(is_text, revision)
        async with self._namespace_lock:
            await self._store.set(
                self._namespace,
                name,
                payload,
                retention=retention_minutes,
                on_expire=self._callback_name,
            )

    async def _clear_metadata_best_effort(self, name: str) -> None:
        try:
            namespace_escaped = self._store._encode_namespace(self._namespace)
            key_bytes = self._store._encode_key(name)
            async with self._store._write_lock:
                await self._store._run_in_executor(
                    self._store._delete_entry_and_schedule_callback,
                    self._namespace,
                    namespace_escaped,
                    key_bytes,
                    0,
                )
        except Exception:
            logger.exception("[TMP] Failed clearing metadata", name=name)

    async def _restore_metadata_after_overwrite_failure(
        self,
        name: str,
        previous_metadata: _Metadata | None,
    ) -> None:
        path = self._base_dir / name
        try:
            if not path.exists() or not path.is_file() or path.is_symlink():
                await self._clear_metadata_best_effort(name)
                return
        except Exception:
            await self._clear_metadata_best_effort(name)
            return

        if previous_metadata is None:
            await self._clear_metadata_best_effort(name)
            return

        is_text, revision = previous_metadata
        await self._write_metadata(
            name,
            is_text,
            self._retention_minutes,
            revision=revision,
        )

    async def _read_metadata(self, name: str) -> _Metadata | None:
        async with self._namespace_lock:
            value = await self._store.get(self._namespace, name)
        return self._decode_metadata(value)

    async def _resolve_or_infer_metadata(
        self,
        name: str,
        path: Path,
        data: bytes,
        now: float,
    ) -> _Metadata | None:
        meta = await self._read_metadata(name)
        if meta is not None:
            return meta

        try:
            mtime = await self._run_in_executor(self._get_mtime, path)
        except Exception:
            logger.exception("[TMP] Failed to stat temp file", name=name)
            return None

        if now - mtime >= self._retention_seconds:
            await self._run_in_executor(self._delete_file, name)
            return None

        return self._is_utf8(data), self._new_revision()
