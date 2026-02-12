from __future__ import annotations

import math
import time

from loguru import logger

from app.services.store import ExpiryCallbackDeferred, ExpiryCallbackEvent


class TempFileReconcilerMixin:
    async def _adopt_existing_files(self) -> None:
        now = time.time()
        await self._reconcile_files(now, adopt_fresh=True)

    async def _reconcile_files(self, now: float, adopt_fresh: bool = True) -> None:
        entries = await self._run_in_executor(self._scan_files)
        if not entries:
            self._total_size_bytes = 0
            return

        cutoff = now - self._retention_seconds
        internal_artifact_cutoff = now - self.INTERNAL_ARTIFACT_RETENTION_SECONDS
        delete_names: list[str] = []
        delete_internal_names: list[str] = []
        adopt_entries: list[tuple[str, float]] = []
        candidate_entries: list[tuple[str, float]] = []

        current_total = 0
        async with self._namespace_lock:
            for name, mtime, size in entries:
                current_total += size
                if self._is_internal_artifact_name(name):
                    if mtime < internal_artifact_cutoff:
                        delete_internal_names.append(name)
                    continue

                candidate_entries.append((name, mtime))

            metadata_by_name = await self._store.get_many(
                self._namespace,
                [name for name, _ in candidate_entries],
            )
            for name, mtime in candidate_entries:
                value = metadata_by_name.get(name)
                if self._decode_metadata(value) is not None:
                    continue
                if mtime < cutoff:
                    delete_names.append(name)
                elif adopt_fresh:
                    adopt_entries.append((name, mtime))

        self._total_size_bytes = current_total

        if delete_internal_names:
            deleted_internal_size = await self._run_in_executor(
                self._delete_files,
                delete_internal_names,
            )
            self._total_size_bytes = max(
                0,
                self._total_size_bytes - deleted_internal_size,
            )

        if delete_names:
            deleted_size = await self._run_in_executor(
                self._delete_files,
                delete_names,
            )
            self._total_size_bytes = max(
                0,
                self._total_size_bytes - deleted_size,
            )

        for name, mtime in adopt_entries:
            path = self._base_dir / name
            try:
                data = await self._run_in_executor(self._read_file_bytes, path)
            except Exception:
                logger.exception("[TMP] Failed to read temp file", name=name)
                continue
            is_text = self._is_utf8(data)
            remaining = max(1, math.ceil(
                (self._retention_seconds - (now - mtime)) / 60))
            try:
                await self._write_metadata(
                    name,
                    is_text,
                    remaining,
                    revision=self._new_revision(),
                )
            except Exception:
                logger.exception(
                    "[TMP] Failed to adopt temp file metadata", name=name)
                continue

    async def _on_expire(self, event: ExpiryCallbackEvent) -> None:
        if not self._is_runtime_thread():
            if self._closed:
                raise ExpiryCallbackDeferred(
                    "TempFileService is closed; defer callback"
                )
            try:
                await self._run_on_runtime_thread(self._on_expire, event)
            except RuntimeError as exc:
                if self._closed:
                    raise ExpiryCallbackDeferred(
                        "TempFileService runtime is unavailable; defer callback"
                    ) from exc
                raise
            return
        if self._executor_shutdown:
            raise ExpiryCallbackDeferred(
                "TempFileService executor is shut down; defer callback"
            )
        try:
            path = self._base_dir / event.key
            if path.parent != self._base_dir:
                logger.warning(
                    "[TMP] Expiry callback skipped key outside temp dir",
                    key=event.key,
                )
                return
            try:
                mtime = await self._run_in_executor(self._get_mtime, path)
            except FileNotFoundError:
                return
            except ValueError:
                logger.warning(
                    "[TMP] Expiry callback skipped non-regular file", key=event.key
                )
                return

            if event.expire_ts > 0 and mtime > float(event.expire_ts):
                logger.debug(
                    "[TMP] Skipping stale expiry callback for newer temp file",
                    key=event.key,
                    expire_ts=event.expire_ts,
                    mtime=mtime,
                )
                return

            event_metadata = self._decode_metadata(event.value)
            expected_revision = event_metadata[1] if event_metadata is not None else None
            if expected_revision is not None:
                current_meta = await self._read_metadata(event.key)
                if current_meta is not None:
                    _, current_revision = current_meta
                    if current_revision != expected_revision:
                        logger.debug(
                            "[TMP] Skipping stale expiry callback due to revision mismatch",
                            key=event.key,
                            expected_revision=expected_revision,
                            current_revision=current_revision,
                        )
                        return

            deleted_size = await self._run_in_executor(self._delete_file, event.key)
            self._total_size_bytes -= deleted_size
        except Exception:
            logger.exception("[TMP] Expiry callback failed", key=event.key)
            raise
