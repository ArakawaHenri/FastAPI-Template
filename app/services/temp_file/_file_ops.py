from __future__ import annotations

import errno
import os
import stat
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar
from urllib.parse import quote

from loguru import logger

from ._filename import (
    encode_filename,
    escape_leading_dots,
    is_utf8,
    is_windows_reserved_name,
    pct_encode_char,
)

type _ScannedFileEntry = tuple[str, float, int]


class TempFileFileOpsMixin:
    _base_dir: Path
    INTERNAL_STAGING_PREFIX: ClassVar[str]
    INTERNAL_BACKUP_PREFIX: ClassVar[str]

    if TYPE_CHECKING:
        @staticmethod
        def _tighten_file_permissions(path: Path) -> None:
            ...

    def _sanitize_filename(self, filename: str) -> str:
        if not filename or filename.strip() == "":
            raise ValueError("Filename must be non-empty")
        filename = self._encode_filename(filename)
        if filename.startswith("."):
            filename = self._escape_leading_dots(filename)
        return filename

    def _normalize_content(self, content: str | bytes | bytearray | memoryview) -> tuple[bool, bytes]:
        if isinstance(content, str):
            return True, content.encode("utf-8")
        if isinstance(content, memoryview):
            return False, content.tobytes()
        if isinstance(content, (bytes, bytearray)):
            return False, bytes(content)
        raise TypeError("Content must be str or bytes-like")

    def _write_unique(self, base_name: str, data: bytes, now: float) -> str:
        base_path = self._base_dir / base_name
        stem = base_path.stem
        suffix = base_path.suffix

        attempt = 0
        while True:
            if attempt == 0:
                candidate = base_name
            else:
                candidate = (
                    f"{stem}.{attempt}{suffix}"
                    if suffix
                    else f"{base_name}.{attempt}"
                )

            path = self._base_dir / candidate
            try:
                self._write_new_file(path, data)
                self._touch_file(path, now)
                return candidate
            except FileExistsError:
                attempt += 1
                continue
            except Exception:
                # Best-effort cleanup if file was created
                try:
                    if path.exists():
                        path.unlink()
                except Exception:
                    logger.exception("[TMP] Failed to remove partial file")
                raise

    @classmethod
    def _new_staging_name(cls, base_name: str) -> str:
        safe_base = quote(base_name, safe="")
        return f"{cls.INTERNAL_STAGING_PREFIX}{safe_base}.{uuid.uuid4().hex}"

    @classmethod
    def _new_backup_name(cls, base_name: str) -> str:
        safe_base = quote(base_name, safe="")
        return f"{cls.INTERNAL_BACKUP_PREFIX}{safe_base}.{uuid.uuid4().hex}"

    @classmethod
    def _is_internal_artifact_name(cls, name: str) -> bool:
        if name.startswith(cls.INTERNAL_STAGING_PREFIX):
            return True
        if name.startswith(cls.INTERNAL_BACKUP_PREFIX):
            return True
        return False

    def _write_staging_file(self, staging_name: str, data: bytes, now: float) -> None:
        path = self._base_dir / staging_name
        try:
            self._write_new_file(path, data)
            self._touch_file(path, now)
        except Exception:
            try:
                st = path.lstat()
                if stat.S_ISREG(st.st_mode):
                    path.unlink()
            except FileNotFoundError:
                pass
            except Exception:
                logger.exception("[TMP] Failed to remove partial staging file")
            raise

    def _swap_staged_overwrite(self, base_name: str, staging_name: str) -> str | None:
        path = self._base_dir / base_name
        staging_path = self._base_dir / staging_name
        backup_name: str | None = None
        backup_path: Path | None = None
        backup_moved = False
        try:
            try:
                st = path.lstat()
            except FileNotFoundError:
                st = None
            if st is not None:
                if stat.S_ISLNK(st.st_mode):
                    raise ValueError(
                        f"Refusing to overwrite symlink temp file: {base_name}"
                    )
                if not stat.S_ISREG(st.st_mode):
                    raise ValueError(f"Path is not a regular file: {base_name}")
                backup_name = self._new_backup_name(base_name)
                backup_path = self._base_dir / backup_name
                os.replace(path, backup_path)
                backup_moved = True

            os.replace(staging_path, path)
            self._tighten_file_permissions(path)
            return backup_name
        except Exception:
            try:
                st = staging_path.lstat()
                if stat.S_ISREG(st.st_mode):
                    staging_path.unlink()
            except FileNotFoundError:
                pass
            except Exception:
                logger.exception("[TMP] Failed to remove staging file")
            if backup_moved and backup_path is not None:
                try:
                    os.replace(backup_path, path)
                    self._tighten_file_permissions(path)
                except Exception:
                    logger.exception(
                        "[TMP] Failed to restore backup after overwrite swap failure",
                        name=base_name,
                    )
            raise

    def _rollback_overwrite_after_metadata_failure(
        self,
        base_name: str,
        backup_name: str | None,
    ) -> int:
        """Rollback failed overwrite by deleting file and restoring backup.

        Returns:
            The size of the file that was deleted (0 if no file was deleted).
        """
        path = self._base_dir / base_name
        if backup_name is None:
            return self._delete_file(base_name)

        backup_path = self._base_dir / backup_name
        deleted_size = 0
        try:
            st = path.lstat()
        except FileNotFoundError:
            st = None
        if st is not None:
            if stat.S_ISLNK(st.st_mode):
                raise ValueError(
                    f"Refusing rollback on symlink temp file: {base_name}"
                )
            if not stat.S_ISREG(st.st_mode):
                raise ValueError(f"Path is not a regular file: {base_name}")
            deleted_size = st.st_size
            path.unlink()
        os.replace(backup_path, path)
        self._tighten_file_permissions(path)
        return deleted_size

    def _discard_backup_file(self, backup_name: str | None) -> int:
        if not backup_name:
            return 0
        return self._delete_file(backup_name)

    def _touch_file(self, path: Path, now: float) -> None:
        st = path.lstat()
        if stat.S_ISLNK(st.st_mode):
            raise ValueError(f"Refusing to touch symlink temp file: {path.name}")
        if not stat.S_ISREG(st.st_mode):
            raise ValueError(f"Path is not a regular file: {path.name}")
        os.utime(path, (now, now))

    def _delete_file(self, name: str) -> int:
        path = self._base_dir / name
        try:
            st = path.lstat()
            if stat.S_ISLNK(st.st_mode):
                logger.warning(
                    "[TMP] Refusing to delete symlink temp file", name=name
                )
                return 0
            if stat.S_ISREG(st.st_mode):
                size = st.st_size
                path.unlink()
                return size
        except FileNotFoundError:
            pass
        return 0

    def _delete_files(self, names: list[str]) -> int:
        deleted_size = 0
        for name in names:
            try:
                deleted_size += self._delete_file(name)
            except Exception:
                logger.exception("[TMP] Failed to delete file", name=name)
        return deleted_size

    def _scan_files(self) -> list[_ScannedFileEntry]:
        entries: list[_ScannedFileEntry] = []
        lock_name = self._cleanup_lock_path().name
        try:
            for path in self._base_dir.iterdir():
                if path.name == lock_name:
                    continue
                try:
                    st = path.lstat()
                except Exception:
                    logger.exception(
                        "[TMP] Failed to stat file", name=path.name)
                    continue
                if stat.S_ISLNK(st.st_mode):
                    logger.warning(
                        "[TMP] Ignoring symlink in temp dir", name=path.name)
                    continue
                if not stat.S_ISREG(st.st_mode):
                    continue
                mtime = st.st_mtime
                size = st.st_size
                entries.append((path.name, mtime, size))
        except Exception:
            logger.exception("[TMP] Failed scanning temp dir")
        return entries

    @staticmethod
    def _get_mtime(path: Path) -> float:
        st = path.lstat()
        if not stat.S_ISREG(st.st_mode):
            raise ValueError(f"Path is not a regular file: {path.name}")
        return st.st_mtime

    @staticmethod
    def _get_size_bytes(path: Path) -> int:
        st = path.lstat()
        if stat.S_ISLNK(st.st_mode):
            raise ValueError(f"Path is symlink: {path.name}")
        if not stat.S_ISREG(st.st_mode):
            raise ValueError(f"Path is not a regular file: {path.name}")
        return st.st_size

    def _cleanup_lock_path(self) -> Path:
        return self._base_dir / ".tmp_cleanup.lock"

    @staticmethod
    def _encode_filename(name: str) -> str:
        return encode_filename(name)

    @staticmethod
    def _pct_encode_char(ch: str) -> str:
        return pct_encode_char(ch)

    @staticmethod
    def _is_windows_reserved_name(name: str) -> bool:
        return is_windows_reserved_name(name)

    @staticmethod
    def _escape_leading_dots(name: str) -> str:
        return escape_leading_dots(name)

    @staticmethod
    def _is_utf8(data: bytes) -> bool:
        return is_utf8(data)

    def _write_new_file(self, path: Path, data: bytes) -> None:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        fd = os.open(path, flags, 0o600)
        try:
            with os.fdopen(fd, "wb", closefd=False) as handle:
                handle.write(data)
                handle.flush()
        finally:
            os.close(fd)
        self._tighten_file_permissions(path)

    def _write_replace_file(self, path: Path, data: bytes) -> None:
        flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        fd = os.open(path, flags, 0o600)
        try:
            with os.fdopen(fd, "wb", closefd=False) as handle:
                handle.write(data)
                handle.flush()
        finally:
            os.close(fd)
        self._tighten_file_permissions(path)

    def _read_file_bytes(self, path: Path) -> bytes:
        flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            fd = os.open(path, flags)
        except OSError as exc:
            if exc.errno == errno.ELOOP:
                raise ValueError(
                    f"Refusing to read symlink temp file '{path.name}'"
                ) from exc
            raise
        try:
            with os.fdopen(fd, "rb", closefd=False) as handle:
                return handle.read()
        finally:
            os.close(fd)
