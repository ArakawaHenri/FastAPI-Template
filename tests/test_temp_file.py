from __future__ import annotations

import asyncio
import os
import stat
import threading
import time
from pathlib import Path

import pytest

from app.services.store.main import StoreConfig, StoreService
from app.services.temp_file.main import TempFileConfig, TempFileService


def _make_store(tmp_path: Path) -> StoreService:
    return StoreService(
        StoreConfig(
            path=str(tmp_path / "store_lmdb"),
            map_size_mb=16,
            map_size_growth_factor=2,
            map_high_watermark=0.9,
            max_dbs=128,
            max_readers=256,
            sync=False,
            metasync=False,
            writemap=True,
            map_async=True,
            max_key_bytes=256,
            max_namespace_bytes=256,
            max_value_bytes=4 * 1024 * 1024,
            cleanup_max_deletes=10_000,
            worker_threads=2,
        )
    )


async def _make_service(
    tmp_path: Path,
    retention_days: int = 1,
    max_file_size_mb: int = 1024,
    max_total_size_mb: int = 0,
) -> tuple[TempFileService, StoreService]:
    store = _make_store(tmp_path)
    service = TempFileService(
        TempFileConfig(
            base_dir=str(tmp_path / "tmp_files"),
            retention_days=retention_days,
            cleanup_interval_seconds=60,
            worker_threads=2,
            max_file_size_mb=max_file_size_mb,
            max_total_size_mb=max_total_size_mb,
        ),
        store,
    )
    await service._bootstrap()
    return service, store


@pytest.mark.asyncio
async def test_temp_file_callback_name_is_namespace_scoped(tmp_path: Path):
    store = _make_store(tmp_path)
    service1 = TempFileService(
        TempFileConfig(
            base_dir=str(tmp_path / "tmp_files_1"),
            retention_days=1,
            cleanup_interval_seconds=60,
            worker_threads=2,
            namespace="tmp_files_1",
        ),
        store,
    )
    service2 = TempFileService(
        TempFileConfig(
            base_dir=str(tmp_path / "tmp_files_2"),
            retention_days=1,
            cleanup_interval_seconds=60,
            worker_threads=2,
            namespace="tmp_files_2",
        ),
        store,
    )

    await service1._bootstrap()
    await service2._bootstrap()

    assert service1._callback_name != service2._callback_name
    assert service1._callback_name.startswith("tmp_file_cleanup:")
    assert service2._callback_name.startswith("tmp_file_cleanup:")
    assert service1._callback_name in store._callback_registry
    assert service2._callback_name in store._callback_registry

    await service2.shutdown()
    await service1.shutdown()
    await store.shutdown()


def test_temp_file_callback_name_is_stable_for_same_namespace():
    assert (
        TempFileService._build_callback_name("tmp_files")
        == TempFileService._build_callback_name("tmp_files")
    )
    assert (
        TempFileService._build_callback_name("tmp_files")
        != TempFileService._build_callback_name("tmp_files_2")
    )


@pytest.mark.asyncio
async def test_temp_file_lifespan_ctor_reuses_shared_instance_and_capacity(tmp_path: Path):
    store_path = str(tmp_path / "store_lmdb")
    store1 = await StoreService.LifespanTasks.ctor(path=store_path, map_size_mb=16)
    store2 = await StoreService.LifespanTasks.ctor(path=store_path, map_size_mb=16)
    assert store1 is store2

    service1 = await TempFileService.LifespanTasks.ctor(
        base_dir=str(tmp_path / "tmp_files"),
        retention_days=1,
        cleanup_interval_seconds=60,
        total_size_recalc_seconds=60,
        worker_threads=2,
        max_file_size_mb=0,
        max_total_size_mb=1,
        store_provider=lambda: store1,
    )
    service2 = await TempFileService.LifespanTasks.ctor(
        base_dir=str(tmp_path / "tmp_files"),
        retention_days=1,
        cleanup_interval_seconds=60,
        total_size_recalc_seconds=60,
        worker_threads=2,
        max_file_size_mb=0,
        max_total_size_mb=1,
        store_provider=lambda: store2,
    )
    assert service1 is service2

    await service1.save("a.bin", b"x" * (800 * 1024))
    with pytest.raises(ValueError, match="total size exceeds max"):
        await service2.save("b.bin", b"x" * (400 * 1024))

    await TempFileService.LifespanTasks.dtor(service1)
    assert not service1._closed
    await TempFileService.LifespanTasks.dtor(service2)
    assert service1._closed

    await StoreService.LifespanTasks.dtor(store1)
    assert not store1._closed
    await StoreService.LifespanTasks.dtor(store2)
    assert store1._closed


@pytest.mark.asyncio
async def test_temp_file_lifespan_ctor_rejects_shared_config_mismatch(tmp_path: Path):
    store = await StoreService.LifespanTasks.ctor(
        path=str(tmp_path / "store_lmdb"),
        map_size_mb=16,
    )
    service = await TempFileService.LifespanTasks.ctor(
        base_dir=str(tmp_path / "tmp_files"),
        retention_days=1,
        cleanup_interval_seconds=60,
        total_size_recalc_seconds=60,
        worker_threads=2,
        max_file_size_mb=0,
        max_total_size_mb=1,
        store_provider=lambda: store,
    )
    try:
        with pytest.raises(RuntimeError, match="config mismatch"):
            await TempFileService.LifespanTasks.ctor(
                base_dir=str(tmp_path / "tmp_files"),
                retention_days=1,
                cleanup_interval_seconds=60,
                total_size_recalc_seconds=60,
                worker_threads=2,
                max_file_size_mb=0,
                max_total_size_mb=2,
                store_provider=lambda: store,
            )
    finally:
        await TempFileService.LifespanTasks.dtor(service)
        await StoreService.LifespanTasks.dtor(store)


@pytest.mark.asyncio
async def test_temp_file_lifespan_ctor_waits_for_shared_bootstrap(tmp_path: Path, monkeypatch):
    store = await StoreService.LifespanTasks.ctor(
        path=str(tmp_path / "store_lmdb"),
        map_size_mb=16,
    )
    started = asyncio.Event()
    release = asyncio.Event()
    bootstrap_calls = 0
    original_bootstrap = TempFileService._bootstrap

    async def delayed_bootstrap(self: TempFileService) -> None:
        nonlocal bootstrap_calls
        if not self._is_runtime_thread():
            bootstrap_calls += 1
            started.set()
            await release.wait()
        await original_bootstrap(self)

    monkeypatch.setattr(TempFileService, "_bootstrap", delayed_bootstrap)

    task1 = asyncio.create_task(
        TempFileService.LifespanTasks.ctor(
            base_dir=str(tmp_path / "tmp_files"),
            retention_days=1,
            cleanup_interval_seconds=60,
            total_size_recalc_seconds=60,
            worker_threads=2,
            max_file_size_mb=0,
            max_total_size_mb=1,
            store_provider=lambda: store,
        )
    )
    await asyncio.wait_for(started.wait(), timeout=2)

    task2 = asyncio.create_task(
        TempFileService.LifespanTasks.ctor(
            base_dir=str(tmp_path / "tmp_files"),
            retention_days=1,
            cleanup_interval_seconds=60,
            total_size_recalc_seconds=60,
            worker_threads=2,
            max_file_size_mb=0,
            max_total_size_mb=1,
            store_provider=lambda: store,
        )
    )
    await asyncio.sleep(0.05)
    assert not task2.done()

    release.set()
    service1 = await task1
    service2 = await task2
    assert service1 is service2
    assert bootstrap_calls == 1

    await TempFileService.LifespanTasks.dtor(service1)
    await TempFileService.LifespanTasks.dtor(service2)
    await StoreService.LifespanTasks.dtor(store)


@pytest.mark.asyncio
async def test_temp_file_lifespan_ctor_recovers_after_bootstrap_failure(tmp_path: Path, monkeypatch):
    store = await StoreService.LifespanTasks.ctor(
        path=str(tmp_path / "store_lmdb"),
        map_size_mb=16,
    )
    original_bootstrap = TempFileService._bootstrap

    async def failing_bootstrap(self: TempFileService) -> None:
        raise RuntimeError("bootstrap failed")

    monkeypatch.setattr(TempFileService, "_bootstrap", failing_bootstrap)
    with pytest.raises(RuntimeError, match="bootstrap failed"):
        await TempFileService.LifespanTasks.ctor(
            base_dir=str(tmp_path / "tmp_files"),
            retention_days=1,
            cleanup_interval_seconds=60,
            total_size_recalc_seconds=60,
            worker_threads=2,
            max_file_size_mb=0,
            max_total_size_mb=1,
            store_provider=lambda: store,
        )

    monkeypatch.setattr(TempFileService, "_bootstrap", original_bootstrap)
    service = await TempFileService.LifespanTasks.ctor(
        base_dir=str(tmp_path / "tmp_files"),
        retention_days=1,
        cleanup_interval_seconds=60,
        total_size_recalc_seconds=60,
        worker_threads=2,
        max_file_size_mb=0,
        max_total_size_mb=1,
        store_provider=lambda: store,
    )
    await TempFileService.LifespanTasks.dtor(service)
    await StoreService.LifespanTasks.dtor(store)


@pytest.mark.asyncio
async def test_temp_file_lifespan_ctor_does_not_deadlock_when_creator_is_cancelled(
    tmp_path: Path,
    monkeypatch,
):
    store = await StoreService.LifespanTasks.ctor(
        path=str(tmp_path / "store_lmdb"),
        map_size_mb=16,
    )
    started = asyncio.Event()
    blocker = asyncio.Event()
    original_bootstrap = TempFileService._bootstrap

    async def blocking_bootstrap(self: TempFileService) -> None:
        if not self._is_runtime_thread():
            started.set()
            await blocker.wait()
        await original_bootstrap(self)

    monkeypatch.setattr(TempFileService, "_bootstrap", blocking_bootstrap)

    task1 = asyncio.create_task(
        TempFileService.LifespanTasks.ctor(
            base_dir=str(tmp_path / "tmp_files"),
            retention_days=1,
            cleanup_interval_seconds=60,
            total_size_recalc_seconds=60,
            worker_threads=2,
            max_file_size_mb=0,
            max_total_size_mb=1,
            store_provider=lambda: store,
        )
    )
    await asyncio.wait_for(started.wait(), timeout=2)

    task2 = asyncio.create_task(
        TempFileService.LifespanTasks.ctor(
            base_dir=str(tmp_path / "tmp_files"),
            retention_days=1,
            cleanup_interval_seconds=60,
            total_size_recalc_seconds=60,
            worker_threads=2,
            max_file_size_mb=0,
            max_total_size_mb=1,
            store_provider=lambda: store,
        )
    )
    await asyncio.sleep(0.05)
    task1.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task1
    with pytest.raises(RuntimeError, match="failed to initialize"):
        await asyncio.wait_for(task2, timeout=2)

    monkeypatch.setattr(TempFileService, "_bootstrap", original_bootstrap)
    service = await TempFileService.LifespanTasks.ctor(
        base_dir=str(tmp_path / "tmp_files"),
        retention_days=1,
        cleanup_interval_seconds=60,
        total_size_recalc_seconds=60,
        worker_threads=2,
        max_file_size_mb=0,
        max_total_size_mb=1,
        store_provider=lambda: store,
    )
    await TempFileService.LifespanTasks.dtor(service)
    await StoreService.LifespanTasks.dtor(store)


@pytest.mark.asyncio
async def test_save_and_read_text(tmp_path: Path):
    service, store = await _make_service(tmp_path)
    name = await service.save("note.txt", "hello")
    assert name == "note.txt"
    data = await service.read("note.txt")
    assert data == "hello"
    await service.shutdown()
    await store.shutdown()


@pytest.mark.asyncio
async def test_save_and_read_binary(tmp_path: Path):
    service, store = await _make_service(tmp_path)
    payload = b"\x00\x01\x02"
    name = await service.save("blob.bin", payload)
    assert name == "blob.bin"
    data = await service.read("blob.bin")
    assert isinstance(data, (bytes, bytearray))
    assert data == payload
    await service.shutdown()
    await store.shutdown()


@pytest.mark.asyncio
async def test_save_rejects_oversized_payload(tmp_path: Path):
    service, store = await _make_service(tmp_path, max_file_size_mb=1)
    payload = b"x" * (2 * 1024 * 1024)
    with pytest.raises(ValueError, match="exceeds max size"):
        await service.save("too_large.bin", payload)
    await service.shutdown()
    await store.shutdown()


@pytest.mark.asyncio
async def test_save_allows_large_payload_when_file_limit_zero(tmp_path: Path):
    service, store = await _make_service(tmp_path, max_file_size_mb=0)
    payload = b"x" * (2 * 1024 * 1024)
    name = await service.save("large.bin", payload)
    assert name == "large.bin"
    assert await service.read("large.bin") == payload
    await service.shutdown()
    await store.shutdown()


@pytest.mark.asyncio
async def test_read_rejects_oversized_file(tmp_path: Path):
    service, store = await _make_service(tmp_path, max_file_size_mb=1)
    raw_path = Path(service._config.base_dir) / "big.bin"
    raw_path.write_bytes(b"x" * (2 * 1024 * 1024))
    with pytest.raises(ValueError, match="exceeds max size"):
        await service.read("big.bin")
    await service.shutdown()
    await store.shutdown()


@pytest.mark.asyncio
async def test_total_size_cap_rejects_write_but_service_keeps_running(tmp_path: Path):
    service, store = await _make_service(
        tmp_path,
        max_file_size_mb=0,
        max_total_size_mb=1,
    )

    await service.save("a.bin", b"x" * (800 * 1024))
    with pytest.raises(ValueError, match="total size exceeds max"):
        await service.save("b.bin", b"x" * (400 * 1024))

    # Service remains operational after rejection.
    await service.save("c.bin", b"x" * (100 * 1024))
    assert isinstance(await service.read("c.bin"), (bytes, bytearray))

    await service.shutdown()
    await store.shutdown()


@pytest.mark.asyncio
async def test_duplicate_filename_suffix(tmp_path: Path):
    service, store = await _make_service(tmp_path)
    name1 = await service.save("dup.txt", "a")
    name2 = await service.save("dup.txt", "b")
    name3 = await service.save("dup.txt", "c")

    assert name1 == "dup.txt"
    assert name2 == "dup.1.txt"
    assert name3 == "dup.2.txt"

    assert await service.read("dup.txt") == "a"
    assert await service.read("dup.1.txt") == "b"
    assert await service.read("dup.2.txt") == "c"
    await service.shutdown()
    await store.shutdown()


@pytest.mark.asyncio
async def test_leading_dot_is_escaped(tmp_path: Path):
    service, store = await _make_service(tmp_path)
    name = await service.save(".env", "secret")
    assert name.startswith("%2E")

    # Read using the original name; sanitize should map to the stored filename
    data = await service.read(".env")
    assert data == "secret"

    on_disk = Path(service._config.base_dir) / name
    assert on_disk.exists()
    await service.shutdown()
    await store.shutdown()


@pytest.mark.asyncio
async def test_temp_dir_and_files_permissions_are_restricted(tmp_path: Path):
    if os.name != "posix":
        pytest.skip("permission mode checks require posix")

    service, store = await _make_service(tmp_path)
    name = await service.save("perm.txt", "secret")

    dir_mode = stat.S_IMODE(Path(service._config.base_dir).stat().st_mode)
    file_mode = stat.S_IMODE((Path(service._config.base_dir) / name).stat().st_mode)
    assert dir_mode & 0o077 == 0
    assert file_mode & 0o077 == 0

    await service.shutdown()
    await store.shutdown()


@pytest.mark.asyncio
async def test_read_symlink_is_rejected(tmp_path: Path):
    service, store = await _make_service(tmp_path)
    target = tmp_path / "outside.txt"
    target.write_text("outside", encoding="utf-8")
    link = Path(service._config.base_dir) / "link.txt"
    try:
        os.symlink(target, link)
    except (NotImplementedError, OSError):
        pytest.skip("symlink not supported on this platform")

    warnings: list[str] = []
    from loguru import logger
    sink_id = logger.add(
        lambda msg: warnings.append(msg.record["message"]),
        level="WARNING",
    )
    try:
        assert await service.read("link.txt") is None
    finally:
        logger.remove(sink_id)

    assert any("Refusing to read symlink temp file" in msg for msg in warnings)

    await service.shutdown()
    await store.shutdown()


@pytest.mark.asyncio
async def test_overwrite_symlink_replaces_with_regular_file(tmp_path: Path):
    service, store = await _make_service(tmp_path)
    target = tmp_path / "outside.txt"
    target.write_text("outside", encoding="utf-8")
    link = Path(service._config.base_dir) / "swap.txt"
    try:
        os.symlink(target, link)
    except (NotImplementedError, OSError):
        pytest.skip("symlink not supported on this platform")

    await service.save_overwrite("swap.txt", "inside")
    path = Path(service._config.base_dir) / "swap.txt"
    assert not path.is_symlink()
    assert await service.read("swap.txt") == "inside"
    assert target.read_text(encoding="utf-8") == "outside"

    await service.shutdown()
    await store.shutdown()


@pytest.mark.asyncio
async def test_overwrite_directory_symlink_replaces_with_regular_file(tmp_path: Path):
    service, store = await _make_service(tmp_path)
    target_dir = tmp_path / "outside_dir"
    target_dir.mkdir()
    link = Path(service._config.base_dir) / "swap_dir.txt"
    try:
        os.symlink(target_dir, link)
    except (NotImplementedError, OSError):
        pytest.skip("symlink not supported on this platform")

    await service.save_overwrite("swap_dir.txt", "inside")
    path = Path(service._config.base_dir) / "swap_dir.txt"
    assert not path.is_symlink()
    assert await service.read("swap_dir.txt") == "inside"
    assert target_dir.exists() and target_dir.is_dir()

    await service.shutdown()
    await store.shutdown()


@pytest.mark.asyncio
async def test_save_overwrite_same_name_concurrent_keeps_single_target_file(
    tmp_path: Path,
):
    service, store = await _make_service(tmp_path)

    first = threading.Event()
    proceed = threading.Event()
    original_write_staging = service._write_staging_file

    def delayed_write_staging(staging_name: str, data: bytes, now: float) -> None:
        if not first.is_set():
            first.set()
            proceed.wait(timeout=2)
        original_write_staging(staging_name, data, now)

    service._write_staging_file = delayed_write_staging  # type: ignore[method-assign]

    async def writer(payload: str) -> str:
        return await service.save_overwrite("race.txt", payload)

    t1 = asyncio.create_task(writer("one"))
    await asyncio.wait_for(asyncio.to_thread(first.wait, 2), timeout=3)
    t2 = asyncio.create_task(writer("two"))
    proceed.set()
    results = await asyncio.gather(t1, t2)

    assert results == ["race.txt", "race.txt"]
    base_dir = Path(service._config.base_dir)
    user_files = sorted(
        p.name
        for p in base_dir.iterdir()
        if not TempFileService._is_internal_artifact_name(p.name)
    )
    assert user_files == ["race.txt"]
    assert await service.read("race.txt") in {"one", "two"}

    await service.shutdown()
    await store.shutdown()


@pytest.mark.asyncio
async def test_save_overwrite_is_not_deleted_by_stale_expiry_callback(tmp_path: Path):
    service, store = await _make_service(tmp_path)
    callback_lock = StoreService._try_acquire_file_lock(
        tmp_path / "store_lmdb" / ".store_callbacks.lock"
    )
    assert callback_lock is not None

    try:
        await service.save_overwrite("same.txt", "old")
        await store.cleanup_expired(now=time.time() + 2 * 24 * 60 * 60)
        await service.save_overwrite("same.txt", "new")
    finally:
        StoreService._release_file_lock(callback_lock)

    await asyncio.wait_for(store.wait_for_callbacks(), timeout=5)
    assert await service.read("same.txt") == "new"

    await service.shutdown()
    await store.shutdown()


@pytest.mark.asyncio
async def test_save_overwrite_metadata_failure_restores_previous_file(
    tmp_path: Path, monkeypatch
):
    service, store = await _make_service(tmp_path)
    await service.save_overwrite("same.txt", "old")

    original_set_metadata = service._set_metadata
    failed_once = False

    async def fail_once(*args, **kwargs):
        nonlocal failed_once
        if not failed_once:
            failed_once = True
            raise RuntimeError("metadata down")
        return await original_set_metadata(*args, **kwargs)

    monkeypatch.setattr(service, "_set_metadata", fail_once)

    with pytest.raises(RuntimeError, match="metadata down"):
        await service.save_overwrite("same.txt", "new")

    assert await service.read("same.txt") == "old"

    await service.shutdown()
    await store.shutdown()


@pytest.mark.asyncio
async def test_internal_artifacts_are_not_adopted_and_stale_artifacts_are_cleaned(
    tmp_path: Path,
):
    service, store = await _make_service(tmp_path)

    fresh_name = service._new_staging_name("fresh.txt")
    stale_name = service._new_backup_name("stale.txt")
    now = time.time()
    stale_time = now - TempFileService.INTERNAL_ARTIFACT_RETENTION_SECONDS - 10

    await service._run_in_executor(
        service._write_staging_file, fresh_name, b"fresh", now
    )
    await service._run_in_executor(
        service._write_staging_file, stale_name, b"stale", stale_time
    )

    await service.shutdown()
    await store.shutdown()

    service2, store2 = await _make_service(tmp_path)
    base_dir = Path(service2._config.base_dir)

    assert (base_dir / fresh_name).exists()
    assert not (base_dir / stale_name).exists()

    async with store2.create_namespace_lock(service2._namespace):
        assert await store2.get(service2._namespace, fresh_name) is None

    await service2.cleanup_expired(
        now=time.time() + TempFileService.INTERNAL_ARTIFACT_RETENTION_SECONDS + 20
    )
    assert not (base_dir / fresh_name).exists()

    await service2.shutdown()
    await store2.shutdown()


@pytest.mark.asyncio
async def test_cleanup_expired_removes_files(tmp_path: Path):
    service, store = await _make_service(tmp_path, retention_days=1)
    name = await service.save("old.txt", "stale")
    path = Path(service._config.base_dir) / name

    # Simulate time passing beyond retention
    await store.cleanup_expired(now=time.time() + 2 * 24 * 60 * 60)
    await store.wait_for_callbacks()

    assert not path.exists()
    await service.shutdown()
    await store.shutdown()


@pytest.mark.asyncio
async def test_cleanup_untracked_files(tmp_path: Path):
    service, store = await _make_service(tmp_path, retention_days=1)
    base_dir = Path(service._config.base_dir)

    # Create an untracked file with old mtime
    untracked = base_dir / "stale.bin"
    untracked.write_bytes(b"x")
    old = time.time() - 2 * 24 * 60 * 60
    os.utime(untracked, (old, old))

    await service.cleanup_expired(now=time.time())

    assert not untracked.exists()
    await service.shutdown()
    await store.shutdown()


@pytest.mark.asyncio
async def test_recalculate_total_size_now_repairs_counter(tmp_path: Path):
    service, store = await _make_service(tmp_path)
    name = await service.save("size.bin", b"x" * 128)
    file_size = (Path(service._config.base_dir) / name).lstat().st_size

    # Simulate counter drift and ensure periodic recalculation API self-heals it.
    service._total_size_bytes = 0
    await service._recalculate_total_size_now()

    assert service._get_total_size_bytes() == file_size
    await service.shutdown()
    await store.shutdown()


@pytest.mark.asyncio
async def test_total_size_recalc_loop_starts_only_with_total_limit(tmp_path: Path):
    service1, store1 = await _make_service(tmp_path / "no_limit", max_total_size_mb=0)
    assert service1._size_recalc_task is None
    await service1.shutdown()
    await store1.shutdown()

    service2, store2 = await _make_service(tmp_path / "with_limit", max_total_size_mb=1)
    assert service2._size_recalc_task is not None
    await service2.shutdown()
    await store2.shutdown()


@pytest.mark.asyncio
async def test_bootstrap_cleanup_reclaims_total_size_counter(tmp_path: Path):
    service, store = await _make_service(tmp_path, retention_days=1, max_total_size_mb=1)
    base_dir = Path(service._config.base_dir)

    # Create an untracked stale file that should be removed during bootstrap.
    stale = base_dir / "stale.bin"
    stale.write_bytes(b"x" * (800 * 1024))
    old = time.time() - 2 * 24 * 60 * 60
    os.utime(stale, (old, old))

    await service.shutdown()
    await store.shutdown()

    service2, store2 = await _make_service(tmp_path, retention_days=1, max_total_size_mb=1)
    assert not stale.exists()

    saved = await service2.save("new.bin", b"y" * (500 * 1024))
    assert saved == "new.bin"

    await service2.shutdown()
    await store2.shutdown()
