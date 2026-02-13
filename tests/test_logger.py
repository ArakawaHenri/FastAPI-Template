from __future__ import annotations

import threading
from pathlib import Path

import pytest

import app.core.logger as logger_module


def _reset_logging_state(monkeypatch) -> None:
    monkeypatch.setattr(logger_module, "_LOGGING_REFCOUNT", 0)
    monkeypatch.setattr(logger_module, "_LOGGING_SHUTTING_DOWN", False)
    monkeypatch.setattr(logger_module, "_LOGGING_INITIALIZING", False)
    monkeypatch.setattr(logger_module, "_LOGGING_CONFIG", None)


@pytest.mark.asyncio
async def test_logging_setup_shutdown_is_ref_counted(monkeypatch, tmp_path: Path):
    add_calls: list[tuple[tuple, dict]] = []
    remove_calls: list[tuple[tuple, dict]] = []
    configure_calls: list[tuple[tuple, dict]] = []
    complete_calls: list[int] = []
    basic_config_calls: list[tuple[tuple, dict]] = []

    def _add_sink(*a, **kw) -> int:
        add_calls.append((a, kw))
        return len(add_calls)

    monkeypatch.setattr(
        logger_module.logger,
        "add",
        _add_sink,
    )
    monkeypatch.setattr(
        logger_module.logger,
        "remove",
        lambda *a, **kw: remove_calls.append((a, kw)),
    )
    monkeypatch.setattr(
        logger_module.logger,
        "configure",
        lambda *a, **kw: configure_calls.append((a, kw)),
    )

    async def _complete() -> None:
        complete_calls.append(1)

    monkeypatch.setattr(logger_module.logger, "complete", lambda: _complete())
    monkeypatch.setattr(
        logger_module.logging,
        "basicConfig",
        lambda *a, **kw: basic_config_calls.append((a, kw)),
    )
    _reset_logging_state(monkeypatch)

    logger_module.setup_logging(tmp_path, debug=False)
    logger_module.setup_logging(tmp_path, debug=False)

    # First setup performs one remove + two add calls (console/file sinks).
    assert len(remove_calls) == 1
    assert len(add_calls) == 2
    assert len(configure_calls) == 1
    assert len(basic_config_calls) == 1
    assert logger_module._LOGGING_REFCOUNT == 2

    # First shutdown only decrements refcount.
    await logger_module.shutdown_logging()
    assert len(complete_calls) == 0
    assert len(remove_calls) == 1
    assert logger_module._LOGGING_REFCOUNT == 1

    # Last shutdown performs flush/remove once.
    await logger_module.shutdown_logging()
    assert len(complete_calls) == 1
    assert len(remove_calls) == 2
    assert logger_module._LOGGING_REFCOUNT == 0
    assert logger_module._LOGGING_CONFIG is None


def test_logging_setup_reuses_existing_config(monkeypatch, tmp_path: Path):
    add_calls: list[tuple[tuple, dict]] = []
    warning_calls: list[tuple[tuple, dict]] = []

    def _add_sink(*a, **kw) -> int:
        add_calls.append((a, kw))
        return len(add_calls)

    monkeypatch.setattr(
        logger_module.logger,
        "add",
        _add_sink,
    )
    monkeypatch.setattr(logger_module.logger, "remove", lambda *a, **kw: None)
    monkeypatch.setattr(logger_module.logger, "configure", lambda *a, **kw: None)
    monkeypatch.setattr(logger_module.logging, "basicConfig", lambda *a, **kw: None)
    monkeypatch.setattr(
        logger_module.logger,
        "warning",
        lambda *a, **kw: warning_calls.append((a, kw)),
    )
    _reset_logging_state(monkeypatch)

    logger_module.setup_logging(tmp_path, debug=False)
    logger_module.setup_logging(tmp_path / "another", debug=True)

    assert logger_module._LOGGING_REFCOUNT == 2
    assert len(add_calls) == 2
    assert len(warning_calls) == 1


def test_logging_setup_failure_rolls_back_state(monkeypatch, tmp_path: Path):
    add_calls = 0

    def flaky_add(*_args, **_kwargs):
        nonlocal add_calls
        add_calls += 1
        if add_calls == 2:
            raise RuntimeError("sink setup failed")
        return add_calls

    monkeypatch.setattr(logger_module.logger, "add", flaky_add)
    monkeypatch.setattr(logger_module.logger, "remove", lambda *a, **kw: None)
    monkeypatch.setattr(logger_module.logger, "configure", lambda *a, **kw: None)
    monkeypatch.setattr(logger_module.logging, "basicConfig", lambda *a, **kw: None)
    _reset_logging_state(monkeypatch)

    with pytest.raises(RuntimeError, match="sink setup failed"):
        logger_module.setup_logging(tmp_path, debug=False)

    assert logger_module._LOGGING_REFCOUNT == 0
    assert logger_module._LOGGING_INITIALIZING is False
    assert logger_module._LOGGING_CONFIG is None

    monkeypatch.setattr(
        logger_module.logger,
        "add",
        lambda *a, **kw: 1,
    )
    logger_module.setup_logging(tmp_path, debug=False)
    assert logger_module._LOGGING_REFCOUNT == 1


def test_logging_setup_blocks_during_initialization(monkeypatch, tmp_path: Path):
    start_add = threading.Event()
    release_add = threading.Event()
    first_done = threading.Event()
    second_done = threading.Event()
    second_started = threading.Event()
    errors: list[BaseException] = []

    add_calls = 0

    def blocking_add(*_args, **_kwargs):
        nonlocal add_calls
        add_calls += 1
        if add_calls == 1:
            start_add.set()
            release_add.wait(timeout=2)
        return add_calls

    monkeypatch.setattr(logger_module.logger, "add", blocking_add)
    monkeypatch.setattr(logger_module.logger, "remove", lambda *a, **kw: None)
    monkeypatch.setattr(logger_module.logger, "configure", lambda *a, **kw: None)
    monkeypatch.setattr(logger_module.logging, "basicConfig", lambda *a, **kw: None)
    _reset_logging_state(monkeypatch)

    def first_setup():
        try:
            logger_module.setup_logging(tmp_path, debug=False)
        except BaseException as exc:  # pragma: no cover - defensive
            errors.append(exc)
        finally:
            first_done.set()

    def second_setup():
        second_started.set()
        try:
            logger_module.setup_logging(tmp_path, debug=False)
        except BaseException as exc:  # pragma: no cover - defensive
            errors.append(exc)
        finally:
            second_done.set()

    t1 = threading.Thread(target=first_setup)
    t2 = threading.Thread(target=second_setup)
    t1.start()
    assert start_add.wait(timeout=1.0)

    t2.start()
    assert second_started.wait(timeout=1.0)
    assert second_done.wait(timeout=0.1) is False

    release_add.set()

    assert first_done.wait(timeout=1.0)
    assert second_done.wait(timeout=1.0)
    t1.join(timeout=1.0)
    t2.join(timeout=1.0)
    assert not errors
    assert logger_module._LOGGING_REFCOUNT == 2
