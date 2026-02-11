from __future__ import annotations

from pathlib import Path

import pytest

import app.core.logger as logger_module


@pytest.mark.asyncio
async def test_logging_setup_shutdown_is_ref_counted(monkeypatch, tmp_path: Path):
    add_calls: list[tuple[tuple, dict]] = []
    remove_calls: list[tuple[tuple, dict]] = []
    configure_calls: list[tuple[tuple, dict]] = []
    complete_calls: list[int] = []
    basic_config_calls: list[tuple[tuple, dict]] = []

    monkeypatch.setattr(
        logger_module.logger,
        "add",
        lambda *a, **kw: (add_calls.append((a, kw)) or len(add_calls)),
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
    monkeypatch.setattr(logger_module, "_LOGGING_REFCOUNT", 0)
    monkeypatch.setattr(logger_module, "_LOGGING_SHUTTING_DOWN", False)
    monkeypatch.setattr(logger_module, "_LOGGING_CONFIG", None)

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

    monkeypatch.setattr(
        logger_module.logger,
        "add",
        lambda *a, **kw: (add_calls.append((a, kw)) or len(add_calls)),
    )
    monkeypatch.setattr(logger_module.logger, "remove", lambda *a, **kw: None)
    monkeypatch.setattr(logger_module.logger, "configure", lambda *a, **kw: None)
    monkeypatch.setattr(logger_module.logging, "basicConfig", lambda *a, **kw: None)
    monkeypatch.setattr(
        logger_module.logger,
        "warning",
        lambda *a, **kw: warning_calls.append((a, kw)),
    )
    monkeypatch.setattr(logger_module, "_LOGGING_REFCOUNT", 0)
    monkeypatch.setattr(logger_module, "_LOGGING_SHUTTING_DOWN", False)
    monkeypatch.setattr(logger_module, "_LOGGING_CONFIG", None)

    logger_module.setup_logging(tmp_path, debug=False)
    logger_module.setup_logging(tmp_path / "another", debug=True)

    assert logger_module._LOGGING_REFCOUNT == 2
    assert len(add_calls) == 2
    assert len(warning_calls) == 1
