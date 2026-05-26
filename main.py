from __future__ import annotations

import sys
from pathlib import Path

from fastapiex.settings import BaseSettings, GetSettings, Settings, init_settings

SETTINGS_PATH = Path(__file__).resolve().with_name("settings.yaml")


@Settings("server")
class ServerSettings(BaseSettings):
    address: str = "0.0.0.0"
    port: int = 8000
    workers: int = 1
    loop: str = "auto"


def select_granian_loop(configured: str = "auto") -> str:
    if configured and configured != "auto":
        return configured
    return "winloop" if sys.platform == "win32" else "uvloop"


def main() -> None:
    init_settings(settings_path=SETTINGS_PATH)

    from granian import Granian

    server_settings: ServerSettings = GetSettings(ServerSettings)
    Granian(
        "app.main:app",
        address=server_settings.address,
        port=server_settings.port,
        interface="asgi",
        workers=server_settings.workers,
        loop=select_granian_loop(server_settings.loop),
    ).serve()


if __name__ == "__main__":
    main()

