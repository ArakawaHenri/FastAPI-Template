from __future__ import annotations

import argparse
import os

from app.core.settings import settings


def main(args) -> None:
    command = [
        "granian",
        "--interface",
        "asgi",
        "--host",
        args.host,
        "--port",
        str(args.port),
        "--workers",
        str(args.workers),
    ]
    if settings.reload:
        command.append("--reload")
    command.append("app.main:app")
    os.execvp(command[0], command)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="FastAPI Template: Granian runner")
    parser.add_argument(
        "--host",
        "-H",
        type=str,
        default="0.0.0.0",
        help="Host to run the server on",
    )
    parser.add_argument(
        "--port",
        "-p",
        type=int,
        default=8000,
        help="Port to run the server on",
    )
    parser.add_argument(
        "--workers",
        "-w",
        type=int,
        default=1,
        help="Number of worker processes",
    )
    args = parser.parse_args()

    main(args)
