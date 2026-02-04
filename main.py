from __future__ import annotations

import argparse

import uvicorn

from app.core.settings import settings


def main(args):
    uvicorn.run(
        "app:app",
        host=args.host,
        port=args.port,
        reload=settings.reload,
        proxy_headers=settings.use_proxy_headers,
        forwarded_allow_ips=(
            settings.forwarded_allow_ips if settings.use_proxy_headers else None
        ),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="FastAPI Template: Initializer")
    parser.add_argument("--host", "-H", type=str,
                        default="0.0.0.0", help="Host to run the server on")
    parser.add_argument("--port", "-p", type=int,
                        default=8000, help="Port to run the server on")
    args = parser.parse_args()

    main(args)
