import argparse

import uvicorn

from app.core.settings import settings


def main(args):
    uvicorn.run(
        "app:app",
        host=args.host,
        port=args.port,
        reload=settings.RELOAD,
        proxy_headers=settings.USE_PROXY_HEADERS,
        forwarded_allow_ips=(
            settings.FORWARDED_ALLOW_IPS if settings.USE_PROXY_HEADERS else None
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
