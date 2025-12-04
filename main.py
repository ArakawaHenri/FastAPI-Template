import argparse

import uvicorn


def main(args):
    uvicorn.run("app:app", host=args.host, port=args.port, reload=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Transaction Classifier: Initializer")
    parser.add_argument("--host", "-H", type=str,
                        default="0.0.0.0", help="Host to run the server on")
    parser.add_argument("--port", "-p", type=int,
                        default=8000, help="Port to run the server on")
    args = parser.parse_args()

    main(args)
