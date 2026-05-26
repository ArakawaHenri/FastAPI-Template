# FastAPI Template

Production-ready FastAPI service template built around `fastapiex-settings` and `fastapiex-di`.

This repository is intentionally small, but includes the pieces most FastAPI services repeatedly need:

- typed YAML/env settings through `fastapiex-settings`
- service discovery, eager startup, and shutdown cleanup through `fastapiex-di`
- Loguru logging with request-id propagation and stdlib logging interception
- unified exception responses for validation, HTTP, business, and unexpected errors
- zlmdb-backed KV store with namespaces, TTL, `set_if_absent`, expiry index, and cleanup locking
- temporary-file manager with buckets, atomic overwrites, unique saves, retention cleanup, and text/binary reads
- SQLAlchemy async database engines and transactional `AsyncSession` dependency
- OpenAI async clients with internal per-client semaphore limits and guarded streaming calls

## Requirements

- Python >= 3.12
- Package manager: `uv`

## Quick Start

Install dependencies:

```bash
uv sync
```

Create a local settings file:

```bash
cp settings.yaml.example settings.yaml
```

Run the service:

```bash
uv run python main.py
```

The default server binds to `0.0.0.0:8000`.

Useful endpoints:

- `GET /v1/healthz`
- `GET /v1/diagnostics/dependencies`
- `PUT /v1/kv/{namespace}/{key}`
- `GET /v1/kv/{namespace}/{key}`
- `POST /v1/temp-files/{bucket}/{filename}`

Run tests:

```bash
uv run pytest
```

## Runtime

`main.py` initializes settings from the local `settings.yaml`, reads the `server` section, and starts Granian:

```yaml
server:
  address: 0.0.0.0
  port: 8000
  workers: 1
  loop: auto
```

`loop: auto` selects `uvloop` on Unix-like systems and `winloop` on Windows.

`settings.yaml` is intentionally ignored by git. Use `settings.yaml.example` as the committed reference and keep local or deployment-specific values in ignored settings files.

To use another settings file:

```bash
FASTAPIEX__SETTINGS__PATH=/path/to/settings.yaml uv run python main.py
```

## Project Layout

```text
.
├── app/
│   ├── api/                  # routers and API schemas
│   ├── core/                 # process-wide logging
│   ├── middleware/           # request logging and exception handlers
│   ├── service/
│   │   ├── database/         # SQLAlchemy async engines and session dependency
│   │   ├── openai_client/    # AsyncOpenAI clients with internal semaphore proxying
│   │   ├── shared/           # file-lock and periodic-cleanup helpers
│   │   ├── store/            # zlmdb KV store
│   │   └── temp_file/        # temporary-file manager
│   └── main.py               # FastAPI app factory
├── tests/                    # unit and integration tests
├── main.py                   # Granian runner
├── settings.yaml.example     # committed settings example
├── pyproject.toml
└── uv.lock
```

## Configuration

Settings are declared with `@Settings` and `@SettingsMap` and loaded by `fastapiex-settings`.

Only `settings.yaml.example` is committed. Copy it to `settings.yaml` for local development, or point `FASTAPIEX__SETTINGS__PATH` at a deployment-specific YAML file. Do not commit real secrets or environment-specific runtime paths.

### App

```yaml
app:
  title: FastAPI Template
  version: 0.1.0
  description: Production-ready FastAPI template with FastAPIEx DI/settings.
  debug_mode: false
  log_dir: ./logs
  cors_origins:
    - https://example.com
```

- `debug_mode`: includes request path and extra validation details in error responses.
- `log_dir`: created automatically.
- `cors_origins`: empty list disables CORS; `["*"]` allows all origins and disables credentials.

### Store

```yaml
store:
  path: ./data/store.zlmdb
  max_size_mb: 1024
  sync: false
  writemap: true
  max_key_bytes: 1024
  max_namespace_bytes: 256
  max_value_bytes: 104857600
  cleanup_interval_seconds: 60
  cleanup_max_deletes: 100000
```

`StoreService` is registered as `store_service` and starts eagerly. It provides:

- `set(namespace, key, value, retention_seconds=None)`
- `set_if_absent(namespace, key, value, retention_seconds=None)`
- `get(namespace, key)`
- `delete(namespace, key)`
- `cleanup_expired()`
- `count(namespace=None)`
- `stats(include_slots=True)`
- `sync(force=True)`

Values are stored through zlmdb's CBOR map. Expiry uses a zlmdb-managed ordered secondary index, so cleanup does not need to scan payload records and overwritten TTL entries do not leave stale expiry rows.

### Temporary Files

```yaml
temp_file:
  base_dir: ./temp
  retention_days: 7
  cleanup_interval_seconds: 60
  buckets:
    - logs
    - uploads
    - exports
    - scratch
```

`TempFileService` is registered as `temp_file_service` and starts eagerly. It supports:

- `save(filename, content, bucket="logs")`
- `save_overwrite(filename, content, bucket="logs")`
- `read(filename, bucket="logs")`
- `path_for(filename, bucket="logs")`
- `cleanup_expired()`

Names are sanitized. Overwrites are atomic: content is written to a temp file, fsynced, then moved into place.

### Database

```yaml
database:
  main:
    url: sqlite+aiosqlite:///./data/app.sqlite3
    echo: false
    pool_size: 5
    max_overflow: 10
    pool_recycle: 3600
    pool_pre_ping: true
```

Each entry under `database` becomes a service named `{key}_database_service`.

Example:

```python
from typing import Annotated

from fastapiex.di import Inject
from sqlalchemy.ext.asyncio import AsyncSession


async def handler(
    session: Annotated[
        AsyncSession,
        Inject(AsyncSession, Inject("main_database_service")),
    ],
):
    ...
```

`DatabaseSessionServiceT` is transient and commits on success, rolls back on exception.

### OpenAI Clients

```yaml
openai_clients:
  completions:
    api_key: sk-placeholder
    base_url: ~
    timeout: 60.0
    organization: ~
    model: gpt-4o-mini
    concurrency_limit: 20
```

Each entry under `openai_clients` becomes a service named `{key}_openai_client_service`.

OpenAI concurrency is owned by the client itself. There is no separate semaphore service to configure or keep in sync.

```python
from typing import Annotated

from fastapiex.di import Inject
from app.service.openai_client.main import AsyncOpenAIClientService


async def handler(
    client: Annotated[
        AsyncOpenAIClientService,
        Inject("completions_openai_client_service"),
    ],
):
    result = await client.chat.completions.create(
        model=client.model,
        messages=[{"role": "user", "content": "Hello"}],
    )
```

Nested async SDK calls are proxied through the internal semaphore. Streaming results keep the semaphore held until the async stream is consumed or closed.

## Dependency Injection Conventions

Services live under `app/service` and are discovered by:

```python
install_di(app, service_packages=["app.service"])
```

Conventions:

- singleton service class names end with `Service`
- transient service class names end with `ServiceT`
- register keyed singletons with `@Service("key")`
- register keyed maps with `@ServiceMap("{}_suffix", mapping=lambda: GetSettingsMap("section"))`
- use `eager=True` for infrastructure services that must start before serving traffic
- use `Inject("key")` for explicit service lookup
- use type injection only when there is one unambiguous provider for the type

Do not import service modules from `app/main.py` before `install_di()`. Let DI discovery import them during startup.

## API and Error Handling

Handlers should stay thin and delegate work to services.

Business errors should use the exceptions in `app/middleware/exceptions.py`, for example:

```python
from app.middleware.exceptions import BadRequestException, NotFoundException


if item is None:
    raise NotFoundException("item", item_id)

if not valid:
    raise BadRequestException("Invalid request", details={"field": "name"})
```

Error response shape:

```json
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Validation error",
    "request_id": "6b74a3b5-...",
    "details": {
      "validation_errors": []
    },
    "path": "/v1/example"
  }
}
```

`path` and validation body are debug-only. See [docs/exception_handling.md](docs/exception_handling.md) for details.

## Logging

Logging is configured in `app/core/logging.py`.

- Loguru handles console and rotating file sinks.
- stdlib logging is intercepted and forwarded to Loguru.
- `X-Request-ID` is propagated through a context variable and emitted in logs.
- the response always includes `X-Request-ID`.

Avoid logging secrets, request bodies, bearer tokens, or upstream API keys.

## Testing

The test suite covers:

- app lifespan and DI startup
- diagnostics endpoint wiring
- SQLAlchemy engine/session behavior
- OpenAI internal semaphore proxying, including async stream consumption
- zlmdb store TTL, expiry-index maintenance, count, stats, and validation
- temp-file atomic overwrites, binary/text reads, and cleanup
- stable validation error envelope

Run:

```bash
uv run pytest
uv run python -m compileall -q app main.py tests
uv lock --check
```

## Deployment Checklist

- set `app.debug_mode: false`
- configure `app.cors_origins` explicitly
- keep `settings.yaml` ignored and provide secrets through deployment-specific settings
- point `FASTAPIEX__SETTINGS__PATH` to deployment-specific settings
- ensure `logs`, `data`, and `temp` paths are writable
- size zlmdb `max_size_mb` for expected data volume
- configure OpenAI `concurrency_limit` for upstream rate limits
- run `uv run pytest` before release

## Notes

- This template is intentionally local-first. Add Docker, Alembic, metrics, auth, or tracing only when the target service needs them.
- `fastapiex-settings` returns live mutable settings objects; avoid mutating them in request handlers.
- `StoreService` and `TempFileService` use file locks so cleanup loops do not duplicate across workers sharing the same filesystem path.
- SQLite is the default database because it works in tests and local development. Production services should choose the appropriate async SQLAlchemy driver.
