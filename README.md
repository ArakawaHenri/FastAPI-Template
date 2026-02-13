# FastAPI Template

Internal production template for FastAPI services. This README focuses on structure, conventions, and operational notes.

## Requirements

- Python >= 3.12
- Package manager: uv

## Quick start

1. Install dependencies:

```bash
uv sync
```

1. Pick a config example and copy it to `.env`:

```bash
cp .env.debug_example .env
# or
cp .env.production_example .env
```

1. Run the service:

```bash
# Development
uv run python main.py --host 0.0.0.0 --port 8000

# Production
uv run python main.py --host 0.0.0.0 --port 8000 --workers 1
```

## Runtime notes

- Production runtime uses `granian` by default.
- `main.py` launches granian and applies `RELOAD` from settings.
- Granian has experimental free-threading support on CPython 3.14t.

```bash
uv sync --python 3.14t
uv run python main.py --host 0.0.0.0 --port 8000
```

- Treat 3.14t mode as experimental and validate your workload before production rollout.

## Configuration

Settings are loaded via `pydantic-settings` from environment variables and `.env` in the project root.

- `APP_NAME`, `APP_VERSION`: basic service metadata.
- `DEBUG_MODE`: when true, validation errors include request body and unhandled exceptions are re-raised.
- `RELOAD`: development-only hot reload flag for local runs.
- `CORS_ORIGINS`: JSON list of allowed origins (example: `["https://example.com"]`).
- `LOG_DIR`: log output directory (created automatically if missing).
- `TMP_DIR`, `TMP_RETENTION_DAYS`, `TMP_WORKER_THREADS`, `TMP_MAX_FILE_SIZE_MB`, `TMP_MAX_TOTAL_SIZE_MB`, `TMP_TOTAL_SIZE_RECALC_SECONDS`: temp file storage, fixed pool size, size limits, and total-size recalc interval for `TempFileService`.
- `STORE_LMDB__*`: LMDB store configuration (including callback runner pool size via `STORE_LMDB__CALLBACK_WORKER_THREADS`; see notes below).
- `SEMAPHORES__<name>`: nested config for semaphore services (uses `env_nested_delimiter="__"`).
- `DATABASE__<name>__*`: nested database config. Defaults in examples use MySQL + `aiomysql`.

Use `.env.production_example` and `.env.debug_example` as starting points.

## Configuration reference

| Key | Type | Example | Notes |
| --- | --- | --- | --- |
| `APP_NAME` | string | `fastapi_app` | Service name |
| `APP_VERSION` | string | `1.0.0` | Service version |
| `DEBUG_MODE` | bool | `false` | Production should be `false` |
| `RELOAD` | bool | `false` | Development only |
| `CORS_ORIGINS` | JSON list | `["https://example.com"]` | Empty list disables CORS |
| `LOG_DIR` | string | `./logs` | Created automatically |
| `TMP_DIR` | string | `./tmp` | Temp file base directory |
| `TMP_RETENTION_DAYS` | int | `3` | Temp file retention days |
| `TMP_WORKER_THREADS` | int | `4` | TempFileService fixed thread-pool size |
| `TMP_MAX_FILE_SIZE_MB` | int | `1024` | Max size per temp file (`0` means unlimited) |
| `TMP_MAX_TOTAL_SIZE_MB` | int | `0` | Max total size of temp dir (`0` means unlimited) |
| `TMP_TOTAL_SIZE_RECALC_SECONDS` | int | `3600` | Total-size recalc interval in seconds (`>= 60`, used only when total-size cap is enabled) |
| `DATABASE__main__URL` | string | `mysql+aiomysql://root:password@127.0.0.1:3306/app_db` | Async SQLAlchemy URL |
| `STORE_LMDB__PATH` | string | `./store_lmdb` | LMDB store path |
| `STORE_LMDB__CALLBACK_WORKER_THREADS` | int | `4` | Store expiry-callback fixed thread-pool size |
| `STORE_LMDB__MAX_DBS` | int | `256` | Must be `>= 0` (`0` disables user-namespace quota) |
| `SEMAPHORES__db` | int | `5` | Example nested config |
| `SEMAPHORES__example` | int | `10` | Used by `/api/v1/example/` demo route |

## Project structure

```
.
├── app/
│   ├── api/                  # API routers (/api, /api/v1)
│   ├── core/                 # DI container, service registry, logging, settings
│   ├── lifespan/             # Startup/shutdown orchestration
│   ├── middleware/           # Request logging + unified exception handlers
│   ├── services/             # Store, temp-file, database, semaphore, examples
│   └── main.py               # FastAPI app factory
├── tests/                    # Unit/integration tests
├── docs/                     # Additional docs
├── main.py                   # CLI runner (granian)
├── pyproject.toml            # Dependencies and tool config
└── Dockerfile                # Container image definition
```

## Programming guide and conventions

### API layer

- Keep handlers thin; delegate business logic to services.
- Use `Inject(...)` to obtain services; avoid manually constructing dependencies in handlers.
- Return structured errors via exceptions from `app/middleware/exception.py`.

### Services and DI

- Naming: singletons end with `Service`, transients end with `ServiceT`.
- Register services via decorators:
`@Service("key", lifetime=..., eager=...)`,
`@Service` / `@Service()` (anonymous singleton),
and `@ServiceDict("template", dict=..., lifetime=..., eager=...)`.
- `lifetime` accepts `ServiceLifetime`, `0/1`, or
`"Singleton"/"Transient"` (case-insensitive) and defaults to singleton.
- `eager=True` is singleton-only and instantiates at startup.
- Use `Inject("key")` for key-based resolution; use type-based injection only when a single service of that type exists.
- For type-based resolution, `ServiceContainer` uses the factory (`ctor`) return annotation as the service type.
- Anonymous services are type-only (`aget_by_type`) and follow container constraints:
one anonymous service per type, and no coexistence with a named service of the same type.
- Session example: `session: AsyncSession = Inject(AsyncSession, Inject("main_database_service"))`.
- `ServiceContainer` itself is single-loop; do not use one container across event loops/threads.
- Lifespan binds one container per current loop into `app.state.sc_registry` (free-threaded safe).
- `Inject(...)` resolves services only from `sc_registry` (no fallback to `app.state.services`).
- `StoreService` and `TempFileService` are process-shared in lifespan (same path/config reuses one instance with ref-counted teardown).
- Singletons should not depend on transients unless explicitly allowed.

### Temporary files

- `TempFileService` is registered as key `temp_file_service` and type `TempFileService`.
- It is configured as eager singleton and manages temporary files under `TMP_DIR`.
- `TMP_MAX_FILE_SIZE_MB` enforces a per-file size cap for both save and read (`0` means unlimited).
- `TMP_MAX_TOTAL_SIZE_MB` enforces a global temp-dir size cap for writes (`0` means unlimited).
- `TMP_TOTAL_SIZE_RECALC_SECONDS` controls periodic total-size recalculation (enabled only when `TMP_MAX_TOTAL_SIZE_MB > 0`).
- `save(name, content)` stores text or binary; duplicate names become `filename.1.ext`, `filename.2.ext`, etc.
- Leading dots are escaped (e.g., `.env` → `%2Eenv`) to avoid hidden files.
- `read(name)` returns `str` if the original content was text, otherwise `bytes`.
- Cleanup runs on a single worker via a file lock (cross-platform via `filelock`).

### LMDB store

- `StoreService` is registered as key `store_service` and type `StoreService`.
- It is configured as eager singleton and provides a local LMDB-backed key-value store with TTL.
- Expiration uses a secondary index plus an expmeta DB to avoid reading old payloads on overwrite.
- `STORE_LMDB__MAX_DBS` controls user-namespace quota and must be `>= 0`; `0` disables the quota.
- Namespaces marked as internal are excluded from user-namespace quota counting.
- Cleanup runs on a single worker via a file lock (cross-platform via `filelock`).

### Lifespan

- Lifespan imports service modules, reads `ServiceRegistry`, performs DAG ordering + cycle checks, and auto-registers services.
- For contextmanager-style services, yield the instance exactly once and ensure cleanup in `finally`.
- Keep import-time side effects limited to lightweight metadata registration (decorators).

### Error handling

- Use the exception types in `app/middleware/exception.py`.
- See the dedicated **Error handling** section below for response format and mappings.

### Logging

- Log files are rotated daily and retained for 7 days.
- Avoid logging sensitive request bodies or tokens.
- In debug mode, loguru `backtrace` and `diagnose` are enabled; disable in production.
- Logging setup/teardown is process-shared and ref-counted so free-threaded workers do not duplicate handlers or tear logging down early.

### Middleware order

- Order matters: request logging should run before the transient finalizer.
- `TransientServiceFinalizerMiddleware` must run after any middleware that creates transients.

### Concurrency model

- Multi-worker mode is supported with constraints.
- In process-worker mode, each worker process creates its own `ServiceContainer` during lifespan startup.
- In free-threaded mode where workers can share one app object, each worker loop registers its own container in `app.state.sc_registry`.
- In free-threaded mode, workers in the same process reuse one `StoreService` / `TempFileService` backend instance (per path/config).
- Never share in-memory service instances across workers; only share external state (LMDB/files/DB).
- Background cleanup loops and callback dispatchers are leader-elected via file locks; at most one worker runs each loop at a time.
- Expiry callback names must be deterministic and identical across workers for the same feature.
- Every worker must register the same callback names during startup.
- Expiry callbacks should be idempotent; callback jobs may be replayed after worker restarts/crashes.
- If a callback job reaches a worker that has no handler registered for that callback name, the event is skipped and logged as `error`.
- `TempFileService` now uses a stable callback name derived from namespace (`tmp_file_cleanup:<namespace>`). Keep namespace consistent across workers when they share the same temp-file domain.
- Keep worker count moderate and scale primarily by adding instances/replicas.

### Tests

- Tests rely on `TestClient` to trigger lifespan startup/shutdown.

## Error handling

This project uses a unified exception system in `app/middleware/exception.py`.

### Custom exceptions

- `NotFoundException` (404)
- `UnauthorizedException` (401)
- `ForbiddenException` (403)
- `BadRequestException` (400)
- `ConflictException` (409)
- `RateLimitException` (429)

Example:

```python
from app.middleware.exception import NotFoundException, BadRequestException

if user is None:
    raise NotFoundException("User", user_id)

if "@" not in email:
    raise BadRequestException(
        "Invalid email format",
        details={"field": "email", "value": email}
    )
```

### Error response format

```json
{
  "error": {
    "code": "ERROR_CODE",
    "message": "Human-readable error message",
    "details": {
      "additional": "context"
    },
    "path": "/api/v1/users/123"
  }
}
```

`path` is only included when `DEBUG_MODE=true`.

### Automatically handled exceptions

| HTTP error | Status | Code |
| --- | --- | --- |
| 400 Bad Request | 400 | BAD_REQUEST |
| 401 Unauthorized | 401 | UNAUTHORIZED |
| 403 Forbidden | 403 | FORBIDDEN |
| 404 Not Found | 404 | NOT_FOUND |
| 405 Method Not Allowed | 405 | METHOD_NOT_ALLOWED |
| 409 Conflict | 409 | CONFLICT |
| 422 Unprocessable Entity | 422 | VALIDATION_ERROR |
| 429 Too Many Requests | 429 | RATE_LIMIT_EXCEEDED |
| 500 Internal Server Error | 500 | INTERNAL_ERROR |

### Debug vs production

- `DEBUG_MODE=false`: hide sensitive details; return generic 500 error for uncaught exceptions.
- `DEBUG_MODE=true`: include request body in validation errors and re-raise uncaught exceptions.

### Best practices

- Raise typed exceptions in services; keep handlers thin.
- Provide `details` for client actionability.
- Use appropriate log level (`warning`/`error` for recoverable business failures, `exception` for unexpected faults).

## Deployment checklist

- Confirm `DEBUG_MODE=false` and `RELOAD=false`.
- Configure `CORS_ORIGINS` explicitly or keep it empty.
- Remove example routes under `/api/v1/example`.
- Ensure log directory permissions for `LOG_DIR`.
- Verify tmp directory policy if using `TMP_DIR`.
- In multi-worker deployments, ensure all workers load the same config for temp-file namespace and store callback setup.
- In multi-worker deployments, ensure startup successfully registers callbacks in every worker before serving traffic.
- Run `PYTHONPATH=. uv run pytest` before deployment.

## Notes and pitfalls

- Example routes under `/api/v1/example` are for reference only; remove them for production services.
- If transient services are resolved outside of a request context, their destructors will not run automatically.
- Do not read/write `app.state.services` directly in application code; resolve via `Inject(...)` so loop-local registry routing is applied.
- Expiry callback names missing in the active worker are logged as `error`; register callbacks consistently across workers.
