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
uv run fastapi dev app/main.py

# Production
uv run uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## Configuration

Settings are loaded via `pydantic-settings` from environment variables and `.env` in the project root.

- `APP_NAME`, `APP_VERSION`: basic service metadata.
- `DEBUG_MODE`: when true, validation errors include request body and unhandled exceptions are re-raised.
- `RELOAD`: development-only hot reload flag for local runs.
- `CORS_ORIGINS`: JSON list of allowed origins (example: `["https://example.com"]`).
- `USE_PROXY_HEADERS`: enable `X-Forwarded-*` handling in uvicorn.
- `FORWARDED_ALLOW_IPS`: comma-separated list of trusted proxy IPs/hosts.
- `LOG_DIR`: log output directory (created automatically if missing).
- `TMP_DIR`, `TMP_RETENTION_DAYS`: reserved for temporary files/cleanup workflows.
- `SEMAPHORES__<name>`: nested config for semaphore services (uses `env_nested_delimiter="__"`).

Use `.env.production_example` and `.env.debug_example` as starting points.

## Configuration reference

| Key | Type | Example | Notes |
| --- | --- | --- | --- |
| `APP_NAME` | string | `fastapi_app` | Service name |
| `APP_VERSION` | string | `1.0.0` | Service version |
| `DEBUG_MODE` | bool | `false` | Production should be `false` |
| `RELOAD` | bool | `false` | Development only |
| `CORS_ORIGINS` | JSON list | `["https://example.com"]` | Empty list disables CORS |
| `USE_PROXY_HEADERS` | bool | `true` | Set when behind a reverse proxy |
| `FORWARDED_ALLOW_IPS` | string | `10.0.0.0/8,127.0.0.1` | Trusted proxy IPs/hosts |
| `LOG_DIR` | string | `./log` | Created automatically |
| `TMP_DIR` | string | `./tmp` | Reserved |
| `TMP_RETENTION_DAYS` | int | `3` | Reserved |
| `SEMAPHORES__db` | int | `5` | Example nested config |

## Project structure

```
app/
├── api/                      # Routing entrypoints
│   ├── main.py               # /api router aggregation
│   └── v1/                   # Versioned APIs
│       ├── main.py           # /api/v1 router aggregation
│       └── example/          # Exception examples (remove in production)
├── core/                     # Infrastructure
│   ├── dependencies.py       # ServiceContainer (DI) and inject helper
│   ├── logger.py             # Loguru setup and stdlib interception
│   └── settings.py           # Pydantic settings
├── lifespan/                 # Startup/shutdown wiring
│   └── main.py               # Service registration and teardown
├── middleware/               # Request/response middleware
│   ├── exception.py          # Error types and handlers
│   └── logging.py            # Request logging
├── services/                 # Business services and examples
│   └── base.py               # BaseService and lifecycle contract
└── main.py                   # FastAPI app factory
main.py                       # CLI runner for uvicorn
```

## Programming guide and conventions

### API layer

- Keep handlers thin; delegate business logic to services.
- Use `inject(...)` to obtain services; avoid manually constructing dependencies in handlers.
- Return structured errors via exceptions from `app/middleware/exception.py`.

### Services and DI

- Naming: singletons end with `Service`, transients end with `ServiceT`.
- Register services in `app/lifespan/main.py` with a clear key name; prefer explicit keys.
- Use `inject("key")` for key-based resolution; use type-based injection only when a single service of that type exists.
- Do not resolve services outside an event loop or across multiple loops.
- Singletons should not depend on transients unless explicitly allowed.

### Lifespan

- Register all services in lifespan to control creation order and teardown.
- For generator-based services, yield the instance and ensure cleanup in `finally`.
- Avoid heavy work at import time; defer to lifespan registration.

### Error handling

- Use the custom exceptions for consistent error shapes and status codes.
- `DEBUG_MODE=true` re-raises uncaught errors; production should keep it `false`.

### Logging

- Log files are rotated daily and retained for 7 days.
- Avoid logging sensitive request bodies or tokens.
- In debug mode, loguru `backtrace` and `diagnose` are enabled; disable in production.

### Middleware order

- Order matters: request logging should run before the transient finalizer.
- `TransientServiceFinalizerMiddleware` must run after any middleware that creates transients.

### Concurrency model

> [!CAUTION]
> **Do NOT use multiple workers (e.g., `--workers` flag or Gunicorn with multiple workers).**
> The `ServiceContainer` is not thread-safe and assumes a single asyncio event loop.

- Run a single uvicorn process per container/instance.
- Scale horizontally by deploying multiple independent instances (Docker/K8s replicas).
- If you need multiple workers, ensure each worker creates its own `ServiceContainer` instance (untested).

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
- Use appropriate log level (`warning` for expected errors, `exception` for unexpected).

## Deployment checklist

- Confirm `DEBUG_MODE=false` and `RELOAD=false`.
- Configure `CORS_ORIGINS` explicitly or keep it empty.
- If behind a proxy, set `USE_PROXY_HEADERS=true` and configure `FORWARDED_ALLOW_IPS`.
- Remove example routes under `/api/v1/example`.
- Ensure log directory permissions for `LOG_DIR`.
- Verify tmp directory policy if using `TMP_DIR`.
- Run `PYTHONPATH=. uv run pytest` before deployment.

## Notes and pitfalls

- Example routes under `/api/v1/example` are for reference only; remove them for production services.
- `CORS_ORIGINS` defaults to empty; configure explicitly for browsers.
- Enable `USE_PROXY_HEADERS` only when behind trusted reverse proxies and set `FORWARDED_ALLOW_IPS` accordingly.
- `DEBUG_MODE=true` exposes request body in validation errors; do not enable in production.
- If transient services are resolved outside of a request context, their destructors will not run automatically.
