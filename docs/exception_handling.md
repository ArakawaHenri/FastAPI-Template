# Exception Handling

The template provides a unified exception model so API errors stay predictable:

- validation errors use a stable `VALIDATION_ERROR` envelope
- FastAPI/Starlette HTTP exceptions are normalized
- business exceptions carry machine-readable error codes
- unexpected exceptions are logged and hidden behind a generic 500 response unless debug mode is enabled

Implementation lives in `app/middleware/exceptions.py`; registration lives in `app/main.py`.

## Built-in Exceptions

Base class:

```python
AppException(
    message: str,
    *,
    status_code: int = 500,
    error_code: str = "INTERNAL_ERROR",
    details: dict[str, Any] | None = None,
)
```

Built-in subclasses:

| Exception | Status | Code |
| --- | --- | --- |
| `BadRequestException` | 400 | `BAD_REQUEST` |
| `UnauthorizedException` | 401 | `UNAUTHORIZED` |
| `ForbiddenException` | 403 | `FORBIDDEN` |
| `NotFoundException` | 404 | `RESOURCE_NOT_FOUND` |
| `ConflictException` | 409 | `CONFLICT` |
| `RateLimitException` | 429 | `RATE_LIMIT_EXCEEDED` |
| `ServiceUnavailableException` | 503 | `SERVICE_UNAVAILABLE` |

Example:

```python
from app.middleware.exceptions import BadRequestException, NotFoundException


if user is None:
    raise NotFoundException("user", user_id)

if not payload.name:
    raise BadRequestException(
        "Missing name",
        details={"field": "name"},
    )
```

## Response Shape

```json
{
  "error": {
    "code": "ERROR_CODE",
    "message": "Human-readable message",
    "request_id": "6b74a3b5-...",
    "details": {
      "optional": "context"
    },
    "path": "/v1/example"
  }
}
```

Fields:

- `code`: machine-readable error code
- `message`: human-readable summary
- `request_id`: included when request logging middleware has assigned one
- `details`: optional context for clients
- `path`: included only when `app.debug_mode=true`

## Automatically Handled Errors

### `RequestValidationError`

- status: 422
- code: `VALIDATION_ERROR`
- details: `details.validation_errors`
- debug mode: includes `details.body`

### `HTTPException`

| Status | Code |
| --- | --- |
| 400 | `BAD_REQUEST` |
| 401 | `UNAUTHORIZED` |
| 403 | `FORBIDDEN` |
| 404 | `NOT_FOUND` |
| 405 | `METHOD_NOT_ALLOWED` |
| 409 | `CONFLICT` |
| 429 | `RATE_LIMIT_EXCEEDED` |
| 503 | `SERVICE_UNAVAILABLE` |

Unmapped statuses use `HTTP_ERROR`.

### `AppException`

Uses the exception instance's `status_code`, `error_code`, `message`, and `details`.

### Unhandled `Exception`

- status: 500
- code: `INTERNAL_ERROR`
- debug mode: includes traceback in `details.traceback`

## Registration

Handlers are registered from specific to general:

```python
app.add_exception_handler(RequestValidationError, validation_exception_handler)
app.add_exception_handler(HTTPException, http_exception_handler)
app.add_exception_handler(AppException, app_exception_handler)
app.add_exception_handler(Exception, global_exception_handler)
```

## Extending

```python
from fastapi import status

from app.middleware.exceptions import AppException, app_exception_handler


class PaymentRequiredException(AppException):
    def __init__(self, amount: float, currency: str = "USD") -> None:
        super().__init__(
            message=f"Payment of {amount} {currency} required",
            status_code=status.HTTP_402_PAYMENT_REQUIRED,
            error_code="PAYMENT_REQUIRED",
            details={"amount": amount, "currency": currency},
        )


app.add_exception_handler(PaymentRequiredException, app_exception_handler)
```

## Practices

- raise typed exceptions from services; keep API handlers thin
- include `details` when the client can act on the error
- keep production `app.debug_mode` disabled
- avoid logging request bodies, credentials, bearer tokens, or upstream API keys
