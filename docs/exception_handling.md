# 异常处理文档

## 目标

模板提供统一异常模型，保证：

- API 错误响应结构一致
- 常见 HTTP/校验异常自动转为标准错误
- 业务异常可读、可扩展

核心实现位于 `app/middleware/exception.py`，注册位于 `app/main.py`。

## 自定义异常类型

基类：`AppException(message, status_code, error_code, details)`

内置子类：

- `NotFoundException` (404, `NOT_FOUND`)
- `UnauthorizedException` (401, `UNAUTHORIZED`)
- `ForbiddenException` (403, `FORBIDDEN`)
- `BadRequestException` (400, `BAD_REQUEST`)
- `ConflictException` (409, `CONFLICT`)
- `RateLimitException` (429, `RATE_LIMIT_EXCEEDED`)

示例：

```python
from app.middleware.exception import BadRequestException, NotFoundException

if user is None:
    raise NotFoundException("User", user_id)

if "@" not in email:
    raise BadRequestException(
        "Invalid email format",
        details={"field": "email", "value": email},
    )
```

## 统一错误响应格式

```json
{
  "error": {
    "code": "ERROR_CODE",
    "message": "Human-readable message",
    "details": {
      "optional": "context"
    },
    "path": "/api/v1/example"
  }
}
```

字段说明：

- `code`：机器可读错误码
- `message`：人类可读信息
- `details`：可选，附加上下文
- `path`：仅在 `DEBUG_MODE=true` 且有请求路径时返回

## 自动处理逻辑

### 1. `RequestValidationError`

- HTTP 状态：`422`
- 错误码：`VALIDATION_ERROR`
- `details.validation_errors`：字段级校验错误列表
- 当 `DEBUG_MODE=true` 时，附加 `details.body`（已做 JSON 安全序列化）

### 2. `HTTPException`

状态码映射：

| 状态码 | 错误码 |
| --- | --- |
| 400 | `BAD_REQUEST` |
| 401 | `UNAUTHORIZED` |
| 403 | `FORBIDDEN` |
| 404 | `NOT_FOUND` |
| 405 | `METHOD_NOT_ALLOWED` |
| 409 | `CONFLICT` |
| 429 | `RATE_LIMIT_EXCEEDED` |

未命中映射时使用 `HTTP_ERROR`。

### 3. `AppException`

- 直接使用异常对象中的 `status_code` / `error_code` / `message` / `details`

### 4. 未捕获 `Exception`

- `DEBUG_MODE=true`：重新抛出异常（便于开发期看到完整堆栈）
- `DEBUG_MODE=false`：返回 `500 INTERNAL_ERROR`

## 注册位置与顺序

`app/main.py` 中按“具体到通用”顺序注册：

```python
app.add_exception_handler(RequestValidationError, validation_exception_handler)
app.add_exception_handler(HTTPException, http_exception_handler)
app.add_exception_handler(AppException, app_exception_handler)
app.add_exception_handler(Exception, global_exception_handler)
```

## 扩展自定义异常

```python
from fastapi import status
from app.middleware.exception import AppException, app_exception_handler

class PaymentRequiredException(AppException):
    def __init__(self, amount: float, currency: str = "USD"):
        super().__init__(
            message=f"Payment of {amount} {currency} required",
            status_code=status.HTTP_402_PAYMENT_REQUIRED,
            error_code="PAYMENT_REQUIRED",
            details={"amount": amount, "currency": currency},
        )

# 注册（例如在应用创建阶段）
app.add_exception_handler(PaymentRequiredException, app_exception_handler)
```

## 实践建议

- 业务逻辑层抛业务异常，API Handler 只负责参数/编排。
- 优先使用内置异常类型，避免直接 `raise Exception(...)`。
- 为客户端需要分支处理的错误补充 `details` 字段。
- 生产环境务必关闭 `DEBUG_MODE`。

## 相关文件

- `app/middleware/exception.py`：异常类型与处理器实现
- `app/main.py`：异常处理器注册
- `app/api/v1/example/example.py`：示例路由
- `tests/api/v1/test_demo.py`：示例测试
