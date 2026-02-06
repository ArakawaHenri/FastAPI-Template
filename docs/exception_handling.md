# 异常处理系统文档

## 概述

本 FastAPI 模板提供了一个全面的异常处理系统，包含：

- **自定义异常类**：用于不同的业务场景
- **统一的错误响应格式**：确保 API 响应一致性
- **自动异常处理**：无需在每个端点手动处理错误

---

## 自定义异常类

### 1. NotFoundException (404)

用于资源不存在的情况。

```python
from app.middleware.exception import NotFoundException

# 基本用法
raise NotFoundException("User", 123)
# 响应: {"error": {"code": "RESOURCE_NOT_FOUND", "message": "User not found: 123", ...}}

# 不带标识符
raise NotFoundException("Settings")
# 响应: {"error": {"code": "RESOURCE_NOT_FOUND", "message": "Settings not found", ...}}
```

### 2. UnauthorizedException (401)

用于需要认证但未提供凭证的情况。

```python
from app.middleware.exception import UnauthorizedException

# 默认消息
raise UnauthorizedException()
# 响应: {"error": {"code": "UNAUTHORIZED", "message": "Authentication required"}}

# 自定义消息
raise UnauthorizedException("Invalid API token")
# 响应: {"error": {"code": "UNAUTHORIZED", "message": "Invalid API token"}}
```

### 3. ForbiddenException (403)

用于认证用户无权访问资源的情况。

```python
from app.middleware.exception import ForbiddenException

raise ForbiddenException("You don't have permission to delete this resource")
# 响应: {"error": {"code": "FORBIDDEN", "message": "You don't have permission..."}}
```

### 4. BadRequestException (400)

用于客户端请求无效的情况。

```python
from app.middleware.exception import BadRequestException

raise BadRequestException(
    "Invalid email format",
    details={"field": "email", "value": "invalid-email"}
)
# 响应: {
#   "error": {
#     "code": "BAD_REQUEST",
#     "message": "Invalid email format",
#     "details": {"field": "email", "value": "invalid-email"}
#   }
# }
```

### 5. ConflictException (409)

用于资源冲突的情况（如重复创建）。

```python
from app.middleware.exception import ConflictException

raise ConflictException(
    "User with this email already exists",
    details={"email": "user@example.com"}
)
# 响应: {"error": {"code": "CONFLICT", "message": "User with this email...", ...}}
```

### 6. RateLimitException (429)

用于超过速率限制的情况。

```python
from app.middleware.exception import RateLimitException

raise RateLimitException(retry_after=60)
# 响应: {
#   "error": {
#     "code": "RATE_LIMIT_EXCEEDED",
#     "message": "Rate limit exceeded",
#     "details": {"retry_after": 60}
#   }
# }
```

---

## 使用示例

### 基础端点示例

```python
from fastapi import APIRouter
from app.middleware.exception import NotFoundException, BadRequestException

router = APIRouter()

@router.get("/users/{user_id}")
async def get_user(user_id: int):
    user = await db.get_user(user_id)
    
    if user is None:
        raise NotFoundException("User", user_id)
    
    return user

@router.post("/users")
async def create_user(email: str, name: str):
    if not email or "@" not in email:
        raise BadRequestException(
            "Invalid email format",
            details={"field": "email"}
        )
    
    existing = await db.get_user_by_email(email)
    if existing:
        raise ConflictException(
            "User with this email already exists",
            details={"email": email}
        )
    
    return await db.create_user(email, name)
```

### 带权限检查的示例

```python
from app.middleware.exception import ForbiddenException, UnauthorizedException

@router.delete("/posts/{post_id}")
async def delete_post(post_id: int, current_user: User = Depends(get_current_user)):
    # 检查认证
    if current_user is None:
        raise UnauthorizedException()
    
    post = await db.get_post(post_id)
    if post is None:
        raise NotFoundException("Post", post_id)
    
    # 检查权限
    if post.author_id != current_user.id:
        raise ForbiddenException("You can only delete your own posts")
    
    await db.delete_post(post_id)
    return {"message": "Post deleted successfully"}
```

---

## 错误响应格式

所有错误都返回统一的 JSON 格式：

```json
{
  "error": {
    "code": "ERROR_CODE",
    "message": "Human-readable error message",
    "details": {
      "additional": "context",
      "if": "needed"
    },
    "path": "/api/v1/users/123"  // 仅在 DEBUG_MODE=True 时显示
  }
}
```

### 示例响应

**404 Not Found**:

```json
{
  "error": {
    "code": "RESOURCE_NOT_FOUND",
    "message": "User not found: 123",
    "details": {
      "resource": "User",
      "identifier": "123"
    }
  }
}
```

**422 Validation Error**:

```json
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Validation error",
    "details": {
      "validation_errors": [
        {
          "field": "body.email",
          "message": "field required",
          "type": "value_error.missing"
        }
      ]
    }
  }
}
```

---

## 自动处理的异常

系统自动处理以下异常，无需手动配置：

| HTTP 异常 | 状态码 | 错误代码 |
|----------|--------|---------|
| 400 Bad Request | 400 | BAD_REQUEST |
| 401 Unauthorized | 401 | UNAUTHORIZED |
| 403 Forbidden | 403 | FORBIDDEN |
| 404 Not Found | 404 | NOT_FOUND |
| 405 Method Not Allowed | 405 | METHOD_NOT_ALLOWED |
| 409 Conflict | 409 | CONFLICT |
| 422 Unprocessable Entity | 422 | VALIDATION_ERROR |
| 429 Too Many Requests | 429 | RATE_LIMIT_EXCEEDED |
| 500 Internal Server Error | 500 | INTERNAL_ERROR |

---

## 创建自定义异常

如果内置异常不满足需求，可以继承 `AppException` 创建自定义异常：

```python
from app.middleware.exception import AppException
from fastapi import status

class PaymentRequiredException(AppException):
    """Raised when payment is required"""
    def __init__(self, amount: float, currency: str = "USD"):
        super().__init__(
            message=f"Payment of {amount} {currency} required",
            status_code=status.HTTP_402_PAYMENT_REQUIRED,
            error_code="PAYMENT_REQUIRED",
            details={"amount": amount, "currency": currency}
        )

# 使用
raise PaymentRequiredException(amount=9.99)
```

然后在 `main.py` 中注册处理器：

```python
from app.middleware.exception import app_exception_handler
from your_module import PaymentRequiredException

app.add_exception_handler(PaymentRequiredException, app_exception_handler)
```

---

## 调试模式差异

### 生产环境 (`DEBUG_MODE=False`)

- 隐藏敏感信息（如完整堆栈跟踪）
- 不显示请求体内容
- 不显示请求路径
- 返回通用错误消息

### 开发环境 (`DEBUG_MODE=True`)

- 显示详细错误信息
- 包含请求体（用于调试验证错误）
- 显示请求路径
- 未处理的异常会重新抛出以显示完整堆栈跟踪

---

## 最佳实践

### 1. 使用合适的异常类型

```python
# ✅ 好的做法
if user is None:
    raise NotFoundException("User", user_id)

# ❌ 不好的做法
if user is None:
    raise Exception("User not found")  # 会返回 500 而不是 404
```

### 2. 提供有用的错误详情

```python
# ✅ 好的做法
raise BadRequestException(
    "Invalid date format",
    details={
        "field": "start_date",
        "expected": "YYYY-MM-DD",
        "received": "2023/01/01"
    }
)

# ❌ 不好的做法
raise BadRequestException("Invalid date")  # 缺少上下文
```

### 3. 在业务逻辑层抛出异常

```python
# ✅ 好的做法 - 在服务层抛出
class UserService:
    async def get_user(self, user_id: int):
        user = await self.db.find_user(user_id)
        if user is None:
            raise NotFoundException("User", user_id)
        return user

# API 层保持简洁
@router.get("/users/{user_id}")
async def get_user(user_id: int, service: UserService = Depends()):
    return await service.get_user(user_id)
```

### 4. 记录适当的日志级别

- `NotFoundException`, `ValidationError` → `logger.info` or `logger.warning`
- `UnauthorizedException`, `ForbiddenException` → `logger.warning`
- `AppException` 的子类 → `logger.warning`
- 未预期的 `Exception` → `logger.exception` (包含堆栈跟踪)

---

## 测试异常处理

```python
import pytest
from fastapi.testclient import TestClient
from app.middleware.exception import NotFoundException

def test_not_found_exception():
    with pytest.raises(NotFoundException) as exc_info:
        raise NotFoundException("User", 123)
    
    assert exc_info.value.status_code == 404
    assert "User not found" in exc_info.value.message

def test_api_error_response(client: TestClient):
    response = client.get("/api/v1/users/99999")
    assert response.status_code == 404
    
    data = response.json()
    assert data["error"]["code"] == "RESOURCE_NOT_FOUND"
    assert "User" in data["error"]["message"]
```

---

## 相关文件

- `app/middleware/exception.py` - 异常类和处理器实现
- `app/main.py` - 异常处理器注册
- `app/api/v1/example/example.py` - 示例路由
- `tests/api/v1/test_demo.py` - API 示例测试（包含基础错误场景）
