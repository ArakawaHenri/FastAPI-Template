# FastAPI 模板

用于内部生产环境的 FastAPI 服务模板。本说明聚焦结构、规范与注意事项。

## 环境要求

- Python >= 3.12
- 包管理器：uv

## 快速开始

1. 安装依赖：

```bash
uv sync
```

1. 选择配置示例并复制为 `.env`：

```bash
cp .env.debug_example .env
# 或
cp .env.production_example .env
```

1. 运行服务：

```bash
# 开发模式
uv run fastapi dev app/main.py

# 生产模式
uv run uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 配置说明

使用 `pydantic-settings` 从环境变量与项目根目录 `.env` 读取配置。

- `APP_NAME`, `APP_VERSION`：服务元信息。
- `DEBUG_MODE`：为 true 时校验错误会返回请求体，未捕获异常会直接抛出。
- `RELOAD`：开发用热重载开关。
- `CORS_ORIGINS`：允许来源列表，JSON 数组格式（示例：`["https://example.com"]`）。
- `USE_PROXY_HEADERS`：在 uvicorn 中启用 `X-Forwarded-*` 处理。
- `FORWARDED_ALLOW_IPS`：可信代理 IP/Host 列表，逗号分隔。
- `LOG_DIR`：日志目录（不存在会自动创建）。
- `TMP_DIR`, `TMP_RETENTION_DAYS`：临时文件存储与保留时间（`TempFileService`）。
- `STORE_LMDB__*`：LMDB 存储配置（见下方说明）。
- `SEMAPHORES__<name>`：信号量配置（使用 `env_nested_delimiter="__"`）。

可参考 `.env.production_example` 与 `.env.debug_example`。

## 配置参考

| Key | 类型 | 示例 | 说明 |
| --- | --- | --- | --- |
| `APP_NAME` | string | `fastapi_app` | 服务名称 |
| `APP_VERSION` | string | `1.0.0` | 服务版本 |
| `DEBUG_MODE` | bool | `false` | 生产必须为 `false` |
| `RELOAD` | bool | `false` | 仅开发环境 |
| `CORS_ORIGINS` | JSON list | `["https://example.com"]` | 空数组禁用 CORS |
| `USE_PROXY_HEADERS` | bool | `true` | 反向代理后启用 |
| `FORWARDED_ALLOW_IPS` | string | `10.0.0.0/8,127.0.0.1` | 可信代理 IP/Host |
| `LOG_DIR` | string | `./log` | 自动创建 |
| `TMP_DIR` | string | `./tmp` | 临时文件目录 |
| `TMP_RETENTION_DAYS` | int | `3` | 临时文件保留天数 |
| `STORE_LMDB__PATH` | string | `./store_lmdb` | LMDB 存储路径 |
| `STORE_LMDB__MAX_DBS` | int | `256` | 必须 `>= 3` |
| `SEMAPHORES__db` | int | `5` | 嵌套配置示例 |

## 项目结构

```
app/
├── api/                      # 路由入口
│   ├── main.py               # /api 路由汇总
│   └── v1/                   # 版本化 API
│       ├── main.py           # /api/v1 路由汇总
│       └── example/          # 异常示例（生产移除）
├── core/                     # 基础设施
│   ├── dependencies.py       # ServiceContainer (DI) 与 inject
│   ├── logger.py             # loguru 配置与标准库拦截
│   ├── shared/               # 跨 worker 共享工具
│   │   ├── store_cleanup.py  # Store 清理循环 + 文件锁
│   │   └── tmpfile_cleanup.py# Temp 文件清理循环 + 文件锁
│   └── settings.py           # 配置读取
├── lifespan/                 # 启动/关闭流程
│   └── main.py               # 服务注册与释放
├── middleware/               # 中间件
│   ├── exception.py          # 错误类型与处理
│   └── logging.py            # 请求日志
├── services/                 # 业务服务与示例
│   ├── store/                # LMDB 键值存储
│   ├── temp_file/            # 临时文件管理
│   └── base.py               # BaseService 与生命周期约定
└── main.py                   # FastAPI 应用入口
main.py                       # uvicorn 启动脚本
```

## 编程规范与引导

### API 层

- Handler 保持轻量，业务逻辑放在 service 中。
- 使用 `inject(...)` 获取服务，避免在 handler 内自行构造依赖。
- 使用 `app/middleware/exception.py` 中的异常类型统一错误响应。

### 服务与依赖注入

- 命名规范：单例以 `Service` 结尾，瞬态以 `ServiceT` 结尾。
- 在 `app/lifespan/main.py` 注册服务并明确 key，建议优先使用 key。
- `inject("key")` 用于 key 注入；类型注入仅在唯一注册时使用。
- 不要跨事件循环或线程使用 `ServiceContainer`。
- 除特别设置外，单例默认不依赖瞬态服务。

### 临时文件

- `TempFileService`（key: `temp_file_service`）在 `TMP_DIR` 下管理临时文件。
- `save(name, content)` 保存文本或二进制；重名自动写为 `filename.1.ext`、`filename.2.ext`。
- 以 `.` 开头的文件名会被转义（如 `.env` → `%2Eenv`），避免隐藏文件。
- `read(name)`：文本返回 `str`，二进制返回 `bytes`。
- 清理任务通过文件锁确保多 worker 只运行一个实例（依赖 `portalocker`）。

### LMDB 存储

- `StoreService`（key: `store_service`）提供本地 LMDB KV + TTL。
- 过期使用二级索引与 expmeta DB，避免覆写时读取旧 payload。
- `STORE_LMDB__MAX_DBS` 必须 `>= 3`。
- 清理任务通过文件锁确保多 worker 只运行一个实例（依赖 `portalocker`）。

### Lifespan

- 所有服务在 lifespan 中注册与释放，确保生命周期清晰。
- 使用生成器服务时，用 `yield` 返回实例，并在 `finally` 中清理。
- 避免在 import 时创建重资源。

### 错误处理

- 自定义异常用于统一错误结构与状态码。
- 生产环境保持 `DEBUG_MODE=false`。

### 日志

- 日志按天轮转，默认保留 7 天。
- 避免记录敏感信息或请求体。
- `DEBUG_MODE` 为 true 时开启更详细的 backtrace/diagnose。

### 中间件顺序

- 顺序很重要：请求日志应在瞬态清理中间件之前。
- `TransientServiceFinalizerMiddleware` 必须在可能创建瞬态服务的中间件之后。

### 并发模型

- `ServiceContainer` 非线程安全，仅支持单事件循环。
- 不建议单实例多进程；通过多实例（Docker/K8s）水平扩展。

### 测试

- 使用 `TestClient` 触发生命周期启动/关闭。

## 异常处理

本项目使用统一的异常系统，位于 `app/middleware/exception.py`。

### 自定义异常

- `NotFoundException` (404)
- `UnauthorizedException` (401)
- `ForbiddenException` (403)
- `BadRequestException` (400)
- `ConflictException` (409)
- `RateLimitException` (429)

示例：

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

### 错误响应格式

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

`path` 仅在 `DEBUG_MODE=true` 时显示。

### 自动处理的异常

| HTTP 异常 | 状态码 | 错误代码 |
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

### 调试与生产差异

- `DEBUG_MODE=false`：隐藏敏感信息，未捕获异常返回通用 500。
- `DEBUG_MODE=true`：验证错误包含请求体，未捕获异常会被重新抛出。

### 最佳实践

- 在业务层抛出异常，API 层保持简洁。
- 为异常提供 `details`，便于客户端处理。
- 合理选择日志等级（预期错误用 `warning`，非预期错误用 `exception`）。

## 部署检查清单

- 确认 `DEBUG_MODE=false` 与 `RELOAD=false`。
- 显式配置 `CORS_ORIGINS` 或保持为空。
- 若有代理，设置 `USE_PROXY_HEADERS=true` 并配置 `FORWARDED_ALLOW_IPS`。
- 移除 `/api/v1/example` 示例路由。
- 确保 `LOG_DIR` 具备写权限。
- 如使用 `TMP_DIR`，明确清理策略。
- 部署前运行 `PYTHONPATH=. uv run pytest`。

## 注意点

- `/api/v1/example` 仅用于参考，生产应移除。
- `CORS_ORIGINS` 默认为空，需显式配置。
- 仅在可信代理后启用 `USE_PROXY_HEADERS`，并配置 `FORWARDED_ALLOW_IPS`。
- `DEBUG_MODE=true` 会回显请求体，生产必须禁用。
- 在请求上下文之外解析瞬态服务时，析构器不会自动执行。
