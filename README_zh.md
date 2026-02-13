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
uv run python main.py --host 0.0.0.0 --port 8000

# 生产模式
uv run python main.py --host 0.0.0.0 --port 8000 --workers 1
```

## 运行时说明

- 生产运行时默认使用 `granian`。
- `main.py` 会启动 granian，并读取配置中的 `RELOAD` 开关。
- Granian 在 CPython 3.14t 下提供实验性的 free-threading 支持。

```bash
uv sync --python 3.14t
uv run python main.py --host 0.0.0.0 --port 8000
```

- 3.14t 模式属于实验性能力，请在生产上线前对实际业务负载做充分验证。

## 配置说明

使用 `pydantic-settings` 从环境变量与项目根目录 `.env` 读取配置。

- `APP_NAME`, `APP_VERSION`：服务元信息。
- `DEBUG_MODE`：为 true 时校验错误会返回请求体，未捕获异常会直接抛出。
- `RELOAD`：开发用热重载开关。
- `CORS_ORIGINS`：允许来源列表，JSON 数组格式（示例：`["https://example.com"]`）。
- `LOG_DIR`：日志目录（不存在会自动创建）。
- `TMP_DIR`, `TMP_RETENTION_DAYS`, `TMP_WORKER_THREADS`, `TMP_MAX_FILE_SIZE_MB`, `TMP_MAX_TOTAL_SIZE_MB`, `TMP_TOTAL_SIZE_RECALC_SECONDS`：临时文件存储、固定线程池大小、保留时间、大小上限与总量重算周期（`TempFileService`）。
- `STORE_LMDB__*`：LMDB 存储配置（含回调执行固定线程池大小 `STORE_LMDB__CALLBACK_WORKER_THREADS`，见下方说明）。
- `SEMAPHORES__<name>`：信号量配置（使用 `env_nested_delimiter="__"`）。
- `DATABASE__<name>__*`：数据库嵌套配置。示例默认使用 MySQL + `aiomysql`。

可参考 `.env.production_example` 与 `.env.debug_example`。

## 配置参考

| Key | 类型 | 示例 | 说明 |
| --- | --- | --- | --- |
| `APP_NAME` | string | `fastapi_app` | 服务名称 |
| `APP_VERSION` | string | `1.0.0` | 服务版本 |
| `DEBUG_MODE` | bool | `false` | 生产必须为 `false` |
| `RELOAD` | bool | `false` | 仅开发环境 |
| `CORS_ORIGINS` | JSON list | `["https://example.com"]` | 空数组禁用 CORS |
| `LOG_DIR` | string | `./logs` | 自动创建 |
| `TMP_DIR` | string | `./tmp` | 临时文件目录 |
| `TMP_RETENTION_DAYS` | int | `3` | 临时文件保留天数 |
| `TMP_WORKER_THREADS` | int | `4` | TempFileService 固定线程池大小 |
| `TMP_MAX_FILE_SIZE_MB` | int | `1024` | 单个临时文件大小上限（`0` 表示不限） |
| `TMP_MAX_TOTAL_SIZE_MB` | int | `0` | 临时目录总大小上限（`0` 表示不限） |
| `TMP_TOTAL_SIZE_RECALC_SECONDS` | int | `3600` | 总大小重算周期（秒，`>= 60`，仅在启用总容量上限时生效） |
| `DATABASE__main__URL` | string | `mysql+aiomysql://root:password@127.0.0.1:3306/app_db` | SQLAlchemy 异步连接串 |
| `STORE_LMDB__PATH` | string | `./store_lmdb` | LMDB 存储路径 |
| `STORE_LMDB__CALLBACK_WORKER_THREADS` | int | `4` | Store 过期回调执行固定线程池大小 |
| `STORE_LMDB__MAX_DBS` | int | `256` | 必须 `>= 0`（`0` 表示关闭用户 namespace 配额） |
| `SEMAPHORES__db` | int | `5` | 嵌套配置示例 |
| `SEMAPHORES__example` | int | `10` | `/api/v1/example/` 示例路由使用 |

## 项目结构

```
.
├── app/
│   ├── api/                  # API 路由（/api, /api/v1）
│   ├── core/                 # DI 容器、服务注册、日志、配置
│   ├── lifespan/             # 启动/关闭编排
│   ├── middleware/           # 请求日志与统一异常处理
│   ├── services/             # Store、TempFile、Database、Semaphore、示例服务
│   └── main.py               # FastAPI 应用工厂
├── tests/                    # 单元/集成测试
├── docs/                     # 补充文档
├── main.py                   # CLI 启动脚本（granian）
├── pyproject.toml            # 依赖与工具配置
└── Dockerfile                # 镜像构建定义
```

## 编程规范与引导

### API 层

- Handler 保持轻量，业务逻辑放在 service 中。
- 使用 `Inject(...)` 获取服务，避免在 handler 内自行构造依赖。
- 使用 `app/middleware/exception.py` 中的异常类型统一错误响应。

### 服务与依赖注入

- 命名规范：单例以 `Service` 结尾，瞬态以 `ServiceT` 结尾。
- 通过装饰器注册服务：
`@Service("key", lifetime=..., eager=...)`、
`@Service` / `@Service()`（匿名单例）与
`@ServiceDict("template", dict=..., lifetime=..., eager=...)`。
- `lifetime` 支持 `ServiceLifetime`、`0/1`、`"Singleton"/"Transient"`（大小写不敏感），默认单例。
- `eager=True` 仅支持单例，表示启动时立即实例化。
- `Inject("key")` 用于 key 注入；类型注入仅在唯一注册时使用。
- 类型注入时，`ServiceContainer` 会以工厂函数（`ctor`）的返回注解作为服务类型。
- 匿名服务仅支持按类型解析（`aget_by_type`），并遵循容器约束：
同一类型只能存在一个匿名服务，且不能与同类型具名服务共存。
- Session 示例：`session: AsyncSession = Inject(AsyncSession, Inject("main_database_service"))`。
- `ServiceContainer` 本身是单事件循环模型，不要跨事件循环或线程复用同一个容器。
- Lifespan 会将“当前循环对应的容器”绑定到 `app.state.sc_registry`（可适配 free-threaded worker）。
- `Inject(...)` 只会从 `sc_registry` 解析服务（不再回退到 `app.state.services`）。
- `StoreService` 与 `TempFileService` 在 lifespan 中按进程共享（同路径/配置复用同一实例，并通过引用计数析构）。
- 除特别设置外，单例默认不依赖瞬态服务。

### 临时文件

- `TempFileService` 以 key `temp_file_service` 与类型 `TempFileService` 注册。
- 默认为 eager 单例，在 `TMP_DIR` 下管理临时文件。
- `TMP_MAX_FILE_SIZE_MB` 会限制 `save/read` 的单文件最大大小（`0` 表示不限）。
- `TMP_MAX_TOTAL_SIZE_MB` 会限制临时目录写入总容量（`0` 表示不限）。
- `TMP_TOTAL_SIZE_RECALC_SECONDS` 控制总大小周期重算（仅在 `TMP_MAX_TOTAL_SIZE_MB > 0` 时启用）。
- `save(name, content)` 保存文本或二进制；重名自动写为 `filename.1.ext`、`filename.2.ext`。
- 以 `.` 开头的文件名会被转义（如 `.env` → `%2Eenv`），避免隐藏文件。
- `read(name)`：文本返回 `str`，二进制返回 `bytes`。
- 清理任务通过文件锁确保多 worker 只运行一个实例（依赖 `filelock`）。

### LMDB 存储

- `StoreService` 以 key `store_service` 与类型 `StoreService` 注册。
- 默认为 eager 单例，提供本地 LMDB KV + TTL。
- 过期使用二级索引与 expmeta DB，避免覆写时读取旧 payload。
- `STORE_LMDB__MAX_DBS` 用于控制用户 namespace 配额，必须 `>= 0`；`0` 表示关闭配额限制。
- 被标记为 internal 的 namespace 不计入用户 namespace 配额。
- 清理任务通过文件锁确保多 worker 只运行一个实例（依赖 `filelock`）。

### Lifespan

- Lifespan 会自动导入服务模块，读取 `ServiceRegistry`，做依赖 DAG 排序与环检测后自动注册服务。
- 使用 contextmanager 风格服务时，仅 `yield` 一次返回实例，并在 `finally` 中清理。
- import 阶段只做轻量元数据注册（装饰器），避免重资源副作用。

### 错误处理

- 使用 `app/middleware/exception.py` 中定义的异常类型。
- 具体响应结构与状态码映射见下方 **异常处理** 章节。

### 日志

- 日志按天轮转，默认保留 7 天。
- 避免记录敏感信息或请求体。
- `DEBUG_MODE` 为 true 时开启更详细的 backtrace/diagnose。
- 日志初始化/释放按进程共享并使用引用计数，避免 free-threaded worker 重复挂载 handler 或提前卸载日志系统。

### 中间件顺序

- 顺序很重要：请求日志应在瞬态清理中间件之前。
- `TransientServiceFinalizerMiddleware` 必须在可能创建瞬态服务的中间件之后。

### 并发模型

- 支持多 worker，但需要满足约束条件。
- 进程 worker 模式下，每个 worker 进程会在 lifespan 启动时创建自己的 `ServiceContainer`。
- free-threaded 模式下，如果多个 worker 共享同一个 app 对象，每个 worker 事件循环会在 `app.state.sc_registry` 中注册自己的容器。
- free-threaded 模式下，同一进程内的 worker 会复用同一个 `StoreService` / `TempFileService` 后端实例（按路径/配置划分）。
- 不要在 worker 之间共享内存态服务实例；只共享外部状态（LMDB/文件/数据库）。
- 后台清理循环和回调分发通过文件锁进行主 worker 选举，同一时刻最多一个 worker 执行对应循环。
- 过期回调名称必须是确定性的，并且在所有 worker 中保持一致。
- 每个 worker 启动时都必须注册相同的回调名称。
- 过期回调函数应实现幂等；worker 重启/崩溃恢复后可能出现回放执行。
- 如果某个 worker 没有注册对应回调名，事件会被跳过并记录为 `error` 日志。
- `TempFileService` 使用按 namespace 派生的稳定回调名（`tmp_file_cleanup:<namespace>`）；共享同一临时文件域的 worker 必须使用同一 namespace。
- worker 数量建议适中，优先通过增加实例/副本进行扩展。

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
- 合理选择日志等级（可恢复的业务失败使用 `warning`/`error`，非预期故障使用 `exception`）。

## 部署检查清单

- 确认 `DEBUG_MODE=false` 与 `RELOAD=false`。
- 显式配置 `CORS_ORIGINS` 或保持为空。
- 移除 `/api/v1/example` 示例路由。
- 确保 `LOG_DIR` 具备写权限。
- 如使用 `TMP_DIR`，明确清理策略。
- 多 worker 部署时，确保所有 worker 的 temp-file namespace 与 store callback 配置一致。
- 多 worker 部署时，确保每个 worker 在对外提供流量前都完成回调注册。
- 部署前运行 `PYTHONPATH=. uv run pytest`。

## 注意点

- `/api/v1/example` 仅用于参考，生产应移除。
- 在请求上下文之外解析瞬态服务时，析构器不会自动执行。
- 不要在业务代码中直接读写 `app.state.services`；应通过 `Inject(...)` 解析服务，才能走到按事件循环路由的容器映射。
- 活跃 worker 中缺少对应过期回调名时会记录 `error`；请确保所有 worker 回调注册一致。
