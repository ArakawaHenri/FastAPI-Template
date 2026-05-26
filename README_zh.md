# FastAPI 模板

这是一个面向生产服务的 FastAPI 模板，核心围绕 `fastapiex-settings` 和 `fastapiex-di` 构建。

模板保持较小规模，但包含多数 FastAPI 服务会反复需要的基础能力：

- 使用 `fastapiex-settings` 管理 YAML/env 类型化配置
- 使用 `fastapiex-di` 做服务发现、启动时 eager 初始化与关闭清理
- Loguru 日志、请求 ID 传递、标准库 logging 拦截
- 校验错误、HTTP 错误、业务异常、未知异常的统一响应
- zlmdb KV Store：namespace、TTL、`set_if_absent`、过期索引、清理锁
- 临时文件管理器：bucket、唯一保存、原子覆盖、保留期清理、文本/二进制读取
- SQLAlchemy 异步数据库 engine 与事务型 `AsyncSession` 注入
- OpenAI 异步客户端，内部自带按 client 配置的并发 semaphore，并覆盖 streaming 消费周期

## 环境要求

- Python >= 3.12
- 包管理器：`uv`

## 快速开始

安装依赖：

```bash
uv sync
```

创建本地配置文件：

```bash
cp settings.yaml.example settings.yaml
```

启动服务：

```bash
uv run python main.py
```

默认监听 `0.0.0.0:8000`。

常用端点：

- `GET /v1/healthz`
- `GET /v1/diagnostics/dependencies`
- `PUT /v1/kv/{namespace}/{key}`
- `GET /v1/kv/{namespace}/{key}`
- `POST /v1/temp-files/{bucket}/{filename}`

运行测试：

```bash
uv run pytest
```

## 运行时

`main.py` 会初始化本地 `settings.yaml`，读取 `server` 配置，并启动 Granian：

```yaml
server:
  address: 0.0.0.0
  port: 8000
  workers: 1
  loop: auto
```

`loop: auto` 在类 Unix 系统选择 `uvloop`，在 Windows 选择 `winloop`。

`settings.yaml` 会被 git 忽略。仓库只提交 `settings.yaml.example` 作为配置参考，本地或部署环境的实际配置应放在被忽略的 settings 文件中。

使用其他配置文件：

```bash
FASTAPIEX__SETTINGS__PATH=/path/to/settings.yaml uv run python main.py
```

## 项目结构

```text
.
├── app/
│   ├── api/                  # 路由与 API schema
│   ├── core/                 # 进程级日志配置
│   ├── middleware/           # 请求日志与异常处理
│   ├── service/
│   │   ├── database/         # SQLAlchemy async engine 与 session dependency
│   │   ├── openai_client/    # 内部 semaphore 保护的 AsyncOpenAI client
│   │   ├── shared/           # 文件锁与周期清理工具
│   │   ├── store/            # zlmdb KV Store
│   │   └── temp_file/        # 临时文件管理器
│   └── main.py               # FastAPI app factory
├── tests/                    # 单元/集成测试
├── main.py                   # Granian runner
├── settings.yaml.example     # 提交到仓库的配置示例
├── pyproject.toml
└── uv.lock
```

## 配置

配置模型通过 `@Settings` / `@SettingsMap` 声明，并由 `fastapiex-settings` 读取。

仓库只提交 `settings.yaml.example`。本地开发时复制为 `settings.yaml`；部署时可以通过 `FASTAPIEX__SETTINGS__PATH` 指向环境专属 YAML。不要提交真实密钥或环境专属运行路径。

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

- `debug_mode`：错误响应中包含 path 与更详细的校验信息。
- `log_dir`：不存在时自动创建。
- `cors_origins`：空列表表示关闭 CORS；`["*"]` 表示允许所有来源且不启用 credentials。

### Store

`StoreService` 注册为 `store_service`，启动时 eager 初始化。它提供：

- `set(namespace, key, value, retention_seconds=None)`
- `set_if_absent(namespace, key, value, retention_seconds=None)`
- `get(namespace, key)`
- `delete(namespace, key)`
- `cleanup_expired()`
- `count(namespace=None)`
- `stats(include_slots=True)`
- `sync(force=True)`

值使用 zlmdb CBOR map 存储。TTL 使用 zlmdb 自动维护的有序过期索引，清理时不需要扫描 payload 记录，覆盖 TTL key 时也不会残留旧过期索引。

### 临时文件

`TempFileService` 注册为 `temp_file_service`，启动时 eager 初始化。它支持：

- `save(filename, content, bucket="logs")`
- `save_overwrite(filename, content, bucket="logs")`
- `read(filename, bucket="logs")`
- `path_for(filename, bucket="logs")`
- `cleanup_expired()`

文件名会被安全转义。覆盖写入使用原子流程：写临时文件、fsync、再移动到目标路径。

### 数据库

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

`database` 下每个 key 会注册为 `{key}_database_service`。

事务型 session 示例：

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

`DatabaseSessionServiceT` 是 transient，成功时 commit，异常时 rollback。

### OpenAI Client

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

`openai_clients` 下每个 key 会注册为 `{key}_openai_client_service`。

OpenAI 并发限制由 client 自己持有，不再需要单独配置 semaphore service，因此不会出现 key 不一致问题。

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

嵌套 async SDK 调用会经过内部 semaphore。Streaming 结果会一直持有 semaphore，直到异步流消费结束或关闭。

## DI 约定

服务放在 `app/service` 下，并由下面配置发现：

```python
install_di(app, service_packages=["app.service"])
```

约定：

- 单例服务类名以 `Service` 结尾
- transient 服务类名以 `ServiceT` 结尾
- 使用 `@Service("key")` 注册普通 keyed service
- 使用 `@ServiceMap("{}_suffix", mapping=lambda: GetSettingsMap("section"))` 注册配置映射服务
- 基础设施服务需要启动前可用时使用 `eager=True`
- 优先使用 `Inject("key")` 做明确注入
- 只有唯一类型 provider 时才使用类型注入

不要在 `app/main.py` 里手动提前 import service 模块。让 `install_di()` 在启动阶段扫描和导入。

## API 与异常处理

Handler 应保持轻量，把业务逻辑交给 service。

业务错误使用 `app/middleware/exceptions.py` 中的异常：

```python
from app.middleware.exceptions import BadRequestException, NotFoundException


if item is None:
    raise NotFoundException("item", item_id)

if not valid:
    raise BadRequestException("Invalid request", details={"field": "name"})
```

错误响应格式：

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

`path` 与校验请求体只在 debug 模式返回。详见 [docs/exception_handling.md](docs/exception_handling.md)。

## 日志

日志配置在 `app/core/logging.py`。

- Loguru 负责控制台与文件 sink。
- 标准库 logging 会被拦截转发到 Loguru。
- `X-Request-ID` 通过 context variable 进入日志。
- 响应始终带 `X-Request-ID`。

不要记录密钥、请求体、Bearer token 或上游 API key。

## 测试

测试覆盖：

- app lifespan 与 DI 启动
- diagnostics endpoint wiring
- SQLAlchemy engine/session
- OpenAI 内部 semaphore 代理，包括 async stream 消费
- zlmdb Store TTL、过期索引维护、count、stats 与校验
- temp-file 原子覆盖、文本/二进制读取与清理
- 稳定的校验错误响应结构

运行：

```bash
uv run pytest
uv run python -m compileall -q app main.py tests
uv lock --check
```

## 部署检查清单

- 设置 `app.debug_mode: false`
- 显式配置 `app.cors_origins`
- 保持 `settings.yaml` 被忽略，并通过部署环境配置提供密钥
- 使用 `FASTAPIEX__SETTINGS__PATH` 指向部署环境配置
- 确保 `logs`、`data`、`temp` 路径可写
- 根据预期数据量设置 zlmdb `max_size_mb`
- 按上游限流设置 OpenAI `concurrency_limit`
- 发布前运行 `uv run pytest`

## 注意事项

- 模板刻意保持 local-first。Docker、Alembic、metrics、auth、tracing 等按业务需要再加入。
- `fastapiex-settings` 返回 live mutable settings object，不要在 request handler 中修改它们。
- `StoreService` 与 `TempFileService` 使用文件锁，避免多 worker 共享路径时重复运行清理循环。
- 默认数据库使用 SQLite，便于测试和本地开发；生产服务应选择合适的异步 SQLAlchemy driver。
