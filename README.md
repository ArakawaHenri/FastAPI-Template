# FastAPI Template

A robust, modular, and opinionated FastAPI project template designed for building scalable business applications.

## 🌟 Features

- **Modular Architecture**: DDD-inspired structure with clear separation of concerns (`api`, `core`, `services`,
  `lifespan`).
- **Custom Dependency Injection**: A powerful `ServiceContainer` supporting:
    - **Singleton & Transient Lifetimes**: Manage resource-heavy objects (DB pools) vs per-request objects.
    - **Async Generators**: Perfect for resources requiring setup/teardown context (like sessions).
    - **Lazy Loading**: Services are initialized only when requested (or explicitly in lifespan).
- **Advanced Logging**: Pre-configured `loguru` integration with rotation, retention, and standard library interception.
- **Configuration Management**: Type-safe settings using `pydantic-settings` with `.env` support.
- **API Versioning**: Built-in support for versioned APIs (`/api/v1/...`).

## 📋 Prerequisites

- **Python**: >= 3.12
- **Package Manager**: [uv](https://github.com/astral-sh/uv)

## 🚀 Getting Started

### 1. Installation

Clone the repository and install dependencies:

```bash
uv sync
```

### 2. Configuration

The application uses `pydantic-settings`. You can configure it via environment variables or a `.env` file.

Create a `.env` file in the root directory:

```ini
DEBUG_MODE = true
LOG_DIR = ./log
APP_NAME = "FastAPI Template"
APP_VERSION = "0.1.0"
```

### 3. Running the Application

Use `uvicorn` or `fastapi` CLI to run the app:

```bash
# Development mode
uv run fastapi dev app/main.py

# Production mode
uv run uvicorn app.main:app --host 0.0.0.0 --port 8000
```

The API will be available at `http://localhost:8000`.
API Documentation (Swagger UI) is at `http://localhost:8000/docs`.

## 🧪 Testing

This template includes a testing setup using `pytest` and `TestClient`.

1. **Install Test Dependencies**:
   *(Ensure `pytest` and `httpx` are installed in your environment)*
   ```bash
   uv add --dev pytest httpx
   ```

2. **Run Tests**:
   ```bash
   uv run pytest
   ```

## 🏗 Architecture Overview

### The Service Container (`app/core/dependencies.py`)

The heart of this template is the custom `ServiceContainer`. Unlike standard FastAPI `Depends`, this container allows
for:

- **Decoupled Logic**: Services don't need to know about FastAPI `Request` objects.
- **Complex Lifecycles**: Automatically handles cleanup (dtor) for transient services at the end of a request.

**How to use:**

1. **Define a Service**: Inherit from `BaseService`.
2. **Register in Lifespan** (`app/lifespan/main.py`):
   ```python
   await app.state.services.register(
       "my_service",
       ServiceLifetime.TRANSIENT,
       MyService.LifespanTasks.ctor,
       MyService.LifespanTasks.dtor
   )
   ```
3. **Inject in API**:
   ```python
   @router.get("/")
   async def endpoint(svc: MyService = inject("my_service")):
       ...
   ```

### Folder Structure

```
app/
├── api/            # Route handlers (Controllers)
│   └── v1/         # API Version 1
├── core/           # Core infrastructure (DI, Logging, Settings)
├── lifespan/       # App startup/shutdown logic
├── middleware/     # Custom middlewares
├── services/       # Business logic (Domain layer)
└── main.py         # App entrypoint
```

## 🛠 Development

- **Linting**: Recommended to configure `ruff`.
- **Formatting**: Recommended to configure `ruff` or `black`.
