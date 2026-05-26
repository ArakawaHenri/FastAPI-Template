from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    ok: bool = True


class KVPutRequest(BaseModel):
    value: Any
    retention_seconds: int | None = Field(default=None, ge=1)
    only_if_absent: bool = False


class KVPutResponse(BaseModel):
    stored: bool


class KVGetResponse(BaseModel):
    found: bool
    value: Any | None = None


class DeleteResponse(BaseModel):
    deleted: bool = True


class TempFileSaveRequest(BaseModel):
    content: str
    overwrite: bool = False


class TempFileSaveResponse(BaseModel):
    filename: str
    bucket: str
    path: str


class TempFileReadResponse(BaseModel):
    filename: str
    bucket: str
    content: str


class DependencyDiagnosticsResponse(BaseModel):
    database_ok: bool
    database_name: str
    openai_model: str
    openai_concurrency_limit: int
