from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Response
from fastapiex.di import Inject

from app.api.v1.schemas import (
    DeleteResponse,
    DependencyDiagnosticsResponse,
    HealthResponse,
    KVGetResponse,
    KVPutRequest,
    KVPutResponse,
    TempFileReadResponse,
    TempFileSaveRequest,
    TempFileSaveResponse,
)
from app.middleware.exceptions import BadRequestException, NotFoundException
from app.service.database.engine import DatabaseEngineService
from app.service.openai_client.main import AsyncOpenAIClientService
from app.service.store.main import StoreService
from app.service.temp_file.main import TempFileService

router = APIRouter(prefix="/v1")


@router.get("/healthz", response_model=HealthResponse)
async def healthz() -> HealthResponse:
    return HealthResponse(ok=True)


@router.get("/diagnostics/dependencies", response_model=DependencyDiagnosticsResponse)
async def dependency_diagnostics(
    database: Annotated[DatabaseEngineService, Inject("main_database_service")],
    openai_client: Annotated[AsyncOpenAIClientService, Inject("completions_openai_client_service")],
) -> DependencyDiagnosticsResponse:
    return DependencyDiagnosticsResponse(
        database_ok=await database.is_healthy(),
        database_name=database.db_name,
        openai_model=openai_client.model,
        openai_concurrency_limit=openai_client.concurrency_limit,
    )


@router.put("/kv/{namespace}/{key}", response_model=KVPutResponse)
async def put_value(
    namespace: str,
    key: str,
    payload: KVPutRequest,
    store: Annotated[StoreService, Inject("store_service")],
    response: Response,
) -> KVPutResponse:
    if payload.only_if_absent:
        stored = await store.set_if_absent(
            namespace,
            key,
            payload.value,
            retention_seconds=payload.retention_seconds,
        )
        if not stored:
            response.status_code = 200
        return KVPutResponse(stored=stored)

    await store.set(namespace, key, payload.value, retention_seconds=payload.retention_seconds)
    return KVPutResponse(stored=True)


@router.get("/kv/{namespace}/{key}", response_model=KVGetResponse)
async def get_value(
    namespace: str,
    key: str,
    store: Annotated[StoreService, Inject("store_service")],
) -> KVGetResponse:
    value = await store.get(namespace, key)
    return KVGetResponse(found=value is not None, value=value)


@router.delete("/kv/{namespace}/{key}", response_model=DeleteResponse)
async def delete_value(
    namespace: str,
    key: str,
    store: Annotated[StoreService, Inject("store_service")],
) -> DeleteResponse:
    await store.delete(namespace, key)
    return DeleteResponse()


@router.post("/temp-files/{bucket}/{filename}", response_model=TempFileSaveResponse)
async def save_temp_file(
    bucket: str,
    filename: str,
    payload: TempFileSaveRequest,
    temp_files: Annotated[TempFileService, Inject("temp_file_service")],
) -> TempFileSaveResponse:
    if payload.overwrite:
        saved_name = await temp_files.save_overwrite(filename, payload.content, bucket=bucket)
    else:
        saved_name = await temp_files.save(filename, payload.content, bucket=bucket)
    return TempFileSaveResponse(
        filename=saved_name,
        bucket=bucket,
        path=str(temp_files.path_for(saved_name, bucket=bucket)),
    )


@router.get("/temp-files/{bucket}/{filename}", response_model=TempFileReadResponse)
async def read_temp_file(
    bucket: str,
    filename: str,
    temp_files: Annotated[TempFileService, Inject("temp_file_service")],
) -> TempFileReadResponse:
    try:
        content = await temp_files.read(filename, bucket=bucket)
    except FileNotFoundError as exc:
        raise NotFoundException("temp_file", filename) from exc
    if isinstance(content, bytes):
        raise BadRequestException(
            "Requested temp file is binary; use service-level access for bytes")
    return TempFileReadResponse(filename=filename, bucket=bucket, content=content)
