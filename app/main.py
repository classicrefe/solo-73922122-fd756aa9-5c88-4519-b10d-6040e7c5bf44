"""FastAPI 入口：网页与 JSON API 共用同一套数据湖核心。"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import quote

from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from . import lake
from .lake import (
    MAX_FIELDS,
    MAX_ROWS,
    MAX_UPLOAD_BYTES,
    STATION_COLUMN,
    TIME_COLUMN,
    LakeError,
    Settings,
)

log = logging.getLogger("metlake")

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


def create_app(settings: Settings | None = None) -> FastAPI:
    resolved = settings or Settings.from_env()

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        lake.initialize(resolved)
        log.info(
            "instance=%s data_dir=%s db=%s",
            resolved.instance_name,
            resolved.data_dir,
            resolved.db_path,
        )
        yield

    app = FastAPI(
        title="气象观测数据湖",
        version="1.0.0",
        lifespan=lifespan,
    )
    app.state.settings = resolved
    app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

    # ------------------------------------------------------------------
    # 错误处理
    # ------------------------------------------------------------------

    @app.exception_handler(LakeError)
    async def lake_error_handler(request: Request, exc: LakeError):
        if request.url.path.startswith("/api/"):
            payload = {
                "error": exc.code,
                "detail": str(exc),
                "schema_version": _safe_version(resolved),
            }
            if isinstance(exc, lake.StaleSchema):
                payload["expected_version"] = exc.expected
            return JSONResponse(payload, status_code=exc.status_code)
        if request.method in ("GET", "HEAD"):
            return _render_error(request, exc)
        return _redirect_with_message(request, str(exc), "error")

    @app.exception_handler(404)
    async def not_found_handler(request: Request, exc: Exception):
        if request.url.path.startswith("/api/"):
            return JSONResponse(
                {"error": "not_found", "detail": "资源不存在。"}, status_code=404
            )
        return _render_error(request, lake.NotFound("页面不存在。"))

    @app.exception_handler(500)
    async def internal_error_handler(request: Request, exc: Exception):
        log.exception("unhandled error on %s", request.url.path)
        message = "服务器内部错误，已有批次保持不变。"
        if request.url.path.startswith("/api/"):
            return JSONResponse(
                {"error": "internal", "detail": message, "schema_version": _safe_version(resolved)},
                status_code=500,
            )
        return _render_error(request, lake.StorageError(message))

    # ------------------------------------------------------------------
    # 网页
    # ------------------------------------------------------------------

    @app.get("/", response_class=HTMLResponse)
    def index(request: Request):
        state = lake.read_state(resolved)
        return _render(
            request,
            "index.html",
            state=state,
            settings=resolved,
            ack=state.schema.version,
            latest=_latest_version(resolved),
            msg=request.query_params.get("msg"),
            kind=request.query_params.get("kind", "info"),
        )

    @app.get("/batches/{batch_id}", response_class=HTMLResponse)
    def batch_page(request: Request, batch_id: str):
        view = lake.get_batch(resolved, batch_id)
        columns, rows = lake.rows_as_display(view.table)
        return _render(
            request,
            "batch.html",
            settings=resolved,
            schema=view.schema,
            batch=view.batch,
            columns=columns,
            rows=rows,
            ack=view.schema.version,
            latest=_latest_version(resolved),
            msg=request.query_params.get("msg"),
            kind=request.query_params.get("kind", "info"),
        )

    @app.post("/fields/add")
    def add_field_form(
        request: Request,
        version: int = Form(...),
        name: str = Form(...),
    ):
        schema = lake.add_field(resolved, version, name)
        return _redirect_with_message(
            request, f"已新增测量列，结构已提交为 v{schema.version}。", "ok"
        )

    @app.post("/fields/{field_id}/rename")
    def rename_field_form(
        request: Request,
        field_id: str,
        version: int = Form(...),
        name: str = Form(...),
    ):
        schema = lake.rename_field(resolved, version, field_id, name)
        return _redirect_with_message(
            request, f"已改名，历史批次仍按旧列值展示；结构已提交为 v{schema.version}。", "ok"
        )

    @app.post("/fields/{field_id}/delete")
    def delete_field_form(request: Request, field_id: str, version: int = Form(...)):
        schema = lake.delete_field(resolved, version, field_id)
        return _redirect_with_message(
            request,
            f"已删除测量列（历史 Parquet 未改写）；结构已提交为 v{schema.version}。",
            "ok",
        )

    @app.post("/upload")
    async def upload_form(
        request: Request,
        version: int = Form(...),
        file: UploadFile = File(...),
    ):
        data = await _read_upload(file)
        batch = lake.upload_csv(resolved, version, data)
        return _redirect_with_message(
            request,
            f"批次 {batch.short_id} 已入湖（{batch.row_count} 行，结构 v{batch.schema_version}）。",
            "ok",
            path=f"/batches/{batch.id}",
        )

    @app.get("/sample.csv")
    def sample_download():
        schema = lake.read_schema(resolved)
        payload = lake.sample_csv(schema)
        return _download(
            payload, f"sample-v{schema.version}.csv", "text/csv; charset=utf-8"
        )

    @app.get("/batches/{batch_id}/download.csv")
    def download_csv(batch_id: str):
        view = lake.get_batch(resolved, batch_id)
        payload = lake.csv_bytes(view.table)
        return _download(payload, f"batch-{view.batch.short_id}.csv", "text/csv; charset=utf-8")

    @app.get("/batches/{batch_id}/download.parquet")
    def download_parquet(batch_id: str):
        view = lake.get_batch(resolved, batch_id)
        payload = lake.parquet_bytes(view.table)
        return _download(
            payload, f"batch-{view.batch.short_id}.parquet", "application/vnd.apache.parquet"
        )

    # ------------------------------------------------------------------
    # JSON API（演示双进程与脚本自测）
    # ------------------------------------------------------------------

    @app.get("/healthz")
    def healthz():
        schema = lake.read_schema(resolved)
        return {
            "status": "ok",
            "instance": resolved.instance_name,
            "pid": os.getpid(),
            "schema_version": schema.version,
            "data_dir": str(resolved.data_dir),
            "db_path": str(resolved.db_path),
        }

    @app.get("/api/state")
    def api_state():
        state = lake.read_state(resolved)
        return {
            "schema": _schema_payload(state.schema),
            "batches": [_batch_payload(b) for b in state.batches],
        }

    @app.get("/api/schema")
    def api_schema():
        return _schema_payload(lake.read_schema(resolved))

    @app.post("/api/fields")
    async def api_add_field(request: Request):
        payload = await _json_body(request)
        version = _payload_version(payload)
        name = payload.get("name")
        schema = lake.add_field(resolved, version, name)
        return JSONResponse(_schema_payload(schema), status_code=201)

    @app.patch("/api/fields/{field_id}")
    async def api_rename_field(request: Request, field_id: str):
        payload = await _json_body(request)
        version = _payload_version(payload)
        name = payload.get("name")
        schema = lake.rename_field(resolved, version, field_id, name)
        return _schema_payload(schema)

    @app.delete("/api/fields/{field_id}")
    async def api_delete_field(request: Request, field_id: str):
        payload = await _json_body(request)
        version = _payload_version(payload)
        schema = lake.delete_field(resolved, version, field_id)
        return _schema_payload(schema)

    @app.post("/api/batches")
    async def api_upload(
        request: Request,
        version: int = Form(...),
        file: UploadFile = File(...),
    ):
        data = await _read_upload(file)
        batch = lake.upload_csv(resolved, version, data)
        return JSONResponse(_batch_payload(batch), status_code=201)

    @app.get("/api/batches/{batch_id}")
    def api_batch(batch_id: str, format: str = "json"):
        view = lake.get_batch(resolved, batch_id)
        if format == "csv":
            return _download(
                lake.csv_bytes(view.table),
                f"batch-{view.batch.short_id}.csv",
                "text/csv; charset=utf-8",
            )
        if format == "parquet":
            return _download(
                lake.parquet_bytes(view.table),
                f"batch-{view.batch.short_id}.parquet",
                "application/vnd.apache.parquet",
            )
        if format != "json":
            raise lake.ValidationError("format 仅支持 json / csv / parquet。")
        columns, rows = lake.rows_as_display(view.table)
        return {
            "schema_version": view.schema.version,
            "batch": _batch_payload(view.batch),
            "columns": columns,
            "rows": rows,
        }

    @app.get("/api/integrity")
    def api_integrity():
        report = lake.check_integrity(resolved)
        return {
            "ok": report.ok,
            "batches": report.batches,
            "missing_files": list(report.missing_files),
            "hash_mismatches": list(report.hash_mismatches),
            "orphan_files": list(report.orphan_files),
            "temp_files": list(report.temp_files),
        }

    return app


# ----------------------------------------------------------------------
# 辅助
# ----------------------------------------------------------------------


def _latest_version(settings: Settings) -> int | None:
    try:
        return lake.read_schema(settings).version
    except LakeError:
        return None


def _safe_version(settings: Settings) -> int | None:
    try:
        return lake.read_schema(settings).version
    except Exception:  # 错误处理路径不能再次抛错
        return None


def _render(request: Request, name: str, **context):
    return templates.TemplateResponse(request, name, context)


def _render_error(request: Request, exc: LakeError) -> HTMLResponse:
    context = {
        "request": request,
        "settings": resolved_settings(request),
        "title": _ERROR_TITLES.get(exc.code, "操作失败"),
        "detail": str(exc),
    }
    return templates.TemplateResponse(
        request, "error.html", context, status_code=exc.status_code
    )


def resolved_settings(request: Request) -> Settings:
    return request.app.state.settings


_ERROR_TITLES = {
    "validation": "请求无效",
    "stale_schema": "结构版本已过期",
    "not_found": "未找到",
    "storage_error": "存储错误",
    "integrity_error": "数据完整性错误",
    "bad_request": "请求无效",
}


def _redirect_with_message(
    request: Request, message: str, kind: str, path: str | None = None
) -> RedirectResponse:
    target = path or "/"
    params = f"msg={quote(message, safe='')}&kind={quote(kind, safe='')}"
    url = f"{target}?{params}"
    response = RedirectResponse(url, status_code=303)
    return response


def _download(payload: bytes, filename: str, media_type: str) -> Response:
    return Response(
        content=payload,
        media_type=media_type,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


async def _read_upload(file: UploadFile) -> bytes:
    data = await file.read(MAX_UPLOAD_BYTES + 1)
    if len(data) > MAX_UPLOAD_BYTES:
        raise lake.ValidationError("上传文件过大。")
    return data


async def _json_body(request: Request) -> dict:
    try:
        payload = await request.json()
    except Exception:
        raise lake.ValidationError("请求体必须是 JSON。") from None
    if not isinstance(payload, dict):
        raise lake.ValidationError("请求体必须是 JSON 对象。")
    return payload


def _payload_version(payload: dict) -> int:
    raw = payload.get("schema_version", payload.get("version"))
    if isinstance(raw, bool):
        raise lake.ValidationError("schema_version 必须是整数。")
    try:
        return int(raw)
    except (TypeError, ValueError):
        raise lake.ValidationError("必须携带所见结构版本 schema_version。") from None


def _schema_payload(schema: lake.Schema) -> dict:
    return {
        "schema_version": schema.version,
        "fixed_columns": [STATION_COLUMN, TIME_COLUMN],
        "max_fields": MAX_FIELDS,
        "max_rows": MAX_ROWS,
        "fields": [
            {
                "id": f.id,
                "name": f.name,
                "ordinal": f.ordinal,
                "created_version": f.created_version,
                "deleted_version": f.deleted_version,
            }
            for f in schema.active
        ],
        "deleted_fields": [
            {
                "id": f.id,
                "name": f.name,
                "deleted_version": f.deleted_version,
            }
            for f in schema.deleted
        ],
    }


def _batch_payload(batch: lake.Batch) -> dict:
    return {
        "id": batch.id,
        "short_id": batch.short_id,
        "created_at": batch.created_at,
        "schema_version": batch.schema_version,
        "row_count": batch.row_count,
        "file_name": batch.file_name,
        "file_sha256": batch.file_sha256,
        "fields_at_upload": list(batch.fields_at_upload),
    }


app = create_app()
