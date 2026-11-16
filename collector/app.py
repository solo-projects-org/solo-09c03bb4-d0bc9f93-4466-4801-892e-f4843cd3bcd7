"""采集服务 FastAPI 入口。

API：
- POST /tasks                 按站号、日期建任务（日包）
- POST /tasks/{id}/resume     继续采集（断点续传；锁忙返回 409）
- GET  /tasks                 任务清单
- GET  /tasks/{id}            进度详情（版本/分片/发布信息）
- GET  /tasks/{id}/download   下载已发布原件（未发布返回 409，杜绝半件外发）
- GET  /health                健康检查，含实例 ID
"""
from __future__ import annotations

import os
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from . import db as schema
from .config import settings
from .storage import Storage
from .transfer import (
    TaskLockedError,
    TransferError,
    VerificationError,
    run_resume,
)

app = FastAPI(
    title="CTD Daily Package Collector",
    version="1.0.0",
    description="海洋 CTD 日包断点续传采集服务（FastAPI + SQLite）",
)

storage = Storage(settings.data_dir)
database = schema.Database(storage.db_path())
storage.cleanup_tmp()


def configure_for_testing(test_storage: Storage, test_database: schema.Database) -> None:
    """测试钩子：替换应用级 Storage/Database（生产环境不调用）。"""
    global storage, database
    storage = test_storage
    database = test_database


class TaskCreate(BaseModel):
    station: str = Field(..., min_length=1, max_length=64, examples=["ST01"])
    date: str = Field(..., min_length=1, max_length=32, examples=["2026-11-16"])


@app.get("/health")
def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "instance_id": settings.instance_id,
        "source_base_url": settings.source_base_url,
        "data_dir": settings.data_dir,
    }


@app.post("/tasks", status_code=201)
def create_task(body: TaskCreate) -> dict[str, Any]:
    try:
        return database.task_detail(database.create_task(body.station, body.date)["id"])
    except schema.TaskAlreadyExists:
        existing = database.find_task(body.station, body.date)
        raise HTTPException(
            status_code=409,
            detail={
                "message": "该站号/日期任务已存在",
                "task_id": existing["id"],
            },
        )


@app.get("/tasks")
def list_tasks() -> dict[str, Any]:
    return {"tasks": [database.task_detail(t["id"]) for t in database.list_tasks()]}


@app.get("/tasks/{task_id}")
def get_task(task_id: int) -> dict[str, Any]:
    try:
        return database.task_detail(task_id)
    except schema.TaskNotFound:
        raise HTTPException(status_code=404, detail="任务不存在")


@app.post("/tasks/{task_id}/resume")
def resume_task(task_id: int) -> dict[str, Any]:
    try:
        database.get_task(task_id)
    except schema.TaskNotFound:
        raise HTTPException(status_code=404, detail="任务不存在")

    try:
        return run_resume(database, storage, settings, task_id)
    except TaskLockedError:
        raise HTTPException(
            status_code=409,
            detail="另一采集实例正在处理该任务，请稍后重试",
        )
    except VerificationError as exc:
        raise HTTPException(status_code=502, detail=f"原件校验失败：{exc}")
    except (TransferError, httpx.HTTPError) as exc:
        # 传输失败不等于任务失败：分片已保留，可再次调用 resume 继续
        raise HTTPException(
            status_code=503,
            detail={"message": f"采集未完成，可继续重试：{exc}", "retry": "POST /tasks/{id}/resume"},
        )


@app.get("/tasks/{task_id}/download")
def download(task_id: int) -> FileResponse:
    try:
        database.get_task(task_id)
    except schema.TaskNotFound:
        raise HTTPException(status_code=404, detail="任务不存在")

    publish = database.latest_published(task_id)
    if publish is None or not os.path.isfile(publish["file_path"]):
        raise HTTPException(
            status_code=409,
            detail="原件尚未发布（长度与 SHA-256 校验未通过），拒绝下载半件",
        )

    filename = os.path.basename(publish["file_path"])
    return FileResponse(
        publish["file_path"],
        media_type="text/csv",
        filename=filename,
        headers={
            "X-Task-Id": str(task_id),
            "X-Revision": str(publish["revision"]),
            "X-SHA256": publish["sha256"],
        },
    )
