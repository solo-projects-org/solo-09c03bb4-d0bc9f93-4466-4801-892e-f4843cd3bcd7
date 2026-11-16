"""API 层测试：建任务、继续、进度、下载防半件、重复建任务 409。"""
from __future__ import annotations

import httpx
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client(tmp_path, fake_source):
    # 应用只导入一次；通过测试钩子替换每个用例独立的 Storage/Database
    data_dir = tmp_path / "data"
    from collector import app as app_module
    from collector.config import settings
    from collector.storage import Storage as AppStorage
    from collector import db as app_db

    # 重置线程局部连接，避免指向其他测试已删除的库
    cached = getattr(app_db._local, "conn", None)
    if cached is not None:
        cached.close()
        app_db._local.conn = None

    test_storage = AppStorage(str(data_dir))
    test_db = app_db.Database(test_storage.db_path())

    transport = httpx.MockTransport(fake_source.handler)
    object.__setattr__(settings, "transport", transport)
    object.__setattr__(settings, "instance_id", "api-test")
    app_module.configure_for_testing(test_storage, test_db)

    with TestClient(app_module.app) as c:
        yield c, fake_source, data_dir


def test_health(client):
    c, _, _ = client
    body = c.get("/health").json()
    assert body["status"] == "ok"
    assert body["instance_id"] == "api-test"


def test_create_resume_progress_download_flow(client):
    c, source, data_dir = client
    r = c.post("/tasks", json={"station": "ST01", "date": "2026-11-16"})
    assert r.status_code == 201
    task = r.json()
    tid = task["id"]
    assert task["state"] == "PENDING"

    # 重复建任务 -> 409
    dup = c.post("/tasks", json={"station": "ST01", "date": "2026-11-16"})
    assert dup.status_code == 409
    assert dup.json()["detail"]["task_id"] == tid

    r = c.post(f"/tasks/{tid}/resume")
    assert r.status_code == 200
    detail = r.json()
    assert detail["state"] == "PUBLISHED"
    assert detail["active_version"]["progress_percent"] == 100.0

    r = c.get(f"/tasks/{tid}")
    assert r.status_code == 200
    assert r.json()["current_revision"] == 1

    r = c.get(f"/tasks/{tid}/download")
    assert r.status_code == 200
    assert r.headers["x-revision"] == "1"
    assert r.headers["x-sha256"] == source.manifest()["sha256"]
    assert r.content == source.blobs[1]


def test_download_rejected_while_unpublished(client):
    c, source, _ = client
    tid = c.post("/tasks", json={"station": "ST02", "date": "2026-11-16"}).json()["id"]
    source.sticky_drop = True
    source.drop_after = 200
    r = c.post(f"/tasks/{tid}/resume")
    assert r.status_code == 503  # 断流后可重试

    r = c.get(f"/tasks/{tid}/download")
    assert r.status_code == 409  # 半件不得下载

    source.sticky_drop = False
    source.drop_after = None
    r = c.post(f"/tasks/{tid}/resume")
    assert r.status_code == 200
    r = c.get(f"/tasks/{tid}/download")
    assert r.status_code == 200


def test_unknown_task_404(client):
    c, _, _ = client
    assert c.get("/tasks/9999").status_code == 404
    assert c.post("/tasks/9999/resume").status_code == 404


def test_resume_after_reversion_serves_old_and_then_new(client):
    c, source, _ = client
    tid = c.post("/tasks", json={"station": "ST01", "date": "2026-11-16"}).json()["id"]
    c.post(f"/tasks/{tid}/resume")
    old = c.get(f"/tasks/{tid}/download").content

    source.bump()
    source.sticky_drop = True
    source.drop_after = 150
    r = c.post(f"/tasks/{tid}/resume")
    assert r.status_code == 503

    # 新版未完成期间，下载仍返回旧版一致原件
    served = c.get(f"/tasks/{tid}/download")
    assert served.headers["x-revision"] == "1"
    assert served.content == old

    source.sticky_drop = False
    source.drop_after = None
    r = c.post(f"/tasks/{tid}/resume")
    assert r.status_code == 200
    served = c.get(f"/tasks/{tid}/download")
    assert served.headers["x-revision"] == "2"
    assert served.content == source.blobs[2]
