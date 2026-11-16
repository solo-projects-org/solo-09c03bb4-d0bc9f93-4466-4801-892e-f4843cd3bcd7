"""发布隔离与旧卷串件修复。

场景还原：ST.01 与 ST_01 同日同修订先后发布，旧实现把站号清洗成同一发布
文件名，前站下载到后站内容而响应摘要未变。本文件覆盖：
- 新发布按任务目录隔离，相似站号互不串件；
- 旧卷已串件时，resume 在源站离线情况下用本任务留存完整分片恢复原发布件
  （长度与 SHA-256 符合原登记），健康旧件不受影响、不回退已发布修订；
- 无法恢复时标为不可用并拒绝下载；
- 写文件与更新登记之间退出后重试幂等；双实例修复串行化。
"""
from __future__ import annotations

import dataclasses
import hashlib
import os
import sqlite3
import threading

import httpx
import pytest
from fastapi.testclient import TestClient

from collector import db as schema
from collector.config import Settings
from collector.storage import Storage
from collector.transfer import (
    TaskLockedError,
    repair_publishes,
    run_resume,
)

DATE = "2026-11-16"
ST_DOT = "ST.01"   # 含点站号：合法，必须保留
ST_UND = "ST_01"   # 与 ST.01 清洗后同名的站号


def make_station_blob(station: str, fill: bytes, size: int = 8192) -> bytes:
    """同长度、不同内容的站号文件（证明修复靠的是摘要而非长度巧合）。"""
    head = f"station,{station}\n".encode()
    body = (fill * (size // len(fill) + 1))[: size - len(head)]
    return head + body


class MultiStationSource:
    """多站号模拟源：按 (station, date) 独立供数，支持 Range。"""

    def __init__(self) -> None:
        self.blobs: dict[tuple[str, str], bytes] = {}

    def register(self, station: str, date: str, blob: bytes) -> None:
        self.blobs[(station, date)] = blob

    def manifest(self, station: str, date: str) -> dict:
        blob = self.blobs[(station, date)]
        return {
            "station": station,
            "date": date,
            "revision": 1,
            "etag": f'"{station}-{date}-r1"',
            "size": len(blob),
            "sha256": hashlib.sha256(blob).hexdigest(),
        }

    def handler(self, request: httpx.Request) -> httpx.Response:
        _, kind, station, date = request.url.path.split("/")
        if kind == "manifest":
            return httpx.Response(200, json=self.manifest(station, date))
        assert kind == "data"
        blob = self.blobs[(station, date)]
        size = len(blob)
        headers = {"ETag": f'"{station}-{date}-r1"'}
        rng = request.headers.get("range")
        if rng and rng.startswith("bytes="):
            start_s, _, end_s = rng[len("bytes="):].partition("-")
            start = int(start_s)
            if start >= size:  # 已下满：源站按完整响应处理
                return httpx.Response(200, headers=headers, content=blob)
            end = int(end_s) if end_s else size - 1
            headers["Content-Range"] = f"bytes {start}-{end}/{size}"
            return httpx.Response(206, headers=headers, content=blob[start:end + 1])
        return httpx.Response(200, headers=headers, content=blob)


def offline_handler(request: httpx.Request) -> httpx.Response:
    raise httpx.ConnectError("source offline")


def reset_thread_conn() -> None:
    cached = getattr(schema._local, "conn", None)
    if cached is not None:
        cached.close()
        schema._local.conn = None


def make_env(tmp_path, handler) -> tuple[Settings, Storage, schema.Database]:
    reset_thread_conn()
    storage = Storage(str(tmp_path / "data"))
    database = schema.Database(storage.db_path())
    settings = Settings(
        source_base_url="http://source.test",
        data_dir=storage.data_dir,
        instance_id="repair-test",
        chunk_size=1024,
        progress_flush_bytes=4096,
        transport=httpx.MockTransport(handler),
    )
    return settings, storage, database


def publish_two_colliding_stations(tmp_path):
    """在线发布 ST.01 / ST_01（同长度不同内容），返回现场对象。"""
    src = MultiStationSource()
    blob_a = make_station_blob(ST_DOT, b"A")
    blob_b = make_station_blob(ST_UND, b"B")
    assert len(blob_a) == len(blob_b) and blob_a != blob_b
    src.register(ST_DOT, DATE, blob_a)
    src.register(ST_UND, DATE, blob_b)
    settings, storage, database = make_env(tmp_path, src.handler)
    task_a = database.create_task(ST_DOT, DATE)["id"]
    task_b = database.create_task(ST_UND, DATE)["id"]
    run_resume(database, storage, settings, task_a)
    run_resume(database, storage, settings, task_b)
    return settings, storage, database, task_a, task_b, blob_a, blob_b


def craft_legacy_contamination(
    storage: Storage, database: schema.Database, task_ids: list[int], content: bytes
) -> str:
    """把指定任务的发布登记改指到旧式顶层共享路径，并写入给定内容。

    复现旧卷现场：站号清洗撞名导致多个任务的发布件落在同一路径，
    后发布者覆盖先发布者，而各任务的登记摘要仍是各自原值。
    """
    legacy = os.path.join(storage.published_dir, f"ST_01_{DATE}_r1.csv")
    with open(legacy, "wb") as fh:
        fh.write(content)
    conn = sqlite3.connect(database.path)
    marks = ",".join("?" for _ in task_ids)
    conn.execute(
        f"UPDATE publishes SET file_path=? WHERE task_id IN ({marks})",
        (legacy, *task_ids),
    )
    conn.commit()
    conn.close()
    return legacy


# ---------------------------------------------------------------- 隔离


def test_similar_station_names_publish_isolated(tmp_path):
    """ST.01 与 ST_01 同日同修订先后发布：发布件隔离，下载各归各。"""
    _, storage, database, task_a, task_b, blob_a, blob_b = (
        publish_two_colliding_stations(tmp_path)
    )
    pub_a = database.latest_published(task_a)
    pub_b = database.latest_published(task_b)
    # 发布路径必须不同（旧实现二者同为 published/ST_01_..._r1.csv 互相覆盖）
    assert pub_a["file_path"] != pub_b["file_path"]
    with open(pub_a["file_path"], "rb") as fh:
        assert fh.read() == blob_a
    with open(pub_b["file_path"], "rb") as fh:
        assert fh.read() == blob_b
    # 登记摘要与各自正文一致
    assert pub_a["sha256"] == hashlib.sha256(blob_a).hexdigest()
    assert pub_b["sha256"] == hashlib.sha256(blob_b).hexdigest()


# ---------------------------------------------------------------- 离线修复


def test_legacy_contamination_repaired_while_source_offline(tmp_path):
    """旧卷已串件：源站离线时 resume 用留存分片恢复原发布件。"""
    settings, storage, database, task_a, task_b, blob_a, blob_b = (
        publish_two_colliding_stations(tmp_path)
    )
    legacy = craft_legacy_contamination(storage, database, [task_a, task_b], blob_b)

    # 源站离线：传输必然失败，但修复必须先于网络完成
    offline = dataclasses.replace(
        settings, transport=httpx.MockTransport(offline_handler)
    )
    with pytest.raises(httpx.HTTPError):
        run_resume(database, storage, offline, task_a)

    # ST.01 的发布件已恢复到任务隔离路径，长度与 SHA-256 符合原登记
    pub_a = database.latest_published(task_a)
    assert pub_a["available"] == 1
    assert pub_a["file_path"] != legacy
    with open(pub_a["file_path"], "rb") as fh:
        restored = fh.read()
    assert restored == blob_a
    assert hashlib.sha256(restored).hexdigest() == pub_a["sha256"]
    assert len(restored) == pub_a["size"]

    # 健康旧件（ST_01 在共享路径上的内容）未被修复动作损坏
    pub_b = database.latest_published(task_b)
    assert pub_b["file_path"] == legacy
    with open(pub_b["file_path"], "rb") as fh:
        assert fh.read() == blob_b

    # 不回退已发布修订：当前修订号与发布登记保持
    task = database.get_task(task_a)
    assert task["current_revision"] == 1
    assert database.latest_published(task_a)["revision"] == 1


def test_unrecoverable_marked_unavailable(tmp_path):
    """串件且本任务无完整分片：标为不可用。"""
    settings, storage, database, task_a, task_b, blob_a, blob_b = (
        publish_two_colliding_stations(tmp_path)
    )
    craft_legacy_contamination(storage, database, [task_a, task_b], blob_b)
    os.unlink(storage.part_path(str(task_a), 1))  # 留存分片也没了

    offline = dataclasses.replace(
        settings, transport=httpx.MockTransport(offline_handler)
    )
    with pytest.raises(httpx.HTTPError):
        run_resume(database, storage, offline, task_a)

    pub_a = database.latest_published(task_a)
    assert pub_a["available"] == 0
    assert pub_a["unavailable_reason"]
    # 健康旧件不受影响
    pub_b = database.latest_published(task_b)
    assert pub_b["available"] == 1
    with open(pub_b["file_path"], "rb") as fh:
        assert fh.read() == blob_b


def test_repair_retry_after_crash_between_write_and_register(tmp_path):
    """写文件与更新登记之间退出：重试幂等收敛，不串件、不报错。"""
    settings, storage, database, task_a, task_b, blob_a, blob_b = (
        publish_two_colliding_stations(tmp_path)
    )
    # 旧卷现场：两任务登记同一共享路径，内容为后发布者（ST_01）
    legacy = craft_legacy_contamination(storage, database, [task_a, task_b], blob_b)

    # 模拟“文件已写好、登记未更新”的崩溃现场：只落文件，不动登记
    part = storage.part_path(str(task_a), 1)
    crashed_final = storage.final_path(str(task_a), ST_DOT, DATE, 1)
    storage.publish_atomic(part, crashed_final)
    assert database.latest_published(task_a)["file_path"] == legacy

    # 重试修复：幂等完成
    repairs = repair_publishes(database, storage, task_a)
    assert [r["action"] for r in repairs] == ["restored"]
    pub = database.latest_published(task_a)
    assert pub["file_path"] == crashed_final
    with open(pub["file_path"], "rb") as fh:
        assert fh.read() == blob_a
    # 再跑一次：完全幂等，无动作
    assert repair_publishes(database, storage, task_a) == []
    with open(pub["file_path"], "rb") as fh:
        assert fh.read() == blob_a


def test_two_instances_offline_repair_is_serialized(tmp_path):
    """双实例同时离线 resume：锁串行化，只有一个执行修复，结果一致。"""
    settings, storage, database, task_a, task_b, blob_a, blob_b = (
        publish_two_colliding_stations(tmp_path)
    )
    craft_legacy_contamination(storage, database, [task_a, task_b], blob_b)

    results: list[str] = []
    barrier = threading.Barrier(2)

    def worker(inst: str) -> None:
        s = dataclasses.replace(
            settings,
            instance_id=inst,
            transport=httpx.MockTransport(offline_handler),
        )
        barrier.wait()
        try:
            run_resume(database, storage, s, task_a)
            results.append("ok")
        except TaskLockedError:
            results.append("locked")
        except httpx.HTTPError:
            results.append("offline")  # 拿到锁的实例完成修复后传输失败

    t1 = threading.Thread(target=worker, args=("inst-1",))
    t2 = threading.Thread(target=worker, args=("inst-2",))
    t1.start(); t2.start(); t1.join(); t2.join()

    assert sorted(results) == ["locked", "offline"]
    pub = database.latest_published(task_a)
    assert pub["available"] == 1
    with open(pub["file_path"], "rb") as fh:
        assert fh.read() == blob_a


# ---------------------------------------------------------------- API 级


@pytest.fixture
def api_env(tmp_path):
    from collector import app as app_module
    from collector.config import settings as app_settings

    reset_thread_conn()
    storage = Storage(str(tmp_path / "data"))
    database = schema.Database(storage.db_path())
    src = MultiStationSource()

    def use_handler(handler) -> None:
        object.__setattr__(
            app_settings, "transport", httpx.MockTransport(handler)
        )

    object.__setattr__(app_settings, "instance_id", "repair-api-test")
    use_handler(src.handler)
    app_module.configure_for_testing(storage, database)

    with TestClient(app_module.app) as client:
        yield client, storage, database, src, use_handler


def _create_and_publish(client, station: str) -> int:
    task_id = client.post(
        "/tasks", json={"station": station, "date": DATE}
    ).json()["id"]
    r = client.post(f"/tasks/{task_id}/resume")
    assert r.status_code == 200
    return task_id


def test_api_isolation_and_download_headers_match_body(api_env):
    """API 全流程：相似站号各自发布，下载正文与响应修订/摘要一致。"""
    client, _, _, src, _ = api_env
    blob_a = make_station_blob(ST_DOT, b"A")
    blob_b = make_station_blob(ST_UND, b"B")
    src.register(ST_DOT, DATE, blob_a)
    src.register(ST_UND, DATE, blob_b)
    task_a = _create_and_publish(client, ST_DOT)
    task_b = _create_and_publish(client, ST_UND)

    ra = client.get(f"/tasks/{task_a}/download")
    rb = client.get(f"/tasks/{task_b}/download")
    assert ra.status_code == 200 and rb.status_code == 200
    assert ra.content == blob_a and rb.content == blob_b
    # 正文必须与响应中的修订和摘要一致
    assert ra.headers["x-revision"] == "1"
    assert ra.headers["x-sha256"] == hashlib.sha256(ra.content).hexdigest()
    assert rb.headers["x-sha256"] == hashlib.sha256(rb.content).hexdigest()


def test_api_download_guard_and_offline_repair_flow(api_env):
    """旧卷串件：修复前串件方 409、健康方可下；离线 resume 修复后可下。"""
    client, storage, database, src, use_handler = api_env
    blob_a = make_station_blob(ST_DOT, b"A")
    blob_b = make_station_blob(ST_UND, b"B")
    src.register(ST_DOT, DATE, blob_a)
    src.register(ST_UND, DATE, blob_b)
    task_a = _create_and_publish(client, ST_DOT)
    task_b = _create_and_publish(client, ST_UND)
    craft_legacy_contamination(storage, database, [task_a, task_b], blob_b)

    # 修复前：串件方（ST.01）拒绝外发；健康方（ST_01）正常下载
    assert client.get(f"/tasks/{task_a}/download").status_code == 409
    rb = client.get(f"/tasks/{task_b}/download")
    assert rb.status_code == 200 and rb.content == blob_b
    assert rb.headers["x-sha256"] == hashlib.sha256(blob_b).hexdigest()

    # 源站离线：resume 返回 503，但本地修复已完成
    use_handler(offline_handler)
    r = client.post(f"/tasks/{task_a}/resume")
    assert r.status_code == 503
    ra = client.get(f"/tasks/{task_a}/download")
    assert ra.status_code == 200
    assert ra.content == blob_a
    assert ra.headers["x-sha256"] == hashlib.sha256(blob_a).hexdigest()
    # 健康旧件仍可下载
    assert client.get(f"/tasks/{task_b}/download").content == blob_b


def test_api_unavailable_download_rejected(api_env):
    """无法恢复：标为不可用，下载 409，任务详情可见原因。"""
    client, storage, database, src, use_handler = api_env
    blob_a = make_station_blob(ST_DOT, b"A")
    blob_b = make_station_blob(ST_UND, b"B")
    src.register(ST_DOT, DATE, blob_a)
    src.register(ST_UND, DATE, blob_b)
    task_a = _create_and_publish(client, ST_DOT)
    task_b = _create_and_publish(client, ST_UND)
    craft_legacy_contamination(storage, database, [task_a, task_b], blob_b)
    os.unlink(storage.part_path(str(task_a), 1))

    use_handler(offline_handler)
    assert client.post(f"/tasks/{task_a}/resume").status_code == 503

    detail = client.get(f"/tasks/{task_a}").json()
    assert detail["latest_published"]["available"] is False
    assert detail["latest_published"]["unavailable_reason"]
    r = client.get(f"/tasks/{task_a}/download")
    assert r.status_code == 409
    # 他人任务不受影响
    assert client.get(f"/tasks/{task_b}/download").status_code == 200
