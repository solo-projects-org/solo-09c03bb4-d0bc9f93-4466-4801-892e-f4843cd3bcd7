"""测试夹具：用内存/文件级模拟源站支撑采集引擎测试。"""
from __future__ import annotations

import dataclasses
import hashlib
import os
import sys
import tempfile

# 任何 collector 导入之前先给出可写的默认数据目录，避免模块级初始化触碰 /data
os.environ.setdefault("COLLECTOR_DATA_DIR", tempfile.mkdtemp(prefix="ctd-default-"))

import httpx  # noqa: E402
import pytest  # noqa: E402

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collector import db as schema  # noqa: E402
from collector.config import Settings  # noqa: E402
from collector.storage import Storage  # noqa: E402

STATION = "ST01"
DATE = "2026-11-16"


def make_blob(rev: int, size: int = 4096) -> bytes:
    lines = ["station,date,revision,seq,pressure,temp,salinity,oxygen,ts"]
    seq = 0
    while True:
        lines.append(f"{STATION},{DATE},r{rev},{seq},1.23,2.34,33.4,5.6,2026-01-01T00:00:00Z")
        seq += 1
        blob = ("\n".join(lines) + "\n").encode()
        if len(blob) >= size:
            return blob


def etag_for(rev: int) -> str:
    return f'"{STATION}-{DATE}-r{rev}"'


class FakeSource:
    """可换版/断流/忽略 Range 的模拟源，行为对齐 source/app.py 语义。"""

    def __init__(self) -> None:
        self.revision = 1
        self.blobs: dict[int, bytes] = {1: make_blob(1)}
        self.drop_after: int | None = None
        self.sticky_drop = False  # True 时每次 data 请求都断流，直到显式关闭
        self.ignore_range = False
        self.request_log: list[dict] = []
        # 下一次 data 请求前先换版，模拟“清单与数据之间换版”
        self.bump_before_data = False

    def bump(self) -> dict:
        self.revision += 1
        self.blobs[self.revision] = make_blob(self.revision)
        self.drop_after = None
        return self.manifest()

    def manifest(self) -> dict:
        blob = self.blobs[self.revision]
        return {
            "station": STATION,
            "date": DATE,
            "revision": self.revision,
            "etag": etag_for(self.revision),
            "size": len(blob),
            "sha256": hashlib.sha256(blob).hexdigest(),
        }

    def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.startswith("/manifest/"):
            return httpx.Response(200, json=self.manifest())

        assert path.startswith("/data/")
        if self.bump_before_data:
            self.bump()
            self.bump_before_data = False
        self.request_log.append({"headers": dict(request.headers)})
        blob = self.blobs[self.revision]
        size = len(blob)
        rng = request.headers.get("range")
        drop = self.drop_after
        if not self.sticky_drop:
            self.drop_after = None

        start = 0
        status = 200
        if rng and not self.ignore_range and rng.startswith("bytes="):
            spec = rng[len("bytes="):]
            start_s, _, end_s = spec.partition("-")
            start = int(start_s)
            end = int(end_s) if end_s else size - 1
            chunk = blob[start:end + 1]
            status = 206
            headers = {"Content-Range": f"bytes {start}-{end}/{size}", "ETag": etag_for(self.revision)}
        else:
            chunk = blob
            headers = {"ETag": etag_for(self.revision)}

        if drop is not None:
            chunk = chunk[: max(0, drop)]
            # 模拟连接被掐断：逐字节交付已下发部分，随后抛 ReadError。
            # 逐字节 yield 可保证任意 iter_bytes 块大小下都能先收到部分数据，
            # 这等价于真实 TCP 链路在任意字节边界断开的最坏情况。
            def broken_stream() -> bytes:
                for byte in chunk:
                    yield bytes((byte,))
                raise httpx.ReadError("simulated connection drop")

            return httpx.Response(status, headers=headers, content=broken_stream())

        return httpx.Response(status, headers=headers, content=chunk)


@pytest.fixture
def fake_source() -> FakeSource:
    return FakeSource()


@pytest.fixture
def env(tmp_path, fake_source: FakeSource):
    transport = httpx.MockTransport(fake_source.handler)
    base = Settings(
        source_base_url="http://source.test",
        data_dir=str(tmp_path / "data"),
        instance_id="test-inst",
        chunk_size=32,
        progress_flush_bytes=256,
        transport=transport,
    )
    storage = Storage(base.data_dir)
    # 线程局部连接可能仍指向上一个测试已删除的库文件，强制重建
    import collector.db as db_mod
    cached = getattr(db_mod._local, "conn", None)
    if cached is not None:
        cached.close()
        db_mod._local.conn = None
    database = schema.Database(storage.db_path())
    task_id = database.create_task(STATION, DATE)["id"]
    return _Env(base, storage, database, task_id, fake_source, tmp_path)


@dataclasses.dataclass
class _Env:
    settings: Settings
    storage: Storage
    db: schema.Database
    task_id: int
    source: FakeSource
    tmp: object

    def resume(self):
        from collector.transfer import run_resume
        return run_resume(self.db, self.storage, self.settings, self.task_id)

    def part_path(self, rev: int) -> str:
        return self.storage.part_path(str(self.task_id), rev)
