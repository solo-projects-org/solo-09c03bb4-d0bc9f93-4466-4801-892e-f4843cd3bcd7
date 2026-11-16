"""模拟海洋调查 CTD 日包源站。

能力：
- GET /manifest/{station}/{date}  返回递增修订号、强 ETag、长度、SHA-256
- GET /data/{station}/{date}      下载当前原件，支持 Range（可被配置为忽略）
- POST /admin/revision            换版（修订号 +1，重新生成内容/摘要）
- POST /admin/drop                一次性故障注入：下发 N 字节后断流
- POST /admin/ignore-range        切换是否忽略 Range 请求
- POST /admin/station             登记新测站/日期包

所有内容按 (station, date, revision) 确定性生成，同一修订号在重启/重新生成后
字节与 SHA-256 完全一致，便于校验。
"""
from __future__ import annotations

import asyncio
import contextlib
import hashlib
import os
import time
from dataclasses import dataclass, field
from typing import Dict, Tuple

from fastapi import FastAPI, Request
from fastapi.responses import Response, StreamingResponse
from pydantic import BaseModel, Field

DEFAULT_PACKAGE_BYTES = int(os.environ.get("SOURCE_PACKAGE_BYTES", str(512 * 1024)))

Key = Tuple[str, str]


@dataclass
class Package:
    station: str
    date: str
    revision: int
    ignore_range: bool = False
    # 一次性断流：下次 GET data 时发出 drop_after 字节后主动断开；None 表示正常
    drop_after: int | None = None
    # 一次性延迟：下次 GET data 响应前等待的秒数，用于确定性并发测试
    delay_seconds: float = 0.0
    _blob_cache: bytes | None = field(default=None, repr=False)

    def blob(self) -> bytes:
        if self._blob_cache is None:
            self._blob_cache = generate_blob(self.station, self.date, self.revision)
        return self._blob_cache

    def etag(self) -> str:
        return f'"{self.station}-{self.date}-r{self.revision}"'

    def sha256(self) -> str:
        return hashlib.sha256(self.blob()).hexdigest()

    def length(self) -> int:
        return len(self.blob())


def generate_blob(station: str, date: str, revision: int) -> bytes:
    """确定性生成 CTD CSV 内容（同一三元组永远得到相同字节）。"""
    seed = int(hashlib.sha256(f"{station}|{date}|{revision}".encode()).hexdigest(), 16)
    # 简单 LCG，避免依赖 random 的全局状态
    state = seed
    lines = [
        "station,date,revision,sequence,pressure_dbar,temperature_c,salinity_psu,oxygen_ml_l,recorded_at"
    ]
    target = DEFAULT_PACKAGE_BYTES
    seq = 0
    while True:
        state = (state * 6364136223846793005 + 1442695040888963407) & ((1 << 64) - 1)
        pressure = (state % 60000) / 100.0
        state = (state * 6364136223846793005 + 1442695040888963407) & ((1 << 64) - 1)
        temp = (state % 3500) / 100.0 - 2.0
        state = (state * 6364136223846793005 + 1442695040888963407) & ((1 << 64) - 1)
        salinity = (state % 800) / 100.0 + 30.0
        state = (state * 6364136223846793005 + 1442695040888963407) & ((1 << 64) - 1)
        oxygen = (state % 1200) / 100.0
        lines.append(
            f"{station},{date},r{revision},{seq},{pressure:.2f},{temp:.2f},"
            f"{salinity:.2f},{oxygen:.2f},2026-01-01T00:00:{seq % 60:02d}Z"
        )
        seq += 1
        if sum(len(l) + 1 for l in lines) >= target:
            break
    return ("\n".join(lines) + "\n").encode("utf-8")


@contextlib.asynccontextmanager
async def lifespan(_app: FastAPI):
    get_or_create(DEFAULT_STATION, DEFAULT_DATE)
    yield


app = FastAPI(title="CTD Source Station", version="1.0.0", lifespan=lifespan)
PACKAGES: Dict[Key, Package] = {}
DEFAULT_STATION = os.environ.get("SOURCE_DEFAULT_STATION", "ST01")
DEFAULT_DATE = os.environ.get("SOURCE_DEFAULT_DATE", "2026-11-16")


def get_or_create(station: str, date: str) -> Package:
    if (station, date) not in PACKAGES:
        PACKAGES[(station, date)] = Package(station=station, date=date, revision=1)
    return PACKAGES[(station, date)]


class RevisionBody(BaseModel):
    station: str
    date: str


class DropBody(BaseModel):
    station: str
    date: str
    drop_after: int = Field(..., ge=0, description="下发该字节数后主动断流（一次性）")


class IgnoreRangeBody(BaseModel):
    station: str
    date: str
    ignore: bool


class DelayBody(BaseModel):
    station: str
    date: str
    delay_seconds: float = Field(..., ge=0, le=60, description="下次 data 请求响应前延迟秒数（一次性）")


class StationBody(BaseModel):
    station: str
    date: str
    revision: int = Field(1, ge=1)


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "packages": len(PACKAGES)}


@app.post("/admin/station")
def admin_station(body: StationBody) -> dict:
    pkg = get_or_create(body.station, body.date)
    pkg.revision = max(pkg.revision, body.revision)
    pkg._blob_cache = None
    return _manifest_dict(pkg)


@app.post("/admin/revision")
def admin_revision(body: RevisionBody) -> dict:
    pkg = get_or_create(body.station, body.date)
    pkg.revision += 1
    pkg._blob_cache = None  # 内容随修订号确定性变化
    pkg.drop_after = None
    return _manifest_dict(pkg)


@app.post("/admin/drop")
def admin_drop(body: DropBody) -> dict:
    pkg = get_or_create(body.station, body.date)
    pkg.drop_after = body.drop_after
    return {"station": body.station, "date": body.date, "drop_after": body.drop_after}


@app.post("/admin/ignore-range")
def admin_ignore_range(body: IgnoreRangeBody) -> dict:
    pkg = get_or_create(body.station, body.date)
    pkg.ignore_range = body.ignore
    return {"station": body.station, "date": body.date, "ignore_range": body.ignore}


@app.post("/admin/delay")
def admin_delay(body: DelayBody) -> dict:
    pkg = get_or_create(body.station, body.date)
    pkg.delay_seconds = body.delay_seconds
    return {"station": body.station, "date": body.date, "delay_seconds": body.delay_seconds}


def _manifest_dict(pkg: Package) -> dict:
    return {
        "station": pkg.station,
        "date": pkg.date,
        "revision": pkg.revision,
        "etag": pkg.etag(),
        "size": pkg.length(),
        "sha256": pkg.sha256(),
    }


@app.get("/manifest/{station}/{date}")
def manifest(station: str, date: str) -> dict:
    pkg = get_or_create(station, date)
    return _manifest_dict(pkg)


def _parse_range(range_header: str | None, size: int) -> tuple[int, int] | None:
    """解析 bytes=start-end，仅支持单区间。返回 (start, end_inclusive)。"""
    if not range_header or not range_header.startswith("bytes="):
        return None
    spec = range_header[len("bytes="):]
    if "," in spec:
        return None
    start_s, _, end_s = spec.partition("-")
    try:
        if start_s == "":
            suffix = int(end_s)
            if suffix <= 0:
                return None
            return max(0, size - suffix), size - 1
        start = int(start_s)
        end = int(end_s) if end_s else size - 1
    except ValueError:
        return None
    if start >= size or end < start:
        return None
    return start, min(end, size - 1)


@app.get("/data/{station}/{date}")
async def data(station: str, date: str, request: Request) -> Response:
    pkg = get_or_create(station, date)
    if pkg.delay_seconds > 0:
        delay = pkg.delay_seconds
        pkg.delay_seconds = 0.0
        await asyncio.sleep(delay)
    blob = pkg.blob()
    size = len(blob)
    range_header = request.headers.get("range")
    drop = pkg.drop_after
    # 故障是一次性的，触发后即清除（无论本次请求从哪开始）
    pkg.drop_after = None

    rng = None if pkg.ignore_range else _parse_range(range_header, size)
    if rng is None:
        start = 0
        end = size - 1
        status = 200
    else:
        start, end = rng
        status = 206

    chunk = blob[start:end + 1]
    headers = {
        "ETag": pkg.etag(),
        "Accept-Ranges": "none" if pkg.ignore_range else "bytes",
        "Last-Modified": time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime()),
        "X-Revision": str(pkg.revision),
    }
    if status == 206:
        headers["Content-Range"] = f"bytes {start}-{end}/{size}"
        headers["Content-Length"] = str(len(chunk))
    else:
        headers["Content-Length"] = str(len(chunk))

    if drop is None:
        return Response(content=chunk, status_code=status, headers=headers)

    # 断流注入。要点：httpx 等客户端对“流中途断链”是按读取块（默认 64 KiB）
    # 全有或全无交付的——若已发字节不足一个读取块，断链时这些字节会被整体丢弃。
    # 因此先发一个完整的 64 KiB 大块（客户端必然吐出并落盘），停顿待其消费后，
    # 再发送剩余截断字节并在流中抛异常关闭连接，模拟真实链路在任意字节处断开。
    keep = chunk[: max(0, drop)]
    safe_headers = {k: v for k, v in headers.items() if k.lower() != "content-length"}
    read_block = 64 * 1024

    async def broken_stream():
        if len(keep) >= read_block:
            # 第一个完整读取块，确保对端先收到并落盘
            yield keep[:read_block]
            await asyncio.sleep(0.3)
            tail = keep[read_block:]
            if tail:
                yield tail
                await asyncio.sleep(0.15)
        else:
            # 截断量本身不足一块：仍尽量发出，客户端可能整体丢弃，属可接受边界
            yield keep
            await asyncio.sleep(0.15)
        raise ConnectionResetError("injected connection drop")

    return StreamingResponse(
        broken_stream(), status_code=status, headers=safe_headers
    )
