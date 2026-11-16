"""断点续传采集引擎。

关键不变量：
1. 同一任务的采集由跨实例文件锁（flock）互斥，两个实例同时继续不会交错写同一分片。
2. 分片按修订号隔离；已有字节通过 HTTP Range 续传，整个原件不会载入内存。
3. 若服务器忽略 Range 而返回 200 完整响应，或续传意外得到完整响应，
   必须丢弃旧分片从头重新取得一致原件，绝不直接追加。
4. ETag（强校验）与 206 响应前缀同时校验；换版/字节不一致立即中止并重取。
5. 长度与 SHA-256 同时符合清单才允许发布；发布为原子操作并带防回退守卫。
6. 发布件按任务目录隔离，任何两个任务的发布件不可能同路径（杜绝站号清洗串件）。
7. 每轮继续在任何网络请求之前先修复本任务已登记发布件：旧卷串件/缺失时
   仅用本地留存完整分片恢复（长度与 SHA-256 须符合原登记），源站离线也能修；
   无法恢复的标为不可用并拒绝下载，绝不外发与登记摘要不符的内容。
"""
from __future__ import annotations

import contextlib
import fcntl
import hashlib
import logging
import os
from dataclasses import dataclass
from typing import Any, Iterator

import httpx

from . import db as schema
from .config import Settings
from .storage import Storage

logger = logging.getLogger(__name__)


class TransferError(Exception):
    """可恢复的传输错误（中断后已存字节保留，下次继续复用）。"""


class VerificationError(Exception):
    """原件与清单不一致（长度/摘要/ETag），本次分片作废重取。"""


class TaskLockedError(Exception):
    """另一实例/线程正在采集该任务。"""


@dataclass
class LockHandle:
    fd: int
    path: str


@contextlib.contextmanager
def task_lock(lock_path: str) -> Iterator[LockHandle]:
    os.makedirs(os.path.dirname(lock_path), exist_ok=True)
    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(fd)
            raise TaskLockedError(lock_path) from exc
        yield LockHandle(fd=fd, path=lock_path)
    finally:
        with contextlib.suppress(OSError):
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)


def fetch_manifest(client: httpx.Client, settings: Settings,
                   station: str, date: str) -> dict[str, Any]:
    url = f"{settings.source_base_url}/manifest/{station}/{date}"
    resp = client.get(url)
    if resp.status_code != 200:
        raise TransferError(f"manifest 请求失败: HTTP {resp.status_code}")
    m = resp.json()
    required = ("revision", "etag", "size", "sha256")
    if any(k not in m for k in required):
        raise TransferError(f"清单字段不完整: {m}")
    if not (isinstance(m["etag"], str) and m["etag"].startswith('"')):
        raise TransferError("清单未提供强 ETag")
    return m


def _truncate_part(path: str) -> None:
    with open(path, "wb"):  # 重新建立空分片，旧内容作废
        pass


def _stream_to_part(
    client: httpx.Client,
    settings: Settings,
    station: str,
    date: str,
    version: dict[str, Any],
    part: str,
) -> int:
    """执行一次（可能不完整的）传输，返回结束时分片字节数。"""
    expected_etag = version["etag"]
    expected_size = version["expected_size"]
    have = os.path.getsize(part) if os.path.exists(part) else 0
    if have > expected_size:
        raise VerificationError("分片长度超过清单长度，作废重取")

    headers: dict[str, str] = {}
    if have:
        headers["Range"] = f"bytes={have}-"
    url = f"{settings.source_base_url}/data/{station}/{date}"
    with client.stream("GET", url, headers=headers) as resp:
        if resp.status_code not in (200, 206):
            raise TransferError(f"data 请求失败: HTTP {resp.status_code}")

        etag = resp.headers.get("etag", "")
        if etag != expected_etag:
            # 源站已换版：停止触碰当前分片，由外层重新拉清单处理
            raise TransferError(
                f"ETag 变化（期望 {expected_etag}，实际 {etag or '无'}），源端可能已换版"
            )

        if resp.status_code == 200:
            # 完整响应（首取，或源端忽略 Range）：从头重取，绝不追加到旧字节之后
            mode = "wb"
            start = 0
        else:
            content_range = resp.headers.get("content-range", "")
            start = _parse_content_range_start(content_range)
            if start != have:
                # 206 但起点与已存字节不衔接：不能信任，整体重取
                mode = "wb"
                start = 0
            else:
                mode = "ab"

        digest = hashlib.sha256()
        written = start
        since_flush = 0
        fh = open(part, mode)
        try:
            for chunk in resp.iter_bytes(settings.chunk_size):
                if not chunk:
                    continue
                if written + len(chunk) > expected_size:
                    raise VerificationError("响应体超过清单长度")
                digest.update(chunk)
                fh.write(chunk)
                written += len(chunk)
                since_flush += len(chunk)
                if since_flush >= settings.progress_flush_bytes:
                    fh.flush()
                    os.fsync(fh.fileno())
                    since_flush = 0
        finally:
            # 即使链路中途抛错，也要把已收到的字节落盘，供下次续传复用
            fh.flush()
            os.fsync(fh.fileno())
            fh.close()

        # 响应正常结束（未在流中被截断）。完整到达时做严格校验。
        if written == expected_size:
            if not verify_file_integrity(part, expected_size, version["expected_sha256"]):
                _truncate_part(part)
                raise VerificationError("完整响应摘要与清单不符，分片作废")
        elif resp.status_code == 200:
            # 声称完整却短少：链路截断
            raise TransferError(f"完整响应提前结束：{written}/{expected_size}")
        return written


def _parse_content_range_start(content_range: str) -> int:
    """解析 Content-Range: bytes <start>-<end>/<total> 的起始字节。"""
    try:
        spec = content_range.split(" ", 1)[1]
        return int(spec.split("-", 1)[0])
    except (IndexError, ValueError):
        return -1


def verify_file_integrity(path: str, expected_size: int, expected_sha: str) -> bool:
    """对落盘文件重新流式计算摘要（不信任传输中累加值，防崩溃/交错写入）。

    长度与 SHA-256 必须同时符合登记值；下载接口与发布件修复也用同一判定，
    保证“响应摘要与正文一致”这一承诺在传输、发布、修复、下载四处口径统一。
    """
    try:
        if os.path.getsize(path) != expected_size:
            return False
    except OSError:
        return False
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        while True:
            block = fh.read(1024 * 1024)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest() == expected_sha


def repair_publishes(
    database: schema.Database, storage: Storage, task_id: int
) -> list[dict[str, Any]]:
    """修复本任务已登记发布件（旧卷站号清洗串件 / 发布文件缺失）。

    在任务锁内、任何网络请求之前调用：只依赖本地留存分片与磁盘文件，
    源站离线也能完成恢复。动作列表随任务详情返回，便于审计。

    判定与动作：
    - 唯一路径 + 存在 + 长度符合 + 可用：信任登记，跳过（不每次全量哈希）。
    - 可疑（路径被其他任务登记引用 / 缺失 / 长度不符 / 曾被标不可用）：
      流式核对长度+SHA-256；健康则仅恢复可用标记，不搬动文件。
    - 确认串件/缺失：用本任务留存的完整分片（长度与 SHA-256 须符合原登记）
      原子发布到任务隔离的新路径，再更新登记；分片不符合则标为不可用。
    全程只写本任务专属路径，不触碰其他任务的发布件，不回退已发布修订。
    """
    repairs: list[dict[str, Any]] = []
    publishes = database.list_publishes(task_id)
    if not publishes:
        return repairs
    task = database.get_task(task_id)
    for pub in publishes:
        path = pub["file_path"]
        shared = database.count_other_tasks_using_path(path, task_id) > 0
        exists = os.path.isfile(path)
        size_ok = exists and os.path.getsize(path) == pub["size"]
        if not shared and size_ok and pub["available"]:
            continue  # 快速路径：无嫌疑，信任登记
        if size_ok and verify_file_integrity(path, pub["size"], pub["sha256"]):
            # 内容其实健康（含共享路径但未被覆盖）：仅恢复可用标记
            if not pub["available"]:
                database.mark_publish_restored(pub["id"], path)
                repairs.append(
                    {"revision": pub["revision"], "action": "marked_available",
                     "file_path": path}
                )
            continue
        # 串件/缺失：用本任务留存的完整分片恢复，长度与 SHA-256 须符合原登记
        part = storage.part_path(str(task_id), pub["revision"])
        if os.path.isfile(part) and verify_file_integrity(part, pub["size"], pub["sha256"]):
            final = storage.final_path(
                str(task_id), task["station"], task["date"], pub["revision"]
            )
            storage.publish_atomic(part, final)
            database.mark_publish_restored(pub["id"], final)
            logger.warning(
                "task %s r%s: 发布件串件/缺失，已用留存分片恢复 -> %s",
                task_id, pub["revision"], final,
            )
            repairs.append(
                {"revision": pub["revision"], "action": "restored",
                 "from_part": part, "file_path": final}
            )
        else:
            reason = "发布文件缺失或与登记不符，且本任务无符合登记的完整分片可恢复"
            if pub["available"] or pub["unavailable_reason"] != reason:
                database.mark_publish_unavailable(pub["id"], reason)
            logger.error("task %s r%s: %s", task_id, pub["revision"], reason)
            repairs.append(
                {"revision": pub["revision"], "action": "unavailable", "reason": reason}
            )
    return repairs


def run_resume(
    database: schema.Database,
    storage: Storage,
    settings: Settings,
    task_id: int,
) -> dict[str, Any]:
    """在持有任务锁的前提下完成一轮“继续采集”，返回任务详情。"""
    task = database.get_task(task_id)
    station, date = task["station"], task["date"]
    lock_path = storage.lock_path(str(task_id))

    with task_lock(lock_path):
        # 先于任何网络请求：旧卷串件/缺失的发布件仅用本地留存分片修复，
        # 源站离线也能恢复原发布件；无法恢复的标为不可用并拒绝下载。
        repairs = repair_publishes(database, storage, task_id)
        database.mark_task_downloading(task_id, settings.instance_id)
        try:
            with httpx.Client(
                timeout=settings.http_timeout, transport=settings.transport
            ) as client:
                # 每次继续都重新取清单：发现换版即登记新 ACTIVE 版本
                manifest = fetch_manifest(client, settings, station, date)
                part = storage.part_path(str(task_id), manifest["revision"])
                version = database.upsert_manifest(task_id, manifest, part)

                if version["state"] == schema.REV_PUBLISHED:
                    return _detail_with_repairs(database, task_id, repairs)

                database.mark_version_downloading(version["id"], settings.instance_id)
                try:
                    written = _stream_to_part(
                        client, settings, station, date, version, part
                    )
                    database.update_progress(version["id"], written)

                    if written == version["expected_size"]:
                        _publish(database, storage, version, settings.instance_id)
                        return _detail_with_repairs(database, task_id, repairs)

                    # 未下满且连接正常结束：分片保留，任务回到 INTERRUPTED 待续
                    database.mark_version_interrupted(
                        version["id"], "链路中断，已保留分片待续传"
                    )
                    database.mark_task_interrupted(task_id, "分片不完整，等待继续")
                    return _detail_with_repairs(database, task_id, repairs)
                except VerificationError as exc:
                    database.mark_version_interrupted(version["id"], str(exc))
                    database.update_progress(version["id"], 0)
                    raise
        except (httpx.HTTPError, TransferError, VerificationError) as exc:
            # 版本级归位：任何异常路径下未发布版本都不得停留在 DOWNLOADING
            task = database.get_task(task_id)
            if task["active_version_id"] is not None:
                current = database.get_version(task["active_version_id"])
                if current["state"] == schema.REV_DOWNLOADING:
                    # 以分片实际字节为准同步进度，保证重启后复用的是真实落盘长度
                    actual = (
                        os.path.getsize(current["part_path"])
                        if current["part_path"] and os.path.exists(current["part_path"])
                        else 0
                    )
                    database.update_progress(current["id"], actual)
                    database.mark_version_interrupted(current["id"], str(exc))
            # 任务级状态：若已有发布版，整体保持 PUBLISHED（旧版持续供数）
            database.mark_task_interrupted(task_id, str(exc))
            raise


def _detail_with_repairs(
    database: schema.Database, task_id: int, repairs: list[dict[str, Any]]
) -> dict[str, Any]:
    detail = database.task_detail(task_id)
    detail["repairs"] = repairs
    return detail


def _publish(
    database: schema.Database,
    storage: Storage,
    version: dict[str, Any],
    instance_id: str,
) -> None:
    part = version["part_path"]
    # 发布前最后一次独立校验：长度 + SHA-256 必须与清单完全一致
    if not verify_file_integrity(part, version["expected_size"], version["expected_sha256"]):
        raise VerificationError("发布前终检失败：长度或 SHA-256 与清单不符")

    task = database.get_task(version["task_id"])
    # 发布路径按任务目录隔离：不同任务的发布件永不复用同一路径（防站号清洗串件）
    final = storage.final_path(
        str(version["task_id"]), task["station"], task["date"], version["revision"]
    )
    storage.publish_atomic(part, final)

    # 条件更新：已发布的同修订不会重复登记（崩溃重入/并发保护）
    if not database.publish_version(version, final, instance_id):
        return

    # 防回退守卫：旧修订晚完成时文件留存于 publishes，但不得成为当前版
    latest = database.latest_published(version["task_id"])
    if latest is not None and latest["revision"] >= version["revision"]:
        if latest["revision"] == version["revision"]:
            database.mark_task_published(
                version["task_id"], version["revision"], version["id"]
            )
        # 更高修订已发布：保持当前版不动，本次仅留存旧件
        return
    database.mark_task_published(
        version["task_id"], version["revision"], version["id"]
    )
