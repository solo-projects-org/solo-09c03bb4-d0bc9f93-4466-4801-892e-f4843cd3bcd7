"""SQLite 持久化：任务、版本行、发布记录。

并发模型：两个采集实例 + 线程池通过 WAL + busy_timeout 共享同一数据库文件；
同一任务的写互斥另外由文件锁（flock）保证，数据库层再用条件更新做防回退守卫。
"""
from __future__ import annotations

import os
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any, Optional

# 任务生命周期
TASK_PENDING = "PENDING"
TASK_DOWNLOADING = "DOWNLOADING"
TASK_INTERRUPTED = "INTERRUPTED"
TASK_PUBLISHED = "PUBLISHED"

# 版本行状态
REV_ACTIVE = "ACTIVE"          # 当前应采集的修订号
REV_SUPERSEDED = "SUPERSEDED"  # 已有更新修订号
REV_DOWNLOADING = "DOWNLOADING"
REV_INTERRUPTED = "INTERRUPTED"
REV_PUBLISHED = "PUBLISHED"

_TERMINAL_TASK_STATES = (TASK_PUBLISHED,)

_local = threading.local()


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class Database:
    def __init__(self, path: str) -> None:
        self.path = path
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self._init_schema()
        self.recover_on_startup()

    def _conn(self) -> sqlite3.Connection:
        conn = getattr(_local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.path, timeout=30, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=30000")
            conn.execute("PRAGMA foreign_keys=ON")
            _local.conn = conn
        return conn

    def _init_schema(self) -> None:
        with self._conn():
            self._conn().executescript(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    station TEXT NOT NULL,
                    date TEXT NOT NULL,
                    state TEXT NOT NULL,
                    current_revision INTEGER,
                    active_version_id INTEGER,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    last_error TEXT,
                    UNIQUE(station, date)
                );
                CREATE TABLE IF NOT EXISTS task_versions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id INTEGER NOT NULL REFERENCES tasks(id),
                    revision INTEGER NOT NULL,
                    etag TEXT NOT NULL,
                    expected_size INTEGER NOT NULL,
                    expected_sha256 TEXT NOT NULL,
                    state TEXT NOT NULL,
                    bytes_stored INTEGER NOT NULL DEFAULT 0,
                    part_path TEXT,
                    published_path TEXT,
                    instance_id TEXT,
                    last_error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(task_id, revision)
                );
                CREATE TABLE IF NOT EXISTS publishes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id INTEGER NOT NULL,
                    revision INTEGER NOT NULL,
                    version_id INTEGER NOT NULL,
                    file_path TEXT NOT NULL,
                    sha256 TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    published_at TEXT NOT NULL,
                    UNIQUE(task_id, revision)
                );
                """
            )

    # ---------- 启动恢复 ----------
    def recover_on_startup(self) -> None:
        """上次进程退出时遗留在 DOWNLOADING 的任务/版本一律回到 INTERRUPTED，
        绝不允许重启后误报完成；锁由 flock 自动释放。"""
        conn = self._conn()
        with conn:
            conn.execute(
                "UPDATE task_versions SET state=?, updated_at=? "
                "WHERE state=?",
                (REV_INTERRUPTED, utcnow(), REV_DOWNLOADING),
            )
            conn.execute(
                "UPDATE tasks SET state=?, updated_at=? WHERE state=?",
                (TASK_INTERRUPTED, utcnow(), TASK_DOWNLOADING),
            )

    # ---------- 任务 ----------
    def create_task(self, station: str, date: str) -> dict[str, Any]:
        conn = self._conn()
        now = utcnow()
        try:
            cur = conn.execute(
                "INSERT INTO tasks(station, date, state, created_at, updated_at) "
                "VALUES(?,?,?,?,?)",
                (station, date, TASK_PENDING, now, now),
            )
            conn.commit()
        except sqlite3.IntegrityError:
            raise TaskAlreadyExists(station, date)
        return self.get_task(cur.lastrowid)

    def get_task(self, task_id: int) -> dict[str, Any]:
        row = self._conn().execute(
            "SELECT * FROM tasks WHERE id=?", (task_id,)
        ).fetchone()
        if row is None:
            raise TaskNotFound(task_id)
        return dict(row)

    def find_task(self, station: str, date: str) -> Optional[dict[str, Any]]:
        row = self._conn().execute(
            "SELECT * FROM tasks WHERE station=? AND date=?", (station, date)
        ).fetchone()
        return dict(row) if row else None

    def list_tasks(self) -> list[dict[str, Any]]:
        rows = self._conn().execute(
            "SELECT * FROM tasks ORDER BY id"
        ).fetchall()
        return [dict(r) for r in rows]

    def mark_task_downloading(self, task_id: int, instance_id: str) -> None:
        self._conn().execute(
            "UPDATE tasks SET state=?, updated_at=?, last_error=NULL WHERE id=?",
            (TASK_DOWNLOADING, utcnow(), task_id),
        )
        self._conn().commit()

    def mark_task_interrupted(self, task_id: int, error: str | None) -> None:
        # 已发布的任务整体状态保持 PUBLISHED（旧版仍在供数）
        self._conn().execute(
            "UPDATE tasks SET state=?, updated_at=?, last_error=? "
            "WHERE id=? AND state<>?",
            (TASK_INTERRUPTED, utcnow(), error, task_id, TASK_PUBLISHED),
        )
        self._conn().commit()

    def mark_task_published(self, task_id: int, revision: int, version_id: int) -> None:
        self._conn().execute(
            "UPDATE tasks SET state=?, current_revision=?, active_version_id=?, "
            "updated_at=?, last_error=NULL WHERE id=?",
            (TASK_PUBLISHED, revision, version_id, utcnow(), task_id),
        )
        self._conn().commit()

    # ---------- 版本行 ----------
    def upsert_manifest(
        self, task_id: int, manifest: dict[str, Any], part_path: str
    ) -> dict[str, Any]:
        """根据清单登记/更新 ACTIVE 版本行；发现更高修订号时旧行降为 SUPERSEDED。"""
        conn = self._conn()
        revision = manifest["revision"]
        now = utcnow()
        with conn:
            row = conn.execute(
                "SELECT * FROM task_versions WHERE task_id=? AND revision=?",
                (task_id, revision),
            ).fetchone()
            if row is None:
                cur = conn.execute(
                    "INSERT INTO task_versions(task_id, revision, etag, expected_size, "
                    "expected_sha256, state, bytes_stored, part_path, created_at, updated_at) "
                    "VALUES(?,?,?,?,?,?,0,?,?,?)",
                    (
                        task_id, revision, manifest["etag"], manifest["size"],
                        manifest["sha256"], REV_ACTIVE, part_path, now, now,
                    ),
                )
                version_id = cur.lastrowid
                # 旧版本行降级：未发布的 ACTIVE/INTERRUPTED/DOWNLOADING 一律 SUPERSEDED；
                # 已发布行保持 PUBLISHED 不动（旧版继续供数）
                conn.execute(
                    "UPDATE task_versions SET state=?, updated_at=? "
                    "WHERE task_id=? AND id<>? AND state IN (?,?,?)",
                    (
                        REV_SUPERSEDED, now, task_id, version_id,
                        REV_ACTIVE, REV_INTERRUPTED, REV_DOWNLOADING,
                    ),
                )
            else:
                version_id = row["id"]
                # 同修订号清单变化（理论上不应发生）：以清单为准，重置分片
                conn.execute(
                    "UPDATE task_versions SET etag=?, expected_size=?, expected_sha256=?, "
                    "state=?, part_path=?, updated_at=? WHERE id=? AND state<>?",
                    (
                        manifest["etag"], manifest["size"], manifest["sha256"],
                        REV_ACTIVE, part_path, now, version_id, REV_PUBLISHED,
                    ),
                )
            conn.execute(
                "UPDATE tasks SET current_revision=?, active_version_id=?, "
                "updated_at=? WHERE id=?",
                (revision, version_id, now, task_id),
            )
        return self.get_version(version_id)

    def get_version(self, version_id: int) -> dict[str, Any]:
        row = self._conn().execute(
            "SELECT * FROM task_versions WHERE id=?", (version_id,)
        ).fetchone()
        if row is None:
            raise VersionNotFound(version_id)
        return dict(row)

    def list_versions(self, task_id: int) -> list[dict[str, Any]]:
        rows = self._conn().execute(
            "SELECT * FROM task_versions WHERE task_id=? ORDER BY revision",
            (task_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def active_version(self, task_id: int) -> Optional[dict[str, Any]]:
        row = self._conn().execute(
            "SELECT * FROM task_versions WHERE task_id=? AND state=?",
            (task_id, REV_ACTIVE),
        ).fetchone()
        return dict(row) if row else None

    def mark_version_downloading(self, version_id: int, instance_id: str) -> None:
        self._conn().execute(
            "UPDATE task_versions SET state=?, instance_id=?, updated_at=? WHERE id=?",
            (REV_DOWNLOADING, instance_id, utcnow(), version_id),
        )
        self._conn().commit()

    def update_progress(self, version_id: int, bytes_stored: int) -> None:
        self._conn().execute(
            "UPDATE task_versions SET bytes_stored=?, updated_at=? WHERE id=?",
            (bytes_stored, utcnow(), version_id),
        )
        self._conn().commit()

    def mark_version_interrupted(self, version_id: int, error: str | None) -> None:
        self._conn().execute(
            "UPDATE task_versions SET state=?, last_error=?, instance_id=NULL, "
            "updated_at=? WHERE id=? AND state<>?",
            (REV_INTERRUPTED, error, utcnow(), version_id, REV_PUBLISHED),
        )
        self._conn().commit()

    def publish_version(
        self, version: dict[str, Any], file_path: str, instance_id: str
    ) -> bool:
        """条件更新发布：仅当该版本尚未发布时成功，防崩溃重入/并发重复登记。
        返回 True 表示本次调用完成发布。"""
        conn = self._conn()
        now = utcnow()
        with conn:
            cur = conn.execute(
                "UPDATE task_versions SET state=?, published_path=?, instance_id=NULL, "
                "updated_at=? WHERE id=? AND state<>?",
                (REV_PUBLISHED, file_path, now, version["id"], REV_PUBLISHED),
            )
            if cur.rowcount == 0:
                return False
            conn.execute(
                "INSERT OR IGNORE INTO publishes(task_id, revision, version_id, "
                "file_path, sha256, size, published_at) VALUES(?,?,?,?,?,?,?)",
                (
                    version["task_id"], version["revision"], version["id"],
                    file_path, version["expected_sha256"], version["expected_size"], now,
                ),
            )
        return True

    def latest_published(self, task_id: int) -> Optional[dict[str, Any]]:
        row = self._conn().execute(
            "SELECT * FROM publishes WHERE task_id=? ORDER BY revision DESC LIMIT 1",
            (task_id,),
        ).fetchone()
        return dict(row) if row else None

    # ---------- 视图组装 ----------
    def task_detail(self, task_id: int) -> dict[str, Any]:
        task = self.get_task(task_id)
        versions = self.list_versions(task_id)
        publish = self.latest_published(task_id)
        # 当前版本：按任务记录的当前修订号取（覆盖 ACTIVE/DOWNLOADING/INTERRUPTED/PUBLISHED）
        current: Optional[dict[str, Any]] = None
        if task["active_version_id"] is not None:
            current = self.get_version(task["active_version_id"])
        if current is None and task["current_revision"] is not None:
            row = self._conn().execute(
                "SELECT * FROM task_versions WHERE task_id=? AND revision=?",
                (task_id, task["current_revision"]),
            ).fetchone()
            current = dict(row) if row else None
        if current is None:
            current = self.active_version(task_id)
        return {
            "id": task["id"],
            "station": task["station"],
            "date": task["date"],
            "state": task["state"],
            "current_revision": task["current_revision"],
            "last_error": task["last_error"],
            "created_at": task["created_at"],
            "updated_at": task["updated_at"],
            "active_version": _version_view(current) if current else None,
            "latest_published": _publish_view(publish) if publish else None,
            "versions": [_version_view(v) for v in versions],
        }


def _version_view(v: dict[str, Any]) -> dict[str, Any]:
    pct = 0.0
    if v["expected_size"]:
        pct = round(100.0 * v["bytes_stored"] / v["expected_size"], 2)
    return {
        "revision": v["revision"],
        "state": v["state"],
        "etag": v["etag"],
        "expected_size": v["expected_size"],
        "bytes_stored": v["bytes_stored"],
        "progress_percent": pct,
        "expected_sha256": v["expected_sha256"],
        "published_path": v["published_path"],
        "instance_id": v["instance_id"],
        "last_error": v["last_error"],
        "updated_at": v["updated_at"],
    }


def _publish_view(p: dict[str, Any]) -> dict[str, Any]:
    return {
        "revision": p["revision"],
        "file_path": p["file_path"],
        "sha256": p["sha256"],
        "size": p["size"],
        "published_at": p["published_at"],
    }


class TaskAlreadyExists(Exception):
    def __init__(self, station: str, date: str) -> None:
        self.station = station
        self.date = date


class TaskNotFound(Exception):
    def __init__(self, task_id: int) -> None:
        self.task_id = task_id


class VersionNotFound(Exception):
    def __init__(self, version_id: int) -> None:
        self.version_id = version_id
