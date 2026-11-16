"""磁盘布局与分片/发布文件管理。

DATA_DIR/
├── collector.db
├── locks/<task_id>.lock        # 跨实例 fcntl 互斥
├── parts/<task_id>/r<rev>.part # 未完成分片（按修订号隔离）
├── tmp/...                     # 发布临时文件（原子 replace 前）
└── published/...               # 已发布、可下载的一致原件
"""
from __future__ import annotations

import os


class Storage:
    def __init__(self, data_dir: str) -> None:
        self.data_dir = data_dir
        self.locks_dir = os.path.join(data_dir, "locks")
        self.parts_dir = os.path.join(data_dir, "parts")
        self.tmp_dir = os.path.join(data_dir, "tmp")
        self.published_dir = os.path.join(data_dir, "published")
        for d in (self.locks_dir, self.parts_dir, self.tmp_dir, self.published_dir):
            os.makedirs(d, exist_ok=True)

    def db_path(self) -> str:
        return os.path.join(self.data_dir, "collector.db")

    def lock_path(self, task_id: str) -> str:
        return os.path.join(self.locks_dir, f"{task_id}.lock")

    def part_path(self, task_id: str, revision: int) -> str:
        d = os.path.join(self.parts_dir, task_id)
        os.makedirs(d, exist_ok=True)
        return os.path.join(d, f"r{revision}.part")

    def final_path(self, station: str, date: str, revision: int) -> str:
        safe_station = _safe(station)
        safe_date = _safe(date)
        return os.path.join(
            self.published_dir, f"{safe_station}_{safe_date}_r{revision}.csv"
        )

    def tmp_path(self, task_id: str, revision: int) -> str:
        return os.path.join(self.tmp_dir, f"{task_id}-r{revision}-{os.getpid()}.tmp")

    def cleanup_tmp(self) -> None:
        """启动时清理上次崩溃残留的发布临时文件（正式文件不受影响）。"""
        for name in os.listdir(self.tmp_dir):
            try:
                os.unlink(os.path.join(self.tmp_dir, name))
            except FileNotFoundError:
                pass

    def publish_atomic(self, part: str, final: str) -> None:
        """硬链接到临时名再原子 replace；同文件系统，不复制大文件。"""
        tmp = self.tmp_path(_basename_task(part), _basename_rev(part))
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass
        os.link(part, tmp)
        os.replace(tmp, final)
        _fsync_dir(self.published_dir)


def _safe(value: str) -> str:
    return "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in value)


def _fsync_dir(path: str) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _basename_task(part_path: str) -> str:
    # parts/<task_id>/r<rev>.part
    return os.path.basename(os.path.dirname(part_path))


def _basename_rev(part_path: str) -> int:
    name = os.path.basename(part_path)
    return int(name[1:].split(".", 1)[0])
