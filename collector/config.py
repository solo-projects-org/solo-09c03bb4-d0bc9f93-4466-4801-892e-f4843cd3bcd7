"""采集实例运行配置（全部来自环境变量，带默认值，便于 Compose 覆盖）。"""
from __future__ import annotations

import os
import socket
from dataclasses import dataclass


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw else default


@dataclass(frozen=True)
class Settings:
    source_base_url: str = os.environ.get("SOURCE_BASE_URL", "http://source:8000")
    data_dir: str = os.environ.get("COLLECTOR_DATA_DIR", "/data")
    instance_id: str = os.environ.get("INSTANCE_ID", socket.gethostname())
    # 流式读写块大小；原件永不整体载入内存
    chunk_size: int = _int_env("CHUNK_SIZE", 64 * 1024)
    # 每累计写入多少字节 fsync 一次分片并落盘进度
    progress_flush_bytes: int = _int_env("PROGRESS_FLUSH_BYTES", 256 * 1024)
    http_timeout: float = float(os.environ.get("HTTP_TIMEOUT", "30"))
    # 仅测试使用：注入 httpx transport，生产为 None 走真实链路
    transport: object | None = None


settings = Settings()
