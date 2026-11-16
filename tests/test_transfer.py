"""续传主流程：中断 -> 复用字节续传 -> 校验发布 -> 下载一致原件。"""
from __future__ import annotations

import hashlib
import os

import httpx
import pytest

from collector import db as schema
from collector.transfer import TransferError


def test_full_download_publishes_and_verifies(env):
    detail = env.resume()
    assert detail["state"] == schema.TASK_PUBLISHED
    v = detail["active_version"]
    assert v["state"] == schema.REV_PUBLISHED
    assert v["bytes_stored"] == v["expected_size"]
    assert v["progress_percent"] == 100.0

    publish = detail["latest_published"]
    blob = env.source.blobs[1]
    with open(publish["file_path"], "rb") as fh:
        on_disk = fh.read()
    assert on_disk == blob
    assert hashlib.sha256(on_disk).hexdigest() == publish["sha256"]


def test_interrupted_then_resume_reuses_saved_bytes(env):
    env.source.drop_after = 700
    with pytest.raises(httpx.HTTPError):
        env.resume()

    part = env.part_path(1)
    saved = open(part, "rb").read()
    assert 0 < len(saved) < env.source.manifest()["size"]
    detail = env.db.task_detail(env.task_id)
    assert detail["state"] == schema.TASK_INTERRUPTED
    assert detail["active_version"]["state"] == schema.REV_INTERRUPTED

    # 第二次继续：必须带 Range 复用已存字节，而不是重下整件
    detail = env.resume()
    assert detail["state"] == schema.TASK_PUBLISHED
    range_seen = [
        r["headers"].get("range")
        for r in env.source.request_log
        if r["headers"].get("range")
    ]
    assert range_seen and any(h == f"bytes={len(saved)}-" for h in range_seen)

    with open(detail["latest_published"]["file_path"], "rb") as fh:
        assert fh.read() == env.source.blobs[1]


def test_multiple_drops_each_resume_extends_part(env):
    total = env.source.manifest()["size"]
    previous = 0
    for i in range(3):
        env.source.drop_after = 400
        with pytest.raises(httpx.HTTPError):
            env.resume()
        size = open(env.part_path(1), "rb").read().__len__()
        assert size > previous
        previous = size
        assert size < total
    detail = env.resume()
    assert detail["state"] == schema.TASK_PUBLISHED


def test_ignore_range_full_response_must_restart_not_append(env):
    # 先攒下一段分片
    env.source.drop_after = 500
    with pytest.raises(httpx.HTTPError):
        env.resume()
    saved_len = len(open(env.part_path(1), "rb").read())
    assert saved_len > 0

    # 源站忽略 Range：返回 200 完整响应，采集端必须重写分片而非追加
    env.source.ignore_range = True
    detail = env.resume()
    assert detail["state"] == schema.TASK_PUBLISHED
    last = env.source.request_log[-1]["headers"]
    assert "range" in last  # 客户端仍尝试 Range
    published = open(detail["latest_published"]["file_path"], "rb").read()
    assert published == env.source.blobs[1]
    assert len(published) == detail["active_version"]["expected_size"]


def test_etag_change_aborts_old_part_and_new_revision_completes(env):
    env.source.drop_after = 600
    with pytest.raises(httpx.HTTPError):
        env.resume()
    old_part = env.part_path(1)
    assert os.path.getsize(old_part) > 0

    env.source.bump()  # 换版 r2：下一轮 resume 拉清单即发现
    # r1 分片不被追加：r2 走自己的分片文件
    detail = env.resume()
    assert detail["state"] == schema.TASK_PUBLISHED
    assert detail["current_revision"] == 2
    versions = {v["revision"]: v for v in detail["versions"]}
    assert versions[1]["state"] == schema.REV_SUPERSEDED
    assert versions[2]["state"] == schema.REV_PUBLISHED
    with open(detail["latest_published"]["file_path"], "rb") as fh:
        assert fh.read() == env.source.blobs[2]
    # r1 的中断分片仍原样保留在磁盘上（未被新数据污染）
    assert os.path.getsize(old_part) > 0



def test_etag_mid_transfer_does_not_corrupt_part(env):
    # 首次先存一部分
    env.source.drop_after = 300
    with pytest.raises(httpx.HTTPError):
        env.resume()
    before = open(env.part_path(1), "rb").read()

    # 续传期间源站在清单之后、数据之前换版：ETag 不匹配立即中止，r1 分片原样保留
    env.source.bump_before_data = True
    with pytest.raises(TransferError):
        env.resume()
    after = open(env.part_path(1), "rb").read()
    assert before == after
    # 本轮清单仍是 r1（换版发生在清单之后），版本保持中断态且未被污染
    detail = env.db.task_detail(env.task_id)
    assert detail["active_version"]["revision"] == 1
    assert detail["active_version"]["state"] == schema.REV_INTERRUPTED

    # 下一轮继续拉到 r2 清单并完成；r1 分片字节始终未被改动
    detail = env.resume()
    assert detail["current_revision"] == 2
    assert detail["state"] == schema.TASK_PUBLISHED
    assert open(env.part_path(1), "rb").read() == before


def test_old_revision_finishing_late_cannot_roll_back_current(env):
    # r1 正常发布
    env.resume()
    assert env.db.task_detail(env.task_id)["current_revision"] == 1

    # 换版 r2，制造一个中断的 r2 分片
    env.source.bump()
    env.source.drop_after = 200
    with pytest.raises(httpx.HTTPError):
        env.resume()

    # 源站被外部回退到 r1（清单修订号变小）：旧修订不得覆盖当前版
    env.source.revision = 1
    detail = env.resume()
    # r1 已发布，直接返回；当前版仍记录 r1，且 r2 保留为 ACTIVE 待后续完成
    assert detail["latest_published"]["revision"] == 1
    # 源站恢复 r2 后完成，当前版前进到 r2
    env.source.revision = 2
    detail = env.resume()
    assert detail["state"] == schema.TASK_PUBLISHED
    assert detail["current_revision"] == 2
