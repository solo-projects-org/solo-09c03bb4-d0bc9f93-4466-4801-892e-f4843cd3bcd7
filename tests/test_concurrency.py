"""并发与崩溃安全：双实例锁、重启恢复、发布原子性、下载防半件。"""
from __future__ import annotations

import os
import threading

import pytest

from collector import db as schema
from collector.config import Settings
from collector.transfer import TaskLockedError, run_resume, task_lock


def test_task_lock_is_mutually_exclusive(tmp_path):
    lock = str(tmp_path / "x.lock")
    with task_lock(lock):
        with pytest.raises(TaskLockedError):
            with task_lock(lock):
                pass


def test_two_instances_concurrent_resume_one_waits_rejected(env):
    results: list[str] = []
    barrier = threading.Barrier(2)

    def worker(instance: str) -> None:
        settings = Settings(**{**dataclasses_asdict(env.settings), "instance_id": instance})
        barrier.wait()
        try:
            run_resume(env.db, env.storage, settings, env.task_id)
            results.append("ok")
        except TaskLockedError:
            results.append("locked")
        except Exception as exc:  # pragma: no cover
            results.append(f"error:{exc}")

    t1 = threading.Thread(target=worker, args=("inst-1",))
    t2 = threading.Thread(target=worker, args=("inst-2",))
    t1.start(); t2.start(); t1.join(); t2.join()

    assert sorted(results) == ["locked", "ok"]
    detail = env.db.task_detail(env.task_id)
    assert detail["state"] == schema.TASK_PUBLISHED
    # 只有一个实例登记为完成者；分片不存在交错写入
    with open(detail["latest_published"]["file_path"], "rb") as fh:
        import hashlib
        assert hashlib.sha256(fh.read()).hexdigest() == detail["latest_published"]["sha256"]


def dataclasses_asdict(settings: Settings) -> dict:
    import dataclasses
    return dataclasses.asdict(settings)


def test_restart_recovers_downloading_state_to_interrupted(env):
    # 制造一次中断，状态为 DOWNLOADING 风格（模拟进程在写库后、收尾前被杀）
    env.source.drop_after = 500
    with pytest.raises(Exception):
        env.resume()

    # 手工把状态拨回 DOWNLOADING，模拟崩溃瞬间
    import sqlite3
    conn = sqlite3.connect(env.storage.db_path())
    conn.execute(
        f"UPDATE tasks SET state='{schema.TASK_DOWNLOADING}' WHERE id=?",
        (env.task_id,),
    )
    conn.execute(
        f"UPDATE task_versions SET state='{schema.REV_DOWNLOADING}' WHERE task_id=?",
        (env.task_id,),
    )
    conn.commit(); conn.close()

    # 新进程重新打开库：启动恢复把 DOWNLOADING 归位为 INTERRUPTED，绝不误报完成
    fresh_db = schema.Database(env.storage.db_path())
    detail = fresh_db.task_detail(env.task_id)
    assert detail["state"] == schema.TASK_INTERRUPTED
    assert detail["active_version"]["state"] == schema.REV_INTERRUPTED
    # 分片仍在，恢复后可继续
    detail = run_resume(fresh_db, env.storage, env.settings, env.task_id)
    assert detail["state"] == schema.TASK_PUBLISHED


def test_tmp_leftover_cleaned_on_startup(env):
    leftover = os.path.join(env.storage.tmp_dir, "stale.tmp")
    with open(leftover, "wb") as fh:
        fh.write(b"x")
    # 应用启动流程中的清理动作
    env.storage.cleanup_tmp()
    assert not os.path.exists(leftover)


def test_published_file_remains_available_after_re_interrupt(env):
    detail = env.resume()
    published = detail["latest_published"]["file_path"]
    assert os.path.isfile(published)

    # 换版后新修订下载中断：任务整体仍可通过 latest_published 取到旧版
    env.source.bump()
    env.source.drop_after = 100
    with pytest.raises(Exception):
        env.resume()
    detail = env.db.task_detail(env.task_id)
    assert detail["latest_published"]["revision"] == 1
    assert os.path.isfile(published)


def test_publish_is_idempotent_under_repeated_call(env):
    env.resume()
    detail1 = env.db.task_detail(env.task_id)
    # 再次 resume：版本已 PUBLISHED，直接返回，不产生重复发布
    detail2 = env.resume()
    assert detail1["latest_published"]["file_path"] == detail2["latest_published"]["file_path"]
    import sqlite3
    conn = sqlite3.connect(env.storage.db_path())
    n = conn.execute(
        "SELECT COUNT(*) FROM publishes WHERE task_id=?", (env.task_id,)
    ).fetchone()[0]
    conn.close()
    assert n == 1
