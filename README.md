# CTD 日包断点续传采集系统

海洋调查数据员通过时常断线的链路，按**站号 + 日期**采集测站每日 CTD 原始 CSV 日包。
系统由一个模拟源站和两个对等采集实例组成，提供建任务、断点续传、进度查询和原件下载 API。

## 架构

```
docker compose
├── source        模拟源站（FastAPI）：清单含递增修订号/强 ETag/长度/SHA-256，
│                 支持管理端换版、一次性断流、忽略 Range、响应延迟注入
├── collector-a   采集实例 A（FastAPI + SQLite，单 uvicorn worker）
└── collector-b   采集实例 B（与 A 对等，共享 collector-data 卷）
```

两个采集实例共享同一个数据卷（SQLite 库、分片、发布件、锁文件均在卷内），
任一实例都能接管另一实例中断的任务。

### 磁盘布局（卷内 `/data`）

```
collector.db                 SQLite（WAL 模式）
locks/<task_id>.lock         跨实例 fcntl.flock 互斥锁
parts/<task_id>/r<rev>.part  未完成分片，按修订号隔离，流式追加
tmp/                         发布临时文件（原子 replace 前，启动即清理残留）
published/                   已发布、可下载的一致原件（硬链接 + 原子 replace）
```

## 关键正确性设计

| 需求 | 实现方式 |
| --- | --- |
| 断点续传，不整件入内存 | `httpx` 流式 `iter_bytes` 分块写分片，按周期 `fsync`；续传带 `Range: bytes=<已存>-` |
| 中断/重启复用已存字节 | 分片按 `(task, revision)` 持久化；异常路径 `finally` 强制 flush+fsync，进度按分片实际字节回写 |
| 忽略 Range / 完整响应不得追加 | 收到 `200` 或 206 起点与已存长度不衔接时，以 `wb` 重写分片，整体重新取得一致原件 |
| 换版检测 | 每次继续先拉清单；数据响应强 ETag 与清单不符立即中止，绝不触碰旧分片，新版本独立分片 |
| 长度 + SHA-256 双校验 | 传输完成后对**落盘文件**独立流式重算摘要（不信任内存累加值），两者均符合清单才发布 |
| 原子发布，杜绝半件 | 硬链接到 tmp 再 `os.replace` 原子替换并 fsync 目录；下载接口仅暴露 `publishes` 中登记的文件 |
| 旧修订晚完成不回退 | 发布为条件更新；任务当前版只前进不后退，旧修订即使晚完成也仅留存、不覆盖当前版 |
| 两实例并发继续 | 每任务 `flock(LOCK_EX|LOCK_NB)`，抢锁失败立即返回 `409`，不存在交错写入 |
| 写文件/登记发布时退出 | 启动恢复把残留 `DOWNLOADING` 归位 `INTERRUPTED`（绝不误报完成）；tmp 残留启动清理；发布条件更新防重复登记 |
| 新版完成前旧版供数 | 已发布文件与 `publishes` 记录在新版采集期间保持不动，下载始终返回最新已发布修订 |

## API（采集实例，Swagger：`http://localhost:8001/docs`）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/tasks` | 按 `station`、`date` 建日包任务（重复返回 409） |
| POST | `/tasks/{id}/resume` | 继续采集一轮；锁忙 409、传输中断 503（可重试）、校验失败 502 |
| GET | `/tasks` | 任务列表 |
| GET | `/tasks/{id}` | 进度详情：任务状态、当前版本、各修订分片进度与发布信息 |
| GET | `/tasks/{id}/download` | 下载已发布原件（未发布 409），响应头带 `X-Revision`、`X-SHA256` |
| GET | `/health` | 实例 ID 与配置 |

源站额外提供管理接口（仅演示/故障注入用）：
`POST /admin/revision`（换版）、`/admin/drop`（一次性断流 N 字节）、
`/admin/ignore-range`（切换 Range 支持）、`/admin/delay`（一次性响应延迟）、
`/admin/station`（登记站号）。

## 快速开始

```bash
docker compose up --build
# Swagger
#   源站:      http://localhost:8000/docs
#   采集实例A: http://localhost:8001/docs
#   采集实例B: http://localhost:8002/docs
```

典型演示流程：

```bash
# 1. 建任务
curl -s -X POST localhost:8001/tasks -H 'Content-Type: application/json' \
  -d '{"station":"ST01","date":"2026-11-16"}'
# 2. 制造断流后继续（可反复调用，分片持续增长）
curl -s -X POST localhost:8000/admin/drop -H 'Content-Type: application/json' \
  -d '{"station":"ST01","date":"2026-11-16","drop_after":100000}'
curl -s -X POST localhost:8001/tasks/1/resume
curl -s localhost:8001/tasks/1                 # 查看进度
# 3. 完成后下载
curl -s -OJ localhost:8001/tasks/1/download
# 4. 换版：新版完成前下载仍返回旧版
curl -s -X POST localhost:8000/admin/revision -H 'Content-Type: application/json' \
  -d '{"station":"ST01","date":"2026-11-16"}'
```

无 Docker 环境可用本地 Python 跑等价的三进程端到端脚本：

```bash
PYTHON=/path/to/venv/bin/python tests/e2e_demo.sh
```

## 测试

```bash
python -m pytest tests/          # 18 个单元/API 测试（MockTransport 模拟源站）
tests/e2e_demo.sh                # 真实 HTTP 三进程端到端：续传/换版/并发/恢复/忽略 Range
```

## 状态语义

- 任务：`PENDING → DOWNLOADING → INTERRUPTED ⇄ DOWNLOADING → PUBLISHED`
  （已有发布版后再中断，下载面仍保持旧版可用；任务状态记录最新一次采集结果）
- 版本：`ACTIVE → DOWNLOADING → INTERRUPTED → PUBLISHED`；换版时未发布旧版本降为 `SUPERSEDED`，已发布版本保持 `PUBLISHED`

## 已知边界 / 遗留项

- 源站为演示用内存实现，重启后回到修订号 1（内容按三元组确定性生成，字节仍可复现）；生产源站应持久化修订历史。
- 采集端单 uvicorn worker 运行；水平扩展依赖“多实例 + 共享卷 + flock”，未引入对象存储等外部依赖。
- 锁为阻塞失败式（`LOCK_NB`，立即 409），未实现服务端自动排队，调用方负责退避重试。
- 仅支持单区间 `bytes=start-`；多区间 Range 请求按不支持处理（整体重取）。
- 断流注入的工程细节：httpx 对“流中途断链”按读取块（64 KiB）做全有或全无交付，若 `drop_after` 小于一个读取块，对端可能在断链时丢弃已缓冲字节而得到 0 字节分片（行为等价于“首字节前断链”，重试即可）。源站因此对 ≥64 KiB 的断流先发一个完整读取块保证部分落盘；演示脚本的断流阈值均取 ≥64 KiB。
- 单元测试用 `httpx.MockTransport` 在进程内模拟源站，端到端脚本用真实 uvicorn 三进程验证；当前环境无 Docker 守护进程，`docker compose up --build` 未在本机实际执行，但 Dockerfile/compose 配置与本地三进程拓扑完全对应。
