#!/usr/bin/env bash
# 端到端自测：源站 + 两个采集实例（真实 HTTP，模拟 docker compose 拓扑）。
# 覆盖：建任务 -> 断流续传 -> 半件拒发 -> 换版供旧 -> 并发 409 -> 崩溃恢复 -> 忽略 Range 重取。
set -euo pipefail

PY="${PYTHON:-/tmp/venv/bin/python}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RUN="$(mktemp -d)"
PORT_SOURCE=18000
PORT_A=18001
PORT_B=18002
SHARED="$RUN/shared-data"
mkdir -p "$SHARED"

SRC_PID=""; A_PID=""; B_PID=""
cleanup() {
  for pid in "$A_PID" "$B_PID" "$SRC_PID"; do
    [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT

wait_health() {
  local url="$1"
  for _ in $(seq 1 100); do
    curl -sf "$url" >/dev/null && return 0
    sleep 0.2
  done
  echo "等待 $url 超时" >&2
  return 1
}

# 从 stdin 的 JSON 中取字段：echo "$json" | jp "d['id']"
jp() { "$PY" -c "import sys,json;d=json.load(sys.stdin);print($1)"; }

start_source() {
  cd "$ROOT"
  SOURCE_PACKAGE_BYTES=524288 exec "$PY" -m uvicorn source.app:app \
    --host 127.0.0.1 --port "$PORT_SOURCE" --log-level warning
}

start_collector() {
  # 用法：start_collector <port> <instance>；PID 由调用方在后台化后用 $! 取
  local port="$1" instance="$2"
  cd "$ROOT"
  COLLECTOR_DATA_DIR="$SHARED" INSTANCE_ID="$instance" \
  SOURCE_BASE_URL="http://127.0.0.1:$PORT_SOURCE" \
    exec "$PY" -m uvicorn collector.app:app \
      --host 127.0.0.1 --port "$port" --log-level warning
}

echo "== 启动源站与两个采集实例（共享数据目录 $SHARED） =="
start_source >"$RUN/source.log" 2>&1 &
SRC_PID=$!
start_collector "$PORT_A" collector-a >"$RUN/collector-a.log" 2>&1 &
A_PID=$!
start_collector "$PORT_B" collector-b >"$RUN/collector-b.log" 2>&1 &
B_PID=$!
wait_health "http://127.0.0.1:$PORT_SOURCE/health"
wait_health "http://127.0.0.1:$PORT_A/health"
wait_health "http://127.0.0.1:$PORT_B/health"
echo "三个服务均已就绪（source=$SRC_PID collector-a=$A_PID collector-b=$B_PID）"

echo; echo "== 1. 清单字段（递增修订号 / 强 ETag / 长度 / SHA-256） =="
MANIFEST=$(curl -sf "http://127.0.0.1:$PORT_SOURCE/manifest/ST01/2026-11-16")
echo "$MANIFEST" | "$PY" -m json.tool
echo "$MANIFEST" | "$PY" -c "import sys,json;m=json.load(sys.stdin);assert m['etag'].startswith('\"') and m['revision']==1 and m['size']>0 and len(m['sha256'])==64;print('清单字段 OK')"

echo; echo "== 2. 建任务并制造首次断流（已存字节须保留） =="
TID=$(curl -sf -X POST "http://127.0.0.1:$PORT_A/tasks" -H 'Content-Type: application/json' \
  -d '{"station":"ST01","date":"2026-11-16"}' | jp "d['id']")
echo "task_id=$TID"
curl -sf -X POST "http://127.0.0.1:$PORT_SOURCE/admin/drop" -H 'Content-Type: application/json' \
  -d '{"station":"ST01","date":"2026-11-16","drop_after":100000}' >/dev/null
CODE=$(curl -s -o "$RUN/r1.json" -w '%{http_code}' -X POST "http://127.0.0.1:$PORT_A/tasks/$TID/resume")
echo "断流后 resume HTTP=$CODE（期望 503）"; test "$CODE" = "503"
PART=$(find "$SHARED/parts/$TID" -name '*.part' | head -1)
SAVED=$(stat -c%s "$PART")
echo "分片已存字节=$SAVED（期望 0 < x < 524302）"
test "$SAVED" -gt 0 -a "$SAVED" -lt 524302

echo; echo "== 3. 未发布时下载半件必须被拒绝 =="
CODE=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$PORT_A/tasks/$TID/download")
echo "download HTTP=$CODE（期望 409）"; test "$CODE" = "409"

echo; echo "== 4. 实例 B 跨实例续传（复用同一分片，带 Range） =="
DETAIL=$(curl -sf -X POST "http://127.0.0.1:$PORT_B/tasks/$TID/resume")
echo "$DETAIL" | "$PY" -c "import sys,json;d=json.load(sys.stdin);assert d['state']=='PUBLISHED' and d['active_version']['progress_percent']==100.0;print('跨实例续传发布 OK，修订号',d['current_revision'])"
curl -sf "http://127.0.0.1:$PORT_B/tasks/$TID/download" -o "$RUN/r1.csv"
curl -sf "http://127.0.0.1:$PORT_SOURCE/data/ST01/2026-11-16" -o "$RUN/source-r1.csv"
cmp "$RUN/r1.csv" "$RUN/source-r1.csv" && echo "r1 原件与源站字节一致 OK"

echo; echo "== 5. 换版 r2：新版完成前旧版持续供数，完成后防回退 =="
curl -sf -X POST "http://127.0.0.1:$PORT_SOURCE/admin/revision" -H 'Content-Type: application/json' \
  -d '{"station":"ST01","date":"2026-11-16"}' >/dev/null
curl -sf -X POST "http://127.0.0.1:$PORT_SOURCE/admin/drop" -H 'Content-Type: application/json' \
  -d '{"station":"ST01","date":"2026-11-16","drop_after":80000}' >/dev/null
CODE=$(curl -s -o /dev/null -w '%{http_code}' -X POST "http://127.0.0.1:$PORT_A/tasks/$TID/resume")
echo "新版中断 resume HTTP=$CODE（期望 503）"; test "$CODE" = "503"
curl -sf -D "$RUN/h.txt" "http://127.0.0.1:$PORT_A/tasks/$TID/download" -o "$RUN/old.csv"
REV=$(grep -i '^x-revision' "$RUN/h.txt" | tr -d '\r' | awk '{print $2}')
echo "中断期间下载修订号=$REV（期望 1）"; test "$REV" = "1"
cmp "$RUN/old.csv" "$RUN/r1.csv" && echo "旧版原件保持可用且未变 OK"
DETAIL=$(curl -sf -X POST "http://127.0.0.1:$PORT_A/tasks/$TID/resume")
echo "$DETAIL" | "$PY" -c "import sys,json;d=json.load(sys.stdin);assert d['current_revision']==2 and d['state']=='PUBLISHED';print('r2 发布 OK')"
curl -sf "http://127.0.0.1:$PORT_A/tasks/$TID/download" -o "$RUN/r2.csv"
curl -sf "http://127.0.0.1:$PORT_SOURCE/data/ST01/2026-11-16" -o "$RUN/source-r2.csv"
cmp "$RUN/r2.csv" "$RUN/source-r2.csv" && echo "r2 原件与源站字节一致 OK"

echo; echo "== 6. 并发继续同一任务：确定性延迟下 B 必须收到 409 =="
TID3=$(curl -sf -X POST "http://127.0.0.1:$PORT_A/tasks" -H 'Content-Type: application/json' \
  -d '{"station":"ST03","date":"2026-11-16"}' | jp "d['id']")
curl -sf -X POST "http://127.0.0.1:$PORT_SOURCE/admin/station" -H 'Content-Type: application/json' \
  -d '{"station":"ST03","date":"2026-11-16"}' >/dev/null
curl -sf -X POST "http://127.0.0.1:$PORT_SOURCE/admin/delay" -H 'Content-Type: application/json' \
  -d '{"station":"ST03","date":"2026-11-16","delay_seconds":3}' >/dev/null
curl -s -o "$RUN/st03-a.json" -w '%{http_code}' -X POST "http://127.0.0.1:$PORT_A/tasks/$TID3/resume" > "$RUN/code-a.txt" &
A_REQ_PID=$!
# 轮询直到 A 已进入 DOWNLOADING（锁已持有）
for _ in $(seq 1 50); do
  STATE=$(curl -sf "http://127.0.0.1:$PORT_A/tasks/$TID3" | jp "d['state']" || true)
  [ "$STATE" = "DOWNLOADING" ] && break
  sleep 0.1
done
echo "A 状态=$STATE（期望 DOWNLOADING）"; test "$STATE" = "DOWNLOADING"
CODE_B=$(curl -s -o "$RUN/lock.json" -w '%{http_code}' -X POST "http://127.0.0.1:$PORT_B/tasks/$TID3/resume")
echo "并发时 B 的 HTTP=$CODE_B（期望 409）"; test "$CODE_B" = "409"
"$PY" -c "import json;print('锁冲突信息:',json.load(open('$RUN/lock.json'))['detail'])"
wait "$A_REQ_PID"; test "$(cat "$RUN/code-a.txt")" = "200"
curl -sf "http://127.0.0.1:$PORT_B/tasks/$TID3/download" -o "$RUN/st03.csv"
curl -sf "http://127.0.0.1:$PORT_SOURCE/data/ST03/2026-11-16" -o "$RUN/source-st03.csv"
cmp "$RUN/st03.csv" "$RUN/source-st03.csv" && echo "A 完成后发布件一致，无交错写入 OK"

echo; echo "== 7. 进程崩溃恢复：DOWNLOADING 归位 INTERRUPTED，不误报完成 =="
# 制造一个中断版本，再把库状态拨到 DOWNLOADING 模拟被杀瞬间
curl -sf -X POST "http://127.0.0.1:$PORT_SOURCE/admin/revision" -H 'Content-Type: application/json' \
  -d '{"station":"ST01","date":"2026-11-16"}' >/dev/null
curl -sf -X POST "http://127.0.0.1:$PORT_SOURCE/admin/drop" -H 'Content-Type: application/json' \
  -d '{"station":"ST01","date":"2026-11-16","drop_after":40000}' >/dev/null
curl -s -o /dev/null -X POST "http://127.0.0.1:$PORT_A/tasks/$TID/resume" || true
"$PY" - "$SHARED/collector.db" <<'PYEOF'
import sqlite3, sys
conn = sqlite3.connect(sys.argv[1])
conn.execute("UPDATE tasks SET state='DOWNLOADING'")
conn.execute("UPDATE task_versions SET state='DOWNLOADING' WHERE state='INTERRUPTED'")
conn.commit(); conn.close()
print("已构造崩溃现场（DOWNLOADING 残留）")
PYEOF
kill "$A_PID"
wait "$A_PID" 2>/dev/null || true
# 确认端口已释放后再拉起新实例
for _ in $(seq 1 50); do
  curl -sf -m1 "http://127.0.0.1:$PORT_A/health" >/dev/null 2>&1 || break
  sleep 0.1
done
start_collector "$PORT_A" collector-a-restart >"$RUN/collector-a-restart.log" 2>&1 &
A_PID=$!
wait_health "http://127.0.0.1:$PORT_A/health"
NEW_INST=$(curl -sf "http://127.0.0.1:$PORT_A/health" | jp "d['instance_id']")
echo "重启后实例 ID=$NEW_INST（期望 collector-a-restart，确认是新进程）"
test "$NEW_INST" = "collector-a-restart"
STATE=$(curl -sf "http://127.0.0.1:$PORT_A/tasks/$TID" | jp "d['state']")
echo "重启后任务状态=$STATE（期望 INTERRUPTED）"; test "$STATE" = "INTERRUPTED"
# 已发布的 r2 在恢复期间仍可下载
curl -sf -D "$RUN/rec.h.txt" -o "$RUN/during-recovery.csv" "http://127.0.0.1:$PORT_A/tasks/$TID/download"
grep -i '^x-revision' "$RUN/rec.h.txt" | tr -d '\r'
DETAIL=$(curl -sf -X POST "http://127.0.0.1:$PORT_A/tasks/$TID/resume")
echo "$DETAIL" | "$PY" -c "import sys,json;d=json.load(sys.stdin);assert d['state']=='PUBLISHED' and d['current_revision']==3;print('r3 恢复后续传完成 OK')"
curl -sf "http://127.0.0.1:$PORT_A/tasks/$TID/download" -o "$RUN/r3.csv"
curl -sf "http://127.0.0.1:$PORT_SOURCE/data/ST01/2026-11-16" -o "$RUN/source-r3.csv"
cmp "$RUN/r3.csv" "$RUN/source-r3.csv" && echo "r3 发布件与源站一致 OK"
cmp "$RUN/during-recovery.csv" "$RUN/r2.csv" && echo "恢复期间旧版 r2 下载未中断 OK"

echo; echo "== 8. 源端忽略 Range：收到完整响应必须整体重取，禁止追加 =="
TID2=$(curl -sf -X POST "http://127.0.0.1:$PORT_A/tasks" -H 'Content-Type: application/json' \
  -d '{"station":"ST02","date":"2026-11-16"}' | jp "d['id']")
curl -sf -X POST "http://127.0.0.1:$PORT_SOURCE/admin/station" -H 'Content-Type: application/json' \
  -d '{"station":"ST02","date":"2026-11-16"}' >/dev/null
curl -sf -X POST "http://127.0.0.1:$PORT_SOURCE/admin/drop" -H 'Content-Type: application/json' \
  -d '{"station":"ST02","date":"2026-11-16","drop_after":100000}' >/dev/null
curl -s -o /dev/null -X POST "http://127.0.0.1:$PORT_A/tasks/$TID2/resume" || true
SAVED2=$(find "$SHARED/parts/$TID2" -name '*.part' -exec stat -c%s {} \; | head -1)
echo "ST02 中断分片字节=$SAVED2（期望 >=65536，warmup 块先落盘）"; test "$SAVED2" -ge 65536
curl -sf -X POST "http://127.0.0.1:$PORT_SOURCE/admin/ignore-range" -H 'Content-Type: application/json' \
  -d '{"station":"ST02","date":"2026-11-16","ignore":true}' >/dev/null
DETAIL=$(curl -sf -X POST "http://127.0.0.1:$PORT_A/tasks/$TID2/resume")
echo "$DETAIL" | "$PY" -c "import sys,json;d=json.load(sys.stdin);assert d['state']=='PUBLISHED';print('忽略 Range 后整体重取并发布 OK')"
curl -sf "http://127.0.0.1:$PORT_A/tasks/$TID2/download" -o "$RUN/st02.csv"
curl -sf "http://127.0.0.1:$PORT_SOURCE/data/ST02/2026-11-16" -o "$RUN/source-st02.csv"
cmp "$RUN/st02.csv" "$RUN/source-st02.csv" && echo "ST02 原件一致 OK"

echo; echo "全部端到端检查通过 ✔  运行目录：$RUN"
