#!/usr/bin/env bash
# Locust로 B 시나리오(요청 1건 = 이벤트 1건)를 규모별로 실행한다. VU(동시 사용자) 100, DB 저장 O, 256B.
# 사용: load/locust/run-scale.sh <nest|spring> [규모=1k,10k,100k,500k,1m,3m] [MQ=kafka,redis,rabbit]
#   예: load/locust/run-scale.sh spring              # 6개 규모 × 3 MQ, 각 1회
#       load/locust/run-scale.sh nest 10k,100k redis # 일부만
# 결과: results/locust-<app>-<시각>/
#   raw.jsonl            한 줄 = 1회 (scenarios.sh와 같은 필드 + Locust HTTP 통계)
#   <app>-B-<규모>-<mq>.html   Locust HTTP 리포트(차트 포함)
#   <app>-B-<규모>-<mq>.png    CAPTURE=1이면 리포트 화면 캡처(Node + Playwright 필요)
# 필요: locust (pip install locust), 대상 앱 실행 중, 앱은 한 번에 하나만
set -u
APP=${1:?"사용: $0 <nest|spring> [규모] [MQ]"}; SCALES=${2:-1k,10k,100k,500k,1m,3m}; MQS=${3:-kafka,redis,rabbit}
case "$APP" in nest) BASE=http://localhost:3000 ;; spring) BASE=http://localhost:8080 ;; *) echo "app은 nest|spring"; exit 1 ;; esac
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
LOCUST=${LOCUST:-locust}
OUT=${OUT:-"$ROOT/results/locust-$APP-$(date +%Y%m%d-%H%M%S)"}
mkdir -p "$OUT"
VUS=${VUS:-100}

count_of() { case $1 in 1k) echo 1000 ;; 10k) echo 10000 ;; 100k) echo 100000 ;; 500k) echo 500000 ;; 1m) echo 1000000 ;; 3m) echo 3000000 ;; *) echo "$1" ;; esac; }

run_locust() { # mq count [html csv]
  # 대기 시간은 건수에 비례(500건/초 기준, 최소 300초), 실행 상한은 100건/초 기준 + 15분
  local wait=$(( $2 / 500 > 300 ? $2 / 500 : 300 )) poll=0.1
  [ "$2" -ge 100000 ] && poll=1
  (cd "$ROOT" && MQ=$1 COUNT=$2 SIZE=256 DB=true WAIT_SEC=$wait POLL_SEC=$poll \
    "$LOCUST" -f load/locust/locustfile.py --headless -u $VUS -r $VUS -t $(( $2 / 100 + 900 ))s \
      --host $BASE ${3:+--html $3} ${4:+--csv $4} 2>&1) | grep -o 'RESULT {.*}' | head -1 | sed 's/^RESULT //'
}

for s in ${SCALES//,/ }; do
  count=$(count_of $s)
  for mq in ${MQS//,/ }; do
    name=$APP-B-$s-$mq
    curl -s -X DELETE $BASE/runs >/dev/null
    run_locust $mq $(( count > 10000 ? 10000 : count )) >/dev/null   # 워밍업 1회(기록 안 함)
    curl -s -X DELETE $BASE/runs >/dev/null
    echo "$(date +%H:%M:%S) RUN $name"
    res=$(run_locust $mq $count "$OUT/$name.html" "$OUT/$name")
    python3 - "$res" "$OUT/${name}_stats.csv" "$APP" "B-$s" "$mq" "$count" "$VUS" >> "$OUT/raw.jsonl" <<'PY'
import sys, json, csv
res, path, app, sid, mq, count, vus = sys.argv[1:8]
try:
    r = json.loads(res)
except ValueError:  # 시간 초과 등으로 결과가 없으면 미완료로 기록
    r = {}
http = {}
try:
    for row in csv.DictReader(open(path)):
        if row['Name'] == 'Aggregated':
            http = {'httpRequests': int(row['Request Count']), 'httpFails': int(row['Failure Count']),
                    'httpRps': round(float(row['Requests/s']), 1), 'httpAvgMs': round(float(row['Average Response Time']), 1),
                    'httpP95Ms': float(row['95%'] or 0)}
except FileNotFoundError:
    pass
lat = r.get('latencyMs') or {}
print(json.dumps({'app': app, 'tool': 'locust', 'scenario': sid, 'mq': mq, 'run': 1, 'count': int(count), 'size': 256,
                  'db': True, 'vus': int(vus), 'tps': r.get('tps', 0), 'totalMs': r.get('totalMs', 0),
                  'p50': lat.get('p50', 0), 'p95': lat.get('p95', 0), 'p99': lat.get('p99', 0),
                  'processed': r.get('processed', 0), 'dbCount': r.get('dbCount'), 'done': bool(r.get('done')), **http}))
PY
    [ "${CAPTURE:-0}" = 1 ] && node "$ROOT/load/locust/capture.js" "$OUT/$name.html" "$OUT/$name.png" >/dev/null 2>&1
    echo "$(date +%H:%M:%S) DONE $name $(tail -1 "$OUT/raw.jsonl" | grep -o '"tps": [0-9]*, "totalMs": [0-9]*' ) $(tail -1 "$OUT/raw.jsonl" | grep -o '"done": [a-z]*')"
  done
done
echo "결과: $OUT"
