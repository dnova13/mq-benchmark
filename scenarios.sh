#!/usr/bin/env bash
# 규모별 시나리오 실행. 정의와 해석은 SCENARIOS.md 참고.
# 사용: ./scenarios.sh <nest|spring> [repeat] [ids=all]
#   예: ./scenarios.sh spring                # 전체 12개, 반복은 시나리오별 기본값
#       ./scenarios.sh nest 1 A-1k,B-1k      # 일부만, 모두 1회
# 결과: results/<app>-<시각>.jsonl (한 줄 = 1회 측정). OUT 환경 변수로 경로를 바꿀 수 있다.
set -e
APP=${1:?"사용: $0 <nest|spring> [repeat] [ids]"}; REPEAT=${2:-}; IDS=${3:-all}
case "$APP" in nest) BASE=http://localhost:3000 ;; spring) BASE=http://localhost:8080 ;; *) echo "app은 nest|spring"; exit 1 ;; esac
ROOT=$(cd "$(dirname "$0")" && pwd)
OUT=${OUT:-"$ROOT/results/$APP-$(date +%Y%m%d-%H%M%S).jsonl"}

# 건수만 바꾸고 나머지(256B, DB 저장, B는 VU(동시 사용자) 100)는 고정한다.
# id=종류:건수:크기:db:vus:기본반복   (A는 vus 미사용)
SCENARIOS="A-1k=A:1000:256:true:0:5
A-10k=A:10000:256:true:0:5
A-100k=A:100000:256:true:0:3
A-500k=A:500000:256:true:0:3
A-1m=A:1000000:256:true:0:2
A-3m=A:3000000:256:true:0:1
B-1k=B:1000:256:true:100:5
B-10k=B:10000:256:true:100:5
B-100k=B:100000:256:true:100:3
B-500k=B:500000:256:true:100:3
B-1m=B:1000000:256:true:100:2
B-3m=B:3000000:256:true:100:1"

run_once() { # kind count size db vus mq
  local kind=$1 count=$2 size=$3 db=$4 vus=$5 mq=$6 res
  # 대기 시간은 건수에 비례(500건/초 기준, 최소 300초)
  local wait=$(( count / 500 > 300 ? count / 500 : 300 ))
  local poll=0.1; [ "$count" -ge 100000 ] && poll=1
  if [ "$kind" = A ]; then
    res=$(curl -s --max-time $(( wait * 2 + 300 )) "$BASE/bench/$mq?count=$count&size=$size&db=$db&timeout=$wait" || true)
  else
    res=$(k6 run -q -e BASE=$BASE -e MQ=$mq -e COUNT=$count -e SIZE=$size -e DB=$db -e VUS=$vus \
      -e MAX_DURATION=$(( wait * 2 ))s -e WAIT_SEC=$wait -e POLL_SEC=$poll "$ROOT/load/k6/orders.js" 2>&1 \
      | grep -o 'RESULT {.*}' | head -1 | sed 's/^RESULT //; s/\\"/"/g' || true)
  fi
  echo "$res"
}

echo "$SCENARIOS" | while IFS== read -r id spec; do
  if [ "$IDS" != all ] && ! echo ",$IDS," | grep -q ",$id,"; then continue; fi
  IFS=: read -r kind count size db vus rep <<< "$spec"
  rep=${REPEAT:-$rep}
  for mq in kafka redis rabbit; do
    curl -s -X DELETE "$BASE/runs" >/dev/null
    # 워밍업 1회(기록 안 함). 1만 건보다 크면 1만 건으로 대신한다
    run_once "$kind" "$(( count > 10000 ? 10000 : count ))" "$size" "$db" "$vus" "$mq" >/dev/null
    for n in $(seq 1 "$rep"); do
      curl -s -X DELETE "$BASE/runs" >/dev/null
      res=$(run_once "$kind" "$count" "$size" "$db" "$vus" "$mq")
      line=$(echo "$res" | python3 -c "
import sys, json
try:
    r = json.loads(sys.stdin.read())
except ValueError:  # 시간 초과 등으로 응답이 없으면 미완료로 기록
    r = {}
lat = r.get('latencyMs') or {}
print(json.dumps({'app':'$APP','scenario':'$id','mq':'$mq','run':$n,'count':$count,'size':$size,'db':'$db'=='true','vus':$vus,
  'tps':r.get('tps',0),'totalMs':r.get('totalMs',0),'p50':lat.get('p50',0),'p95':lat.get('p95',0),'p99':lat.get('p99',0),
  'dbCount':r.get('dbCount'),'done':bool(r.get('done'))}))")
      echo "$line" | tee -a "$OUT"
    done
  done
done

echo; echo "== 중앙값 요약 ($OUT) =="
python3 - "$OUT" <<'PY'
import sys, json, statistics as st, collections
g = collections.defaultdict(list)
for l in open(sys.argv[1]):
    r = json.loads(l); g[(r['scenario'], r['mq'])].append(r)
print(f"{'시나리오':<8}{'MQ':<8}{'TPS':>8}{'p50':>8}{'p95':>8}{'p99':>8}")
for (s, m), rs in sorted(g.items(), key=lambda kv: (kv[0][0][0], kv[1][0]['count'], kv[0][1])):
    f = lambda k: int(st.median(x[k] for x in rs))
    print(f"{s:<8}{m:<8}{f('tps'):>8}{f('p50'):>8}{f('p95'):>8}{f('p99'):>8}")
PY
