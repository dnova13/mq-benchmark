#!/usr/bin/env bash
# 시나리오 매트릭스 실행. 사용법은 SCENARIOS.md 참고.
# 사용: ./scenarios.sh <nest|spring> [repeat=5] [ids=all]
#   예: ./scenarios.sh nest            # 기본 시나리오(A1~B5), 워밍업 1회 + 5회 반복
#       ./scenarios.sh spring 3 A4,B2  # 일부 시나리오만, 3회 반복
#       ./scenarios.sh spring 3 large  # 대용량(A6,A7,A8,B6,B7). all에는 포함되지 않는다
# 결과: results/<app>-<시각>.jsonl (한 줄 = 1회 측정)
set -e
APP=${1:?"사용: $0 <nest|spring> [repeat] [ids]"}; REPEAT=${2:-5}; IDS=${3:-all}
LARGE="A6,A7,A8,B6,B7"
BASIC="A1,A2,A3,A4,A5,B1,B2,B3,B4,B5"
case "$IDS" in all) IDS=$BASIC ;; large) IDS=$LARGE ;; esac
case "$APP" in nest) BASE=http://localhost:3000 ;; spring) BASE=http://localhost:8080 ;; *) echo "app은 nest|spring"; exit 1 ;; esac
ROOT=$(cd "$(dirname "$0")" && pwd)
OUT="$ROOT/results/$APP-$(date +%Y%m%d-%H%M%S).jsonl"

# id=종류:count:size:db:vus   (A는 vus 미사용)
SCENARIOS="A1=A:1000:256:false:0
A2=A:10000:256:false:0
A3=A:1000:256:true:0
A4=A:10000:256:true:0
A5=A:10000:1024:true:0
B1=B:1000:256:true:50
B2=B:10000:256:true:100
B3=B:10000:256:false:100
B4=B:10000:256:true:10
B5=B:10000:256:true:200
A6=A:100000:256:true:0
A7=A:1000000:256:true:0
A8=A:1000000:256:false:0
B6=B:100000:256:true:100
B7=B:1000000:256:true:100"

run_once() { # id kind count size db vus mq
  local id=$1 kind=$2 count=$3 size=$4 db=$5 vus=$6 mq=$7 res
  # 대기 시간은 건수에 비례(500건/초 기준, 최소 300초). 10,000건 이하는 기존과 같다.
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
  IFS=: read -r kind count size db vus <<< "$spec"
  for mq in kafka redis rabbit; do
    curl -s -X DELETE "$BASE/runs" >/dev/null
    # 워밍업 1회(기록 안 함). 대용량은 10,000건으로 대신한다
    run_once "$id" "$kind" "$(( count > 10000 ? 10000 : count ))" "$size" "$db" "$vus" "$mq" >/dev/null
    for n in $(seq 1 "$REPEAT"); do
      curl -s -X DELETE "$BASE/runs" >/dev/null
      res=$(run_once "$id" "$kind" "$count" "$size" "$db" "$vus" "$mq")
      line=$(echo "$res" | python3 -c "
import sys, json
try:
    r = json.loads(sys.stdin.read())
except ValueError:  # 시간 초과 등으로 응답이 없으면 미완료로 기록
    r = {'tps': 0, 'totalMs': 0, 'latencyMs': {}, 'done': False}
lat = r.get('latencyMs') or {}
print(json.dumps({'app':'$APP','scenario':'$id','mq':'$mq','run':$n,'count':$count,'size':$size,'db':'$db'=='true','vus':$vus,
  'tps':r.get('tps',0),'totalMs':r.get('totalMs',0),'p50':lat.get('p50',0),'p95':lat.get('p95',0),'p99':lat.get('p99',0),'done':bool(r.get('done'))}))")
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
print(f"{'시나리오':<6}{'MQ':<8}{'TPS':>8}{'p50':>7}{'p95':>7}{'p99':>7}")
for (s, m), rs in sorted(g.items()):
    f = lambda k: int(st.median(x[k] for x in rs))
    print(f"{s:<6}{m:<8}{f('tps'):>8}{f('p50'):>7}{f('p95'):>7}{f('p99'):>7}")
PY
