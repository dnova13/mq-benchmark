#!/usr/bin/env bash
# 시나리오 매트릭스 실행. 사용법은 SCENARIOS.md 참고.
# 사용: ./scenarios.sh <nest|spring> [repeat=5] [ids=all]
#   예: ./scenarios.sh nest            # 전체 시나리오, 워밍업 1회 + 5회 반복
#       ./scenarios.sh spring 3 A4,B2  # 일부 시나리오만, 3회 반복
# 결과: results/<app>-<시각>.jsonl (한 줄 = 1회 측정)
set -e
APP=${1:?"사용: $0 <nest|spring> [repeat] [ids]"}; REPEAT=${2:-5}; IDS=${3:-all}
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
B5=B:10000:256:true:200"

run_once() { # id kind count size db vus mq
  local id=$1 kind=$2 count=$3 size=$4 db=$5 vus=$6 mq=$7 res
  if [ "$kind" = A ]; then
    res=$(curl -s --max-time 900 "$BASE/bench/$mq?count=$count&size=$size&db=$db")
  else
    res=$(k6 run -q -e BASE=$BASE -e MQ=$mq -e COUNT=$count -e SIZE=$size -e DB=$db -e VUS=$vus "$ROOT/load/k6/orders.js" 2>&1 \
      | grep -o 'RESULT {.*}' | head -1 | sed 's/^RESULT //; s/\\"/"/g')
  fi
  echo "$res"
}

echo "$SCENARIOS" | while IFS== read -r id spec; do
  if [ "$IDS" != all ] && ! echo ",$IDS," | grep -q ",$id,"; then continue; fi
  IFS=: read -r kind count size db vus <<< "$spec"
  for mq in kafka redis rabbit; do
    curl -s -X DELETE "$BASE/runs" >/dev/null
    run_once "$id" "$kind" "$count" "$size" "$db" "$vus" "$mq" >/dev/null   # 워밍업 1회(기록 안 함)
    for n in $(seq 1 "$REPEAT"); do
      curl -s -X DELETE "$BASE/runs" >/dev/null
      res=$(run_once "$id" "$kind" "$count" "$size" "$db" "$vus" "$mq")
      line=$(echo "$res" | python3 -c "
import sys, json
r = json.loads(sys.stdin.read())
print(json.dumps({'app':'$APP','scenario':'$id','mq':'$mq','run':$n,'count':$count,'size':$size,'db':'$db'=='true','vus':$vus,
  'tps':r['tps'],'totalMs':r['totalMs'],'p50':r['latencyMs']['p50'],'p95':r['latencyMs']['p95'],'p99':r['latencyMs']['p99'],'done':r['done']}))")
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
