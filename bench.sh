#!/usr/bin/env bash
# 사용: ./bench.sh <nest|spring> [count=100000] [size=256] [db=false]
# 선택한 앱이 실행 중일 때 kafka, redis, rabbit을 차례로 측정한다.
set -e
APP=${1:-nest}; COUNT=${2:-100000}; SIZE=${3:-256}; DB=${4:-false}
case "$APP" in
  nest) PORT=3000 ;;
  spring) PORT=8080 ;;
  *) echo "사용: $0 <nest|spring> [count] [size]"; exit 1 ;;
esac
for MQ in kafka redis rabbit; do
  echo "== $APP / $MQ =="
  curl -s --max-time 600 "localhost:$PORT/bench/$MQ?count=$COUNT&size=$SIZE&db=$DB"; echo
done
