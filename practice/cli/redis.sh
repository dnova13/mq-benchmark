#!/usr/bin/env bash
# Redis 기본 조작과 Streams 실습. docker compose로 띄운 bench-redis 컨테이너의 redis-cli를 쓴다.
# 사용: practice/cli/redis.sh list | <단계번호> | all
set -eu
source "$(dirname "$0")/_lib.sh"

cli() { show "redis-cli $*"; docker exec -i bench-redis redis-cli "$@"; }

STEPS=(
  "문자열 키: SET, GET, INCR, 만료(TTL)"
  "자료구조: Hash, List, Set"
  "Streams 발행과 조회: XADD, XLEN, XRANGE"
  "컨슈머 그룹: XGROUP, XREADGROUP, XPENDING, XACK"
  "벤치마크 스트림 들여다보기: bench:stream"
  "정리: 실습 키 삭제"
)

step1() {
  note "가장 기본인 문자열 키. 캐시, 카운터에 쓴다"
  cli SET practice:greeting "hello"
  cli GET practice:greeting
  cli INCR practice:counter
  cli INCRBY practice:counter 10
  note "60초 뒤 자동 삭제. TTL은 남은 초(-1: 만료 없음, -2: 키 없음)"
  cli SET practice:session "user-1" EX 60
  cli TTL practice:session
  cli KEYS 'practice:*'
}

step2() {
  note "Hash: 필드가 있는 객체. 사용자 정보 등"
  cli HSET practice:user:1 name kim age 30
  cli HGETALL practice:user:1
  note "List: 순서가 있는 목록. 양쪽 끝에서 넣고 뺀다(간단한 큐)"
  cli RPUSH practice:jobs job-1 job-2 job-3
  cli LPOP practice:jobs
  cli LRANGE practice:jobs 0 -1
  note "Set: 중복 없는 집합"
  cli SADD practice:tags kafka redis rabbitmq redis
  cli SMEMBERS practice:tags
}

step3() {
  note "Stream은 지워지지 않고 쌓이는 로그다. *는 ID를 자동 생성(밀리초-순번)"
  cli XADD practice:stream '*' orderId 1 amount 1000
  cli XADD practice:stream '*' orderId 2 amount 2000
  cli XADD practice:stream '*' orderId 3 amount 3000
  cli XLEN practice:stream
  note "- + 는 처음부터 끝까지. 읽어도 메시지는 남는다"
  cli XRANGE practice:stream - +
  cli XREVRANGE practice:stream + - COUNT 1
}

step4() {
  note "컨슈머 그룹: 같은 그룹의 컨슈머들이 메시지를 나눠 받는다. 0은 스트림 처음부터"
  docker exec bench-redis redis-cli XGROUP CREATE practice:stream practice-group 0 MKSTREAM >/dev/null 2>&1 \
    && show "redis-cli XGROUP CREATE practice:stream practice-group 0 MKSTREAM" || note "그룹이 이미 있다(BUSYGROUP)"
  note "> 는 '아직 이 그룹에 전달되지 않은 새 메시지'. c1이 2건을 가져간다"
  cli XREADGROUP GROUP practice-group c1 COUNT 2 STREAMS practice:stream '>'
  note "가져갔지만 ack하지 않은 메시지(PEL). 컨슈머가 죽으면 여기 남는다"
  cli XPENDING practice:stream practice-group
  note "처리를 마쳤다고 알린다. 첫 번째 대기 메시지 ID를 ack"
  id=$(docker exec bench-redis redis-cli XPENDING practice:stream practice-group - + 1 | head -1)
  [ -n "$id" ] && cli XACK practice:stream practice-group "$id"
  cli XPENDING practice:stream practice-group
  note "c2는 남은 메시지를 받는다(c1이 가져간 것은 다시 오지 않음)"
  cli XREADGROUP GROUP practice-group c2 COUNT 10 STREAMS practice:stream '>'
  cli XINFO GROUPS practice:stream
}

step5() {
  note "벤치마크 앱이 쓰는 스트림. 앱이 떠 있어야 그룹이 보인다"
  cli EXISTS bench:stream
  docker exec bench-redis redis-cli EXISTS bench:stream | grep -q 1 || { note "bench:stream이 없다. 앱을 한 번 띄우면 생긴다"; return; }
  cli XLEN bench:stream
  cli XINFO GROUPS bench:stream
  note "AOF(영속성) 설정 확인: appendonly yes"
  cli CONFIG GET appendonly
}

step6() {
  cli DEL practice:greeting practice:counter practice:session practice:user:1 practice:jobs practice:tags practice:stream
  cli KEYS 'practice:*'
}

main "$@"
