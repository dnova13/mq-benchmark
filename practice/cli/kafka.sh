#!/usr/bin/env bash
# Kafka 기본 조작 실습. bench-kafka 컨테이너 안의 kafka-*.sh 도구를 쓴다.
# 사용: practice/cli/kafka.sh list | <단계번호> | all
set -eu
source "$(dirname "$0")/_lib.sh"

BS=localhost:39092      # 컨테이너 안에서 본 브로커 주소
TOPIC=practice-topic
GROUP=practice-group

kafka() { local tool=$1; shift; show "$tool $*"; docker exec -i bench-kafka /opt/kafka/bin/$tool --bootstrap-server $BS "$@"; }

STEPS=(
  "토픽 만들기와 확인: 파티션 3개"
  "메시지 발행: key:value 형식"
  "메시지 소비: 처음부터, 컨슈머 그룹으로"
  "컨슈머 그룹과 lag 확인"
  "오프셋 되감기(재처리)"
  "벤치마크 토픽 들여다보기: bench-topic"
  "정리: 실습 토픽과 그룹 삭제"
)

step1() {
  note "토픽은 메시지를 담는 이름 있는 로그다. 파티션 수만큼 병렬로 읽을 수 있다"
  kafka kafka-topics.sh --create --if-not-exists --topic $TOPIC --partitions 3 --replication-factor 1
  kafka kafka-topics.sh --list
  note "Leader, Replicas, Isr: 단일 노드라 모두 브로커 1번"
  kafka kafka-topics.sh --describe --topic $TOPIC
}

step2() {
  note "key의 해시로 파티션이 정해진다. key가 같으면 항상 같은 파티션이라 순서가 보장된다(user-1의 두 건)"
  local msgs='user-1:order-1\nuser-2:order-2\nuser-1:order-3\nuser-4:order-4\nuser-5:order-5\nuser-6:order-6\n'
  show "printf '$msgs' | kafka-console-producer.sh --topic $TOPIC --property parse.key=true --property key.separator=:"
  printf "$msgs" | docker exec -i bench-kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server $BS --topic $TOPIC --property parse.key=true --property key.separator=:
  note "파티션별 마지막 오프셋(= 쌓인 메시지 수)"
  show "kafka-get-offsets.sh --topic $TOPIC"
  docker exec bench-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server $BS --topic $TOPIC
}

step3() {
  note "그룹 $GROUP으로 처음부터 읽는다. 5초 동안 새 메시지가 없으면 끝난다"
  kafka kafka-console-consumer.sh --topic $TOPIC --group $GROUP --from-beginning --timeout-ms 5000 \
    --property print.key=true --property print.partition=true --property print.offset=true 2>/dev/null || true
  note "같은 그룹으로 다시 읽으면 아무것도 안 나온다. 그룹이 어디까지 읽었는지(커밋 오프셋) 기억하기 때문이다"
  kafka kafka-console-consumer.sh --topic $TOPIC --group $GROUP --timeout-ms 3000 2>/dev/null || true
}

step4() {
  note "CURRENT-OFFSET: 그룹이 읽은 위치, LOG-END-OFFSET: 마지막 메시지, LAG: 아직 안 읽은 수"
  kafka kafka-consumer-groups.sh --describe --group $GROUP
  kafka kafka-consumer-groups.sh --list
}

step5() {
  note "그룹의 오프셋을 처음으로 되돌린다(그룹에 붙은 컨슈머가 없어야 한다). 실무에서는 장애 후 재처리에 쓴다"
  kafka kafka-consumer-groups.sh --group $GROUP --topic $TOPIC --reset-offsets --to-earliest --execute
  echo
  note "다시 읽으면 처음부터 나온다"
  kafka kafka-console-consumer.sh --topic $TOPIC --group $GROUP --timeout-ms 5000 --property print.key=true 2>/dev/null || true
}

step6() {
  note "벤치마크 앱이 쓰는 토픽(파티션 10). 앱이 떠 있으면 그룹 bench-spring-group / bench-nest-group이 보인다"
  if ! docker exec bench-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server $BS --list | grep -qx bench-topic; then
    note "bench-topic이 없다. 앱을 한 번 띄우면 생긴다"; return
  fi
  kafka kafka-topics.sh --describe --topic bench-topic
  for g in $(docker exec bench-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server $BS --list | grep '^bench-'); do
    kafka kafka-consumer-groups.sh --describe --group $g
  done
}

step7() {
  docker exec bench-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server $BS --list | grep -qx $GROUP \
    && kafka kafka-consumer-groups.sh --delete --group $GROUP
  kafka kafka-topics.sh --delete --topic $TOPIC || true
  kafka kafka-topics.sh --list
}

main "$@"
