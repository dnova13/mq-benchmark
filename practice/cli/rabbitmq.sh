#!/usr/bin/env bash
# RabbitMQ 기본 조작 실습. bench-rabbitmq 컨테이너 안의 rabbitmqadmin, rabbitmqctl을 쓴다.
# 사용: practice/cli/rabbitmq.sh list | <단계번호> | all
set -eu
source "$(dirname "$0")/_lib.sh"

admin() { show "rabbitmqadmin $*"; docker exec -i bench-rabbitmq rabbitmqadmin -u guest -p guest "$@"; }
ctl() { show "rabbitmqctl $*"; docker exec -i bench-rabbitmq rabbitmqctl -q "$@"; }

STEPS=(
  "상태 보기: 큐, exchange 목록"
  "큐 만들고 기본 exchange로 발행"
  "메시지 꺼내기: 엿보기(requeue)와 소비(ack)"
  "direct exchange와 routing key"
  "fanout exchange: 하나 발행, 여러 큐가 받음"
  "벤치마크 큐 들여다보기: bench-queue"
  "정리: 실습 큐와 exchange 삭제"
)

step1() {
  note "messages: 쌓인 메시지, consumers: 붙어 있는 컨슈머 수"
  ctl list_queues name messages consumers
  note "기본으로 있는 exchange들. 이름이 빈 것('')이 기본 exchange"
  admin list exchanges name type
}

step2() {
  note "durable: 브로커가 재시작해도 큐가 남는다"
  admin declare queue name=practice-queue durable=true
  note "기본 exchange는 routing key = 큐 이름인 큐로 보낸다"
  admin publish exchange=amq.default routing_key=practice-queue payload='{"orderId":1}'
  admin publish exchange=amq.default routing_key=practice-queue payload='{"orderId":2}'
  admin publish exchange=amq.default routing_key=practice-queue payload='{"orderId":3}'
  ctl list_queues name messages
}

step3() {
  note "ack_requeue_true: 꺼내 보고 다시 넣는다(엿보기). 큐의 메시지 수는 그대로다"
  admin get queue=practice-queue count=1 ackmode=ack_requeue_true
  ctl list_queues name messages
  note "ack_requeue_false: 꺼내고 ack한다(소비). 큐에서 사라진다. Kafka·Redis Streams와 다른 점"
  admin get queue=practice-queue count=2 ackmode=ack_requeue_false
  ctl list_queues name messages
}

step4() {
  note "direct: routing key가 binding key와 같은 큐로만 보낸다"
  admin declare exchange name=practice.direct type=direct
  admin declare queue name=practice-orders durable=true
  admin declare queue name=practice-payments durable=true
  admin declare binding source=practice.direct destination=practice-orders routing_key=order
  admin declare binding source=practice.direct destination=practice-payments routing_key=payment
  admin publish exchange=practice.direct routing_key=order payload='order-created'
  admin publish exchange=practice.direct routing_key=payment payload='payment-done'
  admin publish exchange=practice.direct routing_key=unknown payload='no-route'
  note "unknown은 묶인 큐가 없어 버려진다"
  ctl list_queues name messages
}

step5() {
  note "fanout: routing key를 무시하고 묶인 모든 큐에 복사한다(알림, 캐시 무효화 등)"
  admin declare exchange name=practice.fanout type=fanout
  admin declare queue name=practice-email durable=true
  admin declare queue name=practice-sms durable=true
  admin declare binding source=practice.fanout destination=practice-email
  admin declare binding source=practice.fanout destination=practice-sms
  admin publish exchange=practice.fanout routing_key= payload='user-signed-up'
  ctl list_queues name messages
}

step6() {
  note "벤치마크 앱이 쓰는 큐. 앱이 떠 있으면 consumers가 10이다(동시성 10)"
  ctl list_queues name durable messages consumers
  ctl list_consumers queue_name channel_pid prefetch_count
}

step7() {
  for q in practice-queue practice-orders practice-payments practice-email practice-sms; do
    admin delete queue name=$q 2>/dev/null || true
  done
  for e in practice.direct practice.fanout; do admin delete exchange name=$e 2>/dev/null || true; done
  ctl list_queues name messages
}

main "$@"
