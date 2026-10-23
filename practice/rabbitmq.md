# RabbitMQ 실습

## 핵심 개념
- **RabbitMQ**는 AMQP 프로토콜을 쓰는 메시지 브로커다. 발행자는 큐에 직접 넣지 않고 **exchange**에 보낸다. exchange가 규칙(binding)에 따라 큐로 나눠 준다.
- **큐**에 쌓인 메시지는 컨슈머가 받아 **ack**하면 사라진다. 이 점이 Kafka·Redis Streams와 가장 다르다. RabbitMQ는 로그가 아니라 할 일 목록에 가깝다.
- 같은 큐에 컨슈머가 여러 개면 메시지를 **나눠** 받는다(작업 분배).
- ack 전에 컨슈머 연결이 끊기면 메시지가 큐로 돌아가고, 다음에 받을 때 `redelivered=true`가 된다.
- **prefetch**: 컨슈머가 ack 안 한 메시지를 최대 몇 개까지 받을지 정한다. 벤치마크는 100이다.
- **durable 큐 + persistent 메시지**를 써야 브로커가 재시작해도 메시지가 남는다.

```
발행자 ──▶ [exchange] ──binding(routing key)──▶ [큐] ──▶ 컨슈머(ack)
```

| exchange 종류 | 규칙 | 쓰임새 |
|---|---|---|
| 기본(`""`) | routing key = 큐 이름인 큐로 | 큐에 바로 넣는 것처럼 쓸 때. 이 프로젝트의 벤치마크가 이 방식이다 |
| direct | routing key가 binding key와 같은 큐로 | 종류별로 나눠 보내기(주문, 결제) |
| fanout | 묶인 모든 큐로 복사(routing key 무시) | 알림, 캐시 무효화처럼 여러 곳에 동시에 |
| topic | `order.*`, `order.#` 같은 패턴 매칭 | 계층형 이벤트 구독 |

## CLI 실습: `practice/cli/rabbitmq.sh`
```bash
practice/cli/rabbitmq.sh list
practice/cli/rabbitmq.sh 1
```
| 단계 | 하는 일 | 핵심 명령 |
|---|---|---|
| 1 | 큐, exchange 목록 | `rabbitmqctl list_queues name messages consumers`, `rabbitmqadmin list exchanges` |
| 2 | 큐 만들고 기본 exchange로 발행 | `rabbitmqadmin declare queue name=practice-queue durable=true`, `publish exchange=amq.default routing_key=practice-queue` |
| 3 | 엿보기와 소비 | `get queue=... ackmode=ack_requeue_true`(다시 넣음), `ackmode=ack_requeue_false`(제거) |
| 4 | direct exchange | `declare exchange type=direct`, `declare binding ... routing_key=order` |
| 5 | fanout exchange | 한 번 발행 → 묶인 큐 2개에 모두 들어감 |
| 6 | 벤치마크 큐 보기 | `list_queues`, `list_consumers` (prefetch 확인) |
| 7 | 정리 | `delete queue`, `delete exchange` |

직접 쳐 보려면 컨테이너 안의 도구를 쓴다.
```bash
docker exec -it bench-rabbitmq rabbitmqctl list_queues name messages consumers
docker exec -it bench-rabbitmq rabbitmqadmin -u guest -p guest list queues
```

### 자주 쓰는 명령
| 명령 | 뜻 |
|---|---|
| `rabbitmqctl list_queues name messages_ready messages_unacknowledged consumers` | 큐별 대기, 미확인, 컨슈머 수 |
| `rabbitmqctl list_exchanges` / `list_bindings` | exchange, binding 목록 |
| `rabbitmqctl list_connections` / `list_channels name prefetch_count messages_unacknowledged` | 접속과 채널 |
| `rabbitmqctl purge_queue q` | 큐 비우기 |
| `rabbitmqadmin declare queue name=q durable=true` | 큐 생성 |
| `rabbitmqadmin declare exchange name=e type=topic` | exchange 생성 |
| `rabbitmqadmin declare binding source=e destination=q routing_key=order.*` | 묶기 |
| `rabbitmqadmin publish exchange=e routing_key=k payload='...'` | 발행 |
| `rabbitmqadmin get queue=q count=5 ackmode=ack_requeue_true` | 엿보기 |

## GUI: RabbitMQ Management
http://localhost:31567 (guest / guest) → **Queues and Streams** → `practice-queue` → Get messages, Publish message. Exchanges 탭에서 binding을 본다. 자세한 내용은 [gui.md](gui.md#rabbitmq-management).

## 코드 예제
```bash
cd practice/node && npm install
node rabbitmq.js produce 5
node rabbitmq.js consume         # 다른 터미널에서 하나 더 띄우면 나눠 받는다
node rabbitmq.js fanout
node rabbitmq.js info

cd practice/java
./gradlew run -Pex=RabbitPractice --args="produce 5"
./gradlew run -Pex=RabbitPractice --args="consume"
```
핵심 코드(Node, `practice/node/rabbitmq.js`):
```js
const conn = await amqp.connect('amqp://guest:guest@localhost:35672');
const ch = await conn.createChannel();
await ch.assertQueue('practice-queue', { durable: true });                          // 큐(없으면 생성)
ch.sendToQueue('practice-queue', Buffer.from('hello'), { persistent: true });        // 발행
await ch.prefetch(10);
await ch.consume('practice-queue', (msg) => { /* 처리 */ ch.ack(msg); });           // 소비 + ack
```
- `sendToQueue`가 `false`를 돌려주면 쓰기 버퍼가 찬 것이다. 대량 발행이면 `drain` 이벤트를 기다린다. 벤치마크의 Nest 구현이 이렇게 한다: `nest/src/mq/rabbitmq/rabbitmq.publisher.ts`
- Spring은 `RabbitTemplate.convertAndSend`와 `@RabbitListener`를 쓴다: `spring/.../mq/rabbitmq/`

## 앱 API
```bash
curl -X POST localhost:8080/practice/rabbit/messages -H 'Content-Type: application/json' -d '{"payload":"order-1"}'
curl "localhost:8080/practice/rabbit/messages?count=1&ack=false"   # 엿보기: 다시 큐로(redelivered=true가 됨)
curl "localhost:8080/practice/rabbit/messages?count=1"             # 소비: 큐에서 사라짐
curl localhost:8080/practice/rabbit/info                           # 메시지 수, 컨슈머 수
curl -X DELETE localhost:8080/practice/rabbit
```

## 연습 문제
1. `node rabbitmq.js consume`을 두 개 띄우고 `produce 10`을 실행한다. 두 컨슈머가 번갈아 받는지(라운드 로빈) 본다.
2. `rabbitmq.js`의 `ch.ack(msg)`를 지우고 consume을 실행한 뒤 Ctrl+C로 끈다. Management의 Ready/Unacked가 어떻게 바뀌는지 보고, 다시 consume하면 `redelivered=true`로 오는지 확인한다.
3. topic exchange를 만들고 `order.*`, `order.#`로 큐 두 개를 묶는다. routing key `order.created`, `order.payment.done`으로 발행해 어느 큐에 들어가는지 비교한다.
4. prefetch를 1로 바꾸고, 처리에 1초씩 걸리게 한 컨슈머 둘을 띄운다. 빠른 컨슈머가 더 많이 받는지 본다.
