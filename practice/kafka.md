# Kafka 실습

## 핵심 개념
- **Kafka**는 메시지를 **토픽**이라는 로그에 순서대로 쌓는다. 읽어도 지워지지 않고, 보존 기간(기본 7일)이 지나면 지워진다.
- 토픽은 **파티션**으로 나뉜다. 파티션 안에서만 순서가 보장되고, 파티션 수만큼 병렬로 읽을 수 있다.
  - 메시지의 **key**를 해시해서 파티션을 정한다. key가 같으면 항상 같은 파티션이라 순서가 지켜진다(예: 같은 사용자의 주문).
  - 벤치마크는 파티션 10개, key = userId다.
- 각 메시지는 파티션 안에서 **오프셋**(0, 1, 2 ...)을 가진다.
- **컨슈머 그룹**: 같은 그룹의 컨슈머들이 파티션을 나눠 맡는다. 파티션 하나는 그룹 안의 컨슈머 하나만 읽는다. 그래서 컨슈머가 파티션 수보다 많으면 남는 컨슈머는 논다.
  - 그룹은 어디까지 읽었는지(**커밋 오프셋**)를 Kafka에 저장한다. 재시작하면 거기서부터 읽는다.
  - **그룹이 다르면 각자 처음부터 전부 받는다.** 같은 데이터를 여러 서비스가 따로 소비할 수 있다.
  - **lag** = 마지막 오프셋 - 커밋 오프셋 = 아직 안 읽은 수. 컨슈머가 밀리는지 보는 핵심 지표다.
- 이 프로젝트는 ZooKeeper 없이 **KRaft** 단일 노드로 띄운다.

```
토픽 practice-topic
 ├─ 파티션 0: [0][1][2]...        ← 그룹 A의 컨슈머 1
 ├─ 파티션 1: [0][1]...           ← 그룹 A의 컨슈머 2
 └─ 파티션 2: [0][1][2][3]...     ← 그룹 A의 컨슈머 2
그룹 B는 같은 메시지를 따로 처음부터 읽을 수 있다
```

## CLI 실습: `practice/cli/kafka.sh`
```bash
practice/cli/kafka.sh list
practice/cli/kafka.sh 1
```
| 단계 | 하는 일 | 핵심 명령 |
|---|---|---|
| 1 | 토픽 생성과 확인 | `kafka-topics.sh --create --topic practice-topic --partitions 3`, `--describe` |
| 2 | key:value로 발행 | `kafka-console-producer.sh --property parse.key=true --property key.separator=:`, `kafka-get-offsets.sh` |
| 3 | 그룹으로 소비 | `kafka-console-consumer.sh --group practice-group --from-beginning` → 다시 읽으면 아무것도 안 나옴 |
| 4 | 그룹과 lag 확인 | `kafka-consumer-groups.sh --describe --group practice-group` |
| 5 | 오프셋 되감기 | `--reset-offsets --to-earliest --execute` → 처음부터 다시 읽힘 |
| 6 | 벤치마크 토픽 보기 | `--describe --topic bench-topic`, `bench-*` 그룹 |
| 7 | 정리 | 그룹 삭제, 토픽 삭제 |

직접 쳐 보려면 컨테이너 안의 도구를 쓴다. 브로커 주소는 컨테이너 안에서 `localhost:39092`다.
```bash
docker exec -it bench-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:39092 --list
docker exec -it bench-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:39092 \
  --topic practice-topic --from-beginning --property print.key=true --property print.partition=true
```

### 자주 쓰는 명령 (`--bootstrap-server localhost:39092` 생략)
| 명령 | 뜻 |
|---|---|
| `kafka-topics.sh --list` / `--describe --topic t` | 토픽 목록 / 파티션, 리더, 설정 |
| `kafka-topics.sh --create --topic t --partitions 3 --replication-factor 1` | 생성 |
| `kafka-topics.sh --alter --topic t --partitions 6` | 파티션 늘리기(줄이기는 안 됨. key→파티션 매핑이 바뀜) |
| `kafka-configs.sh --alter --entity-type topics --entity-name t --add-config retention.ms=3600000` | 보존 기간 1시간 |
| `kafka-console-producer.sh --topic t` | 표준 입력으로 발행 |
| `kafka-console-consumer.sh --topic t --group g [--from-beginning]` | 소비 |
| `kafka-get-offsets.sh --topic t` | 파티션별 마지막 오프셋 |
| `kafka-consumer-groups.sh --list` / `--describe --group g` | 그룹 목록 / 오프셋, lag |
| `kafka-consumer-groups.sh --group g --topic t --reset-offsets --to-earliest --execute` | 처음으로 되감기. `--to-datetime 2026-10-23T10:00:00.000`, `--shift-by -10`도 가능 |

## GUI: Kafka UI
http://localhost:38080 → **Topics** → `practice-topic` → Messages(메시지 보기), Produce Message(발행). **Consumers**에서 그룹별 lag. 자세한 내용은 [gui.md](gui.md#kafka-ui-kafka).

## 코드 예제
```bash
cd practice/node && npm install
node kafka.js produce 6            # key = user-0, user-1, user-2 반복
node kafka.js consume              # 그룹 practice-node (다른 터미널에서 같은 그룹 하나 더 → 파티션 나눠 가짐)
node kafka.js consume other-group  # 다른 그룹 → 처음부터 전부 받음
node kafka.js info

cd practice/java
./gradlew run -Pex=KafkaPractice --args="produce 6"
./gradlew run -Pex=KafkaPractice --args="consume practice-java"
```
핵심 코드(Node, `practice/node/kafka.js`):
```js
const kafka = new Kafka({ clientId: 'practice-node', brokers: ['localhost:39092'] });
const producer = kafka.producer();
await producer.connect();
await producer.send({ topic: 'practice-topic', messages: [{ key: 'user-1', value: 'hello' }] }); // 발행

const consumer = kafka.consumer({ groupId: 'practice-node' });
await consumer.connect();
await consumer.subscribe({ topic: 'practice-topic', fromBeginning: true });
await consumer.run({ eachMessage: async ({ partition, message }) => { /* 처리 */ } });  // 끝나면 자동 커밋
```
- `fromBeginning`은 그룹에 커밋 오프셋이 **없을 때만** 처음부터 읽는다. 이미 읽은 그룹은 커밋 위치부터 읽는다.
- 벤치마크 구현: Nest `nest/src/mq/kafka/`(eachBatch로 배치 병렬 처리), Spring `spring/.../mq/kafka/`(`@KafkaListener`, 동시성 10)

## 앱 API
```bash
curl -X POST localhost:8080/practice/kafka/messages -H 'Content-Type: application/json' -d '{"key":"user-1","payload":"hello"}'
# 응답의 partition, offset을 보면 같은 key는 같은 파티션에 들어간다
curl "localhost:8080/practice/kafka/messages?count=5"    # 최근 5건. 오프셋을 커밋하지 않는 조회라 메시지도 그룹도 그대로
curl localhost:8080/practice/kafka/info                  # 파티션별 마지막 오프셋
curl -X DELETE localhost:8080/practice/kafka             # 토픽 삭제
```

## 연습 문제
1. `node kafka.js consume`(같은 그룹)을 터미널 4개에서 띄운다. 파티션이 3개라 하나는 아무것도 못 받는 것을 확인한다. Kafka UI → Consumers에서 파티션 배정을 본다.
2. 컨슈머를 모두 끄고 `produce 10`을 실행한다. Kafka UI에서 lag이 10이 되는 것을 본 뒤, consume을 다시 띄워 0으로 줄어드는지 확인한다.
3. CLI 5단계처럼 `--reset-offsets --to-earliest`로 되감은 뒤 다시 소비한다. 장애 후 재처리를 이렇게 한다.
4. 토픽 파티션을 3 → 6으로 늘리고(`--alter`), 같은 key로 발행해 보낸 파티션이 바뀌는지 본다. key 기반 순서 보장이 파티션 수에 묶여 있다는 점을 확인한다.
