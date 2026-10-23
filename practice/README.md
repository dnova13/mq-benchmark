# MQ 실습: Redis Streams, Kafka, RabbitMQ

세 MQ의 기본 조작을 직접 해 보고, GUI로 상태를 확인하는 실습 자료다. 벤치마크와 같은 docker compose 인프라를 쓰지만, **실습용 이름**을 따로 써서 벤치마크 데이터와 섞이지 않는다.

| MQ | 실습 대상 | 벤치마크 대상(건드리지 않음) |
|---|---|---|
| Redis Streams | `practice:stream`, `practice:*` 키 | `bench:stream` |
| Kafka | `practice-topic` (파티션 3) | `bench-topic` |
| RabbitMQ | `practice-queue`, `practice.*` exchange | `bench-queue` |

## 실습 방법 4가지
같은 실습 대상을 쓰므로 섞어서 해도 된다. 예를 들어 앱 API로 넣고, GUI에서 보고, CLI로 꺼낼 수 있다.

| 방법 | 위치 | 필요한 것 | 좋은 점 |
|---|---|---|---|
| 1. CLI 스크립트 | `practice/cli/*.sh` | Docker만 | 브로커 명령을 그대로 익힌다. 단계별로 설명이 같이 나온다 |
| 2. GUI | 브라우저 | Docker만 | 토픽, 큐, 스트림 상태를 눈으로 본다 |
| 3. 코드 예제 | `practice/node`, `practice/java` | Node 18+ 또는 JDK 21 | 클라이언트 라이브러리로 발행과 소비를 짜 본다 |
| 4. 앱 API | Spring(8080), Nest(3000)의 `/practice` | 앱 실행 | curl 한 줄로 발행과 조회를 해 본다 |

## 시작하기
```bash
# 인프라 + GUI
docker compose --profile gui up -d
docker compose ps        # 4개 브로커/DB + bench-redisinsight, bench-kafka-ui가 Up
```
| GUI | 주소 | 로그인 |
|---|---|---|
| RedisInsight (Redis) | http://localhost:35540 | 없음. 처음 접속 시 약관 동의 |
| Kafka UI (Kafka) | http://localhost:38080 | 없음 |
| RabbitMQ Management | http://localhost:31567 | guest / guest |

GUI 화면별 사용법은 [gui.md](gui.md)에 있다.

## MQ별 실습 가이드
처음이라면 Redis → RabbitMQ → Kafka 순서를 권한다. 개념이 단순한 것부터다.

| 문서 | 내용 |
|---|---|
| [redis.md](redis.md) | 키/값 기본, Hash·List·Set, Streams, 컨슈머 그룹과 ack |
| [rabbitmq.md](rabbitmq.md) | 큐, exchange(direct·fanout), routing key, ack와 재전달 |
| [kafka.md](kafka.md) | 토픽과 파티션, key와 순서, 컨슈머 그룹, 오프셋과 lag, 재처리 |
| [gui.md](gui.md) | RedisInsight, Kafka UI, RabbitMQ Management 화면 사용법 |

## 1. CLI 스크립트
```bash
practice/cli/redis.sh list      # 단계 목록
practice/cli/redis.sh 3         # 3단계만 실행
practice/cli/redis.sh all       # 처음부터 끝까지
practice/cli/kafka.sh list
practice/cli/rabbitmq.sh list
```
각 단계는 실행하는 명령(`$ ...`)과 설명(`# ...`)을 같이 보여 준다. 마지막 단계가 정리(실습 데이터 삭제)다.

## 3. 코드 예제
세 MQ 모두 같은 형태다: `produce`(발행), `consume`(소비, Ctrl+C로 종료), `info`(상태). RabbitMQ는 `fanout`도 있다.
```bash
# Node (kafkajs, ioredis, amqplib)
cd practice/node && npm install
node redis.js produce 5
node redis.js consume c1
node kafka.js produce 5
node kafka.js consume
node rabbitmq.js produce 5
node rabbitmq.js consume

# Java (kafka-clients, lettuce-core, amqp-client. Spring 없이 순수 클라이언트)
cd practice/java
./gradlew run -Pex=RedisPractice --args="produce 5"
./gradlew run -Pex=KafkaPractice --args="consume practice-java"
./gradlew run -Pex=RabbitPractice --args="fanout"
```
접속 주소는 환경 변수로 바꿀 수 있다: `REDIS_HOST`/`REDIS_PORT`(Node), `REDIS_URL`(Java), `KAFKA_BROKER`, `RABBITMQ_URL`.

**해 보면 좋은 것:** 터미널 두 개로 `consume`을 띄운다(Redis는 컨슈머 이름을 다르게: `c1`, `c2`). 그다음 `produce`를 실행하면 메시지가 두 컨슈머에 **나눠서** 들어간다. RabbitMQ는 같은 큐의 컨슈머끼리 나눈다. Kafka는 같은 그룹이면 나누고, 그룹이 다르면 각자 전부 받는다.

## 4. 앱 API (`/practice`)
Spring(`localhost:8080`)과 Nest(`localhost:3000`)에 같은 API가 있다. 벤치마크 API와 별개다.

| 메서드와 경로 | 설명 |
|---|---|
| `POST /practice/{mq}/messages` | 발행. 본문 `{"key":"user-1","payload":"hello"}` (key는 선택, Kafka 파티션 결정에 쓰임) |
| `GET /practice/{mq}/messages?count=10&ack=true` | 최근 메시지 조회. 아래 표 참고 |
| `GET /practice/{mq}/info` | 상태: Redis 길이·그룹, Kafka 파티션별 오프셋, RabbitMQ 메시지·컨슈머 수 |
| `DELETE /practice/{mq}` | 실습 대상 삭제(스트림, 토픽, 큐) |

`{mq}`는 `kafka`, `redis`, `rabbit` 중 하나다.

**조회(`GET .../messages`)는 MQ 성격대로 동작한다.**
| MQ | 동작 | 메시지가 남나 |
|---|---|---|
| Redis | `XREVRANGE`로 최근 N건 | 남는다(스트림은 로그) |
| Kafka | 파티션마다 끝에서 N건 앞으로 이동해 읽음. 오프셋은 커밋하지 않음 | 남는다(토픽은 로그) |
| RabbitMQ | `basic.get`으로 N건 꺼냄. `ack=true`면 ack, `ack=false`면 다시 큐로(엿보기) | `ack=true`면 **사라진다** |

```bash
# Spring 기준 (Nest는 포트만 3000)
curl -X POST localhost:8080/practice/kafka/messages -H 'Content-Type: application/json' -d '{"key":"user-1","payload":"hello"}'
curl "localhost:8080/practice/kafka/messages?count=5"
curl localhost:8080/practice/kafka/info

curl -X POST localhost:8080/practice/rabbit/messages -H 'Content-Type: application/json' -d '{"payload":"order-1"}'
curl "localhost:8080/practice/rabbit/messages?count=1&ack=false"   # 엿보기: 다시 큐로 돌아감
curl "localhost:8080/practice/rabbit/messages?count=1"             # 소비: 큐에서 사라짐
curl -X DELETE localhost:8080/practice/rabbit
```
구현 위치: Spring `com.bench.practice`, Nest `src/practice`. MQ 모듈과 접속 설정만 공유하고 연결은 따로 써서 벤치마크에 영향이 없다.

## 정리
```bash
practice/cli/redis.sh 6; practice/cli/kafka.sh 7; practice/cli/rabbitmq.sh 7   # 실습 데이터만 삭제
docker compose --profile gui stop redisinsight kafka-ui                          # GUI만 끄기
docker compose --profile gui down -v                                             # 전부 끄고 데이터 삭제
```
