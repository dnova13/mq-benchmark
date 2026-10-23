# MQ Benchmark (Kafka vs Redis Streams vs RabbitMQ)

## 프로젝트 목적
- Kafka, Redis Streams, RabbitMQ의 **성능 비교**(처리량 TPS, 지연)
- 추후 코딩할 때 **즉시 실행**할 수 있는 로컬 테스트 환경
- 백엔드는 NestJS와 Spring Boot 두 가지로 구성해 같은 시나리오를 비교
- 모든 브로커는 **단일 노드**이고 클러스터는 구성하지 않는다.

## 규칙
- 답변, 주석, 설명은 한국어로 작성한다.
- 커밋 메시지와 코드 네이밍만 영어를 사용한다.

## 구조
```
mq-benchmark/
├─ docker-compose.yml   # MariaDB, Redis, Kafka(KRaft), RabbitMQ
├─ load/                # 부하 스크립트: k6(load/k6/orders.js), Locust(load/locust/locustfile.py, run-scale.sh, capture.js)
├─ scenarios.sh         # 규모별 시나리오(A/B × 1천~300만) 자동 실행
├─ bench.sh             # 시나리오 A 간편 호출
├─ results/             # 측정 결과(jsonl, git 제외)
├─ reports/             # 결과 리포트(scale-result.md), 원본(scale-*-raw.jsonl), 표(scale-*-tables.md), 그래프(img/)
├─ report.py            # jsonl -> 마크다운 표
├─ compare.py           # Spring vs Nest 비교표
├─ charts.py            # 결과 그래프(PNG, matplotlib + 한글 폰트)
├─ docs/mq/             # MQ 개념 정리(MQ란, Redis Streams, RabbitMQ, Kafka) + 도식 PNG(img/, diagrams.py로 생성)
├─ practice/            # MQ 실습: CLI 스크립트(cli/), Node·Java 예제(node/, java/), 가이드(README.md, redis/kafka/rabbitmq/gui.md)
├─ nest/                # NestJS 10 (TypeScript, @nestjs/config + .env), 포트 3000
└─ spring/              # Spring Boot 3.4 + Java 21 (Gradle, properties), 포트 8080
```

### 앱 내부 구조 (MQ별 모듈 분리, 다른 프로젝트로 복사해 재사용하는 것이 목적)
| | Spring (`com.bench`) | Nest (`src/`) |
|---|---|---|
| 공통 인터페이스 | `mq/common` (`Message`, `MessageHandler`, `MessagePublisher`) | `common/messaging/message.interface.ts` |
| Kafka | `mq/kafka` + `application-kafka.properties` | `mq/kafka` + `config/kafka.config.ts` |
| RabbitMQ | `mq/rabbitmq` + `application-rabbitmq.properties` | `mq/rabbitmq` + `config/rabbitmq.config.ts` |
| Redis Streams | `mq/redis` + `application-redis.properties` | `mq/redis-stream` + `config/redis.config.ts` |
| 벤치 전용 | `benchmark/{controller,service,repository}` | `bench/` (`order/` 포함), `database/` |
| MQ 끄기 | `mq.<kafka\|rabbitmq\|redis>.enabled=false` | `MQ_<KAFKA\|RABBITMQ\|REDIS>_ENABLED=false` |

- MQ 모듈은 벤치 코드를 모른다. 컨슈머는 앱이 구현한 `MessageHandler`를 호출하고, 벤치는 `MessagePublisher`로 발행한다.
- 꺼진 MQ로 벤치 API를 호출하면 400이다.
- 연습용 API(`/practice/{mq}/...`, Spring `com.bench.practice`, Nest `src/practice`)는 벤치마크와 별개인 `practice-topic`, `practice:stream`, `practice-queue`만 쓰고 연결도 따로 만든다.

## 인프라 (docker-compose.yml)
원래 포트 앞에 `3`을 붙인 외부 포트를 쓴다.

| 서비스 | 이미지 | 외부 포트 | 계정 |
|---|---|---|---|
| MariaDB | mariadb:11 | **33307** | root / 1234 (DB: `bench`) |
| Redis | redis:7-alpine | **36397** | 없음 |
| Kafka | apache/kafka:3.9.0 (KRaft, ZooKeeper 없음) | **39092** | 없음 |
| RabbitMQ | rabbitmq:3-management | **35672** (AMQP), **31567** (관리 UI) | guest / guest |
| RedisInsight (GUI, `gui` 프로필) | redis/redisinsight:2.68 | **35540** | 없음(첫 접속 시 약관 동의) |
| Kafka UI (GUI, `gui` 프로필) | kafbat/kafka-ui:v1.2.0 | **38080** | 없음 |

- Redis 포트는 요청대로 `36397`이다. 일반적인 규칙(6379 앞에 3)을 따르면 `36379`이므로 오타일 수 있다. 바꾸려면 compose와 양쪽 설정을 같이 수정한다.
- RabbitMQ 관리 UI 15672 앞에 3을 붙이면 `315672`로 포트 범위(65535)를 넘는다. 그래서 `31567`을 쓴다.
- Kafka의 advertised listener는 `localhost:39092`(호스트의 앱용)와 `kafka:29092`(같은 compose 네트워크의 컨테이너용, Kafka UI가 사용)다.
- GUI 컨테이너는 `gui` 프로필이라 `docker compose up -d`에서는 뜨지 않는다. `docker compose --profile gui up -d`로 켠다. 벤치마크 측정 중에는 끄는 편이 정확하다.

```bash
docker compose up -d                  # 전체 기동(벤치마크용)
docker compose --profile gui up -d    # GUI까지(실습용)
docker compose down -v    # 종료와 데이터 삭제
```

## 클라이언트 구성 (검색 기준으로 가장 많이 쓰는 것)
| MQ | NestJS | Spring Boot |
|---|---|---|
| Kafka | `kafkajs` (`Kafka`, `Producer`, `Consumer`) | `spring-kafka` (`KafkaTemplate`, `@KafkaListener`) |
| Redis Streams | `ioredis` (`XADD`, `XREADGROUP`, `XACK`, 컨슈머 그룹) | `spring-boot-starter-data-redis` (Lettuce, `StreamMessageListenerContainer`) |
| RabbitMQ | `amqplib` (`sendToQueue`, `consume`, `ack`) | `spring-boot-starter-amqp` (`RabbitTemplate`, `@RabbitListener`) |

- ioredis는 주간 다운로드가 약 700만~1000만, amqplib는 약 150만~250만, kafkajs는 약 250만으로 각각 가장 많이 쓰인다. 수치는 추적 사이트와 시점마다 다르다.
- NestJS 내장 Kafka/RabbitMQ transport도 내부적으로 kafkajs와 amqplib를 쓴다. 이 프로젝트는 순수 클라이언트를 직접 써서 오버헤드 차이를 줄였다.
- NestJS에는 Redis Streams 공식 transport가 없다(내장 Redis transport는 pub/sub이고 영속성이 없다). 그래서 ioredis로 직접 구현했다.
- kafkajs는 2024년 중반 이후 커밋이 거의 없다. 대안은 `@confluentinc/kafka-javascript`다.

## 실행
```bash
# 1) 인프라
docker compose up -d

# 2) NestJS
cd nest && npm install && npm run start:dev

# 3) Spring Boot (JDK 21 필요)
cd spring && ./gradlew bootRun     # 또는 ./gradlew build -x test 후 java -jar build/libs/mq-bench-spring-0.0.1.jar
```

## 문서
- `GUIDE.md`: 전체 테스트 가이드 / `nest/README.md`, `spring/README.md`: 앱별 구조, 설정, API, 다른 프로젝트로 가져가는 법
- `SCENARIOS.md`: 시나리오 A, B 정의, 실행 절차, 결과 해석 기준
- `docs/mq/README.md`: MQ 개념 문서 목차와 노션 페이지 구성 제안. `01-what-is-mq.md`, `02-redis-streams.md`, `03-rabbitmq.md`, `04-kafka.md`(그림은 `docs/mq/img/`, `python3 docs/mq/diagrams.py`로 다시 생성)
- `practice/README.md`: MQ 실습 안내(CLI 스크립트, GUI, Node·Java 예제, 앱 연습용 API `/practice`). MQ별 `practice/redis.md`, `kafka.md`, `rabbitmq.md`, GUI 화면 사용법 `practice/gui.md`
- `docs/k6-guide.md`: k6로 B 시나리오 실행하는 방법(scenarios.sh, 직접 실행, 변수, 웹 대시보드와 HTML 리포트, 결과 읽는 법)
- `docs/locust-guide.md`: Locust로 B 시나리오 실행하는 방법(규모별 스크립트, 웹 화면, 캡처). 결과 리포트에는 Locust 결과를 넣지 않는다
- `docs/nest-vs-spring-concepts.md`: I/O vs CPU, Node vs Spring, 구조/생태계, MQ 쓰임새 개념 정리(일반 경향, 측정값 아님)

## 결과
- `reports/scale-result.md`: 규모별 결과와 분석(2026-10-23, k6, 앱당 114회, 4코어 머신). Spring MQ 비교, Nest MQ 비교, Spring vs Nest, 그래프 포함
  - A(대량 발행): Spring은 Kafka가 6개 규모 모두 1위(약 7,000~7,500 TPS), Nest는 Kafka ≈ Redis. Nest가 Spring보다 약 1.2배 빠름(Redis는 약 1.5배)
  - B(요청 1건 = 이벤트 1건, VU(동시 사용자) 100): 두 앱 모두 Redis가 처리량 1위. Spring이 Nest보다 약 1.15배 빠르고 지연이 훨씬 짧음. Spring Kafka는 300만 건에서도 p95 0.1초대
  - 처리량은 1만~10만 건부터 평탄하고, MQ 순위는 규모에 따라 거의 바뀌지 않음
- 이전 결과(2026-10-09, 시나리오 A1~B5)는 폐기했다.
- 재현은 `scale-result.md`의 "재현" 절을 본다.

## 시나리오 요약
- **A (대량 발행):** `GET /bench/{mq}?count&size&db` 1번 호출로 서버가 N건 발행. MQ와 컨슈머가 쌓인 메시지를 비우는 속도 측정
- **B (요청 1건 = 이벤트 1건):** k6 또는 Locust가 `POST /orders?runId=`를 총 N번 호출. VU(동시 사용자) 100명이 N번을 나눠 보낸다. 실무형 흐름(API → MQ → 컨슈머 → MariaDB)
- 시나리오: `A-1k`~`A-3m`, `B-1k`~`B-3m` (1천, 1만, 10만, 50만, 100만, 300만). 256B, DB 저장 O 고정. 반복은 규모에 따라 5~1회
- 공통 API: `POST /runs`, `GET /runs/{runId}`, `DELETE /runs`(결과 초기화)
- 결과 필드: `tps`, `totalMs`, `latencyMs{p50,p95,p99}`, `dbCount`
- 실행: `./scenarios.sh <nest|spring> [repeat] [ids]`, Locust는 `load/locust/run-scale.sh <app> [규모] [MQ]`
- 문서와 리포트에서 VU는 "VU(동시 사용자)"로 풀어 쓴다.

## 구현 메모
- 토픽, 큐, 스트림 이름은 `bench-topic`, `bench-queue`, `bench:stream`이다.
- Kafka: 파티션 10개, replica 1개. Nest 컨슈머 그룹은 `bench-nest-group`, Spring은 `bench-spring-group`이다.
- Redis Streams: 컨슈머 그룹을 만들고 `XREADGROUP` 후 `XACK`로 at-least-once를 보장한다. Nest는 블로킹 읽기용 커넥션을 따로 쓴다.
- RabbitMQ: durable 큐, persistent 메시지, prefetch 100, 처리 후 ack로 설정했다.
- **병렬도 10으로 통일:** DB 풀 10, Kafka 파티션 10, Rabbit과 Redis 컨슈머 10. 바꿀 때는 Nest와 Spring을 같이 바꾼다.
- 소비 후 `orders_result`에 `INSERT IGNORE`(멱등)하며, `db=true`일 때만 저장한다. 종료 판정은 처리(저장) 건수가 `expected`에 도달한 시점이다.
- **공정한 비교를 위해** 세 MQ의 페이로드 크기, 메시지 수, ack 방식, 영속성 옵션을 가능한 한 맞춘다. 옵션을 바꾸면 결과에 기록한다.
- 동시에 같은 브로커에 Nest와 Spring을 붙이면 컨슈머 그룹이 달라 각자 전체 메시지를 받는다. 비교 테스트는 한 번에 하나의 앱만 실행하는 편이 정확하다.
- 검증 상태: Nest, Spring 모두 규모별 시나리오 12개를 k6로 전부 실행했다(앱당 114회, 모두 `done`이고 `dbCount`가 건수와 일치). Locust 스크립트는 1천·1만 건으로 동작을 확인했다.
- 진행 중 `GET /runs/{id}`는 건수만 반환한다. 지연(p50/p95/p99)과 `dbCount`는 완료 시 한 번 계산해 캐시하고, 미완료여도 `?final=true`면 계산한다(1M건 정렬과 `COUNT(*)` 비용 때문).
- 대용량 대응 변경(2026-10-22): MariaDB 버퍼 풀 1G, `DELETE /runs`에서 Rabbit 큐 purge, 미등록 runId 메시지는 저장 안 함, Nest Rabbit backpressure, Nest Kafka sessionTimeout 60초. 상세는 `SCENARIOS.md`
- 구조 개편(2026-10-23): MQ별 모듈 분리, Spring Maven→Gradle과 yml→properties, Nest `@nestjs/config`. 배치 크기, ack, prefetch, 동시성, 이름은 그대로라 측정 조건은 같다. 이벤트를 만들 때 Kafka key(userId)를 같이 만들도록 바뀌어, Nest가 Kafka 발행마다 하던 `JSON.parse`가 없어졌다.
- Locust는 보낸 요청의 응답이 모두 돌아온 뒤 종료한다(전에는 즉시 종료해 진행 중이던 몇 건이 빠졌다). 결과는 `RESULT`의 `processed`를 기준으로 본다.
