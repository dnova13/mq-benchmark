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
├─ load/                # 부하 스크립트: k6(load/k6/orders.js), Locust(load/locust/locustfile.py)
├─ scenarios.sh         # 시나리오 매트릭스 자동 실행
├─ bench.sh             # 시나리오 A 간편 호출
├─ results/             # 측정 결과(jsonl, git 제외)
├─ reports/             # 결과 리포트(spring-result.md), 원본(spring-raw.jsonl), report.py 출력
├─ report.py            # jsonl -> 마크다운 표
├─ compare.py           # Spring vs Nest 비교표
├─ nest/                # NestJS 10 (TypeScript), 포트 3000
└─ spring/              # Spring Boot 3.4 + Java 21 (Maven), 포트 8080
```

## 인프라 (docker-compose.yml)
원래 포트 앞에 `3`을 붙인 외부 포트를 쓴다.

| 서비스 | 이미지 | 외부 포트 | 계정 |
|---|---|---|---|
| MariaDB | mariadb:11 | **33307** | root / 1234 (DB: `bench`) |
| Redis | redis:7-alpine | **36397** | 없음 |
| Kafka | apache/kafka:3.9.0 (KRaft, ZooKeeper 없음) | **39092** | 없음 |
| RabbitMQ | rabbitmq:3-management | **35672** (AMQP), **31567** (관리 UI) | guest / guest |

- Redis 포트는 요청대로 `36397`이다. 일반적인 규칙(6379 앞에 3)을 따르면 `36379`이므로 오타일 수 있다. 바꾸려면 compose와 양쪽 설정을 같이 수정한다.
- RabbitMQ 관리 UI 15672 앞에 3을 붙이면 `315672`로 포트 범위(65535)를 넘는다. 그래서 `31567`을 쓴다.
- Kafka의 advertised listener는 `localhost:39092`다. 호스트에서 실행하는 앱만 접속할 수 있다.

```bash
docker compose up -d      # 전체 기동
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
cd spring && mvn spring-boot:run
```

## 문서
- `GUIDE.md`: 전체 테스트 가이드 / `nest/GUIDE.md`, `spring/GUIDE.md`: 앱별 가이드와 API
- `SCENARIOS.md`: 시나리오 A, B 정의, 실행 절차, 결과 해석 기준
- `docs/nest-vs-spring-concepts.md`: I/O vs CPU, Node vs Spring, 구조/생태계, MQ 쓰임새 개념 정리(일반 경향, 측정값 아님)

## 결과
- `reports/spring-result.md`: Spring 기준 전체 결과와 결론(2026-10-09, 150회 측정). 요약: Kafka 8/10 시나리오 1위, DB 저장이 붙으면 MQ 간 격차가 1.4~1.7배로 축소
- `reports/nest-vs-spring.md`: Nest 전체 결과(150회)와 Spring 비교. 요약: 전체 비율 Nest/Spring=1.09, 30케이스 중 21개는 범위가 겹쳐 우열 불가. Nest에서는 Redis 7/10 1위(Spring은 Kafka 8/10 1위)로 MQ 순위가 앱에 따라 뒤집힘. 원인은 컨슈머 구현 방식(배치 병렬 vs 건별) 차이 가능성이 큼
- 재현: `./scenarios.sh <app> 5` 후 `python3 report.py`, `python3 compare.py reports/spring-raw.jsonl reports/nest-raw.jsonl`

## 시나리오 요약
- **A (대량 발행):** `GET /bench/{mq}?count&size&db` 1번 호출로 서버가 N건 발행. MQ 자체 성능 측정
- **B (요청 1건 = 이벤트 1건):** k6 또는 Locust가 `POST /orders?runId=`를 N번 호출. 실무형 흐름(API → MQ → 컨슈머 → MariaDB)
- 공통 API: `POST /runs`, `GET /runs/{runId}`, `DELETE /runs`(결과 초기화)
- 결과 필드: `tps`, `totalMs`, `latencyMs{p50,p95,p99}`, `dbCount`
- 전체 실행: `./scenarios.sh <nest|spring> [repeat] [ids]`

## 구현 메모
- 토픽, 큐, 스트림 이름은 `bench-topic`, `bench-queue`, `bench:stream`이다.
- Kafka: 파티션 10개, replica 1개. Nest 컨슈머 그룹은 `bench-nest-group`, Spring은 `bench-spring-group`이다.
- Redis Streams: 컨슈머 그룹을 만들고 `XREADGROUP` 후 `XACK`로 at-least-once를 보장한다. Nest는 블로킹 읽기용 커넥션을 따로 쓴다.
- RabbitMQ: durable 큐, persistent 메시지, prefetch 100, 처리 후 ack로 설정했다.
- **병렬도 10으로 통일:** DB 풀 10, Kafka 파티션 10, Rabbit과 Redis 컨슈머 10. 바꿀 때는 Nest와 Spring을 같이 바꾼다.
- 소비 후 `orders_result`에 `INSERT IGNORE`(멱등)하며, `db=true`일 때만 저장한다. 종료 판정은 처리(저장) 건수가 `expected`에 도달한 시점이다.
- **공정한 비교를 위해** 세 MQ의 페이로드 크기, 메시지 수, ack 방식, 영속성 옵션을 가능한 한 맞춘다. 옵션을 바꾸면 결과에 기록한다.
- 동시에 같은 브로커에 Nest와 Spring을 붙이면 컨슈머 그룹이 달라 각자 전체 메시지를 받는다. 비교 테스트는 한 번에 하나의 앱만 실행하는 편이 정확하다.
- 검증 상태: Nest, Spring 모두 빌드와 시나리오 A, B(k6, Locust)를 1,000건 규모로 실행해 `dbCount`와 처리 건수 일치를 확인했다. Nest, Spring 모두 전체 매트릭스(각 150회)를 실행했다.
- Locust는 종료 순간 진행 중이던 요청이 통계에서 빠질 수 있다. 결과는 `RESULT`의 `processed`를 기준으로 본다.
