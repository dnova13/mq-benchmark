# Spring Boot 가이드

## 실행
```bash
cd spring
mvn spring-boot:run     # http://localhost:8080, JDK 21 필요
# 또는 IDE에서 BenchApplication 실행
```
먼저 루트에서 `docker compose up -d`로 인프라를 띄워야 한다.
MariaDB(33307)에 연결하므로 DB가 꺼져 있으면 기동에 실패한다.

## 테스트(시나리오 A)
```bash
curl "localhost:8080/bench/kafka?count=10000&size=256&db=true"
curl "localhost:8080/bench/redis?count=10000&size=256&db=true"
curl "localhost:8080/bench/rabbit?count=10000&size=256&db=true"
# 한 번에: ../bench.sh spring 10000 256 true
# 시나리오 B(k6/Locust)와 전체 매트릭스는 ../SCENARIOS.md
```

## 파일 구성
| 파일 | 역할 |
|---|---|
| `pom.xml` | Boot 3.4.1, Java 21, kafka, amqp, data-redis, jdbc, mariadb |
| `application.yml` | DB, 브로커, 병렬도(Hikari 10, Kafka/Rabbit 동시성 10) 설정 |
| `BenchApplication.java` | 진입점 |
| `EventFactory.java` | 주문 이벤트 생성(Nest와 동일 규칙) |
| `RunRegistry.java` | 회차(run) 관리, 처리 건수와 지연(p50, p95, p99) 집계 |
| `DbService.java` | `orders_result` 생성과 `INSERT IGNORE` (JdbcTemplate) |
| `Processor.java` | 세 컨슈머 공통 처리 |
| `Publisher.java` | Kafka, Rabbit, Redis(파이프라인) 발행 |
| `MqConfig.java` | 토픽(파티션 10), 큐, Redis Streams 컨슈머 10명 |
| `Listeners.java` | `@KafkaListener`, `@RabbitListener` |
| `BenchController.java` | A, B 시나리오 엔드포인트 |

## 참고
- Kafka 컨슈머 그룹은 `bench-spring-group`이고 파티션은 10개다.
- Redis Streams는 `StreamMessageListenerContainer`(Lettuce)를 쓴다.
- Kafka 발행은 비동기 `send` 후 청크마다 `flush`한다. 더 높은 처리량을 보려면 Kafka `linger.ms`, `batch.size`를 조정한다.

## API
| 메서드와 경로 | 설명 |
|---|---|
| `GET /bench/{mq}?count&size&db&timeout` | 시나리오 A. 서버가 N건 발행하고 완료까지 대기 후 결과 반환 |
| `POST /runs?mq&expected&size&db` | 시나리오 B 시작. `runId` 발급 |
| `POST /orders?runId=...` | 시나리오 B. 주문 1건을 이벤트 1건으로 발행(202). 본문 `{userId, amount}`는 선택 |
| `GET /runs/{runId}` | 진행 상황과 결과 조회 |
| `DELETE /runs` | 결과 테이블 `orders_result`를 TRUNCATE하고 메모리 런을 삭제 |

`mq`는 `kafka`, `redis`, `rabbit` 중 하나다. 부하 테스트는 루트 `SCENARIOS.md`를 본다.
