# MQ Bench - Spring Boot

Kafka, RabbitMQ, Redis Streams 연동을 MQ별 패키지로 나눈 Spring Boot 앱이다.
같은 앱이 부하 테스트 서버 역할도 한다. MQ 패키지는 벤치 코드에 의존하지 않으므로, 새 프로젝트에 그대로 복사해 쓸 수 있다.

| 항목 | 내용 |
|---|---|
| 스택 | Spring Boot 3.4.1, Java 21, Gradle 8.14 (Groovy DSL, Wrapper 포함) |
| Kafka | `spring-kafka` (`KafkaTemplate`, `@KafkaListener`) |
| RabbitMQ | `spring-boot-starter-amqp` (`RabbitTemplate`, `@RabbitListener`) |
| Redis Streams | `spring-boot-starter-data-redis` (Lettuce, `StreamMessageListenerContainer`) |
| DB | MariaDB, `spring-boot-starter-jdbc` (`JdbcTemplate`, Hikari 풀 10) |
| 포트 | 8080 |

## 실행
```bash
# 루트에서 인프라 먼저 (MariaDB 33307, Redis 36397, Kafka 39092, RabbitMQ 35672)
docker compose up -d

cd spring
./gradlew bootRun                                  # 개발 실행
./gradlew build -x test                            # 빌드 → build/libs/mq-bench-spring-0.0.1.jar
java -jar build/libs/mq-bench-spring-0.0.1.jar     # jar 실행
```
- JDK 21이 필요하다. IDE에서는 `BenchApplication`을 실행한다.
- MariaDB에 연결하므로 DB가 꺼져 있으면 기동에 실패한다.

## 폴더 구조
```
spring/
├─ build.gradle, settings.gradle, gradlew, gradle/wrapper/
└─ src/main/
   ├─ java/com/bench/
   │  ├─ BenchApplication.java
   │  ├─ mq/                              # ← 다른 프로젝트로 가져가는 부분
   │  │  ├─ common/                       # MQ 공통 인터페이스
   │  │  │  ├─ Message.java               #   발행 메시지(key, payload)
   │  │  │  ├─ MessageHandler.java        #   소비 콜백. 앱이 구현한다
   │  │  │  └─ MessagePublisher.java      #   발행 인터페이스(type, publish, purge)
   │  │  ├─ kafka/                        # Kafka
   │  │  │  ├─ KafkaMqProperties.java     #   mq.kafka.* 바인딩
   │  │  │  ├─ KafkaMqConfig.java         #   토픽 생성 + 발행/컨슈머 빈 등록(on/off 조건)
   │  │  │  ├─ KafkaMessagePublisher.java
   │  │  │  └─ KafkaMessageConsumer.java  #   @KafkaListener
   │  │  ├─ rabbitmq/                     # RabbitMQ (같은 구성)
   │  │  └─ redis/                        # Redis Streams (그룹 생성 + 리스너 컨테이너)
   │  └─ benchmark/                       # 부하 테스트 전용 코드
   │     ├─ controller/BenchController.java   # /bench, /runs, /orders
   │     ├─ service/                          # RunService(집계), OrderProcessor(MessageHandler 구현), OrderEventFactory
   │     └─ repository/OrderResultRepository.java  # orders_result INSERT IGNORE
   └─ resources/
      ├─ application.properties           # 공통(서버, DB) + MQ 설정 파일 import
      ├─ application-kafka.properties
      ├─ application-rabbitmq.properties
      └─ application-redis.properties
```
- `mq.*` 패키지는 `com.bench.mq.common`에만 의존한다. 벤치 코드(`benchmark`)는 `MessageHandler`를 구현하고 `MessagePublisher`를 주입받아 쓴다.
- 발행 구현과 컨슈머는 각 MQ의 `*Config`에서 `@Bean`으로 등록한다. 그래서 `@ConditionalOnProperty` 조건 하나로 MQ 전체를 끄고 켤 수 있다.

## 설정
`application.properties`가 `spring.config.import`로 MQ별 파일 3개를 불러온다.

### Kafka (`application-kafka.properties`)
| 키 | 기본값 | 의미 |
|---|---|---|
| `mq.kafka.enabled` | `true` | `false`면 토픽, 발행, 컨슈머를 등록하지 않는다 |
| `mq.kafka.topic` | `bench-topic` | 토픽 이름 |
| `mq.kafka.partitions` / `replicas` | `10` / `1` | 기동 시 생성할 토픽 설정 |
| `spring.kafka.bootstrap-servers` | `localhost:39092` | 브로커 |
| `spring.kafka.consumer.group-id` | `bench-spring-group` | 컨슈머 그룹 |
| `spring.kafka.listener.concurrency` | `10` | 컨슈머 스레드 수(파티션 수와 같게) |

### RabbitMQ (`application-rabbitmq.properties`)
| 키 | 기본값 | 의미 |
|---|---|---|
| `mq.rabbitmq.enabled` | `true` | `false`면 큐 선언, 발행, 컨슈머를 등록하지 않는다 |
| `mq.rabbitmq.queue` / `durable` | `bench-queue` / `true` | 큐 이름과 내구성 |
| `spring.rabbitmq.host` / `port` | `localhost` / `35672` | 브로커 |
| `spring.rabbitmq.listener.simple.prefetch` | `100` | 컨슈머당 미확인 메시지 수 |
| `spring.rabbitmq.listener.simple.concurrency` | `10` | 컨슈머 수 |

### Redis Streams (`application-redis.properties`)
| 키 | 기본값 | 의미 |
|---|---|---|
| `mq.redis.enabled` | `true` | `false`면 그룹, 리스너, 발행을 등록하지 않는다 |
| `mq.redis.stream-key` / `group` | `bench:stream` / `bench-spring-group` | 스트림과 컨슈머 그룹 |
| `mq.redis.workers` | `10` | 같은 그룹의 컨슈머 수 |
| `mq.redis.batch-size` / `poll-timeout` | `100` / `1s` | `XREADGROUP COUNT`와 블로킹 대기 |
| `spring.data.redis.host` / `port` | `localhost` / `36397` | Redis |

### MQ 끄기
```bash
java -jar build/libs/mq-bench-spring-0.0.1.jar --mq.kafka.enabled=false
# 또는 환경 변수: MQ_KAFKA_ENABLED=false
```
꺼진 MQ로 벤치 API를 호출하면 400을 돌려준다.

## 다른 프로젝트로 가져가기
예: Kafka만 쓰는 새 프로젝트

1. **복사:** `mq/common/`, `mq/kafka/` 폴더와 `application-kafka.properties`를 복사하고, 패키지 이름을 새 프로젝트에 맞게 바꾼다.
2. **의존성:** `build.gradle`에 `implementation 'org.springframework.kafka:spring-kafka'`를 추가한다.
3. **설정 import:** `application.properties`에 `spring.config.import=classpath:application-kafka.properties`를 넣고, 토픽과 그룹 이름을 바꾼다.
4. **소비:** `MessageHandler`를 구현한 빈을 하나 등록한다. 컨슈머가 이 빈을 주입받는다. 이 빈이 없으면 기동에 실패한다.
   ```java
   @Component
   public class OrderEventHandler implements MessageHandler {
       @Override
       public void handle(String payload) {
           // 역직렬화 후 비즈니스 처리. 예외를 던지면 ack하지 않는다
       }
   }
   ```
5. **발행:** `MessagePublisher`(또는 `KafkaMessagePublisher`)를 주입받아 보낸다. key가 같으면 같은 파티션으로 가서 순서가 보장된다.
   ```java
   @Service
   public class OrderService {
       private final MessagePublisher publisher;
       public OrderService(MessagePublisher publisher) { this.publisher = publisher; }

       public void place(Order order) {
           publisher.publish(List.of(new Message(order.userId().toString(), toJson(order))));
       }
   }
   ```
   - MQ를 여러 개 쓰면 `MessagePublisher` 빈도 여러 개가 된다. 이때는 구현 타입으로 주입받거나, `List<MessagePublisher>`를 받아 `type()`으로 고른다.
   - RabbitMQ와 Redis Streams는 key를 무시하므로 `Message.of(payload)`로 보내도 된다.

## 연습용 API (`/practice`)
MQ 기본 동작을 curl로 해 보는 API다. 벤치마크와 별개인 `practice-topic`, `practice:stream`, `practice-queue`만 다룬다. 구현 위치는 `com.bench.practice`이다.

| 메서드와 경로 | 설명 |
|---|---|
| `POST /practice/{mq}/messages` | 발행. 본문 `{"key":"user-1","payload":"hello"}` |
| `GET /practice/{mq}/messages?count&ack` | 최근 메시지 조회. Kafka·Redis는 남고, RabbitMQ는 `ack=true`면 사라진다(`ack=false`는 엿보기) |
| `GET /practice/{mq}/info` | 상태(길이, 오프셋, 메시지·컨슈머 수) |
| `DELETE /practice/{mq}` | 실습 대상 삭제 |

```bash
curl -X POST localhost:8080/practice/kafka/messages -H 'Content-Type: application/json' -d '{"key":"user-1","payload":"hello"}'
curl "localhost:8080/practice/kafka/messages?count=5"
```
실습 전체 가이드(CLI, GUI, 코드 예제)는 `../practice/README.md`를 본다.

## 벤치 API
| 메서드와 경로 | 설명 |
|---|---|
| `GET /bench/{mq}?count&size&db&timeout` | 시나리오 A. 서버가 N건 발행하고 완료까지 대기 후 결과 반환 |
| `POST /runs?mq&expected&size&db` | 시나리오 B 시작. `runId` 발급 |
| `POST /orders?runId=...` | 시나리오 B. 주문 1건을 이벤트 1건으로 발행(202). 본문 `{userId, amount}`는 선택 |
| `GET /runs/{runId}?final` | 진행 상황. 지연과 `dbCount`는 완료 시(또는 `final=true`) 계산 |
| `DELETE /runs` | `orders_result` TRUNCATE, Rabbit 큐와 Redis 스트림 비우기, 메모리 런 삭제 |

`mq`는 `kafka`, `redis`, `rabbit` 중 하나다.

```bash
curl "localhost:8080/bench/kafka?count=10000&size=256&db=true"
../bench.sh spring 10000 256 true      # 세 MQ 차례로
../scenarios.sh spring                 # 규모별 12개 시나리오 (../SCENARIOS.md)
```

## 참고
- Kafka 발행은 비동기 `send` 후 묶음마다 `flush`한다. 처리량을 더 높이려면 `spring.kafka.producer.properties.linger.ms`, `batch.size`를 조정한다.
- RabbitMQ 리스너는 정상 처리 후 자동 ack(`AUTO`)한다. 예외가 나면 nack 후 재큐잉된다(Spring AMQP 기본값).
- Redis Streams 리스너는 처리 후 `XACK`한다. 처리 중 예외가 난 메시지는 PEL에 남는다(재처리는 구현하지 않았다).
- 전체 테스트 절차는 루트 `GUIDE.md`, 시나리오 정의는 `SCENARIOS.md`를 본다.
