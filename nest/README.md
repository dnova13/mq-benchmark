# MQ Bench - NestJS

Kafka, RabbitMQ, Redis Streams 연동을 MQ별 Nest 모듈로 나눈 앱이다.
같은 앱이 부하 테스트 서버 역할도 한다. MQ 모듈은 벤치 코드에 의존하지 않으므로, 새 프로젝트에 폴더째 복사해 쓸 수 있다.

| 항목 | 내용 |
|---|---|
| 스택 | NestJS 10, TypeScript 5, Node 20 이상, `@nestjs/config` |
| Kafka | `kafkajs` (`Producer`, `Consumer.eachBatch`) |
| RabbitMQ | `amqplib` (`sendToQueue`, `consume`, `ack`) |
| Redis Streams | `ioredis` (`XADD`, `XREADGROUP`, `XACK`, 컨슈머 그룹) |
| DB | MariaDB, `mysql2` 풀 10 |
| 포트 | 3000 |

## 실행
```bash
# 루트에서 인프라 먼저 (MariaDB 33307, Redis 36397, Kafka 39092, RabbitMQ 35672)
docker compose up -d

cd nest
npm install
cp .env.example .env      # 선택. 없으면 src/config/*.config.ts의 기본값을 쓴다
npm run start:dev         # 개발 실행(파일 변경 시 재시작)
npm run build && node dist/main.js   # 빌드 후 실행(벤치 측정은 이쪽)
```

## 폴더 구조
```
src/
├─ main.ts                          # 부트스트랩, 종료 훅
├─ app.module.ts                    # ConfigModule.forRoot(.env) + BenchModule
├─ config/                          # registerAs 설정. 환경 변수 → 기본값 순
│  ├─ database.config.ts
│  ├─ kafka.config.ts
│  ├─ rabbitmq.config.ts
│  └─ redis.config.ts
├─ common/messaging/
│  └─ message.interface.ts          # Message, MessageHandler, MessagePublisher, MESSAGE_HANDLER 토큰
├─ mq/                              # ← 다른 프로젝트로 가져가는 부분
│  ├─ kafka/
│  │  ├─ kafka.module.ts            #   KafkaModule.register({ imports, handler })
│  │  ├─ kafka.constants.ts         #   KAFKA_CLIENT 토큰
│  │  ├─ kafka.publisher.ts         #   MessagePublisher 구현
│  │  └─ kafka.consumer.ts          #   eachBatch → handler.handle()
│  ├─ rabbitmq/                     # RabbitMqModule (연결 1개, 채널 1개 공유)
│  └─ redis-stream/                 # RedisStreamModule (발행/ack용 커넥션 + 워커별 읽기 커넥션)
├─ database/                        # DatabaseModule, OrderResultRepository(orders_result)
└─ bench/                           # 부하 테스트 전용 코드
   ├─ bench.module.ts               # MQ 모듈 조건부 로드(ConditionalModule)
   ├─ bench.controller.ts           # /bench, /runs, /orders
   └─ order/                        # OrderModule: RunService(집계), OrderProcessorService(MessageHandler 구현), order-event.ts
```
- MQ 모듈은 `common/messaging`과 `config/<mq>.config.ts`에만 의존한다.
- 각 MQ 모듈은 `register({ imports, handler })` 형태의 동적 모듈이다. 컨슈머는 `MESSAGE_HANDLER` 토큰으로 `handler`를 받는다. 그래서 의존 방향이 `bench → mq → order`로 한쪽이고, 순환 의존이 없다.

## 설정 (`.env`)
값이 없으면 `src/config/*.config.ts`의 기본값을 쓴다. 전체 목록은 `.env.example`에 있다.

| 변수 | 기본값 | 의미 |
|---|---|---|
| `PORT` | `3000` | HTTP 포트 |
| `MQ_KAFKA_ENABLED` / `MQ_RABBITMQ_ENABLED` / `MQ_REDIS_ENABLED` | 켬 | `false`면 해당 모듈을 로드하지 않는다 |
| `DB_HOST` / `DB_PORT` / `DB_POOL_SIZE` | `localhost` / `33307` / `10` | MariaDB |
| `KAFKA_BROKERS` / `KAFKA_TOPIC` / `KAFKA_PARTITIONS` | `localhost:39092` / `bench-topic` / `10` | 브로커와 토픽(기동 시 생성) |
| `KAFKA_GROUP_ID` | `bench-nest-group` | 컨슈머 그룹 |
| `KAFKA_CONSUMER_CONCURRENCY` | `10` | `partitionsConsumedConcurrently` |
| `KAFKA_SESSION_TIMEOUT_MS` | `60000` | 큰 배치 처리 중 세션 유지 |
| `RABBITMQ_URL` / `RABBITMQ_QUEUE` | `amqp://guest:guest@localhost:35672` / `bench-queue` | 브로커와 큐 |
| `RABBITMQ_DURABLE` / `RABBITMQ_PERSISTENT` / `RABBITMQ_PREFETCH` | `true` / `true` / `100` | 큐 내구성, 메시지 영속성, prefetch |
| `REDIS_HOST` / `REDIS_PORT` | `localhost` / `36397` | Redis |
| `REDIS_STREAM_KEY` / `REDIS_GROUP` | `bench:stream` / `bench-nest-group` | 스트림과 컨슈머 그룹 |
| `REDIS_WORKERS` / `REDIS_BATCH_SIZE` / `REDIS_BLOCK_MS` | `10` / `100` / `1000` | 워커 수, `XREADGROUP COUNT`, `BLOCK` |

```bash
MQ_KAFKA_ENABLED=false node dist/main.js   # Kafka 없이 기동
```
꺼진 MQ로 벤치 API를 호출하면 400을 돌려준다.

## 다른 프로젝트로 가져가기
예: RabbitMQ만 쓰는 새 프로젝트

1. **복사:** `src/common/messaging/`, `src/mq/rabbitmq/`, `src/config/rabbitmq.config.ts`를 같은 경로로 복사한다.
2. **의존성:** `npm i @nestjs/config amqplib && npm i -D @types/amqplib`
   - Kafka는 `kafkajs`, Redis Streams는 `ioredis`가 필요하다.
3. **설정:** `AppModule`에 `ConfigModule.forRoot({ isGlobal: true })`를 넣고, `.env`에 `RABBITMQ_*` 값을 적는다.
4. **소비:** `MessageHandler`를 구현한 provider를 만들고, 그 provider를 export하는 모듈을 `imports`로 넘긴다.
   ```ts
   @Injectable()
   export class OrderEventHandler implements MessageHandler {
     async handle(payload: string) {
       const event = JSON.parse(payload);
       // 비즈니스 처리. 예외를 던지면 ack하지 않는다
     }
   }

   @Module({ providers: [OrderEventHandler], exports: [OrderEventHandler] })
   export class OrderEventsModule {}

   @Module({
     imports: [
       ConfigModule.forRoot({ isGlobal: true }),
       RabbitMqModule.register({ imports: [OrderEventsModule], handler: OrderEventHandler }),
     ],
   })
   export class AppModule {}
   ```
5. **발행:** `RabbitMqModule`을 import한 모듈에서 `RabbitMqPublisher`를 주입받아 보낸다.
   ```ts
   @Injectable()
   export class OrderService {
     constructor(private publisher: RabbitMqPublisher) {}

     async place(order: Order) {
       await this.publisher.publish([{ key: String(order.userId), payload: JSON.stringify(order) }]);
     }
   }
   ```
   - key는 Kafka에서만 쓰인다(같은 key는 같은 파티션, 순서 보장). RabbitMQ와 Redis Streams는 무시한다.
   - 켜고 끄기가 필요하면 `ConditionalModule.registerWhen(RabbitMqModule.register(...), (env) => env.MQ_RABBITMQ_ENABLED !== 'false')`로 감싸고, 주입하는 쪽은 `@Optional()`을 붙인다(`bench/bench.module.ts` 참고).

## 연습용 API (`/practice`)
MQ 기본 동작을 curl로 해 보는 API다. 벤치마크와 별개인 `practice-topic`, `practice:stream`, `practice-queue`만 다룬다. 구현 위치는 `src/practice`이다.

| 메서드와 경로 | 설명 |
|---|---|
| `POST /practice/{mq}/messages` | 발행. 본문 `{"key":"user-1","payload":"hello"}` |
| `GET /practice/{mq}/messages?count&ack` | 최근 메시지 조회. Kafka·Redis는 남고, RabbitMQ는 `ack=true`면 사라진다(`ack=false`는 엿보기) |
| `GET /practice/{mq}/info` | 상태(길이, 오프셋, 메시지·컨슈머 수) |
| `DELETE /practice/{mq}` | 실습 대상 삭제 |

```bash
curl -X POST localhost:3000/practice/kafka/messages -H 'Content-Type: application/json' -d '{"key":"user-1","payload":"hello"}'
curl "localhost:3000/practice/kafka/messages?count=5"
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
curl "localhost:3000/bench/kafka?count=10000&size=256&db=true"
../bench.sh nest 10000 256 true        # 세 MQ 차례로
../scenarios.sh nest                   # 규모별 12개 시나리오 (../SCENARIOS.md)
```

## 컨슈머 처리 방식 바꾸기 (묶음 → 건별)
지금은 Kafka와 Redis Streams는 **여러 건을 묶어 동시에** 처리하고, RabbitMQ는 prefetch(100)만큼 **동시에** 처리한다. Node에서 흔히 쓰는 방식이다. Spring처럼 한 건씩 처리하려면 아래처럼 바꾼다.
- 벤치마크 결과는 지금 방식(묶음·동시)으로 쟀다.
- 왜 다른지는 `../docs/nest-vs-spring-concepts.md`의 "컨슈머 처리 방식이 다른 이유"를 본다.

### Kafka: `eachBatch` → `eachMessage`
```ts
// src/mq/kafka/kafka.consumer.ts
await this.consumer.run({
  partitionsConsumedConcurrently: this.cfg.partitionsConsumedConcurrently, // 파티션끼리는 동시에
  eachMessage: async ({ message }) => {
    await this.handler.handle(message.value.toString()); // 파티션 안에서는 한 건씩. 끝나면 오프셋 확정
  },
});
```

### Redis Streams: `Promise.all` → 한 건씩 처리하고 건마다 ack
```ts
// src/mq/redis-stream/redis-stream.consumer.ts (loop 안)
for (const [id, fields] of entries as [string, string[]][]) {
  await this.handler.handle(fields[1]);
  await this.redis.xack(streamKey, group, id); // 건마다 XACK
}
```
한 번에 읽는 건수도 1로 하려면 `.env`에 `REDIS_BATCH_SIZE=1`을 준다.

### RabbitMQ: 채널마다 prefetch 1
amqplib는 prefetch 수만큼 메시지를 미리 받아 콜백을 **동시에** 부른다. 그래서 콜백 안에서 `await`해도 여러 건이 함께 처리된다. 건별로 하려면 채널마다 prefetch를 1로 주고, 채널을 동시성 수(10)만큼 연다. 그러면 Spring의 "스레드 10개가 한 건씩"과 같은 모양이 된다.
```ts
// src/mq/rabbitmq/rabbitmq.connection.ts: 소비용 채널을 따로 열 수 있게 연결을 꺼내 준다
private constructor(readonly connection: amqp.ChannelModel, readonly channel: amqp.Channel) {}
```
```ts
// src/mq/rabbitmq/rabbitmq.consumer.ts
async onModuleInit() {
  for (let i = 0; i < 10; i++) {
    const ch = await this.rabbit.connection.createChannel();
    await ch.prefetch(1); // 이 채널은 ack 전까지 1건만 받는다
    await ch.consume(this.cfg.queue, async (msg) => {
      if (!msg) return;
      await this.handler.handle(msg.content.toString());
      ch.ack(msg);
    });
  }
}
```
- 지금 구현은 발행과 소비가 채널 하나를 함께 쓴다(`RabbitMqConnection.channel`). 위처럼 소비용 채널을 따로 열면, 종료 시 이 채널들도 닫히도록 연결 종료(`onApplicationShutdown`)에 맡기면 된다.

위 코드 조각은 이 프로젝트에서 타입 검사(`tsc --noEmit`)로 확인했다.

## 참고
- Kafka 컨슈머는 배치를 1,000건씩 나눠 `Promise.all`로 병렬 처리하고, 그때마다 offset을 확정하고 heartbeat를 보낸다.
- RabbitMQ 발행은 쓰기 버퍼가 차면 `drain`을 기다린다. 컨슈머는 처리 후 수동 `ack`한다.
- Redis Streams 컨슈머는 `XREADGROUP COUNT 100 BLOCK 1000`으로 읽어 병렬 처리한 뒤 한 번에 `XACK`한다. 처리 중 예외가 난 메시지는 PEL에 남는다(재처리는 구현하지 않았다).
- NestJS 내장 Kafka/RabbitMQ transport도 내부적으로 kafkajs와 amqplib를 쓴다. 이 프로젝트는 오버헤드를 줄이려고 클라이언트를 직접 쓴다. Redis Streams는 내장 transport가 없다.
- 전체 테스트 절차는 루트 `GUIDE.md`, 시나리오 정의는 `SCENARIOS.md`를 본다.
