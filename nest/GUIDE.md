# NestJS 가이드

## 실행
```bash
cd nest
npm install
npm run start:dev      # http://localhost:3000, 파일 변경 시 자동 재시작
```
먼저 루트에서 `docker compose up -d`로 인프라를 띄워야 한다.

## 테스트(시나리오 A)
```bash
curl "localhost:3000/bench/kafka?count=10000&size=256&db=true"
curl "localhost:3000/bench/redis?count=10000&size=256&db=true"
curl "localhost:3000/bench/rabbit?count=10000&size=256&db=true"
# 한 번에: ../bench.sh nest 10000 256 true
# 시나리오 B(k6/Locust)와 전체 매트릭스는 ../SCENARIOS.md
```

## 파일 구성 (`src/`)
| 파일 | 역할 |
|---|---|
| `main.ts`, `app.module.ts` | 부트스트랩(포트 3000)과 모듈 등록 |
| `event.ts` | 주문 이벤트 생성(Spring과 동일 규칙) |
| `run.service.ts` | 회차(run) 관리, 처리 건수와 지연(p50, p95, p99) 집계 |
| `db.service.ts` | mysql2 풀(10), `orders_result` 생성과 `INSERT IGNORE` |
| `processor.service.ts` | 세 컨슈머 공통 처리(`db=true`면 INSERT 후 기록) |
| `kafka.service.ts` | kafkajs, 토픽 `bench-topic`(파티션 10), `eachBatch` 병렬 처리 |
| `redis-stream.service.ts` | ioredis `XADD`(파이프라인), `XREADGROUP`, `XACK`, 워커 10개 |
| `rabbit.service.ts` | amqplib, 큐 `bench-queue`(durable, persistent, prefetch 100) |
| `bench.controller.ts` | A, B 시나리오 엔드포인트 |

## 접속 정보 변경
브로커 주소는 각 서비스 파일 상단에 하드코딩돼 있다. 포트를 바꾸면 compose와 함께 수정한다.

## 새 MQ 기능 추가
1. `src/xxx.service.ts`에 `publish(events: string[])`를 가진 서비스를 만들고, 수신 시 `ProcessorService.process(raw)`를 호출한다.
2. `app.module.ts`의 `providers`에 등록한다.
3. `bench.controller.ts`의 `publisher()` 매핑에 추가한다.

## API
| 메서드와 경로 | 설명 |
|---|---|
| `GET /bench/{mq}?count&size&db&timeout` | 시나리오 A. 서버가 N건 발행하고 완료까지 대기 후 결과 반환 |
| `POST /runs?mq&expected&size&db` | 시나리오 B 시작. `runId` 발급 |
| `POST /orders?runId=...` | 시나리오 B. 주문 1건을 이벤트 1건으로 발행(202). 본문 `{userId, amount}`는 선택 |
| `GET /runs/{runId}` | 진행 상황과 결과 조회 |
| `DELETE /runs` | 결과 테이블 `orders_result`를 TRUNCATE하고 메모리 런을 삭제 |

`mq`는 `kafka`, `redis`, `rabbit` 중 하나다. 부하 테스트는 루트 `SCENARIOS.md`를 본다.
