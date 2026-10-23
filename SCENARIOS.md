# 테스트 시나리오

두 가지 방식(A, B)을 모두 같은 MQ 3종 × 같은 앱 2종(Nest, Spring)에서 측정한다.

| | 방식 A: 대량 발행 | 방식 B: 요청 1건 = 이벤트 1건 |
|---|---|---|
| 흐름 | API 1번 호출 → 서버가 N건 연속 발행 → 소비 → (DB 저장) | 부하 도구가 `POST /orders`를 N번 호출 → 1건씩 발행 → 소비 → (DB 저장) |
| 측정 대상 | MQ 자체의 한계 성능 | 실무와 비슷한 "API → MQ → 컨슈머 → DB" 구조 |
| 실무 대응 | 로그/클릭 수집, 배치 적재, CDC, 재처리 | 주문, 결제 등 일반 서비스 |
| 도구 | `curl` (`GET /bench/{mq}`) | **k6**, **Locust** |

## 공통 규칙
- 이벤트: 주문 JSON(`id`, `runId`, `userId`, `amount`, `createdAt`, `db`, `pad`). `pad`로 전체 크기를 `size` 바이트에 맞춘다.
- 데이터는 결정적이다. `userId = (i*7919)%1000+1`, `amount = (i*37)%99900+100`이라 Nest와 Spring이 같은 값을 만든다.
- 병렬도 10: DB 커넥션 풀 10(Hikari, mysql2), Kafka 파티션 10, Rabbit 컨슈머 10, Redis 컨슈머 10명(같은 그룹)
- `db=true`면 컨슈머가 `orders_result`에 `INSERT IGNORE`(멱등)한다. 종료 시점은 DB 저장이 끝난 때다.
- 지표: `tps`(처리 건수 ÷ 첫 발행부터 마지막 처리까지의 시간), 지연 p50, p95, p99(이벤트 생성 시각부터 처리 완료 시각까지)
- 메시지 키/순서: Kafka는 `userId`를 키로 쓴다.
- 영속성: Rabbit은 durable 큐와 persistent 메시지, Redis는 AOF, Kafka는 기본 설정이다.

## 시나리오 목록
| ID | 방식 | 건수 | 크기 | DB | 동시 접속(VU) | 목적 |
|---|---|---|---|---|---|---|
| A1 | A | 1,000 | 256B | X | - | 순수 MQ 지연 (워밍업 영향이 큼) |
| A2 | A | 10,000 | 256B | X | - | 순수 MQ 처리량 |
| A3 | A | 1,000 | 256B | O | - | DB 포함 지연 |
| A4 | A | 10,000 | 256B | O | - | DB 포함 처리량 (A 방식 핵심) |
| A5 | A | 10,000 | 1KB | O | - | 페이로드 크기 영향 |
| B1 | B | 1,000 | 256B | O | 50 | 실무형 지연 |
| B2 | B | 10,000 | 256B | O | 100 | 실무형 처리량 (B 방식 핵심) |
| B3 | B | 10,000 | 256B | X | 100 | B에서 DB 영향 제거 |
| B4 | B | 10,000 | 256B | O | 10 | 낮은 동시성 |
| B5 | B | 10,000 | 256B | O | 200 | 높은 동시성 |

### 대용량 시나리오 (`large`, 기본 실행 `all`에는 포함하지 않음)
| ID | 방식 | 건수 | 크기 | DB | 동시 접속(VU) | 목적 |
|---|---|---|---|---|---|---|
| A6 | A | 100,000 | 256B | O | - | 10만 건 DB 포함 처리량 |
| A7 | A | 1,000,000 | 256B | O | - | 100만 건 DB 포함 처리량 |
| A8 | A | 1,000,000 | 256B | X | - | 100만 건 순수 MQ 처리량 |
| B6 | B | 100,000 | 256B | O | 100 | 10만 건 실무형 |
| B7 | B | 1,000,000 | 256B | O | 100 | 100만 건 실무형 |

```bash
./scenarios.sh spring 3 large       # 대용량만 3회 반복
./scenarios.sh nest 1 A7            # 하나만
```
- 워밍업은 같은 MQ로 10,000건 1회(기록 안 함)로 대신한다.
- 대기 시간은 건수에 비례한다(500건/초 기준, 최소 300초). 1M이면 서버 대기 2,000초, k6 최대 실행 4,000초다.
- 대용량은 진행 폴링 간격을 1초로 늘린다. 진행 중 `GET /runs/{id}`는 건수만 반환하고, 지연과 `dbCount`는 완료 시(또는 `?final=true`) 한 번 계산한다.
- 소요 시간: DB 포함 1M 한 회가 약 2~7분이다(MQ, 앱, 머신에 따라 다름). `large` 3회 반복은 앱 하나당 1~3시간 걸릴 수 있다.
- 디스크: Kafka 로그와 Redis AOF가 1M 시나리오마다 수백 MB씩 늘어난다. 대용량 실행 전후로 `docker compose down -v`로 정리한다.

## 실행 절차
### 1. 자동 실행 (권장)
```bash
docker compose up -d
cd nest && npm run start:dev        # 한 번에 앱 하나만 실행
./scenarios.sh nest                 # 전체: 워밍업 1회(기록 안 함) + 5회 반복, 중앙값 요약 출력
./scenarios.sh nest 3 A4,B2         # 일부 시나리오만, 3회 반복
# 앱을 바꿀 때는 기존 앱을 종료하고 spring을 실행한 뒤 ./scenarios.sh spring
```
- 결과는 `results/<app>-<시각>.jsonl`에 쌓인다. 한 줄이 1회 측정이다.
- 매 회차 전에 `DELETE /runs`로 `orders_result`를 비운다.
- B 방식은 k6를 쓴다.

### 2. 수동 실행
```bash
# 방식 A
curl "localhost:3000/bench/kafka?count=10000&size=256&db=true"

# 방식 B - k6 (BASE를 localhost:8080으로 바꾸면 Spring)
k6 run -e BASE=http://localhost:3000 -e MQ=kafka -e COUNT=10000 -e VUS=100 -e DB=true load/k6/orders.js

# 방식 B - Locust
MQ=kafka COUNT=10000 DB=true locust -f load/locust/locustfile.py \
    --headless -u 100 -r 100 -t 10m --host http://localhost:3000
```
- 두 도구 모두 끝나면 `RESULT {...}` 한 줄을 출력한다. 처리 건수, TPS, 지연이 들어 있다.
- k6와 Locust 자체 통계(HTTP 응답 시간)는 "발행 요청의 응답 시간"이고, MQ와 DB 처리 지연은 `RESULT`의 `latencyMs`다.
- Locust 화면 통계가 `COUNT`보다 몇 건 적을 수 있다. 종료 순간 진행 중이던 요청이 집계에서 빠진 것이니 `RESULT`의 `processed`를 기준으로 본다.
- Locust 종료 시 `didn't have proper args` 로그는 무시해도 된다.

## 결과 해석 기준
- 시나리오마다 MQ별 **중앙값**을 비교한다. 1,000건은 노이즈가 크니 p95 편차도 같이 본다.
- A와 B의 TPS를 직접 비교하지 않는다. B는 HTTP 요청 처리와 부하 도구 성능이 섞인다.
- DB 포함(O)과 제외(X)의 차이가 크면 DB가 병목이라는 뜻이고, 그때는 MQ 간 차이가 줄어든다.
- 같은 부하에서 Nest와 Spring 차이는 MQ 클라이언트와 런타임 차이이므로, MQ 비교와 프레임워크 비교를 분리해서 해석한다.

## 결과 기록 양식
| 시나리오 | 앱 | MQ | TPS(중앙값) | p50 | p95 | p99 | 비고 |
|---|---|---|---|---|---|---|---|
| A4 | nest | kafka | | | | | |
| A4 | nest | redis | | | | | |
| A4 | nest | rabbit | | | | | |

## 알려진 한계
- 단일 머신에서 브로커, 앱, 부하 도구가 같이 돌아 자원을 나눠 쓴다. 절대 수치보다 상대 비교에 쓴다.
- 메시지 생성 시각(`createdAt`)을 청크 단위(1,000건)로 찍기 때문에, A 방식의 지연에는 청크 내 대기가 약간 포함된다.
- 앱 하나가 발행과 소비를 모두 하므로, 별도 서비스로 분리한 구조보다 CPU 경합이 있다.
- Outbox 패턴, 재시도, DLQ는 이번 시나리오에 포함하지 않았다.
- 대용량은 앱 메모리에 지연 값을 건당 1개씩 보관한다(1M이면 수십 MB). JVM과 Node 힙은 기본값이다.

## 대용량 대응으로 바꾼 옵션 (2026-10-22)
기존 A1~B5 결과(2026-10-09)는 아래 변경 전에 측정했다.
- MariaDB `innodb-buffer-pool-size=1G` (기본 128MB)
- `DELETE /runs`가 Rabbit 큐도 비운다. 컨슈머는 현재 메모리에 없는 runId의 메시지를 저장하지 않는다(이전 회차 잔여분)
- Nest Rabbit 발행이 `sendToQueue`가 false면 `drain`을 기다린다(backpressure)
- Nest Kafka 컨슈머 `sessionTimeout` 60초, 배치를 1,000건씩 처리하고 그때마다 heartbeat

## 구조 개편 (2026-10-23)
- 두 앱을 MQ별 모듈로 나누고 Spring은 Gradle과 properties로 바꿨다. 측정 조건(배치, ack, prefetch, 동시성 10, 영속성, 이름, API)은 같다.
- 차이 한 가지: Kafka key(userId)를 이벤트 생성 시 같이 만든다. Nest는 전에 Kafka 발행마다 `JSON.parse`로 key를 꺼냈는데 이 비용이 없어졌다. Nest Kafka 발행 수치가 조금 좋아질 수 있다.
