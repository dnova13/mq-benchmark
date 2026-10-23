# 테스트 시나리오 (규모별)

Kafka, Redis Streams, RabbitMQ를 Spring Boot와 NestJS 두 앱에서 같은 조건으로 측정한다.
**건수만 1천 → 300만으로 바꾸고**, 나머지 조건은 고정한다. 규모가 커질 때 MQ 순위와 앱 차이가 어떻게 변하는지 보는 것이 목적이다.

## 두 가지 방식: A와 B
| | 방식 A: 대량 발행 | 방식 B: 요청 1건 = 이벤트 1건 |
|---|---|---|
| 흐름 | `GET /bench/{mq}?count=N` **1번 호출** → 서버가 N건을 1,000건씩 묶어 연속 발행 → 소비 → DB 저장 | 부하 도구가 `POST /orders`를 **총 N번** 호출 → 요청마다 1건 발행 → 소비 → DB 저장 |
| HTTP 요청 수 | 1번 | N번 |
| 보는 것 | HTTP를 빼고 **MQ와 컨슈머가 쌓인 메시지를 얼마나 빨리 비우나** | 사용자가 몰릴 때 **API → MQ → 컨슈머 → DB 전체가 얼마나 버티나** |
| 실무 대응 | 로그/클릭 수집, 배치 적재, CDC, 밀린 메시지 재처리 | 주문, 결제 등 일반 API 서비스 |
| 도구 | `curl` | **k6**(리포트 기준), **Locust**(직접 실행용 스크립트 제공) |

### VU(동시 사용자)란
- **VU(동시 사용자)** = 부하 도구가 만드는 가상 사용자 수다. 각 VU는 요청을 보내고 응답을 받으면 바로 다음 요청을 보낸다.
- VU(동시 사용자)는 **동시에 진행 중인 요청 수**이고, 전체 요청 수(N)와는 별개다.
- B는 VU(동시 사용자) 100명이 **합계 N번**을 나눠서 보낸다. 100 × N이 아니다. 예를 들어 N=1만이면 1인당 평균 약 100번이다.
- 동시에 보내는 이유는 실제 서비스처럼 요청이 몰려야 앱, MQ, DB가 한계에 닿기 때문이다. 한 명이 순서대로 보내면 왕복 시간 때문에 부하 도구가 먼저 병목이 된다.

## 시나리오 (12개)
고정 조건: 메시지 256B, **DB 저장 O**, B는 **VU(동시 사용자) 100**.

| 규모 | 방식 A | 방식 B | 반복 |
|---|---|---|---:|
| 1천 | `A-1k` | `B-1k` | 5 |
| 1만 | `A-10k` | `B-10k` | 5 |
| 10만 | `A-100k` | `B-100k` | 3 |
| 50만 | `A-500k` | `B-500k` | 3 |
| 100만 | `A-1m` | `B-1m` | 2 |
| 300만 | `A-3m` | `B-3m` | 1 |

- 각 시나리오를 MQ 3종(kafka, redis, rabbit)으로 돌린다. MQ마다 **워밍업 1회**(1만 건, 기록 안 함) 뒤 반복 횟수만큼 기록한다.
- 반복이 1~2회인 대용량 결과는 편차를 알 수 없으니 경향으로만 본다.
- 결과 리포트(`reports/scale-result.md`)는 k6 측정만 쓴다. Locust는 `docs/locust-guide.md`대로 직접 돌린다.

## 공통 규칙
- 이벤트: 주문 JSON(`id`, `runId`, `userId`, `amount`, `createdAt`, `db`, `pad`). `pad`로 전체 크기를 256바이트에 맞춘다.
- 데이터는 결정적이다. `userId = (i*7919)%1000+1`, `amount = (i*37)%99900+100`이라 Nest와 Spring이 같은 값을 만든다.
- 병렬도 10: DB 커넥션 풀 10, Kafka 파티션 10, Rabbit 컨슈머 10, Redis 컨슈머 10명(같은 그룹)
- 컨슈머는 `orders_result`에 `INSERT IGNORE`(멱등)한다. 저장 건수가 N에 닿으면 끝난 것으로 본다.
- Kafka는 `userId`를 키로 쓴다. Rabbit은 durable 큐와 persistent 메시지, Redis는 AOF, Kafka는 기본 설정이다.
- 매 회차 전에 `DELETE /runs`로 결과 테이블, Rabbit 큐, Redis 스트림을 비운다.

## 지표
| 지표 | 뜻 |
|---|---|
| `tps` | 처리 건수 ÷ (첫 발행부터 마지막 처리까지 걸린 시간). 초당 처리 건수 |
| `latencyMs` p50/p95/p99 | 이벤트 생성 시각부터 처리(저장) 완료 시각까지 |
| `dbCount` | DB에 저장된 건수. N과 같아야 정상 |
| (Locust) `httpRps`, `httpP95Ms` | 발행 요청 자체의 초당 요청 수와 응답 시간. MQ 처리 지연과는 다르다 |

## 실행
```bash
docker compose down -v && docker compose up -d    # 깨끗한 상태
# 앱은 한 번에 하나만 띄운다 (Nest와 Spring을 같이 띄우면 같은 큐를 나눠 먹는다)
cd spring && java -jar build/libs/mq-bench-spring-0.0.1.jar &
./scenarios.sh spring                  # 12개 전체, 반복은 시나리오별 기본값
./scenarios.sh spring 1 A-1k,B-1k      # 일부만, 모두 1회
```
- 결과는 `results/<app>-<시각>.jsonl`(또는 `OUT=경로`)에 한 줄씩 쌓인다.
- 대기 시간은 건수에 비례한다(500건/초 기준, 최소 300초). 300만 건이면 서버 대기 6,000초다.
- 100만 건 이상은 진행 조회 간격을 1초로 늘린다. 진행 중 `GET /runs/{id}`는 건수만 반환하고, 지연과 `dbCount`는 완료 시 한 번 계산한다.

### 수동 실행
```bash
# 방식 A
curl "localhost:8080/bench/kafka?count=10000&size=256&db=true"

# 방식 B - k6 (VUS = VU(동시 사용자) 수, COUNT = 전체 요청 수)
k6 run -e BASE=http://localhost:8080 -e MQ=kafka -e COUNT=10000 -e VUS=100 -e DB=true load/k6/orders.js

# 방식 B - Locust (-u = VU(동시 사용자) 수)
MQ=kafka COUNT=10000 DB=true locust -f load/locust/locustfile.py \
    --headless -u 100 -r 100 -t 30m --host http://localhost:8080 --html report.html
```
- 두 도구 모두 끝나면 `RESULT {...}` 한 줄을 출력한다.
- Locust는 보낸 요청의 응답이 모두 돌아온 뒤 종료한다. CSV의 요청 수가 N보다 조금 적게 찍힐 수 있는데, 마지막 통계 집계 시점 차이다. HTML 리포트와 `RESULT`의 `processed`를 기준으로 본다.

### Locust로 규모별 실행
실행 방법은 `docs/locust-guide.md`를 본다.

### 리포트 만들기
```bash
python3 report.py reports/scale-spring-raw.jsonl          # Spring MQ 비교표
python3 report.py reports/scale-nest-raw.jsonl            # Nest MQ 비교표
python3 compare.py reports/scale-spring-raw.jsonl reports/scale-nest-raw.jsonl   # Spring vs Nest
python3 charts.py reports/scale-{spring,nest}-raw.jsonl   # 그래프
```

## 결과 해석 기준
- 같은 규모 안에서만 MQ를 비교한다(예: `A-1m`의 Kafka와 Redis).
- **A와 B의 TPS는 직접 비교하지 않는다.** B에는 HTTP 처리와 부하 도구 성능이 섞인다.
- B가 A보다 한참 낮으면 병목이 MQ가 아니라 HTTP 요청 처리 쪽이라는 뜻이다.
- 같은 규모에서 Nest와 Spring의 차이는 MQ 클라이언트, 런타임, **컨슈머 구현 방식**(Nest는 묶어서 병렬, Spring은 건별)이 섞인 결과다.
- 1천 건은 실행 시간이 1초 안팎이라 노이즈가 크다. 1만 건 이상을 중심으로 본다.

## 알려진 한계
- 단일 머신에서 브로커, 앱, 부하 도구, DB가 자원을 나눠 쓴다. 절대 수치보다 상대 비교에 쓴다.
- 메시지 생성 시각을 1,000건 단위로 찍어서, A 방식 지연에는 청크 안 대기가 조금 섞인다.
- A 방식은 발행이 소비보다 훨씬 빨리 끝나서, 지연에는 큐에서 기다린 시간이 대부분이다. 건수에 비례해 커진다.
- 앱 하나가 발행과 소비를 모두 해서 CPU 경합이 있다.
- 지연 값을 건당 하나씩 앱 메모리에 보관한다(300만 건이면 수십 MB). JVM과 Node 힙은 기본값이다.
- Outbox 패턴, 재시도, DLQ는 포함하지 않았다.
