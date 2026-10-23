# Locust로 부하 테스트하기

B 시나리오(요청 1건 = 이벤트 1건)를 Locust로 실행하는 방법이다. 결과 리포트(`reports/scale-result.md`)는 k6로 쟀고, Locust는 직접 돌려 보고 싶을 때 쓴다.

## 무엇을 하나
- Locust가 VU(동시 사용자) 100명을 만들어 `POST /orders`를 **합계 N번** 보낸다. 요청 1번이 이벤트 1건이다.
  - VU(동시 사용자): 동시에 요청을 보내는 가상 사용자 수다. 전체 요청 수(N)와는 별개이고, 100명이 N번을 나눠 보낸다.
- 요청을 다 보내면 서버의 `GET /runs/{runId}`를 조회하면서, 이벤트가 MQ를 거쳐 DB에 모두 저장될 때까지 기다린다. 그다음 `RESULT {...}` 한 줄을 출력한다.
- 조건은 k6 시나리오와 같다: 256B 메시지, DB 저장 O, MQ마다 워밍업 1회(1만 건, 기록 안 함).

## 준비
```bash
pip install locust                       # Python 3.9 이상
docker compose up -d                     # 인프라
# 앱은 하나만 띄운다 (Nest와 Spring을 같이 띄우면 같은 큐를 나눠 먹는다)
cd spring && ./gradlew bootRun           # :8080
# 또는
cd nest && npm run build && node dist/main.js   # :3000
```
화면 캡처까지 하려면 Node와 Playwright도 필요하다.
```bash
npm i -D playwright && npx playwright install chromium
```

## 1. 규모별 한 번에 실행 (권장)
```bash
load/locust/run-scale.sh <nest|spring> [규모] [MQ]

load/locust/run-scale.sh spring                      # 1천~300만 6개 규모 × 3 MQ, 각 1회
load/locust/run-scale.sh nest 10k,100k redis         # 규모와 MQ를 골라서
CAPTURE=1 load/locust/run-scale.sh spring 100k       # Locust 리포트 화면을 PNG로 캡처
```
- 규모: `1k`, `10k`, `100k`, `500k`, `1m`, `3m` (쉼표로 여러 개)
- MQ: `kafka`, `redis`, `rabbit` (쉼표로 여러 개)

| 환경 변수 | 기본값 | 뜻 |
|---|---|---|
| `VUS` | `100` | VU(동시 사용자) 수 |
| `LOCUST` | `locust` | locust 실행 파일 경로(가상환경에 설치했으면 지정) |
| `OUT` | `results/locust-<app>-<시각>` | 결과 폴더 |
| `CAPTURE` | `0` | `1`이면 회차마다 HTML 리포트를 PNG로 캡처 |
| `PLAYWRIGHT_PATH` | `playwright` | Playwright 모듈 위치(전역 설치 경로 등) |
| `CHROMIUM_PATH` | 없음 | 브라우저 실행 파일을 직접 지정할 때 |

### 결과 파일 (`results/locust-<app>-<시각>/`)
| 파일 | 내용 |
|---|---|
| `raw.jsonl` | 한 줄 = 1회. `tps`, `p50/p95/p99`(이벤트 생성 → DB 저장), `processed`, `dbCount`, `done`과 Locust HTTP 통계 `httpRps`, `httpAvgMs`, `httpP95Ms` |
| `<app>-B-<규모>-<mq>.html` | Locust HTTP 리포트. 초당 요청 수, 응답 시간, 사용자 수 차트 포함 |
| `<app>-B-<규모>-<mq>.png` | `CAPTURE=1`일 때 위 리포트 화면 캡처 |
| `*_stats.csv` 등 | Locust CSV 통계 |

진행 상황은 이렇게 찍힌다.
```
10:32:44 RUN spring-B-10k-redis
10:32:58 DONE spring-B-10k-redis "tps": 976, "totalMs": 10247 "done": true
```

### 표와 그래프로 보기
`raw.jsonl`은 `scenarios.sh` 결과와 같은 필드를 쓴다. 그래서 기존 스크립트로 정리할 수 있다.
```bash
python3 report.py results/locust-spring-XXXX/raw.jsonl                     # MQ별 표(B만 나온다)
python3 compare.py results/locust-spring-XXXX/raw.jsonl results/locust-nest-XXXX/raw.jsonl   # Spring vs Nest
# k6 결과와 나란히 비교하는 그래프 → reports/img/k6-vs-locust.png
python3 charts.py reports/scale-spring-raw.jsonl reports/scale-nest-raw.jsonl \
    results/locust-spring-XXXX/raw.jsonl results/locust-nest-XXXX/raw.jsonl
```

## 2. 한 번만 직접 실행
```bash
MQ=kafka COUNT=10000 DB=true locust -f load/locust/locustfile.py \
    --headless -u 100 -r 100 -t 30m --host http://localhost:8080 --html report.html
```
- `-u`: VU(동시 사용자) 수, `-r`: 초당 늘리는 사용자 수, `-t`: 최대 실행 시간(목표 건수에 닿으면 그 전에 끝난다)
- `COUNT`: 전체 요청 수, `MQ`: `kafka|redis|rabbit`, `DB=true`: 컨슈머가 DB에 저장
- 대용량이면 대기 시간을 늘린다: `WAIT_SEC=3600 POLL_SEC=1` (기본 300초, 0.1초)
- 화면 캡처: `node load/locust/capture.js report.html report.png`

## 3. 웹 화면으로 보기
```bash
MQ=kafka COUNT=100000 DB=true locust -f load/locust/locustfile.py --host http://localhost:8080
```
1. 브라우저에서 http://localhost:8089 를 연다.
2. Number of users에 `100`(VU(동시 사용자)), Ramp up에 `100`을 넣고 Start를 누른다.
3. Charts 탭에서 초당 요청 수와 응답 시간을 실시간으로 본다. 원하는 시점에 화면을 캡처하면 된다.
4. 목표 건수를 다 보내면 자동으로 멈춘다. 터미널에 `RESULT {...}`가 찍힌다.

## 결과를 읽을 때
- **`tps`, `p95`(RESULT)** = 이벤트가 MQ와 컨슈머를 거쳐 **DB에 저장될 때까지**의 처리량과 지연이다. MQ 비교는 이 값으로 한다.
- **Locust 화면의 RPS와 응답 시간** = `POST /orders` 요청 자체의 값이다. 서버는 발행만 하고 바로 202를 돌려주므로, MQ 처리 지연과는 다르다.
- Locust는 보낸 요청의 응답이 모두 돌아온 뒤 종료한다. CSV 요청 수가 N보다 조금 적게 찍힐 수 있는데, 마지막 통계 집계 시점 차이다. HTML 리포트와 `RESULT`의 `processed`를 기준으로 본다.
- Locust는 Python 기반이라 같은 머신에서 k6보다 처리량이 낮게 나온다(이 프로젝트 측정 머신에서 약 절반). 부하 도구가 병목일 수 있으니, MQ끼리 비교할 때는 같은 도구 결과끼리만 비교한다.
- 300만 건은 MQ 하나에 45분 이상 걸릴 수 있다. 큰 규모는 몇 개만 골라서 돌리는 것을 권한다.

## 문제 해결
| 증상 | 확인 |
|---|---|
| `RESULT`가 `done: false` | 앱 로그, `docker compose ps`. 큰 규모면 `WAIT_SEC`를 늘린다 |
| 캡처 PNG가 안 생김 | `node -e "require('playwright')"`가 되는지, `PLAYWRIGHT_PATH`/`CHROMIUM_PATH` 지정 |
| 캡처의 차트가 비어 있음 | 실행이 2초보다 짧으면 차트 기록이 없다(1천 건 등). 1만 건 이상에서 확인 |
| `didn't have proper args` 로그 | 종료 시 나오는 Locust 로그로, 무시해도 된다 |
