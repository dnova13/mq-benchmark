# k6로 부하 테스트하기

B 시나리오(요청 1건 = 이벤트 1건)를 k6로 실행하는 방법이다. 결과 리포트(`reports/scale-result.md`)의 B 결과는 모두 이 방식으로 쟀다.
A 시나리오(대량 발행)는 k6가 아니라 `curl` 한 번으로 실행한다(아래 "A 시나리오" 참고).

## 무엇을 하나
- k6가 VU(동시 사용자)를 만들어 `POST /orders`를 **합계 N번** 보낸다. 요청 1번이 이벤트 1건이다.
  - VU(동시 사용자): 동시에 요청을 보내는 가상 사용자 수다. 각 VU(동시 사용자)는 응답을 받으면 바로 다음 요청을 보낸다.
  - 전체 요청 수(N)와는 별개다. VU(동시 사용자) 100명이 N번을 나눠 보낸다(k6 `shared-iterations`). 100 × N이 아니다.
- 흐름은 `load/k6/orders.js` 기준으로 이렇다.
  1. `setup()`: `DELETE /runs`로 이전 결과를 지우고, `POST /runs`로 `runId`를 받는다.
  2. 본 실행: VU(동시 사용자)들이 `POST /orders?runId=...`를 합계 N번 보낸다. 서버는 MQ에 발행하고 바로 202를 돌려준다.
  3. `teardown()`: `GET /runs/{runId}`를 조회하면서, 이벤트가 MQ와 컨슈머를 거쳐 DB에 모두 저장될 때까지 기다린다. 끝나면 `RESULT {...}` 한 줄을 출력한다.

## 준비
```bash
# k6 설치 (하나 선택)
brew install k6                                          # macOS
sudo apt-get install k6                                  # Ubuntu (k6 공식 apt 저장소 등록 후)
winget install k6 --source winget                        # Windows
docker run --rm -i --network host grafana/k6 run - <load/k6/orders.js   # 설치 없이 Docker로
k6 version

# 인프라와 앱 (앱은 하나만 띄운다. Nest와 Spring을 같이 띄우면 같은 큐를 나눠 먹는다)
docker compose up -d
cd spring && ./gradlew bootRun                           # :8080
# 또는
cd nest && npm run build && node dist/main.js            # :3000
```

## 1. 시나리오 전체를 한 번에 (권장)
`scenarios.sh`가 A와 B 12개 시나리오를 MQ 3종으로 돌린다. B는 내부에서 k6를 부른다.
```bash
./scenarios.sh spring                     # 12개 전체(1천~300만), 반복은 시나리오별 기본값
./scenarios.sh nest 1 B-1k,B-10k          # B만 일부, 1회씩
OUT=reports/my-run.jsonl ./scenarios.sh spring 1 B-100k   # 결과 파일 경로 지정
```
- 시나리오: `B-1k`, `B-10k`, `B-100k`, `B-500k`, `B-1m`, `B-3m` (VU(동시 사용자) 100, 256B, DB 저장 O)
- MQ마다 워밍업 1회(1만 건, 기록 안 함) 뒤 반복 횟수만큼 기록한다. 회차 전마다 `DELETE /runs`로 결과 테이블과 큐를 비운다.
- 대기 시간과 k6 최대 실행 시간은 건수에 맞춰 자동으로 늘린다(500건/초 기준, 최소 300초).
- 결과는 `results/<app>-<시각>.jsonl`에 한 줄씩 쌓이고, 끝나면 중앙값 요약이 출력된다.

### 표와 그래프
```bash
python3 report.py results/spring-XXXX.jsonl                  # MQ별 표
python3 compare.py results/spring-XXXX.jsonl results/nest-XXXX.jsonl   # Spring vs Nest
python3 charts.py results/spring-XXXX.jsonl results/nest-XXXX.jsonl    # reports/img/*.png
```

## 2. k6를 직접 실행
```bash
k6 run -e BASE=http://localhost:8080 -e MQ=kafka -e COUNT=10000 -e VUS=100 -e DB=true load/k6/orders.js
```

| 변수 | 기본값 | 뜻 |
|---|---|---|
| `BASE` | `http://localhost:3000` | 대상 앱 주소. Spring은 `http://localhost:8080` |
| `MQ` | `kafka` | `kafka`, `redis`, `rabbit` |
| `COUNT` | `1000` | 전체 요청 수 N(= 이벤트 수) |
| `VUS` | `50` | VU(동시 사용자) 수. 리포트 기준은 100 |
| `SIZE` | `256` | 메시지 크기(바이트) |
| `DB` | `true` | 컨슈머가 DB에 저장할지 |
| `MAX_DURATION` | `10m` | 요청 보내는 단계의 최대 시간. 큰 규모면 늘린다 |
| `WAIT_SEC` | `300` | 요청을 다 보낸 뒤 처리 완료를 기다리는 최대 시간(초) |
| `POLL_SEC` | `0.1` | 완료 여부 조회 간격(초). 10만 건 이상이면 `1` 권장 |
| `STALL_SEC` | `15` | 처리 건수가 이 시간 동안 늘지 않으면 유실로 보고 종료 |

큰 규모 예시(100만 건):
```bash
k6 run -e BASE=http://localhost:8080 -e MQ=redis -e COUNT=1000000 -e VUS=100 -e DB=true \
  -e MAX_DURATION=4000s -e WAIT_SEC=2000 -e POLL_SEC=1 load/k6/orders.js
```

## 3. 그래프로 보기 (웹 대시보드, HTML 리포트)
k6에 들어 있는 웹 대시보드로 실행 중 그래프를 보고, 끝나면 HTML 리포트로 저장할 수 있다. `-q`(조용히) 옵션을 주면 대시보드가 켜지지 않는다.
```bash
K6_WEB_DASHBOARD=true K6_WEB_DASHBOARD_PERIOD=1s K6_WEB_DASHBOARD_EXPORT=k6-report.html \
  k6 run -e BASE=http://localhost:8080 -e MQ=kafka -e COUNT=100000 -e VUS=100 load/k6/orders.js
```
- 실행 중: http://localhost:5665 에서 초당 요청 수, 응답 시간, VU(동시 사용자) 수를 실시간으로 본다.
- 끝나면 `k6-report.html`이 생긴다. 브라우저로 열면 같은 그래프와 요약 표가 나온다.
- `K6_WEB_DASHBOARD_PERIOD=1s`를 꼭 준다. 기본값(10초)에서는 짧은 실행이 "not enough data"로 리포트를 건너뛴다.
- PNG로 캡처하려면 Locust용 캡처 스크립트를 그대로 쓴다: `node load/locust/capture.js k6-report.html k6-report.png` (Node + Playwright 필요)
- 요약 수치만 파일로 남기려면 `--summary-export summary.json`을 붙인다.

## A 시나리오 (대량 발행)
A는 HTTP 요청 1번으로 서버가 N건을 발행하므로 k6가 필요 없다.
```bash
curl "localhost:8080/bench/kafka?count=100000&size=256&db=true&timeout=600"
./scenarios.sh spring 1 A-100k            # 워밍업과 반복까지 자동
```

## 결과를 읽을 때
k6 화면에는 두 종류의 수치가 나온다. 헷갈리지 않게 구분한다.

| 어디 | 무엇 | 뜻 |
|---|---|---|
| `RESULT {...}` 줄 | `tps`, `latencyMs.p50/p95/p99`, `processed`, `dbCount`, `done` | 이벤트가 MQ와 컨슈머를 거쳐 **DB에 저장될 때까지**의 처리량과 지연. **MQ 비교는 이 값으로 한다** |
| k6 요약 표 | `http_req_duration`, `http_reqs`, `iterations` | `POST /orders` 요청 자체의 응답 시간과 횟수. 서버는 발행만 하고 바로 202를 주므로 MQ 처리 지연과는 다르다 |
| k6 요약 표 | `checks` | 응답이 202였는지. 100%가 아니면 서버 오류를 확인한다 |

- `done: true`이고 `dbCount`가 `COUNT`와 같아야 정상이다.
- `tps`는 처리 건수 ÷ (첫 발행부터 마지막 저장까지 걸린 시간)이다. HTTP 초당 요청 수(`http_reqs` rate)보다 낮을 수 있다. 낮으면 컨슈머가 유입을 따라가지 못해 큐에 쌓이고 있다는 뜻이고, 그만큼 지연(p95)이 커진다.
- 1천 건처럼 짧은 실행은 시작과 끝의 고정 비용이 커서 TPS가 낮고 편차가 크다. 1만 건 이상을 기준으로 본다.

## 문제 해결
| 증상 | 확인 |
|---|---|
| `RESULT`가 `done: false` | 앱 로그와 `docker compose ps`. 큰 규모면 `WAIT_SEC`, `MAX_DURATION`을 늘린다 |
| `processed`가 `COUNT`보다 적고 멈춤 | 컨슈머 오류나 앱 재시작. `STALL_SEC` 동안 진행이 없으면 끝나니 앱 로그를 본다 |
| `checks`가 100% 미만, 4xx | 꺼진 MQ로 요청했거나(400) `runId`가 없음(404). `MQ` 값과 앱 설정(`mq.*.enabled`)을 확인 |
| 웹 대시보드가 안 뜸 | `-q` 옵션을 빼고, 5665 포트가 비어 있는지 확인 |
| HTML 리포트가 안 생김 | `K6_WEB_DASHBOARD_PERIOD=1s`를 줬는지, 실행이 너무 짧지 않은지 확인 |
| Nest와 Spring 결과가 이상하게 섞임 | 두 앱을 동시에 띄웠는지 확인. 한 번에 하나만 실행한다 |
