# 테스트 가이드 (빠른 시작)

## 1. 사전 준비
- Docker, Node 20 이상, JDK 21, k6, Locust(`brew install k6`, `pip install locust`). Gradle은 `spring/gradlew`가 내려받는다

## 2. 인프라 기동
```bash
docker compose up -d
docker compose ps          # 4개 컨테이너가 Up인지 확인
```
| 서비스 | 접속 |
|---|---|
| MariaDB | `localhost:33307` (root / 1234, DB `bench`) |
| Redis | `localhost:36397` |
| Kafka | `localhost:39092` |
| RabbitMQ | `localhost:35672`, 관리 UI http://localhost:31567 (guest / guest) |

## 3. 앱 실행 (한 번에 하나만 실행하는 것을 권장)
```bash
cd nest   && npm install && npm run start:dev      # :3000
cd spring && ./gradlew bootRun                     # :8080
```
각 앱의 폴더 구조, 설정, 다른 프로젝트로 가져가는 법은 `nest/README.md`, `spring/README.md`를 본다.

## 4. 벤치마크 (시나리오 A, B)
상세 시나리오는 `SCENARIOS.md`를 본다.

**방식 A: API 1번 호출로 서버가 N건 발행**
```bash
./bench.sh nest 10000 256 true     # 건수, 크기, DB 저장 여부
curl "localhost:3000/bench/kafka?count=10000&size=256&db=true"
```

**방식 B: 요청 1건 = 이벤트 1건 (k6, Locust)**
```bash
# VUS = VU(동시 사용자) 수, COUNT = 전체 요청 수(VU가 나눠서 보냄)
k6 run -e MQ=kafka -e COUNT=10000 -e VUS=100 -e DB=true load/k6/orders.js
MQ=kafka COUNT=10000 DB=true locust -f load/locust/locustfile.py --headless -u 100 -r 100 -t 10m --host http://localhost:3000
```
Spring을 테스트할 때는 k6는 `-e BASE=http://localhost:8080`, Locust는 `--host http://localhost:8080`으로 바꾼다.
k6 상세 사용법(변수, 웹 대시보드, HTML 리포트)은 `docs/k6-guide.md`, Locust 규모별 실행과 캡처 방법은 `docs/locust-guide.md`를 본다.

**전체 시나리오 자동 실행**
```bash
./scenarios.sh nest                   # 규모별 12개(1천~300만), MQ마다 워밍업 1회 + 시나리오별 반복
./scenarios.sh spring 1 A-1k,B-1k     # 일부만, 1회
```
결과는 `results/*.jsonl`에 저장된다.

응답 필드는 아래 뜻이다.
- `totalMs`: 첫 발행부터 마지막 처리까지 걸린 시간
- `tps`: `processed / totalMs`로 계산한 초당 처리량
- `latencyMs`: 이벤트 생성부터 처리 완료까지의 p50, p95, p99
- `dbCount`: `db=true`일 때 DB에 저장된 건수 (`expected`와 같아야 정상)

## 5. 공정한 측정 팁
- 첫 호출은 JIT와 커넥션 워밍업 때문에 느리다. 한 번 버리고 3회 이상 측정해 중앙값을 쓴다. `scenarios.sh`가 이를 자동으로 한다.
- 비교할 때는 `count`, `size`, `db`, 동시 접속 수를 모두 같게 한다.
- Nest와 Spring을 동시에 띄우면 RabbitMQ 큐를 나눠 소비하는 등 서로 간섭한다. 하나씩 실행한다.
- 초기화: `curl -X DELETE localhost:3000/runs`(결과 테이블 비움), 또는 `docker compose down -v && docker compose up -d`

## 6. 문제 해결
| 증상 | 확인 |
|---|---|
| Kafka 연결 실패 | `docker compose logs kafka`, 기동에 5~10초 걸림 |
| Nest `BUSYGROUP` 로그 | 정상(이미 있는 컨슈머 그룹) |
| 응답이 끝나지 않음 | 소비가 안 되는 상태. 앱 로그와 `docker compose ps` 확인 |
| 포트 충돌 | 33307, 36397, 39092, 35672, 31567, 3000, 8080 사용 여부 확인 |
