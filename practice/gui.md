# GUI 사용법

```bash
docker compose --profile gui up -d     # 브로커 + GUI
```
GUI 컨테이너는 `gui` 프로필이라 `docker compose up -d`(벤치마크용)에서는 뜨지 않는다.

| GUI | 주소 | 대상 | 이미지 |
|---|---|---|---|
| RedisInsight | http://localhost:35540 | Redis | `redis/redisinsight:2.68` |
| Kafka UI | http://localhost:38080 | Kafka | `kafbat/kafka-ui:v1.2.0` |
| RabbitMQ Management | http://localhost:31567 | RabbitMQ | `rabbitmq:3-management`에 포함 |

화면을 보면서 하기 좋은 흐름:
1. GUI를 띄워 둔다.
2. 다른 터미널에서 CLI 스크립트, 코드 예제, 앱 API로 메시지를 넣는다.
3. GUI에서 새로고침해 바뀐 상태를 확인한다.

---

## RedisInsight (Redis)

### 처음 접속
1. http://localhost:35540 을 연다.
2. 약관(EULA) 동의 창에서 동의한다. 분석·알림 수집은 꺼도 된다.
3. 데이터베이스 목록에 **bench-redis**(`redis:6379`)가 자동으로 등록돼 있다. compose의 `RI_REDIS_HOST`로 미리 넣어 두었다. 클릭해서 들어간다.
   - 목록에 없으면 "Add Redis database"에서 Host `redis`, Port `6379`로 추가한다. 컨테이너끼리는 서비스 이름으로 접속하기 때문이다(localhost 아님).

### 화면별 사용법
| 메뉴 | 할 수 있는 것 |
|---|---|
| **Browser** | 키 목록과 값 보기. 위쪽 필터에 `practice:*`를 넣으면 실습 키만 보인다. 키 종류(String, Hash, List, Set, Stream)별로 값 편집기가 다르다 |
| Browser → Stream 키 | **Stream data** 탭: 엔트리(ID, 필드) 목록. **Consumer Groups** 탭: 그룹별 컨슈머 수, pending(ack 안 한 수), last delivered ID. 컨슈머를 누르면 pending 메시지 목록이 나온다 |
| **Workbench** | redis-cli처럼 명령을 실행한다. 결과가 표나 JSON으로 보기 좋게 나온다. 예: `XINFO GROUPS practice:stream` |
| **Analyze** (Database Analysis) | 키 종류별 메모리 사용량, 큰 키 찾기 |
| **Pub/Sub** | 채널 구독과 발행 테스트(이 프로젝트는 Streams를 쓰지만 차이를 보기 좋다) |

### 실습 예
1. `practice/cli/redis.sh 3`으로 스트림에 3건을 넣는다.
2. Browser에서 `practice:stream`을 열어 엔트리 3개를 확인한다.
3. `practice/cli/redis.sh 4`를 실행한다.
4. Consumer Groups 탭에서 `practice-group`의 pending 수가 바뀌는 것을 본다.
5. 벤치마크 앱을 띄운 뒤 `bench:stream`의 Consumer Groups를 보면 `bench-spring-group`(또는 nest)에 컨슈머 10개가 붙어 있다.

---

## Kafka UI (Kafka)

### 처음 접속
- http://localhost:38080 을 연다. 클러스터 **bench**가 이미 등록돼 있다(`kafka:29092`).
- Kafka는 호스트용(`localhost:39092`)과 컨테이너용(`kafka:29092`) 리스너를 따로 연다. Kafka UI는 컨테이너 안에서 접속하므로 내부 리스너를 쓴다.

### 화면별 사용법
| 메뉴 | 할 수 있는 것 |
|---|---|
| **Dashboard** | 클러스터 상태(online), 브로커 수, 토픽·파티션 수 |
| **Brokers** | 브로커별 설정(`log.retention.hours` 등)과 디스크 사용량 |
| **Topics** | 토픽 목록. **Add a Topic**으로 생성(이름, 파티션 수, 보존 기간) |
| Topics → 토픽 → **Overview** | 파티션별 시작·끝 오프셋, 메시지 수 |
| Topics → 토픽 → **Messages** | 메시지 보기. Seek Type(Offset, Timestamp), 파티션, 정렬(최신순/오래된순)을 고른다. key, value, 헤더, 파티션, 오프셋이 보인다 |
| Topics → 토픽 → **Produce Message** (오른쪽 위) | key, value를 넣어 직접 발행. key를 같게 해서 여러 번 보내면 같은 파티션에 쌓이는 것을 확인할 수 있다 |
| Topics → 토픽 → **Consumers** | 이 토픽을 읽는 컨슈머 그룹 |
| **Consumers** | 그룹 목록, 상태(Stable/Empty), 파티션별 Current offset, End offset, **Lag**. 그룹을 열면 **Reset offsets**로 오프셋을 되감을 수 있다(그룹에 붙은 컨슈머가 없을 때) |

### 실습 예
1. `practice/cli/kafka.sh 1`, `2`로 토픽을 만들고 메시지 6건을 넣는다.
2. Topics → practice-topic → Messages에서 파티션별로 들어간 메시지를 본다(같은 key는 같은 파티션).
3. Produce Message로 key `user-1` 메시지를 몇 건 더 보낸다.
4. `practice/cli/kafka.sh 3`으로 소비한 뒤 Consumers → practice-group에서 Lag이 0인지 확인한다.
5. 다시 Produce Message로 몇 건 보내고 Lag이 늘어난 것을 본다(아직 안 읽은 수).
6. 벤치마크 실행 중 Consumers → bench-spring-group을 보면 파티션 10개의 Lag이 실시간으로 바뀐다.

---

## RabbitMQ Management

### 처음 접속
- http://localhost:31567 에서 `guest` / `guest`로 로그인한다.

### 화면별 사용법
| 탭 | 할 수 있는 것 |
|---|---|
| **Overview** | 전체 메시지 수(Ready, Unacked), 초당 발행·전달 그래프, 노드 메모리와 디스크 |
| **Connections**, **Channels** | 접속한 앱과 채널. 채널별 prefetch, 미확인(unacked) 수 |
| **Exchanges** | exchange 목록. 하나를 열면 **Bindings**(어느 큐로 가는지)와 **Publish message**(직접 발행, routing key 지정) |
| **Queues and Streams** | 큐 목록과 Ready(대기), Unacked(전달됐지만 ack 전), Total. **Add a new queue**로 생성 |
| 큐 → **Get messages** | 메시지 꺼내 보기. Ack Mode를 고른다: **Nack message requeue true**(엿보기, 다시 큐로), **Automatic ack**(꺼내서 제거) |
| 큐 → **Publish message** | 이 큐로 직접 발행(기본 exchange 사용) |
| 큐 → **Bindings** | 이 큐가 어느 exchange에 어떤 routing key로 묶였는지. 여기서 binding을 추가할 수도 있다 |
| 큐 → **Purge Messages** / **Delete** | 메시지 전체 삭제 / 큐 삭제 |

### 실습 예
1. `practice/cli/rabbitmq.sh 2`로 `practice-queue`에 3건을 넣는다.
2. Queues에서 Ready 3을 확인한다.
3. Get messages에서 "Nack message requeue true"로 1건을 꺼낸다. Ready가 그대로 3이다.
4. "Automatic ack"로 꺼내면 Ready가 줄어든다. Kafka·Redis Streams와 다른 점이다.
5. `practice/cli/rabbitmq.sh 4`, `5`를 실행한다. Exchanges → practice.direct / practice.fanout의 Bindings를 보면 routing key별로 어느 큐에 묶였는지 나온다.
6. 벤치마크 실행 중 Queues → bench-queue를 보면 Consumers 10, 초당 처리량 그래프가 움직인다.

---

## 문제 해결
| 증상 | 확인 |
|---|---|
| GUI 주소가 안 열림 | `docker compose --profile gui ps`로 컨테이너가 Up인지 확인. `--profile gui` 없이 띄웠으면 GUI는 없다 |
| Kafka UI에서 클러스터가 offline | Kafka가 막 뜬 직후일 수 있다(10초쯤 걸림). compose의 Kafka에 `INTERNAL://kafka:29092` 리스너가 있는지 확인 |
| RedisInsight에 DB가 없음 | 약관 동의 후 새로고침. 그래도 없으면 Host `redis`, Port `6379`로 직접 추가 |
| 벤치마크 결과에 영향이 있나 | GUI는 화면을 열어 둔 동안 주기적으로 조회한다. 측정할 때는 GUI를 끄는 편이 정확하다: `docker compose --profile gui stop redisinsight kafka-ui` |
