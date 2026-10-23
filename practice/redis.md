# Redis 실습 (기본 조작 + Streams)

## 핵심 개념
- **Redis**는 메모리에 데이터를 두는 키-값 저장소다. 캐시, 세션, 카운터에 많이 쓴다.
- **Streams**는 Redis 5부터 있는 메시지 로그 자료구조다. 이 프로젝트에서 MQ로 쓰는 것이 이것이다.
  - 메시지(엔트리)는 `밀리초-순번` 형식의 ID를 받고, 지우기 전까지 남는다(로그).
  - **컨슈머 그룹**을 만들면 그룹 안의 컨슈머들이 메시지를 **나눠** 받는다.
  - 받은 메시지는 **PEL**(Pending Entries List)에 들어가고, `XACK`해야 빠진다. ack 전에 컨슈머가 죽으면 PEL에 남아 다른 컨슈머가 가져갈 수 있다(at-least-once).
- Pub/Sub(`PUBLISH`/`SUBSCRIBE`)과의 차이: Pub/Sub는 저장하지 않는다. 구독자가 없으면 메시지가 사라진다. Streams는 저장하고, 나중에 붙은 컨슈머도 읽을 수 있다.

| 개념 | Redis Streams | Kafka에 대응하는 것 | RabbitMQ에 대응하는 것 |
|---|---|---|---|
| 메시지를 담는 곳 | 스트림(키) | 토픽 | 큐 |
| 나눠 받는 단위 | 컨슈머 그룹 | 컨슈머 그룹 | 같은 큐의 컨슈머들 |
| 처리 완료 표시 | `XACK` | 오프셋 커밋 | ack |
| 읽은 뒤 메시지 | 남음 | 남음 | 사라짐 |

## CLI 실습: `practice/cli/redis.sh`
```bash
practice/cli/redis.sh list
practice/cli/redis.sh 1     # 하나씩 실행하며 출력을 확인
```
| 단계 | 하는 일 | 핵심 명령 |
|---|---|---|
| 1 | 문자열 키, 카운터, 만료 | `SET`, `GET`, `INCR`, `SET ... EX 60`, `TTL` |
| 2 | Hash, List, Set | `HSET`/`HGETALL`, `RPUSH`/`LPOP`/`LRANGE`, `SADD`/`SMEMBERS` |
| 3 | 스트림에 쓰고 읽기 | `XADD practice:stream * k v`, `XLEN`, `XRANGE - +`, `XREVRANGE + - COUNT 1` |
| 4 | 컨슈머 그룹 | `XGROUP CREATE ... 0 MKSTREAM`, `XREADGROUP GROUP g c1 COUNT 2 STREAMS s >`, `XPENDING`, `XACK`, `XINFO GROUPS` |
| 5 | 벤치마크 스트림 보기 | `XINFO GROUPS bench:stream`, `CONFIG GET appendonly` |
| 6 | 정리 | `DEL practice:*` |

직접 쳐 보려면 컨테이너의 redis-cli를 쓴다.
```bash
docker exec -it bench-redis redis-cli           # 대화형
docker exec -it bench-redis redis-cli XLEN practice:stream
```

### 자주 쓰는 Streams 명령
| 명령 | 뜻 |
|---|---|
| `XADD s * f v [f v ...]` | 추가. `*`는 ID 자동 생성. `MAXLEN ~ 1000`을 붙이면 길이를 대략 제한 |
| `XRANGE s - +` / `XREVRANGE s + -` | 처음→끝 / 끝→처음 조회. `COUNT n`으로 개수 제한 |
| `XREAD COUNT n BLOCK ms STREAMS s $` | 그룹 없이 새 메시지 기다리기(`$` = 지금 이후) |
| `XGROUP CREATE s g 0 MKSTREAM` | 그룹 생성. `0`은 처음부터, `$`는 지금 이후부터 |
| `XREADGROUP GROUP g c COUNT n BLOCK ms STREAMS s >` | 그룹으로 읽기. `>` = 이 그룹에 아직 전달 안 된 것 |
| `XACK s g id` | 처리 완료 |
| `XPENDING s g` | ack 안 된 메시지 요약(수, 컨슈머별) |
| `XAUTOCLAIM s g c 60000 0` | 60초 넘게 ack 안 된 메시지를 컨슈머 c가 가져오기(죽은 컨슈머 대신 처리) |
| `XINFO STREAM s` / `XINFO GROUPS s` | 스트림, 그룹 정보 |
| `XTRIM s MAXLEN 0` / `DEL s` | 비우기 / 삭제 |

## GUI: RedisInsight
http://localhost:35540 → bench-redis → **Browser**에서 `practice:*`로 필터링한다. 스트림을 열면 Stream data와 Consumer Groups 탭이 있다. 자세한 내용은 [gui.md](gui.md#redisinsight-redis).

## 코드 예제
```bash
cd practice/node && npm install
node redis.js produce 5          # XADD 5건
node redis.js consume c1         # 그룹 practice-node, 컨슈머 c1 (다른 터미널에서 c2도 띄워 보기)
node redis.js info

cd practice/java
./gradlew run -Pex=RedisPractice --args="produce 5"
./gradlew run -Pex=RedisPractice --args="consume c1"     # 그룹 practice-java
```
핵심 코드(Node, `practice/node/redis.js`):
```js
await redis.xadd('practice:stream', '*', 'orderId', '1');                       // 발행
await redis.xgroup('CREATE', 'practice:stream', 'practice-node', '0', 'MKSTREAM'); // 그룹(최초 1회)
const res = await reader.xreadgroup('GROUP', 'practice-node', 'c1', 'COUNT', 10, 'BLOCK', 5000,
                                    'STREAMS', 'practice:stream', '>');           // 소비
await redis.xack('practice:stream', 'practice-node', id);                         // 처리 완료
```
- `XREADGROUP ... BLOCK`은 응답이 올 때까지 커넥션을 붙잡는다. 그래서 읽기용 커넥션을 따로 쓴다(`redis.duplicate()`).
- 이 프로젝트의 벤치마크 구현: Nest `nest/src/mq/redis-stream/`, Spring `spring/.../mq/redis/RedisStreamConfig.java`

## 앱 API
```bash
curl -X POST localhost:8080/practice/redis/messages -H 'Content-Type: application/json' -d '{"key":"user-1","payload":"hello"}'
curl "localhost:8080/practice/redis/messages?count=5"     # XREVRANGE, 메시지는 남는다
curl localhost:8080/practice/redis/info                   # 길이, 그룹
curl -X DELETE localhost:8080/practice/redis
```

## 연습 문제
1. `node redis.js consume c1`과 `c2`를 동시에 띄우고 `produce 10`을 실행한다. 각 컨슈머가 몇 건씩 받는지 본다.
2. `XREADGROUP`으로 2건만 읽고 ack하지 않는다(CLI 4단계 앞부분). RedisInsight의 Consumer Groups에서 pending을 확인한 뒤, `XAUTOCLAIM`으로 다른 컨슈머가 가져가게 해 본다.
3. 그룹을 `$`로 만들면(`XGROUP CREATE practice:stream g2 $`) 기존 메시지를 읽지 않는다. `0`으로 만든 그룹과 비교한다.
4. `XADD practice:stream MAXLEN '~' 100 '*' k v`로 길이를 제한하고, 많이 넣은 뒤 `XLEN`을 본다.
