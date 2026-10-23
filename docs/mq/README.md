# MQ 정리

MQ 개념과 대표 MQ 세 가지(Redis Streams, RabbitMQ, Kafka)를 정리한 문서 모음이다. 노션 "MQ" 카테고리로 옮겨 쓰는 것을 염두에 두고 썼다.

## 개념 문서 (이 폴더)
| 문서 | 내용 | 그림 |
|---|---|---|
| [01. MQ란](01-what-is-mq.md) | 정의, 쓰는 이유, 구성 요소, 점대점 vs 발행/구독, 전달 보장·멱등성·순서·영속성, 주의할 점, 세 MQ 비교표 | `mq-sync-vs-async.png`, `mq-components.png`, `mq-patterns.png` |
| [02. Redis Streams](02-redis-streams.md) | 스트림·엔트리 ID·컨슈머 그룹·PEL, 흐름, 장단점, 이 프로젝트 설정과 결과 | `redis-streams.png` |
| [03. RabbitMQ](03-rabbitmq.md) | exchange·binding·routing key·큐, exchange 종류, ack와 DLX, 장단점, 설정과 결과 | `rabbitmq.png` |
| [04. Kafka](04-kafka.md) | 토픽·파티션·오프셋·컨슈머 그룹·lag, 흐름, 장단점, 설정과 결과 | `kafka.png` |

그림은 `img/`에 PNG로 있다. 노션에 그대로 끌어다 넣으면 된다. 다시 그리려면 `python3 docs/mq/diagrams.py`를 실행한다(matplotlib과 한글 폰트 필요).

## 노션 페이지 구성 제안
```
MQ (카테고리)
├─ 1. MQ란                    ← docs/mq/01-what-is-mq.md
├─ 2. Redis Streams           ← docs/mq/02-redis-streams.md
├─ 3. RabbitMQ                ← docs/mq/03-rabbitmq.md
├─ 4. Kafka                   ← docs/mq/04-kafka.md
├─ 5. Nest vs Spring 개념과 성능 비교     ← docs/nest-vs-spring-concepts.md
├─ 6. 부하 테스트 방법
│   ├─ 시나리오 A·B, VU(동시 사용자)  ← SCENARIOS.md
│   ├─ k6                      ← docs/k6-guide.md
│   └─ Locust                  ← docs/locust-guide.md
├─ 7. MQ·프레임워크별 성능 측정 결과      ← reports/scale-result.md (그래프: reports/img/*.png)
├─ 8. 실습
│   ├─ 개요(CLI·GUI·코드·API)  ← practice/README.md
│   ├─ Redis / RabbitMQ / Kafka ← practice/redis.md, rabbitmq.md, kafka.md
│   └─ GUI 사용법              ← practice/gui.md
└─ 9. 프로젝트 구조            ← spring/README.md, nest/README.md
```

## 읽는 순서
1. **MQ란** → 왜 쓰는지와 공통 용어
2. **Redis Streams → RabbitMQ → Kafka** → 단순한 것부터
3. **실습**으로 직접 해 보기
4. **MQ·프레임워크별 성능 측정 결과**로 같은 조건에서의 차이 보기
