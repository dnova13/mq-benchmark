package com.bench.practice;

import java.util.List;
import java.util.Map;

/** 연습용 API의 MQ별 구현. 벤치마크와 별개인 practice-* 대상만 다룬다. */
public interface PracticeOps {
    /** "kafka" | "redis" | "rabbit" */
    String type();

    /** 실습 대상 이름(토픽, 스트림, 큐) */
    String destination();

    Map<String, Object> publish(String key, String payload);

    /** 최근 메시지 조회. ack=false면 RabbitMQ도 메시지를 다시 큐에 넣는다(엿보기) */
    List<Map<String, Object>> read(int count, boolean ack);

    Map<String, Object> info();

    /** 실습 대상 삭제 */
    void reset();
}
