package com.bench.mq.common;

import java.util.List;

/** MQ별 발행 구현의 공통 인터페이스. 켜진 MQ만 빈으로 등록된다. */
public interface MessagePublisher {
    /** "kafka" | "rabbit" | "redis" */
    String type();

    void publish(List<Message> messages);

    /** 큐/스트림에 남은 메시지를 비운다. 지원하지 않으면 아무것도 하지 않는다. */
    default void purge() {}
}
