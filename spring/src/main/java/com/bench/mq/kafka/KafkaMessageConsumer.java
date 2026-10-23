package com.bench.mq.kafka;

import com.bench.mq.common.MessageHandler;
import org.springframework.kafka.annotation.KafkaListener;

// 그룹 id와 동시성은 spring.kafka.consumer.group-id, spring.kafka.listener.concurrency로 정한다.
public class KafkaMessageConsumer {
    private final MessageHandler handler;

    public KafkaMessageConsumer(MessageHandler handler) { this.handler = handler; }

    @KafkaListener(topics = "${mq.kafka.topic}")
    public void onMessage(String payload) { handler.handle(payload); }
}
