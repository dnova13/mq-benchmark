package com.bench.mq.common;

/** 발행할 메시지. key는 Kafka 파티션 결정에만 쓰고, RabbitMQ와 Redis Streams는 무시한다. */
public record Message(String key, String payload) {
    public static Message of(String payload) { return new Message(null, payload); }
}
