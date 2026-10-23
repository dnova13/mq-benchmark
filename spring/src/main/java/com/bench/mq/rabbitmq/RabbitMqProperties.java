package com.bench.mq.rabbitmq;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/** application-rabbitmq.properties의 mq.rabbitmq.* */
@ConfigurationProperties(prefix = "mq.rabbitmq")
public record RabbitMqProperties(
        @DefaultValue("true") boolean enabled,
        @DefaultValue("bench-queue") String queue,
        @DefaultValue("true") boolean durable) {
}
