package com.bench.mq.kafka;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/** application-kafka.properties의 mq.kafka.* */
@ConfigurationProperties(prefix = "mq.kafka")
public record KafkaMqProperties(
        @DefaultValue("true") boolean enabled,
        @DefaultValue("bench-topic") String topic,
        @DefaultValue("10") int partitions,
        @DefaultValue("1") short replicas) {
}
