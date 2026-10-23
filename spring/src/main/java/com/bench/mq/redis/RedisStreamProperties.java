package com.bench.mq.redis;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/** application-redis.properties의 mq.redis.* */
@ConfigurationProperties(prefix = "mq.redis")
public record RedisStreamProperties(
        @DefaultValue("true") boolean enabled,
        @DefaultValue("bench:stream") String streamKey,
        @DefaultValue("bench-spring-group") String group,
        @DefaultValue("10") int workers,
        @DefaultValue("100") int batchSize,
        @DefaultValue("1s") Duration pollTimeout) {
}
