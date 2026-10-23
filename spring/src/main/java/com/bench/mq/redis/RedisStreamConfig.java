package com.bench.mq.redis;

import com.bench.mq.common.MessageHandler;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.StreamMessageListenerContainer.StreamMessageListenerContainerOptions;

import java.util.Map;

// mq.redis.enabled=false면 컨슈머 그룹, 리스너 컨테이너, 발행을 등록하지 않는다.
@Configuration
@ConditionalOnProperty(prefix = "mq.redis", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(RedisStreamProperties.class)
public class RedisStreamConfig {

    @Bean
    RedisStreamMessagePublisher redisStreamMessagePublisher(StringRedisTemplate redis, RedisStreamProperties props) {
        return new RedisStreamMessagePublisher(redis, props.streamKey());
    }

    // XREADGROUP으로 읽고 처리 후 XACK(at-least-once). 같은 그룹의 컨슈머 workers명이 나눠 읽는다.
    @Bean(destroyMethod = "stop")
    StreamMessageListenerContainer<String, MapRecord<String, String, String>> redisStreamContainer(
            RedisConnectionFactory cf, StringRedisTemplate redis, RedisStreamProperties props, MessageHandler handler) {
        String key = props.streamKey(), group = props.group();
        createGroup(redis, key, group);
        var options = StreamMessageListenerContainerOptions.builder()
                .pollTimeout(props.pollTimeout())
                .batchSize(props.batchSize())
                .build();
        var container = StreamMessageListenerContainer.create(cf, options);
        for (int i = 0; i < props.workers(); i++) {
            container.receive(Consumer.from(group, "consumer-" + i),
                    StreamOffset.create(key, ReadOffset.lastConsumed()),
                    record -> {
                        String data = record.getValue().get(RedisStreamMessagePublisher.FIELD);
                        if (data == null) return; // init 레코드
                        handler.handle(data);
                        redis.opsForStream().acknowledge(key, group, record.getId());
                    });
        }
        container.start();
        return container;
    }

    private static void createGroup(StringRedisTemplate redis, String key, String group) {
        try {
            redis.opsForStream().createGroup(key, ReadOffset.from("$"), group);
        } catch (Exception e) {
            // 스트림이 아직 없으면 그룹을 만들 수 없다. 초기 레코드로 스트림을 만든 뒤 다시 생성한다.
            var id = redis.opsForStream().add(key, Map.of("init", "1"));
            try { redis.opsForStream().createGroup(key, ReadOffset.from("$"), group); } catch (Exception ignored) { }
            if (id != null) redis.opsForStream().delete(key, id);
        }
    }
}
