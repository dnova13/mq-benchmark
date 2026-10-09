package com.bench.mq;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.amqp.core.Queue;
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
import org.springframework.kafka.config.TopicBuilder;

import java.time.Duration;
import java.util.Map;

@Configuration
public class MqConfig {
    public static final String KAFKA_TOPIC = "bench-topic";
    public static final String RABBIT_QUEUE = "bench-queue";
    public static final String STREAM_KEY = "bench:stream";
    public static final String GROUP = "bench-spring-group";
    private static final int WORKERS = 10;

    @Bean
    NewTopic benchTopic() {
        return TopicBuilder.name(KAFKA_TOPIC).partitions(10).replicas(1).build();
    }

    @Bean
    Queue benchQueue() {
        return new Queue(RABBIT_QUEUE, true);
    }

    @Bean(destroyMethod = "stop")
    StreamMessageListenerContainer<String, MapRecord<String, String, String>> streamContainer(
            RedisConnectionFactory cf, StringRedisTemplate redis, Processor processor) {
        try {
            redis.opsForStream().createGroup(STREAM_KEY, ReadOffset.from("$"), GROUP);
        } catch (Exception e) {
            // 스트림이 아직 없으면 그룹을 만들 수 없다. 초기 레코드로 스트림을 만든 뒤 다시 생성한다.
            var id = redis.opsForStream().add(STREAM_KEY, Map.of("init", "1"));
            try { redis.opsForStream().createGroup(STREAM_KEY, ReadOffset.from("$"), GROUP); } catch (Exception ignored) { }
            if (id != null) redis.opsForStream().delete(STREAM_KEY, id);
        }
        var options = StreamMessageListenerContainerOptions.builder()
                .pollTimeout(Duration.ofSeconds(1))
                .batchSize(100)
                .build();
        var container = StreamMessageListenerContainer.create(cf, options);
        for (int i = 0; i < WORKERS; i++) {
            container.receive(Consumer.from(GROUP, "consumer-" + i),
                    StreamOffset.create(STREAM_KEY, ReadOffset.lastConsumed()),
                    record -> {
                        String data = record.getValue().get("data");
                        if (data == null) return; // init 레코드
                        processor.process(data);
                        redis.opsForStream().acknowledge(STREAM_KEY, GROUP, record.getId());
                    });
        }
        container.start();
        return container;
    }
}
