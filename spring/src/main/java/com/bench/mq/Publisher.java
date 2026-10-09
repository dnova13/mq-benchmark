package com.bench.mq;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public class Publisher {
    private final KafkaTemplate<String, String> kafka;
    private final RabbitTemplate rabbit;
    private final StringRedisTemplate redis;

    public Publisher(KafkaTemplate<String, String> kafka, RabbitTemplate rabbit, StringRedisTemplate redis) {
        this.kafka = kafka; this.rabbit = rabbit; this.redis = redis;
    }

    public void publish(String mq, List<String> events) {
        switch (mq) {
            case "kafka" -> {
                // userId를 키로 써서 같은 사용자는 같은 파티션(순서 보장)
                for (String e : events) kafka.send(MqConfig.KAFKA_TOPIC, EventFactory.userIdOf(e), e);
                kafka.flush();
            }
            case "rabbit" -> {
                for (String e : events) rabbit.convertAndSend(MqConfig.RABBIT_QUEUE, e);
            }
            case "redis" -> {
                if (events.size() == 1) { // 단건(시나리오 B)은 공유 커넥션으로 바로 발행
                    redis.opsForStream().add(MqConfig.STREAM_KEY, Map.of("data", events.get(0)));
                    return;
                }
                redis.executePipelined(new SessionCallback<Object>() {
                @Override
                @SuppressWarnings({"unchecked", "rawtypes"})
                public Object execute(RedisOperations ops) {
                    for (String e : events) ops.opsForStream().add(MqConfig.STREAM_KEY, Map.of("data", e));
                    return null;
                }
                });
            }
            default -> throw new IllegalArgumentException("mq must be kafka | redis | rabbit");
        }
    }
}
