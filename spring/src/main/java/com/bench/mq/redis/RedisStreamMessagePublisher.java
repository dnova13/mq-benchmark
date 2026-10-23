package com.bench.mq.redis;

import com.bench.mq.common.Message;
import com.bench.mq.common.MessagePublisher;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.List;
import java.util.Map;

public class RedisStreamMessagePublisher implements MessagePublisher {
    /** 스트림 레코드에서 payload를 담는 필드 이름 */
    static final String FIELD = "data";

    private final StringRedisTemplate redis;
    private final String streamKey;

    public RedisStreamMessagePublisher(StringRedisTemplate redis, String streamKey) {
        this.redis = redis; this.streamKey = streamKey;
    }

    @Override
    public String type() { return "redis"; }

    @Override
    public void publish(List<Message> messages) {
        if (messages.size() == 1) { // 단건은 공유 커넥션으로 바로 XADD
            redis.opsForStream().add(streamKey, Map.of(FIELD, messages.get(0).payload()));
            return;
        }
        // 여러 건은 파이프라인으로 한 번에 보낸다
        redis.executePipelined(new SessionCallback<Object>() {
            @Override
            @SuppressWarnings({"unchecked", "rawtypes"})
            public Object execute(RedisOperations ops) {
                for (Message m : messages) ops.opsForStream().add(streamKey, Map.of(FIELD, m.payload()));
                return null;
            }
        });
    }

    @Override
    public void purge() { redis.opsForStream().trim(streamKey, 0); }
}
