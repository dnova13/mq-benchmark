package com.bench.practice;

import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.Limit;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

// Redis Streams: 읽어도 메시지가 지워지지 않는다(로그). 조회는 XREVRANGE로 최근 것부터
@Component
public class RedisPracticeOps implements PracticeOps {
    static final String STREAM = "practice:stream";
    private final StringRedisTemplate redis;

    public RedisPracticeOps(StringRedisTemplate redis) { this.redis = redis; }

    @Override public String type() { return "redis"; }
    @Override public String destination() { return STREAM; }

    @Override
    public Map<String, Object> publish(String key, String payload) {
        Map<String, String> fields = new LinkedHashMap<>();
        if (key != null) fields.put("key", key);
        fields.put("payload", payload);
        RecordId id = redis.opsForStream().add(STREAM, fields); // XADD practice:stream * key .. payload ..
        return Map.of("id", id.getValue());
    }

    @Override
    public List<Map<String, Object>> read(int count, boolean ack) {
        List<MapRecord<String, Object, Object>> records =
                redis.opsForStream().reverseRange(STREAM, Range.unbounded(), Limit.limit().count(count));
        return records.stream().map(r -> Map.<String, Object>of("id", r.getId().getValue(), "fields", r.getValue())).toList();
    }

    @Override
    public Map<String, Object> info() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("length", redis.opsForStream().size(STREAM));
        if (Boolean.TRUE.equals(redis.hasKey(STREAM))) {
            m.put("groups", redis.opsForStream().groups(STREAM).stream()
                    .map(g -> Map.of("name", g.groupName(), "consumers", g.consumerCount(), "pending", g.pendingCount())).toList());
        }
        return m;
    }

    @Override public void reset() { redis.delete(STREAM); }
}
