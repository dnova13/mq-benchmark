package practice;

import io.lettuce.core.Consumer;
import io.lettuce.core.RedisBusyException;
import io.lettuce.core.RedisClient;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Redis Streams 최소 예제 (Lettuce)
 * <pre>
 * ./gradlew run -Pex=RedisPractice --args="produce 5"     practice:stream에 XADD
 * ./gradlew run -Pex=RedisPractice --args="consume c1"    그룹 practice-java로 XREADGROUP 후 XACK (Ctrl+C로 종료)
 * ./gradlew run -Pex=RedisPractice --args="info"          길이, 그룹
 * </pre>
 */
public class RedisPractice {
    static final String STREAM = "practice:stream";
    static final String GROUP = "practice-java";

    public static void main(String[] args) {
        String cmd = args.length > 0 ? args[0] : "info";
        RedisClient client = RedisClient.create(System.getenv().getOrDefault("REDIS_URL", "redis://localhost:36397"));
        try (StatefulRedisConnection<String, String> conn = client.connect()) {
            RedisCommands<String, String> redis = conn.sync();
            switch (cmd) {
                case "produce" -> produce(redis, args.length > 1 ? Integer.parseInt(args[1]) : 5);
                case "consume" -> consume(redis, args.length > 1 ? args[1] : "c1");
                default -> info(redis);
            }
        } finally {
            client.shutdown();
        }
    }

    static void produce(RedisCommands<String, String> redis, int count) {
        for (int i = 1; i <= count; i++) {
            // ID는 자동 생성(밀리초-순번). 필드는 Map으로 넣는다
            String id = redis.xadd(STREAM, Map.of("orderId", String.valueOf(i), "createdAt", String.valueOf(System.currentTimeMillis())));
            System.out.println("XADD " + STREAM + " → " + id);
        }
    }

    @SuppressWarnings("unchecked") // xreadgroup의 가변 인자 제네릭 경고
    static void consume(RedisCommands<String, String> redis, String consumer) {
        try {
            // "0" = 스트림 처음부터, mkstream = 스트림이 없으면 생성
            redis.xgroupCreate(StreamOffset.from(STREAM, "0"), GROUP, XGroupCreateArgs.Builder.mkstream());
        } catch (RedisBusyException e) {
            // 그룹이 이미 있다
        }
        System.out.println("그룹 " + GROUP + ", 컨슈머 " + consumer + "로 대기 중... (Ctrl+C로 종료)");
        while (true) {
            // lastConsumed = '>' : 아직 이 그룹에 전달되지 않은 메시지. 최대 10건, 5초 대기
            List<StreamMessage<String, String>> messages = redis.xreadgroup(
                    Consumer.from(GROUP, consumer),
                    XReadArgs.Builder.count(10).block(Duration.ofSeconds(5)),
                    StreamOffset.lastConsumed(STREAM));
            for (StreamMessage<String, String> m : messages) {
                System.out.println("받음 " + m.getId() + " " + m.getBody());
                redis.xack(STREAM, GROUP, m.getId()); // 처리 완료 → PEL에서 빠진다
            }
        }
    }

    static void info(RedisCommands<String, String> redis) {
        System.out.println("XLEN " + redis.xlen(STREAM));
        if (redis.exists(STREAM) > 0) redis.xinfoGroups(STREAM).forEach(g -> System.out.println("GROUP " + g));
    }
}
