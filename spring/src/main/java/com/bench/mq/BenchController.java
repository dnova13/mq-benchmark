package com.bench.mq;

import com.bench.mq.RunRegistry.Run;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RestController
public class BenchController {
    private static final int CHUNK = 1000;

    private final Publisher publisher;
    private final DbService db;
    private final RunRegistry runs;
    private final StringRedisTemplate redis;
    private final AmqpAdmin amqpAdmin;

    public BenchController(Publisher publisher, DbService db, RunRegistry runs, StringRedisTemplate redis, AmqpAdmin amqpAdmin) {
        this.publisher = publisher; this.db = db; this.runs = runs; this.redis = redis; this.amqpAdmin = amqpAdmin;
    }

    // dbCount(COUNT(*))도 대용량에서 비싸므로 완료됐거나 final일 때만 조회한다.
    private Map<String, Object> summary(Run run, boolean fin) {
        Map<String, Object> m = new LinkedHashMap<>(runs.summary(run, fin));
        if (run.db && m.containsKey("latencyMs")) m.put("dbCount", db.count(run.runId));
        return m;
    }

    private Run requireRun(String runId) {
        Run run = runs.get(runId);
        if (run == null) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "unknown runId");
        return run;
    }

    // ===== 시나리오 A: API 1번 호출 → 서버가 N건 연속 발행 =====
    // GET /bench/kafka?count=10000&size=256&db=true
    @GetMapping("/bench/{mq}")
    public Map<String, Object> benchA(@PathVariable String mq,
                                      @RequestParam(defaultValue = "10000") int count,
                                      @RequestParam(defaultValue = "256") int size,
                                      @RequestParam(defaultValue = "false") boolean db,
                                      @RequestParam(defaultValue = "300") int timeout) throws InterruptedException {
        Run run = runs.create(mq, db, count, size);
        runs.markStarted(run);
        for (int i = 0; i < count; i += CHUNK) {
            List<String> events = new ArrayList<>();
            for (int j = i; j < Math.min(i + CHUNK, count); j++) events.add(EventFactory.build(run.runId, j, size, db, null, null));
            publisher.publish(mq, events);
        }
        long sendMs = System.currentTimeMillis() - run.startedAt;
        long deadline = System.currentTimeMillis() + timeout * 1000L;
        while (run.processed.get() < count && System.currentTimeMillis() < deadline) Thread.sleep(10);
        Map<String, Object> res = new LinkedHashMap<>();
        res.put("scenario", "A");
        res.put("sendMs", sendMs);
        res.putAll(summary(run, true));
        return res;
    }

    // ===== 시나리오 B: 요청 1건 = 이벤트 1건 =====
    @PostMapping("/runs")
    public Map<String, Object> createRun(@RequestParam String mq,
                                         @RequestParam(defaultValue = "1000") int expected,
                                         @RequestParam(defaultValue = "256") int size,
                                         @RequestParam(defaultValue = "false") boolean db) {
        if (!List.of("kafka", "redis", "rabbit").contains(mq)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "mq must be kafka | redis | rabbit");
        }
        return Map.of("runId", runs.create(mq, db, expected, size).runId);
    }

    public record OrderRequest(Integer userId, Integer amount) {}

    @PostMapping("/orders")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Map<String, Object> order(@RequestParam String runId, @RequestBody(required = false) OrderRequest body) {
        Run run = requireRun(runId);
        runs.markStarted(run);
        long i = run.seq.getAndIncrement();
        publisher.publish(run.mq, List.of(EventFactory.build(run.runId, i, run.size, run.db,
                body == null ? null : body.userId(), body == null ? null : body.amount())));
        return Map.of("accepted", i);
    }

    // final=true면 미완료여도 지연과 dbCount를 계산한다(부하 도구의 마지막 조회용).
    @GetMapping("/runs/{runId}")
    public Map<String, Object> getRun(@PathVariable String runId, @RequestParam(name = "final", defaultValue = "false") boolean fin) {
        return summary(requireRun(runId), fin);
    }

    @DeleteMapping("/runs")
    public Map<String, Object> reset() {
        db.truncate();
        redis.opsForStream().trim(MqConfig.STREAM_KEY, 0); // 이전 회차 스트림 데이터 제거
        amqpAdmin.purgeQueue(MqConfig.RABBIT_QUEUE, false); // 시간 초과로 남은 이전 회차 메시지 제거
        runs.clear();
        return Map.of("reset", true);
    }
}
