package com.bench.benchmark.controller;

import com.bench.benchmark.repository.OrderResultRepository;
import com.bench.benchmark.service.OrderEventFactory;
import com.bench.benchmark.service.RunService;
import com.bench.benchmark.service.RunService.Run;
import com.bench.mq.common.Message;
import com.bench.mq.common.MessagePublisher;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@RestController
public class BenchController {
    private static final int CHUNK = 1000;

    private final Map<String, MessagePublisher> publishers; // 켜진 MQ만 들어 있다
    private final OrderResultRepository repository;
    private final RunService runs;

    public BenchController(List<MessagePublisher> publishers, OrderResultRepository repository, RunService runs) {
        this.publishers = publishers.stream().collect(Collectors.toMap(MessagePublisher::type, Function.identity()));
        this.repository = repository; this.runs = runs;
    }

    private MessagePublisher publisher(String mq) {
        MessagePublisher p = publishers.get(mq);
        if (p == null) throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "mq must be one of " + publishers.keySet());
        return p;
    }

    // dbCount(COUNT(*))도 대용량에서 비싸므로 완료됐거나 final일 때만 조회한다.
    private Map<String, Object> summary(Run run, boolean fin) {
        Map<String, Object> m = new LinkedHashMap<>(runs.summary(run, fin));
        if (run.db && m.containsKey("latencyMs")) m.put("dbCount", repository.count(run.runId));
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
        MessagePublisher pub = publisher(mq);
        Run run = runs.create(mq, db, count, size);
        runs.markStarted(run);
        for (int i = 0; i < count; i += CHUNK) {
            List<Message> events = new ArrayList<>();
            for (int j = i; j < Math.min(i + CHUNK, count); j++) events.add(OrderEventFactory.build(run.runId, j, size, db, null, null));
            pub.publish(events);
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
        publisher(mq);
        return Map.of("runId", runs.create(mq, db, expected, size).runId);
    }

    public record OrderRequest(Integer userId, Integer amount) {}

    @PostMapping("/orders")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Map<String, Object> order(@RequestParam String runId, @RequestBody(required = false) OrderRequest body) {
        Run run = requireRun(runId);
        runs.markStarted(run);
        long i = run.seq.getAndIncrement();
        publisher(run.mq).publish(List.of(OrderEventFactory.build(run.runId, i, run.size, run.db,
                body == null ? null : body.userId(), body == null ? null : body.amount())));
        return Map.of("accepted", i);
    }

    // final=true면 미완료여도 지연과 dbCount를 계산한다(부하 도구의 마지막 조회용).
    @GetMapping("/runs/{runId}")
    public Map<String, Object> getRun(@PathVariable String runId, @RequestParam(name = "final", defaultValue = "false") boolean fin) {
        return summary(requireRun(runId), fin);
    }

    // 회차 사이 초기화: 결과 테이블 TRUNCATE + 큐/스트림 비우기 + 메모리 런 삭제
    @DeleteMapping("/runs")
    public Map<String, Object> reset() {
        repository.truncate();
        publishers.values().forEach(MessagePublisher::purge); // 시간 초과로 남은 이전 회차 메시지 제거
        runs.clear();
        return Map.of("reset", true);
    }
}
