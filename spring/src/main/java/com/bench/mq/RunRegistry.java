package com.bench.mq;

import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class RunRegistry {
    public static class Run {
        public final String runId, mq;
        public final boolean db;
        public final long expected;
        public final int size;
        public final AtomicLong seq = new AtomicLong();
        public final AtomicLong processed = new AtomicLong();
        public volatile long startedAt;
        public volatile long lastProcessedAt;
        public final List<Long> latencies = Collections.synchronizedList(new ArrayList<>());

        Run(String runId, String mq, boolean db, long expected, int size) {
            this.runId = runId; this.mq = mq; this.db = db; this.expected = expected; this.size = size;
        }
    }

    private final Map<String, Run> runs = new ConcurrentHashMap<>();

    public Run create(String mq, boolean db, long expected, int size) {
        Run r = new Run(mq + "-" + System.currentTimeMillis(), mq, db, expected, size);
        runs.put(r.runId, r);
        return r;
    }

    public Run get(String runId) { return runs.get(runId); }

    public void clear() { runs.clear(); }

    public synchronized void markStarted(Run r) {
        if (r.startedAt == 0) r.startedAt = System.currentTimeMillis();
    }

    public void record(String runId, long createdAt) {
        Run r = runs.get(runId);
        if (r == null) return;
        long now = System.currentTimeMillis();
        r.lastProcessedAt = now;
        r.latencies.add(now - createdAt);
        r.processed.incrementAndGet();
    }

    private static long pct(List<Long> sorted, int p) {
        if (sorted.isEmpty()) return 0;
        int idx = Math.min(sorted.size() - 1, (int) Math.ceil(p / 100.0 * sorted.size()) - 1);
        return sorted.get(Math.max(idx, 0));
    }

    public Map<String, Object> summary(Run r) {
        List<Long> sorted;
        synchronized (r.latencies) { sorted = new ArrayList<>(r.latencies); }
        Collections.sort(sorted);
        long processed = r.processed.get();
        long totalMs = (r.lastProcessedAt != 0 && r.startedAt != 0) ? r.lastProcessedAt - r.startedAt : 0;
        Map<String, Object> lat = new LinkedHashMap<>();
        lat.put("p50", pct(sorted, 50)); lat.put("p95", pct(sorted, 95)); lat.put("p99", pct(sorted, 99));
        lat.put("max", sorted.isEmpty() ? 0 : sorted.get(sorted.size() - 1));
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("runId", r.runId); m.put("mq", r.mq); m.put("db", r.db); m.put("expected", r.expected);
        m.put("processed", processed); m.put("done", processed >= r.expected); m.put("totalMs", totalMs);
        m.put("tps", totalMs > 0 ? Math.round(processed * 1000.0 / totalMs) : 0);
        m.put("latencyMs", lat);
        return m;
    }
}
