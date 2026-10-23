package com.bench.benchmark.repository;

import jakarta.annotation.PostConstruct;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class OrderResultRepository {
    private final JdbcTemplate jdbc;

    public OrderResultRepository(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    @PostConstruct
    void init() {
        jdbc.execute("CREATE TABLE IF NOT EXISTS orders_result ("
                + "id VARCHAR(80) PRIMARY KEY, run_id VARCHAR(40) NOT NULL, user_id INT NOT NULL, "
                + "amount INT NOT NULL, payload TEXT, created_at BIGINT NOT NULL, saved_at BIGINT NOT NULL, "
                + "INDEX idx_run (run_id)) ENGINE=InnoDB");
    }

    /** 중복(at-least-once)은 INSERT IGNORE로 무시. 실제로 저장되면 true */
    public boolean insert(String id, String runId, int userId, int amount, String pad, long createdAt) {
        int n = jdbc.update("INSERT IGNORE INTO orders_result "
                + "(id, run_id, user_id, amount, payload, created_at, saved_at) VALUES (?,?,?,?,?,?,?)",
                id, runId, userId, amount, pad, createdAt, System.currentTimeMillis());
        return n == 1;
    }

    public long count(String runId) {
        Long c = jdbc.queryForObject("SELECT COUNT(*) FROM orders_result WHERE run_id = ?", Long.class, runId);
        return c == null ? 0 : c;
    }

    public void truncate() { jdbc.execute("TRUNCATE TABLE orders_result"); }
}
