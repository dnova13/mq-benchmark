package com.bench.mq;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

// 세 MQ 컨슈머가 공통으로 호출하는 처리 로직
@Component
public class Processor {
    private final ObjectMapper mapper = new ObjectMapper();
    private final DbService db;
    private final RunRegistry runs;

    public Processor(DbService db, RunRegistry runs) { this.db = db; this.runs = runs; }

    public void process(String raw) {
        try {
            JsonNode e = mapper.readTree(raw);
            if (runs.get(e.get("runId").asText()) == null) return; // 이전 회차 잔여 메시지는 저장하지 않는다
            if (e.get("db").asBoolean()) {
                boolean inserted = db.insert(e.get("id").asText(), e.get("runId").asText(), e.get("userId").asInt(),
                        e.get("amount").asInt(), e.get("pad").asText(), e.get("createdAt").asLong());
                if (!inserted) return; // 중복 메시지
            }
            runs.record(e.get("runId").asText(), e.get("createdAt").asLong());
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }
}
