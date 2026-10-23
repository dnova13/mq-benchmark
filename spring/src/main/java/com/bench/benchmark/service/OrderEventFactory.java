package com.bench.benchmark.service;

import com.bench.mq.common.Message;

// 주문 이벤트 생성. Nest와 동일한 규칙(결정적 생성)을 쓴다. key는 userId(Kafka 파티션 기준).
public final class OrderEventFactory {
    private OrderEventFactory() {}

    public static Message build(String runId, long i, int size, boolean db, Integer userId, Integer amount) {
        long uid = userId != null ? userId : ((i * 7919) % 1000) + 1;
        long amt = amount != null ? amount : ((i * 37) % 99900) + 100;
        long now = System.currentTimeMillis();
        String head = "{\"id\":\"" + runId + "-" + i + "\",\"runId\":\"" + runId + "\",\"userId\":" + uid
                + ",\"amount\":" + amt + ",\"createdAt\":" + now + ",\"db\":" + db + ",\"pad\":\"";
        int pad = Math.max(0, size - (head.length() + 2));
        return new Message(Long.toString(uid), head + "x".repeat(pad) + "\"}");
    }
}
