package com.bench.practice;

import com.rabbitmq.client.GetResponse;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueInformation;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

// RabbitMQ: 꺼내서 ack하면 큐에서 사라진다. ack=false면 꺼내 본 뒤 다시 넣는다(엿보기)
@Component
public class RabbitPracticeOps implements PracticeOps {
    static final String QUEUE = "practice-queue";
    private final RabbitTemplate rabbit;
    private final AmqpAdmin admin;

    public RabbitPracticeOps(RabbitTemplate rabbit, AmqpAdmin admin) { this.rabbit = rabbit; this.admin = admin; }

    @Override public String type() { return "rabbit"; }
    @Override public String destination() { return QUEUE; }

    private void declare() { admin.declareQueue(new Queue(QUEUE, true)); }

    @Override
    public Map<String, Object> publish(String key, String payload) {
        declare();
        rabbit.convertAndSend(QUEUE, payload); // 기본 exchange, routing key = 큐 이름. 기본값이 persistent
        return Map.of("queued", true);
    }

    @Override
    public List<Map<String, Object>> read(int count, boolean ack) {
        declare();
        return rabbit.execute(ch -> {
            List<Map<String, Object>> out = new ArrayList<>();
            List<Long> tags = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                GetResponse r = ch.basicGet(QUEUE, false); // 한 건씩 당겨 온다(pull)
                if (r == null) break;
                out.add(Map.of("payload", new String(r.getBody(), StandardCharsets.UTF_8),
                        "redelivered", r.getEnvelope().isRedeliver(), "remaining", r.getMessageCount()));
                tags.add(r.getEnvelope().getDeliveryTag());
            }
            // ack: 큐에서 제거 / nack(requeue=true): 다시 큐로
            for (long tag : tags) { if (ack) ch.basicAck(tag, false); else ch.basicNack(tag, false, true); }
            return out;
        });
    }

    @Override
    public Map<String, Object> info() {
        declare();
        QueueInformation q = admin.getQueueInfo(QUEUE);
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("messages", q == null ? 0 : q.getMessageCount());
        m.put("consumers", q == null ? 0 : q.getConsumerCount());
        return m;
    }

    @Override public void reset() { admin.deleteQueue(QUEUE); }
}
