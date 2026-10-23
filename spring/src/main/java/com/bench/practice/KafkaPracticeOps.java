package com.bench.practice;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

// Kafka: 읽어도 메시지가 지워지지 않는다(로그). 조회는 그룹 없이 파티션마다 끝에서 count건 앞으로 이동해 읽는다
@Component
public class KafkaPracticeOps implements PracticeOps {
    static final String TOPIC = "practice-topic";
    static final int PARTITIONS = 3;
    private final KafkaTemplate<String, String> kafka;
    private final KafkaAdmin kafkaAdmin;
    private final ConsumerFactory<String, String> consumerFactory;

    public KafkaPracticeOps(KafkaTemplate<String, String> kafka, KafkaAdmin kafkaAdmin, ConsumerFactory<String, String> consumerFactory) {
        this.kafka = kafka; this.kafkaAdmin = kafkaAdmin; this.consumerFactory = consumerFactory;
    }

    @Override public String type() { return "kafka"; }
    @Override public String destination() { return TOPIC; }

    private AdminClient admin() { return AdminClient.create(kafkaAdmin.getConfigurationProperties()); }

    private void ensureTopic() throws Exception {
        try (AdminClient a = admin()) {
            if (!a.listTopics().names().get().contains(TOPIC)) a.createTopics(List.of(new NewTopic(TOPIC, PARTITIONS, (short) 1))).all().get();
        }
    }

    @Override
    public Map<String, Object> publish(String key, String payload) {
        try {
            ensureTopic();
            RecordMetadata md = kafka.send(TOPIC, key, payload).get().getRecordMetadata(); // key가 같으면 같은 파티션
            return Map.of("partition", md.partition(), "offset", md.offset());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public List<Map<String, Object>> read(int count, boolean ack) {
        try {
            ensureTopic();
            Properties p = new Properties();
            p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // 오프셋을 커밋하지 않는 조회 전용 컨슈머
            List<TopicPartition> tps = IntStream.range(0, PARTITIONS).mapToObj(i -> new TopicPartition(TOPIC, i)).toList();
            List<Map<String, Object>> out = new ArrayList<>();
            try (Consumer<String, String> c = consumerFactory.createConsumer("practice-peek", null, null, p)) {
                c.assign(tps); // 그룹 구독(subscribe) 대신 파티션을 직접 지정
                Map<TopicPartition, Long> end = c.endOffsets(tps), begin = c.beginningOffsets(tps);
                long expected = 0;
                for (TopicPartition tp : tps) {
                    long from = Math.max(begin.get(tp), end.get(tp) - count);
                    c.seek(tp, from);
                    expected += end.get(tp) - from;
                }
                long deadline = System.currentTimeMillis() + 3000;
                while (out.size() < expected && System.currentTimeMillis() < deadline) {
                    for (ConsumerRecord<String, String> r : c.poll(Duration.ofMillis(300))) {
                        out.add(Map.of("partition", r.partition(), "offset", r.offset(), "key", String.valueOf(r.key()), "value", r.value(), "timestamp", r.timestamp()));
                    }
                }
            }
            out.sort(Comparator.comparing(m -> -((Long) m.get("timestamp"))));
            return out.subList(0, Math.min(count, out.size()));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public Map<String, Object> info() {
        try (AdminClient a = admin()) {
            if (!a.listTopics().names().get().contains(TOPIC)) return Map.of("exists", false);
            Map<TopicPartition, OffsetSpec> req = IntStream.range(0, PARTITIONS).boxed()
                    .collect(Collectors.toMap(i -> new TopicPartition(TOPIC, i), i -> OffsetSpec.latest()));
            Map<String, Long> ends = new TreeMap<>();
            a.listOffsets(req).all().get().forEach((tp, o) -> ends.put("partition-" + tp.partition(), o.offset()));
            return Map.of("exists", true, "endOffsets", ends);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void reset() {
        try (AdminClient a = admin()) {
            if (a.listTopics().names().get().contains(TOPIC)) a.deleteTopics(List.of(TOPIC)).all().get();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
