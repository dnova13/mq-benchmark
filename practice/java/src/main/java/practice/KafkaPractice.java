package practice;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Kafka 최소 예제 (kafka-clients)
 * <pre>
 * ./gradlew run -Pex=KafkaPractice --args="produce 5"               practice-topic(파티션 3)에 발행. key = user-(i%3)
 * ./gradlew run -Pex=KafkaPractice --args="consume practice-java"   그룹으로 처음부터 소비 (Ctrl+C로 종료)
 * ./gradlew run -Pex=KafkaPractice --args="info"                    파티션별 마지막 오프셋
 * </pre>
 */
public class KafkaPractice {
    static final String TOPIC = "practice-topic";
    static final String BROKER = System.getenv().getOrDefault("KAFKA_BROKER", "localhost:39092");

    public static void main(String[] args) throws Exception {
        String cmd = args.length > 0 ? args[0] : "info";
        ensureTopic();
        switch (cmd) {
            case "produce" -> produce(args.length > 1 ? Integer.parseInt(args[1]) : 5);
            case "consume" -> consume(args.length > 1 ? args[1] : "practice-java");
            default -> info();
        }
    }

    static void ensureTopic() throws Exception {
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER))) {
            if (!admin.listTopics().names().get().contains(TOPIC)) {
                admin.createTopics(List.of(new NewTopic(TOPIC, 3, (short) 1))).all().get();
            }
        }
    }

    static void produce(int count) throws Exception {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, "all"); // 브로커가 저장을 확인한 뒤 성공으로 본다
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(p)) {
            for (int i = 0; i < count; i++) {
                String key = "user-" + (i % 3); // 같은 key → 같은 파티션 → 순서 보장
                String value = "{\"orderId\":" + (i + 1) + ",\"createdAt\":" + System.currentTimeMillis() + "}";
                RecordMetadata md = producer.send(new ProducerRecord<>(TOPIC, key, value)).get(); // 예제라 건마다 결과를 기다린다
                System.out.printf("발행 → key=%s partition %d, offset %d%n", key, md.partition(), md.offset());
            }
        }
    }

    static void consume(String groupId) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // 커밋 오프셋이 없을 때만 처음부터
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p)) {
            consumer.subscribe(List.of(TOPIC));
            System.out.println("그룹 " + groupId + "로 대기 중... (Ctrl+C로 종료)");
            while (true) {
                // poll로 가져온 뒤 다음 poll 때 오프셋이 자동 커밋된다(enable.auto.commit 기본값 true)
                for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofSeconds(1))) {
                    System.out.printf("받음 p%d offset %d key=%s %s%n", r.partition(), r.offset(), r.key(), r.value());
                }
            }
        }
    }

    static void info() throws Exception {
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER))) {
            Map<TopicPartition, OffsetSpec> req = IntStream.range(0, 3).boxed()
                    .collect(Collectors.toMap(i -> new TopicPartition(TOPIC, i), i -> OffsetSpec.latest()));
            admin.listOffsets(req).all().get()
                    .forEach((tp, info) -> System.out.println(tp + " 마지막 오프셋 " + info.offset()));
            System.out.println("그룹: " + admin.listConsumerGroups().all().get());
        }
    }
}
