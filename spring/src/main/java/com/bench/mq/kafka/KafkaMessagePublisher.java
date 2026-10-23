package com.bench.mq.kafka;

import com.bench.mq.common.Message;
import com.bench.mq.common.MessagePublisher;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.List;

public class KafkaMessagePublisher implements MessagePublisher {
    private final KafkaTemplate<String, String> kafka;
    private final String topic;

    public KafkaMessagePublisher(KafkaTemplate<String, String> kafka, String topic) {
        this.kafka = kafka; this.topic = topic;
    }

    @Override
    public String type() { return "kafka"; }

    // 비동기 send 후 묶음마다 flush. 같은 key는 같은 파티션으로 가서 순서가 보장된다.
    @Override
    public void publish(List<Message> messages) {
        for (Message m : messages) kafka.send(topic, m.key(), m.payload());
        kafka.flush();
    }
}
