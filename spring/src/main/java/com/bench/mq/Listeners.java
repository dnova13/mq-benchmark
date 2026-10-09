package com.bench.mq;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class Listeners {
    private final Processor processor;

    public Listeners(Processor processor) { this.processor = processor; }

    @KafkaListener(topics = MqConfig.KAFKA_TOPIC)
    void onKafka(String msg) { processor.process(msg); }

    @RabbitListener(queues = MqConfig.RABBIT_QUEUE)
    void onRabbit(String msg) { processor.process(msg); }
}
