package com.bench.mq.rabbitmq;

import com.bench.mq.common.Message;
import com.bench.mq.common.MessagePublisher;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.util.List;

public class RabbitMqMessagePublisher implements MessagePublisher {
    private final RabbitTemplate rabbit;
    private final AmqpAdmin admin;
    private final String queue;

    public RabbitMqMessagePublisher(RabbitTemplate rabbit, AmqpAdmin admin, String queue) {
        this.rabbit = rabbit; this.admin = admin; this.queue = queue;
    }

    @Override
    public String type() { return "rabbit"; }

    // 기본 exchange로 큐 이름을 routing key로 보낸다. RabbitTemplate 기본값이 persistent 메시지다.
    @Override
    public void publish(List<Message> messages) {
        for (Message m : messages) rabbit.convertAndSend(queue, m.payload());
    }

    @Override
    public void purge() { admin.purgeQueue(queue, false); }
}
