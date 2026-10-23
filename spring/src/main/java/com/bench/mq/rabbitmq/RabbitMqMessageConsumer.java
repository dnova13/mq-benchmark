package com.bench.mq.rabbitmq;

import com.bench.mq.common.MessageHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;

// prefetch와 동시성은 spring.rabbitmq.listener.simple.*로 정한다. 정상 처리 후 자동 ack한다.
public class RabbitMqMessageConsumer {
    private final MessageHandler handler;

    public RabbitMqMessageConsumer(MessageHandler handler) { this.handler = handler; }

    @RabbitListener(queues = "${mq.rabbitmq.queue}")
    public void onMessage(String payload) { handler.handle(payload); }
}
