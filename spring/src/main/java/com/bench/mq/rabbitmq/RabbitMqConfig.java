package com.bench.mq.rabbitmq;

import com.bench.mq.common.MessageHandler;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

// mq.rabbitmq.enabled=false면 큐, 발행, 컨슈머 모두 등록하지 않는다.
@Configuration
@ConditionalOnProperty(prefix = "mq.rabbitmq", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(RabbitMqProperties.class)
public class RabbitMqConfig {

    @Bean
    Queue rabbitQueue(RabbitMqProperties props) {
        return new Queue(props.queue(), props.durable());
    }

    @Bean
    RabbitMqMessagePublisher rabbitMqMessagePublisher(RabbitTemplate rabbit, AmqpAdmin admin, RabbitMqProperties props) {
        return new RabbitMqMessagePublisher(rabbit, admin, props.queue());
    }

    @Bean
    RabbitMqMessageConsumer rabbitMqMessageConsumer(MessageHandler handler) {
        return new RabbitMqMessageConsumer(handler);
    }
}
