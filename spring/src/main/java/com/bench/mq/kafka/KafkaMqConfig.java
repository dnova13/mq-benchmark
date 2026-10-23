package com.bench.mq.kafka;

import com.bench.mq.common.MessageHandler;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;

// mq.kafka.enabled=false면 토픽, 발행, 컨슈머 모두 등록하지 않는다.
@Configuration
@ConditionalOnProperty(prefix = "mq.kafka", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(KafkaMqProperties.class)
public class KafkaMqConfig {

    @Bean
    NewTopic kafkaTopic(KafkaMqProperties props) {
        return TopicBuilder.name(props.topic()).partitions(props.partitions()).replicas(props.replicas()).build();
    }

    @Bean
    KafkaMessagePublisher kafkaMessagePublisher(KafkaTemplate<String, String> kafka, KafkaMqProperties props) {
        return new KafkaMessagePublisher(kafka, props.topic());
    }

    @Bean
    KafkaMessageConsumer kafkaMessageConsumer(MessageHandler handler) {
        return new KafkaMessageConsumer(handler);
    }
}
