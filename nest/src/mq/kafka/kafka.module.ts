import { DynamicModule, Module } from '@nestjs/common';
import { ConfigModule, ConfigType } from '@nestjs/config';
import { Kafka } from 'kafkajs';
import kafkaConfig from '../../config/kafka.config';
import { MESSAGE_HANDLER, MqModuleOptions } from '../../common/messaging/message.interface';
import { KAFKA_CLIENT } from './kafka.constants';
import { KafkaPublisher } from './kafka.publisher';
import { KafkaConsumer } from './kafka.consumer';

// 사용: KafkaModule.register({ imports: [OrderModule], handler: OrderProcessorService })
@Module({})
export class KafkaModule {
  static register(options: MqModuleOptions): DynamicModule {
    return {
      module: KafkaModule,
      imports: [ConfigModule.forFeature(kafkaConfig), ...(options.imports ?? [])],
      providers: [
        {
          // 클라이언트를 만들기 전에 토픽을 생성해 둔다(프로듀서와 컨슈머가 같은 클라이언트를 쓴다)
          provide: KAFKA_CLIENT,
          inject: [kafkaConfig.KEY],
          useFactory: async (cfg: ConfigType<typeof kafkaConfig>) => {
            const kafka = new Kafka({ clientId: cfg.clientId, brokers: cfg.brokers });
            const admin = kafka.admin();
            await admin.connect();
            await admin.createTopics({
              topics: [{ topic: cfg.topic, numPartitions: cfg.partitions, replicationFactor: cfg.replicationFactor }],
            });
            await admin.disconnect();
            return kafka;
          },
        },
        { provide: MESSAGE_HANDLER, useExisting: options.handler },
        KafkaPublisher,
        KafkaConsumer,
      ],
      exports: [KafkaPublisher],
    };
  }
}
