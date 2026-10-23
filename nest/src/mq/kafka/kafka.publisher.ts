import { Inject, Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ConfigType } from '@nestjs/config';
import { Kafka, Producer } from 'kafkajs';
import kafkaConfig from '../../config/kafka.config';
import { Message, MessagePublisher } from '../../common/messaging/message.interface';
import { KAFKA_CLIENT } from './kafka.constants';

@Injectable()
export class KafkaPublisher implements MessagePublisher, OnModuleInit, OnModuleDestroy {
  readonly type = 'kafka';
  private producer: Producer;

  constructor(
    @Inject(KAFKA_CLIENT) kafka: Kafka,
    @Inject(kafkaConfig.KEY) private cfg: ConfigType<typeof kafkaConfig>,
  ) {
    this.producer = kafka.producer();
  }

  async onModuleInit() { await this.producer.connect(); }

  // key가 같으면 같은 파티션으로 가서 순서가 보장된다
  async publish(messages: Message[]) {
    await this.producer.send({
      topic: this.cfg.topic,
      messages: messages.map((m) => ({ key: m.key, value: m.payload })),
    });
  }

  // Kafka는 로그를 지우지 않는다(보존 기간 설정을 따른다)
  async purge() {}

  async onModuleDestroy() { await this.producer.disconnect(); }
}
