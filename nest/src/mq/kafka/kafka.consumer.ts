import { Inject, Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ConfigType } from '@nestjs/config';
import { Consumer, Kafka } from 'kafkajs';
import kafkaConfig from '../../config/kafka.config';
import { MESSAGE_HANDLER, MessageHandler } from '../../common/messaging/message.interface';
import { KAFKA_CLIENT } from './kafka.constants';

const HEARTBEAT_EVERY = 1000;

@Injectable()
export class KafkaConsumer implements OnModuleInit, OnModuleDestroy {
  private consumer: Consumer;

  constructor(
    @Inject(KAFKA_CLIENT) kafka: Kafka,
    @Inject(kafkaConfig.KEY) private cfg: ConfigType<typeof kafkaConfig>,
    @Inject(MESSAGE_HANDLER) private handler: MessageHandler,
  ) {
    this.consumer = kafka.consumer({ groupId: cfg.groupId, sessionTimeout: cfg.sessionTimeout });
  }

  async onModuleInit() {
    await this.consumer.connect();
    await this.consumer.subscribe({ topic: this.cfg.topic, fromBeginning: false });
    await this.consumer.run({
      partitionsConsumedConcurrently: this.cfg.partitionsConsumedConcurrently,
      eachBatch: async ({ batch, resolveOffset, heartbeat }) => {
        // 배치 내 메시지를 병렬 처리 (DB 풀 등이 동시성을 제한). 1,000건마다 heartbeat로 세션 유지
        const msgs = batch.messages;
        for (let i = 0; i < msgs.length; i += HEARTBEAT_EVERY) {
          const part = msgs.slice(i, i + HEARTBEAT_EVERY);
          await Promise.all(part.map((m) => this.handler.handle(m.value.toString())));
          for (const m of part) resolveOffset(m.offset);
          await heartbeat();
        }
      },
    });
  }

  async onModuleDestroy() { await this.consumer.disconnect(); }
}
