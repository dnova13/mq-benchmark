import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Kafka, Producer, Consumer } from 'kafkajs';
import { ProcessorService } from './processor.service';
import { userIdOf } from './event';

export const KAFKA_TOPIC = 'bench-topic';

@Injectable()
export class KafkaService implements OnModuleInit, OnModuleDestroy {
  private kafka = new Kafka({ clientId: 'bench-nest', brokers: ['localhost:39092'] });
  private producer: Producer = this.kafka.producer();
  // 큰 배치를 DB에 저장하는 동안 세션이 끊기지 않도록 기본 30초보다 길게 둔다.
  private consumer: Consumer = this.kafka.consumer({ groupId: 'bench-nest-group', sessionTimeout: 60000 });

  constructor(private processor: ProcessorService) {}

  async onModuleInit() {
    const admin = this.kafka.admin();
    await admin.connect();
    await admin.createTopics({ topics: [{ topic: KAFKA_TOPIC, numPartitions: 10, replicationFactor: 1 }] });
    await admin.disconnect();

    await this.producer.connect();
    await this.consumer.connect();
    await this.consumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false });
    await this.consumer.run({
      partitionsConsumedConcurrently: 10,
      eachBatch: async ({ batch, resolveOffset, heartbeat }) => {
        // 배치 내 메시지를 병렬 처리 (DB 풀 10이 동시성을 제한). 1,000건마다 heartbeat로 세션 유지
        const msgs = batch.messages;
        for (let i = 0; i < msgs.length; i += 1000) {
          const part = msgs.slice(i, i + 1000);
          await Promise.all(part.map((m) => this.processor.process(m.value.toString())));
          for (const m of part) resolveOffset(m.offset);
          await heartbeat();
        }
      },
    });
  }

  // userId를 키로 써서 같은 사용자는 같은 파티션(순서 보장)
  async publish(events: string[]) {
    await this.producer.send({
      topic: KAFKA_TOPIC,
      messages: events.map((v) => ({ key: String(userIdOf(v)), value: v })),
    });
  }

  async onModuleDestroy() {
    await this.consumer.disconnect();
    await this.producer.disconnect();
  }
}
