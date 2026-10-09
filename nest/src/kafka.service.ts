import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Kafka, Producer, Consumer } from 'kafkajs';
import { ProcessorService } from './processor.service';
import { userIdOf } from './event';

export const KAFKA_TOPIC = 'bench-topic';

@Injectable()
export class KafkaService implements OnModuleInit, OnModuleDestroy {
  private kafka = new Kafka({ clientId: 'bench-nest', brokers: ['localhost:39092'] });
  private producer: Producer = this.kafka.producer();
  private consumer: Consumer = this.kafka.consumer({ groupId: 'bench-nest-group' });

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
        // 배치 내 메시지를 병렬 처리 (DB 풀 10이 동시성을 제한)
        await Promise.all(batch.messages.map((m) => this.processor.process(m.value.toString())));
        for (const m of batch.messages) resolveOffset(m.offset);
        await heartbeat();
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
