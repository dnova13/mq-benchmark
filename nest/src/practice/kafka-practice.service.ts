import { Inject, Injectable, OnModuleDestroy } from '@nestjs/common';
import { ConfigType } from '@nestjs/config';
import { Kafka, Producer, logLevel } from 'kafkajs';
import kafkaConfig from '../config/kafka.config';
import { PracticeOps } from './practice-ops.interface';

const PARTITIONS = 3;

// Kafka: 읽어도 메시지가 지워지지 않는다(로그). 조회는 일회용 그룹으로 파티션마다 끝에서 count건 앞부터 읽는다
@Injectable()
export class KafkaPracticeService implements PracticeOps, OnModuleDestroy {
  readonly type = 'kafka';
  readonly destination = 'practice-topic';
  private kafka: Kafka;
  private producer?: Promise<Producer>;

  constructor(@Inject(kafkaConfig.KEY) cfg: ConfigType<typeof kafkaConfig>) {
    this.kafka = new Kafka({ clientId: 'practice-nest', brokers: cfg.brokers, logLevel: logLevel.WARN });
  }

  private async ensureTopic() {
    const admin = this.kafka.admin();
    await admin.connect();
    try {
      if (!(await admin.listTopics()).includes(this.destination)) {
        await admin.createTopics({ topics: [{ topic: this.destination, numPartitions: PARTITIONS, replicationFactor: 1 }] });
      }
    } finally {
      await admin.disconnect();
    }
  }

  async publish(key: string | undefined, payload: string) {
    await this.ensureTopic();
    const producer = await (this.producer ??= (async () => { const p = this.kafka.producer(); await p.connect(); return p; })());
    const [r] = await producer.send({ topic: this.destination, messages: [{ key, value: payload }] }); // key가 같으면 같은 파티션
    return { partition: r.partition, offset: Number(r.baseOffset) };
  }

  async read(count: number) {
    await this.ensureTopic();
    const admin = this.kafka.admin();
    await admin.connect();
    const offsets = await admin.fetchTopicOffsets(this.destination);
    const starts = offsets.map((o) => ({ partition: o.partition, from: Math.max(Number(o.low), Number(o.high) - count), high: Number(o.high) }));
    const expected = starts.reduce((n, s) => n + (s.high - s.from), 0);
    const out: Record<string, unknown>[] = [];
    if (expected > 0) {
      const groupId = `practice-peek-${Date.now()}`; // 일회용 그룹. 조회 후 삭제한다
      const consumer = this.kafka.consumer({ groupId });
      await consumer.connect();
      await consumer.subscribe({ topic: this.destination, fromBeginning: true });
      let done!: () => void;
      const finished = new Promise<void>((r) => (done = r));
      await consumer.run({
        autoCommit: false,
        eachMessage: async ({ partition, message }) => {
          out.push({ partition, offset: Number(message.offset), key: message.key?.toString() ?? null,
            value: message.value?.toString(), timestamp: Number(message.timestamp) });
          if (out.length >= expected) done();
        },
      });
      for (const s of starts) consumer.seek({ topic: this.destination, partition: s.partition, offset: String(s.from) });
      await Promise.race([finished, new Promise((r) => setTimeout(r, 5000))]);
      await consumer.disconnect();
      await admin.deleteGroups([groupId]).catch(() => undefined);
    }
    await admin.disconnect();
    return out.sort((a, b) => (b.timestamp as number) - (a.timestamp as number)).slice(0, count);
  }

  async info() {
    const admin = this.kafka.admin();
    await admin.connect();
    try {
      if (!(await admin.listTopics()).includes(this.destination)) return { exists: false };
      const offsets = await admin.fetchTopicOffsets(this.destination);
      return { exists: true, endOffsets: Object.fromEntries(offsets.map((o) => [`partition-${o.partition}`, Number(o.high)])) };
    } finally {
      await admin.disconnect();
    }
  }

  async reset() {
    const admin = this.kafka.admin();
    await admin.connect();
    try {
      if ((await admin.listTopics()).includes(this.destination)) await admin.deleteTopics({ topics: [this.destination] });
    } finally {
      await admin.disconnect();
    }
  }

  async onModuleDestroy() { if (this.producer) await (await this.producer).disconnect(); }
}
