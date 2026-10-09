import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import Redis from 'ioredis';
import { ProcessorService } from './processor.service';

export const STREAM_KEY = 'bench:stream';
const GROUP = 'bench-nest-group';
const WORKERS = 10;

@Injectable()
export class RedisStreamService implements OnModuleInit, OnModuleDestroy {
  private redis = new Redis({ host: 'localhost', port: 36397 });
  private readers: Redis[] = []; // XREADGROUP BLOCK은 커넥션을 점유하므로 워커마다 따로 사용
  private running = true;

  constructor(private processor: ProcessorService) {}

  async onModuleInit() {
    try {
      await this.redis.xgroup('CREATE', STREAM_KEY, GROUP, '$', 'MKSTREAM');
    } catch (e) {
      if (!String(e).includes('BUSYGROUP')) throw e;
    }
    for (let i = 0; i < WORKERS; i++) {
      const reader = new Redis({ host: 'localhost', port: 36397 });
      this.readers.push(reader);
      void this.loop(reader, `consumer-${i}`);
    }
  }

  private async loop(reader: Redis, consumer: string) {
    while (this.running) {
      try {
        const res: any = await reader.xreadgroup(
          'GROUP', GROUP, consumer, 'COUNT', 100, 'BLOCK', 1000, 'STREAMS', STREAM_KEY, '>',
        );
        if (!res) continue;
        for (const [, entries] of res) {
          await Promise.all(entries.map(([, fields]: [string, string[]]) => this.processor.process(fields[1])));
          await this.redis.xack(STREAM_KEY, GROUP, ...entries.map(([id]: [string]) => id));
        }
      } catch (e) {
        if (this.running) await new Promise((r) => setTimeout(r, 500));
      }
    }
  }

  async publish(events: string[]) {
    const pipe = this.redis.pipeline();
    for (const e of events) pipe.xadd(STREAM_KEY, '*', 'data', e);
    await pipe.exec();
  }

  async trim() {
    await this.redis.xtrim(STREAM_KEY, 'MAXLEN', 0);
  }

  async onModuleDestroy() {
    this.running = false;
    this.readers.forEach((r) => r.disconnect());
    this.redis.disconnect();
  }
}
