import { Inject, Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ConfigType } from '@nestjs/config';
import Redis from 'ioredis';
import redisConfig from '../../config/redis.config';
import { MESSAGE_HANDLER, MessageHandler } from '../../common/messaging/message.interface';
import { REDIS_CLIENT } from './redis-stream.constants';

// 컨슈머 그룹으로 XREADGROUP 후 XACK(at-least-once). 같은 그룹 워커 workers개가 나눠 읽는다.
@Injectable()
export class RedisStreamConsumer implements OnModuleInit, OnModuleDestroy {
  private readers: Redis[] = []; // XREADGROUP BLOCK은 커넥션을 점유하므로 워커마다 따로 사용
  private running = true;

  constructor(
    @Inject(REDIS_CLIENT) private redis: Redis,
    @Inject(redisConfig.KEY) private cfg: ConfigType<typeof redisConfig>,
    @Inject(MESSAGE_HANDLER) private handler: MessageHandler,
  ) {}

  async onModuleInit() {
    try {
      await this.redis.xgroup('CREATE', this.cfg.streamKey, this.cfg.group, '$', 'MKSTREAM');
    } catch (e) {
      if (!String(e).includes('BUSYGROUP')) throw e;
    }
    for (let i = 0; i < this.cfg.workers; i++) {
      const reader = new Redis({ host: this.cfg.host, port: this.cfg.port });
      this.readers.push(reader);
      void this.loop(reader, `consumer-${i}`);
    }
  }

  private async loop(reader: Redis, consumer: string) {
    const { streamKey, group, batchSize, blockMs } = this.cfg;
    while (this.running) {
      try {
        const res: any = await reader.xreadgroup(
          'GROUP', group, consumer, 'COUNT', batchSize, 'BLOCK', blockMs, 'STREAMS', streamKey, '>',
        );
        if (!res) continue;
        for (const [, entries] of res) {
          // 필드는 [PAYLOAD_FIELD, payload] 순서다
          await Promise.all(entries.map(([, fields]: [string, string[]]) => this.handler.handle(fields[1])));
          await this.redis.xack(streamKey, group, ...entries.map(([id]: [string]) => id));
        }
      } catch (e) {
        if (this.running) await new Promise((r) => setTimeout(r, 500));
      }
    }
  }

  onModuleDestroy() {
    this.running = false;
    this.readers.forEach((r) => r.disconnect());
  }
}
