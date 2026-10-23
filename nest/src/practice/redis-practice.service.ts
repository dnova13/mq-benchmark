import { Inject, Injectable, OnModuleDestroy } from '@nestjs/common';
import { ConfigType } from '@nestjs/config';
import Redis from 'ioredis';
import redisConfig from '../config/redis.config';
import { PracticeOps } from './practice-ops.interface';

// Redis Streams: 읽어도 메시지가 지워지지 않는다(로그). 조회는 XREVRANGE로 최근 것부터
@Injectable()
export class RedisPracticeService implements PracticeOps, OnModuleDestroy {
  readonly type = 'redis';
  readonly destination = 'practice:stream';
  private client?: Redis;

  constructor(@Inject(redisConfig.KEY) private cfg: ConfigType<typeof redisConfig>) {}

  // 처음 쓸 때 연결한다(연습 API를 안 쓰면 연결하지 않음)
  private get redis() {
    return (this.client ??= new Redis({ host: this.cfg.host, port: this.cfg.port }));
  }

  async publish(key: string | undefined, payload: string) {
    const fields = key ? ['key', key, 'payload', payload] : ['payload', payload];
    const id = await this.redis.xadd(this.destination, '*', ...fields); // XADD practice:stream * ...
    return { id };
  }

  async read(count: number) {
    const rows = await this.redis.xrevrange(this.destination, '+', '-', 'COUNT', count);
    return rows.map(([id, f]) => ({ id, fields: Object.fromEntries(f.flatMap((v, i) => (i % 2 ? [] : [[v, f[i + 1]]]))) }));
  }

  async info() {
    const length = await this.redis.xlen(this.destination);
    const groups = (length ? await this.redis.xinfo('GROUPS', this.destination) : []) as any[];
    return {
      length,
      groups: groups.map((g) => {
        const o = Object.fromEntries(g.flatMap((v: any, i: number) => (i % 2 ? [] : [[v, g[i + 1]]])));
        return { name: o.name, consumers: o.consumers, pending: o.pending };
      }),
    };
  }

  async reset() { await this.redis.del(this.destination); }

  onModuleDestroy() { this.client?.disconnect(); }
}
