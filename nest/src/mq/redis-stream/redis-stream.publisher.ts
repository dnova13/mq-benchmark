import { Inject, Injectable } from '@nestjs/common';
import { ConfigType } from '@nestjs/config';
import Redis from 'ioredis';
import redisConfig from '../../config/redis.config';
import { Message, MessagePublisher } from '../../common/messaging/message.interface';
import { PAYLOAD_FIELD, REDIS_CLIENT } from './redis-stream.constants';

@Injectable()
export class RedisStreamPublisher implements MessagePublisher {
  readonly type = 'redis';

  constructor(
    @Inject(REDIS_CLIENT) private redis: Redis,
    @Inject(redisConfig.KEY) private cfg: ConfigType<typeof redisConfig>,
  ) {}

  // 파이프라인으로 XADD를 한 번에 보낸다
  async publish(messages: Message[]) {
    const pipe = this.redis.pipeline();
    for (const m of messages) pipe.xadd(this.cfg.streamKey, '*', PAYLOAD_FIELD, m.payload);
    await pipe.exec();
  }

  async purge() { await this.redis.xtrim(this.cfg.streamKey, 'MAXLEN', 0); }
}
