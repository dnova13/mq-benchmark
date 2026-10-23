import { DynamicModule, Inject, Module, OnApplicationShutdown } from '@nestjs/common';
import { ConfigModule, ConfigType } from '@nestjs/config';
import Redis from 'ioredis';
import redisConfig from '../../config/redis.config';
import { MESSAGE_HANDLER, MqModuleOptions } from '../../common/messaging/message.interface';
import { REDIS_CLIENT } from './redis-stream.constants';
import { RedisStreamPublisher } from './redis-stream.publisher';
import { RedisStreamConsumer } from './redis-stream.consumer';

// 사용: RedisStreamModule.register({ imports: [OrderModule], handler: OrderProcessorService })
@Module({})
export class RedisStreamModule implements OnApplicationShutdown {
  constructor(@Inject(REDIS_CLIENT) private redis: Redis) {}

  static register(options: MqModuleOptions): DynamicModule {
    return {
      module: RedisStreamModule,
      imports: [ConfigModule.forFeature(redisConfig), ...(options.imports ?? [])],
      providers: [
        {
          // 발행(XADD)과 XACK용 공유 커넥션. 블로킹 읽기는 컨슈머가 워커마다 따로 연다.
          provide: REDIS_CLIENT,
          inject: [redisConfig.KEY],
          useFactory: (cfg: ConfigType<typeof redisConfig>) => new Redis({ host: cfg.host, port: cfg.port }),
        },
        { provide: MESSAGE_HANDLER, useExisting: options.handler },
        RedisStreamPublisher,
        RedisStreamConsumer,
      ],
      exports: [RedisStreamPublisher],
    };
  }

  onApplicationShutdown() { this.redis.disconnect(); }
}
