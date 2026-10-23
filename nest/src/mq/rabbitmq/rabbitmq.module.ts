import { DynamicModule, Module } from '@nestjs/common';
import { ConfigModule, ConfigType } from '@nestjs/config';
import rabbitmqConfig from '../../config/rabbitmq.config';
import { MESSAGE_HANDLER, MqModuleOptions } from '../../common/messaging/message.interface';
import { RabbitMqConnection } from './rabbitmq.connection';
import { RabbitMqPublisher } from './rabbitmq.publisher';
import { RabbitMqConsumer } from './rabbitmq.consumer';

// 사용: RabbitMqModule.register({ imports: [OrderModule], handler: OrderProcessorService })
@Module({})
export class RabbitMqModule {
  static register(options: MqModuleOptions): DynamicModule {
    return {
      module: RabbitMqModule,
      imports: [ConfigModule.forFeature(rabbitmqConfig), ...(options.imports ?? [])],
      providers: [
        {
          provide: RabbitMqConnection,
          inject: [rabbitmqConfig.KEY],
          useFactory: (cfg: ConfigType<typeof rabbitmqConfig>) =>
            RabbitMqConnection.create(cfg.url, cfg.queue, cfg.durable, cfg.prefetch),
        },
        { provide: MESSAGE_HANDLER, useExisting: options.handler },
        RabbitMqPublisher,
        RabbitMqConsumer,
      ],
      exports: [RabbitMqPublisher],
    };
  }
}
