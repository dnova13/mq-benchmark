import { Inject, Injectable, OnModuleInit } from '@nestjs/common';
import { ConfigType } from '@nestjs/config';
import rabbitmqConfig from '../../config/rabbitmq.config';
import { MESSAGE_HANDLER, MessageHandler } from '../../common/messaging/message.interface';
import { RabbitMqConnection } from './rabbitmq.connection';

@Injectable()
export class RabbitMqConsumer implements OnModuleInit {
  constructor(
    private rabbit: RabbitMqConnection,
    @Inject(rabbitmqConfig.KEY) private cfg: ConfigType<typeof rabbitmqConfig>,
    @Inject(MESSAGE_HANDLER) private handler: MessageHandler,
  ) {}

  // prefetch 수만큼 동시에 받아 처리하고, 처리 후 ack한다
  async onModuleInit() {
    const ch = this.rabbit.channel;
    await ch.consume(this.cfg.queue, async (msg) => {
      if (!msg) return;
      await this.handler.handle(msg.content.toString());
      ch.ack(msg);
    });
  }
}
