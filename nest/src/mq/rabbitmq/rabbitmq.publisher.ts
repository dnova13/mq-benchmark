import { Inject, Injectable } from '@nestjs/common';
import { ConfigType } from '@nestjs/config';
import rabbitmqConfig from '../../config/rabbitmq.config';
import { Message, MessagePublisher } from '../../common/messaging/message.interface';
import { RabbitMqConnection } from './rabbitmq.connection';

@Injectable()
export class RabbitMqPublisher implements MessagePublisher {
  readonly type = 'rabbit';

  constructor(
    private rabbit: RabbitMqConnection,
    @Inject(rabbitmqConfig.KEY) private cfg: ConfigType<typeof rabbitmqConfig>,
  ) {}

  // 쓰기 버퍼가 차면(false 반환) drain을 기다린다. 기다리지 않으면 대용량에서 전부 Node 메모리에 쌓인다.
  async publish(messages: Message[]) {
    const ch = this.rabbit.channel;
    for (const m of messages) {
      if (!ch.sendToQueue(this.cfg.queue, Buffer.from(m.payload), { persistent: this.cfg.persistent })) {
        await new Promise((r) => ch.once('drain', r));
      }
    }
  }

  async purge() { await this.rabbit.channel.purgeQueue(this.cfg.queue); }
}
