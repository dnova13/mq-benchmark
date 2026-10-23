import { OnApplicationShutdown } from '@nestjs/common';
import * as amqp from 'amqplib';

/** 연결 1개와 채널 1개를 발행과 소비가 함께 쓴다. RabbitMqModule의 팩토리가 만든다. */
export class RabbitMqConnection implements OnApplicationShutdown {
  private constructor(private conn: amqp.ChannelModel, readonly channel: amqp.Channel) {}

  static async create(url: string, queue: string, durable: boolean, prefetch: number) {
    const conn = await amqp.connect(url);
    const channel = await conn.createChannel();
    await channel.assertQueue(queue, { durable });
    await channel.prefetch(prefetch);
    return new RabbitMqConnection(conn, channel);
  }

  async onApplicationShutdown() {
    await this.channel.close().catch(() => undefined);
    await this.conn.close().catch(() => undefined);
  }
}
