import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import * as amqp from 'amqplib';
import { ProcessorService } from './processor.service';

export const RABBIT_QUEUE = 'bench-queue';

@Injectable()
export class RabbitService implements OnModuleInit, OnModuleDestroy {
  private conn: amqp.ChannelModel;
  private channel: amqp.Channel;

  constructor(private processor: ProcessorService) {}

  async onModuleInit() {
    this.conn = await amqp.connect('amqp://guest:guest@localhost:35672');
    this.channel = await this.conn.createChannel();
    await this.channel.assertQueue(RABBIT_QUEUE, { durable: true });
    await this.channel.prefetch(100);
    await this.channel.consume(RABBIT_QUEUE, async (msg) => {
      if (!msg) return;
      await this.processor.process(msg.content.toString());
      this.channel.ack(msg);
    });
  }

  // 쓰기 버퍼가 차면(false 반환) drain을 기다린다. 기다리지 않으면 대용량에서 전부 Node 메모리에 쌓인다.
  async publish(events: string[]) {
    for (const e of events) {
      if (!this.channel.sendToQueue(RABBIT_QUEUE, Buffer.from(e), { persistent: true })) {
        await new Promise((r) => this.channel.once('drain', r));
      }
    }
  }

  async purge() { await this.channel.purgeQueue(RABBIT_QUEUE); }

  async onModuleDestroy() {
    await this.channel?.close();
    await this.conn?.close();
  }
}
