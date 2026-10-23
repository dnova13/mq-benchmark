import { Inject, Injectable, OnModuleDestroy } from '@nestjs/common';
import { ConfigType } from '@nestjs/config';
import * as amqp from 'amqplib';
import rabbitmqConfig from '../config/rabbitmq.config';
import { PracticeOps } from './practice-ops.interface';

// RabbitMQ: 꺼내서 ack하면 큐에서 사라진다. ack=false면 꺼내 본 뒤 다시 넣는다(엿보기)
@Injectable()
export class RabbitMqPracticeService implements PracticeOps, OnModuleDestroy {
  readonly type = 'rabbit';
  readonly destination = 'practice-queue';
  private conn?: Promise<amqp.ChannelModel>;

  constructor(@Inject(rabbitmqConfig.KEY) private cfg: ConfigType<typeof rabbitmqConfig>) {}

  // 요청마다 채널을 열고 닫는다. 연결은 처음 쓸 때 하나 만들어 재사용
  private async withChannel<T>(fn: (ch: amqp.Channel) => Promise<T>): Promise<T> {
    const conn = await (this.conn ??= amqp.connect(this.cfg.url));
    const ch = await conn.createChannel();
    try {
      await ch.assertQueue(this.destination, { durable: true });
      return await fn(ch);
    } finally {
      await ch.close().catch(() => undefined);
    }
  }

  publish(_key: string | undefined, payload: string) {
    return this.withChannel(async (ch) => {
      ch.sendToQueue(this.destination, Buffer.from(payload), { persistent: true }); // 기본 exchange, routing key = 큐 이름
      return { queued: true };
    });
  }

  read(count: number, ack: boolean) {
    return this.withChannel(async (ch) => {
      const got: amqp.GetMessage[] = [];
      for (let i = 0; i < count; i++) {
        const m = await ch.get(this.destination, { noAck: false }); // 한 건씩 당겨 온다(pull)
        if (!m) break;
        got.push(m);
      }
      // ack: 큐에서 제거 / nack(requeue=true): 다시 큐로
      for (const m of got) (ack ? ch.ack(m) : ch.nack(m, false, true));
      return got.map((m) => ({ payload: m.content.toString(), redelivered: m.fields.redelivered, remaining: m.fields.messageCount }));
    });
  }

  info() {
    return this.withChannel(async (ch) => {
      const q = await ch.checkQueue(this.destination);
      return { messages: q.messageCount, consumers: q.consumerCount };
    });
  }

  reset() {
    return this.withChannel(async (ch) => { await ch.deleteQueue(this.destination); });
  }

  async onModuleDestroy() { if (this.conn) await (await this.conn).close().catch(() => undefined); }
}
