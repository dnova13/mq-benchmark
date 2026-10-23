import { BadRequestException, Body, Controller, Delete, Get, Param, Post, Query } from '@nestjs/common';
import { PracticeOps } from './practice-ops.interface';
import { KafkaPracticeService } from './kafka-practice.service';
import { RabbitMqPracticeService } from './rabbitmq-practice.service';
import { RedisPracticeService } from './redis-practice.service';

/**
 * MQ 연습용 API. 벤치마크와 별개인 practice-topic, practice:stream, practice-queue만 다룬다.
 * 사용법: practice/README.md
 */
@Controller('practice')
export class PracticeController {
  private ops: Map<string, PracticeOps>;

  constructor(kafka: KafkaPracticeService, redis: RedisPracticeService, rabbit: RabbitMqPracticeService) {
    this.ops = new Map([kafka, redis, rabbit].map((o) => [o.type, o] as [string, PracticeOps]));
  }

  private of(mq: string) {
    const o = this.ops.get(mq);
    if (!o) throw new BadRequestException(`mq must be one of ${[...this.ops.keys()].join(' | ')}`);
    return o;
  }

  // POST /practice/kafka/messages  {"key":"user-1","payload":"hello"}
  @Post(':mq/messages')
  async publish(@Param('mq') mq: string, @Body() body: { key?: string; payload?: string } = {}) {
    const o = this.of(mq);
    return { mq, destination: o.destination, ...(await o.publish(body.key, body.payload ?? '')) };
  }

  // GET /practice/kafka/messages?count=10&ack=true
  @Get(':mq/messages')
  async read(@Param('mq') mq: string, @Query('count') count = '10', @Query('ack') ack = 'true') {
    const o = this.of(mq);
    const n = Math.max(1, Math.min(Number(count) || 10, 100));
    return { mq, destination: o.destination, messages: await o.read(n, ack !== 'false') };
  }

  @Get(':mq/info')
  async info(@Param('mq') mq: string) {
    const o = this.of(mq);
    return { mq, destination: o.destination, ...(await o.info()) };
  }

  @Delete(':mq')
  async reset(@Param('mq') mq: string) {
    const o = this.of(mq);
    await o.reset();
    return { mq, deleted: o.destination };
  }
}
