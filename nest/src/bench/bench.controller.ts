import { BadRequestException, Body, Controller, Delete, Get, HttpCode, NotFoundException, Optional, Param, Post, Query } from '@nestjs/common';
import { Message, MessagePublisher } from '../common/messaging/message.interface';
import { KafkaPublisher } from '../mq/kafka/kafka.publisher';
import { RabbitMqPublisher } from '../mq/rabbitmq/rabbitmq.publisher';
import { RedisStreamPublisher } from '../mq/redis-stream/redis-stream.publisher';
import { OrderResultRepository } from '../database/order-result.repository';
import { RunService, Run } from './order/run.service';
import { buildOrderEvent } from './order/order-event';

const CHUNK = 1000;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

@Controller()
export class BenchController {
  private publishers = new Map<string, MessagePublisher>(); // 켜진 MQ만 들어 있다

  constructor(
    private repository: OrderResultRepository,
    private runs: RunService,
    @Optional() kafka?: KafkaPublisher,
    @Optional() rabbit?: RabbitMqPublisher,
    @Optional() redis?: RedisStreamPublisher,
  ) {
    for (const p of [kafka, rabbit, redis]) if (p) this.publishers.set(p.type, p);
  }

  private publisher(mq: string) {
    const p = this.publishers.get(mq);
    if (!p) throw new BadRequestException(`mq must be one of ${[...this.publishers.keys()].join(' | ')}`);
    return p;
  }

  // dbCount(COUNT(*))도 대용량에서 비싸므로 완료됐거나 final일 때만 조회한다.
  private async summary(run: Run, final = false) {
    const s = this.runs.summary(run, final);
    return { ...s, dbCount: run.db && s.latencyMs ? await this.repository.count(run.runId) : undefined };
  }

  // ===== 시나리오 A: API 1번 호출 → 서버가 N건 연속 발행 =====
  // GET /bench/kafka?count=10000&size=256&db=true
  @Get('bench/:mq')
  async benchA(
    @Param('mq') mq: string, @Query('count') count = '10000', @Query('size') size = '256',
    @Query('db') db = 'false', @Query('timeout') timeout = '300',
  ) {
    const pub = this.publisher(mq);
    const n = Number(count);
    const run = this.runs.create(mq, db === 'true', n, Number(size));
    this.runs.markStarted(run);
    for (let i = 0; i < n; i += CHUNK) {
      const events: Message[] = [];
      for (let j = i; j < Math.min(i + CHUNK, n); j++) events.push(buildOrderEvent(run.runId, j, run.size, run.db));
      await pub.publish(events);
    }
    const sendMs = Date.now() - run.startedAt;
    const deadline = Date.now() + Number(timeout) * 1000;
    while (run.processed < n && Date.now() < deadline) await sleep(10);
    return { scenario: 'A', sendMs, ...(await this.summary(run, true)) };
  }

  // ===== 시나리오 B: 요청 1건 = 이벤트 1건 =====
  // 1) POST /runs?mq=kafka&expected=10000&size=256&db=true  → runId
  // 2) POST /orders?runId=... (N번 호출, 202 Accepted)
  // 3) GET  /runs/:runId  → 진행 상황과 결과
  @Post('runs')
  createRun(@Query('mq') mq: string, @Query('expected') expected = '1000', @Query('size') size = '256', @Query('db') db = 'false') {
    this.publisher(mq);
    const run = this.runs.create(mq, db === 'true', Number(expected), Number(size));
    return { runId: run.runId };
  }

  @Post('orders')
  @HttpCode(202)
  async order(@Query('runId') runId: string, @Body() body: { userId?: number; amount?: number } = {}) {
    const run = this.runs.get(runId);
    if (!run) throw new NotFoundException('unknown runId');
    this.runs.markStarted(run);
    const i = run.seq++;
    await this.publisher(run.mq).publish([buildOrderEvent(run.runId, i, run.size, run.db, body?.userId, body?.amount)]);
    return { accepted: i };
  }

  // final=true면 미완료여도 지연과 dbCount를 계산한다(부하 도구의 마지막 조회용).
  @Get('runs/:runId')
  async getRun(@Param('runId') runId: string, @Query('final') final = 'false') {
    const run = this.runs.get(runId);
    if (!run) throw new NotFoundException('unknown runId');
    return this.summary(run, final === 'true');
  }

  // 회차 사이 초기화: 결과 테이블 TRUNCATE + 큐/스트림 비우기 + 메모리 런 삭제
  @Delete('runs')
  async reset() {
    await this.repository.truncate();
    for (const p of this.publishers.values()) await p.purge(); // 시간 초과로 남은 이전 회차 메시지 제거
    this.runs.clear();
    return { reset: true };
  }
}
