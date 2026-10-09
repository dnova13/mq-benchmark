import { BadRequestException, Body, Controller, Delete, Get, HttpCode, NotFoundException, Param, Post, Query } from '@nestjs/common';
import { KafkaService } from './kafka.service';
import { RedisStreamService } from './redis-stream.service';
import { RabbitService } from './rabbit.service';
import { DbService } from './db.service';
import { RunService, Run } from './run.service';
import { buildEvent } from './event';

const CHUNK = 1000;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

@Controller()
export class BenchController {
  constructor(
    private kafka: KafkaService,
    private redis: RedisStreamService,
    private rabbit: RabbitService,
    private db: DbService,
    private runs: RunService,
  ) {}

  private publisher(mq: string) {
    const p = { kafka: this.kafka, redis: this.redis, rabbit: this.rabbit }[mq];
    if (!p) throw new BadRequestException('mq must be kafka | redis | rabbit');
    return p;
  }

  private async summary(run: Run) {
    return { ...this.runs.summary(run), dbCount: run.db ? await this.db.count(run.runId) : undefined };
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
      const events: string[] = [];
      for (let j = i; j < Math.min(i + CHUNK, n); j++) events.push(buildEvent(run.runId, j, run.size, run.db));
      await pub.publish(events);
    }
    const sendMs = Date.now() - run.startedAt;
    const deadline = Date.now() + Number(timeout) * 1000;
    while (run.processed < n && Date.now() < deadline) await sleep(10);
    return { scenario: 'A', sendMs, ...(await this.summary(run)) };
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
    await this.publisher(run.mq).publish([buildEvent(run.runId, i, run.size, run.db, body?.userId, body?.amount)]);
    return { accepted: i };
  }

  @Get('runs/:runId')
  async getRun(@Param('runId') runId: string) {
    const run = this.runs.get(runId);
    if (!run) throw new NotFoundException('unknown runId');
    return this.summary(run);
  }

  // 회차 사이 초기화: 결과 테이블 TRUNCATE + 메모리 런 삭제
  @Delete('runs')
  async reset() {
    await this.db.truncate();
    await this.redis.trim(); // 이전 회차 스트림 데이터 제거
    this.runs.clear();
    return { reset: true };
  }
}
