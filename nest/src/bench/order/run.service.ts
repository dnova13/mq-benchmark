import { Injectable } from '@nestjs/common';

export interface Run {
  runId: string;
  mq: string;
  db: boolean;
  expected: number;
  size: number;
  seq: number;
  startedAt: number;
  lastProcessedAt: number;
  processed: number;
  latencies: number[];
  latencyMs?: { p50: number; p95: number; p99: number; max: number }; // 완료 시 한 번 계산해 캐시
}

function pct(sorted: ArrayLike<number>, p: number) {
  if (!sorted.length) return 0;
  return sorted[Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1)];
}

@Injectable()
export class RunService {
  private runs = new Map<string, Run>();

  create(mq: string, db: boolean, expected: number, size: number): Run {
    const run: Run = {
      runId: `${mq}-${Date.now()}`, mq, db, expected, size,
      seq: 0, startedAt: 0, lastProcessedAt: 0, processed: 0, latencies: [],
    };
    this.runs.set(run.runId, run);
    return run;
  }

  get(runId: string) { return this.runs.get(runId); }
  clear() { this.runs.clear(); }

  markStarted(run: Run) { if (!run.startedAt) run.startedAt = Date.now(); }

  record(runId: string, createdAt: number) {
    const run = this.runs.get(runId);
    if (!run) return;
    const now = Date.now();
    run.processed++;
    run.lastProcessedAt = now;
    run.latencies.push(now - createdAt);
  }

  // 정렬은 비용이 커서(1M건이면 수백 ms) 완료됐거나 final일 때만 계산한다. 진행 중 폴링은 건수만 본다.
  summary(run: Run, final = false) {
    const done = run.processed >= run.expected;
    if ((done || final) && !run.latencyMs) {
      const sorted = Float64Array.from(run.latencies).sort();
      const latencyMs = { p50: pct(sorted, 50), p95: pct(sorted, 95), p99: pct(sorted, 99), max: sorted[sorted.length - 1] ?? 0 };
      if (!done) return { ...this.base(run, done), latencyMs };
      run.latencyMs = latencyMs;
    }
    return { ...this.base(run, done), latencyMs: run.latencyMs };
  }

  private base(run: Run, done: boolean) {
    const totalMs = run.lastProcessedAt && run.startedAt ? run.lastProcessedAt - run.startedAt : 0;
    return {
      runId: run.runId, mq: run.mq, db: run.db, expected: run.expected, processed: run.processed,
      done, totalMs, tps: totalMs ? Math.round((run.processed / totalMs) * 1000) : 0,
    };
  }
}
