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
}

function pct(sorted: number[], p: number) {
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

  summary(run: Run) {
    const sorted = [...run.latencies].sort((a, b) => a - b);
    const totalMs = run.lastProcessedAt && run.startedAt ? run.lastProcessedAt - run.startedAt : 0;
    return {
      runId: run.runId, mq: run.mq, db: run.db, expected: run.expected, processed: run.processed,
      done: run.processed >= run.expected, totalMs,
      tps: totalMs ? Math.round((run.processed / totalMs) * 1000) : 0,
      latencyMs: { p50: pct(sorted, 50), p95: pct(sorted, 95), p99: pct(sorted, 99), max: sorted[sorted.length - 1] ?? 0 },
    };
  }
}
