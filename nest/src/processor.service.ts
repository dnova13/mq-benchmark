import { Injectable } from '@nestjs/common';
import { DbService } from './db.service';
import { RunService } from './run.service';

// 세 MQ 컨슈머가 공통으로 호출하는 처리 로직
@Injectable()
export class ProcessorService {
  constructor(private db: DbService, private runs: RunService) {}

  async process(raw: string) {
    const e = JSON.parse(raw);
    if (!this.runs.get(e.runId)) return; // 이전 회차 잔여 메시지는 저장하지 않는다
    if (e.db) {
      const inserted = await this.db.insert(e);
      if (!inserted) return; // 중복 메시지
    }
    this.runs.record(e.runId, e.createdAt);
  }
}
