import { Injectable } from '@nestjs/common';
import { DbService } from './db.service';
import { RunService } from './run.service';

// 세 MQ 컨슈머가 공통으로 호출하는 처리 로직
@Injectable()
export class ProcessorService {
  constructor(private db: DbService, private runs: RunService) {}

  async process(raw: string) {
    const e = JSON.parse(raw);
    if (e.db) {
      const inserted = await this.db.insert(e);
      if (!inserted) return; // 중복 메시지
    }
    this.runs.record(e.runId, e.createdAt);
  }
}
