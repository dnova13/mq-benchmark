import { Injectable } from '@nestjs/common';
import { MessageHandler } from '../../common/messaging/message.interface';
import { OrderResultRepository } from '../../database/order-result.repository';
import { RunService } from './run.service';

// 세 MQ 컨슈머가 공통으로 호출하는 처리 로직 (MessageHandler 구현)
@Injectable()
export class OrderProcessorService implements MessageHandler {
  constructor(private repository: OrderResultRepository, private runs: RunService) {}

  async handle(raw: string) {
    const e = JSON.parse(raw);
    if (!this.runs.get(e.runId)) return; // 이전 회차 잔여 메시지는 저장하지 않는다
    if (e.db) {
      const inserted = await this.repository.insert(e);
      if (!inserted) return; // 중복 메시지
    }
    this.runs.record(e.runId, e.createdAt);
  }
}
