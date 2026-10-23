import { Message } from '../../common/messaging/message.interface';

// 주문 이벤트 생성. Spring과 동일한 규칙(결정적 생성)을 쓴다. key는 userId(Kafka 파티션 기준).
export interface OrderEvent {
  id: string;
  runId: string;
  userId: number;
  amount: number;
  createdAt: number;
  db: boolean;
  pad: string;
}

export function buildOrderEvent(
  runId: string, i: number, size: number, db: boolean,
  userId?: number, amount?: number,
): Message {
  const uid = userId ?? ((i * 7919) % 1000) + 1;
  const amt = amount ?? ((i * 37) % 99900) + 100;
  const e: OrderEvent = { id: `${runId}-${i}`, runId, userId: uid, amount: amt, createdAt: Date.now(), db, pad: '' };
  const base = JSON.stringify(e).length;
  e.pad = 'x'.repeat(Math.max(0, size - base));
  return { key: String(uid), payload: JSON.stringify(e) };
}
