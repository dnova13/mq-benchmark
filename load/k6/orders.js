// 시나리오 B (k6): 요청 1건 = 이벤트 1건
// 실행 예: k6 run -e BASE=http://localhost:3000 -e MQ=kafka -e COUNT=10000 -e VUS=100 -e DB=true load/k6/orders.js
import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE = __ENV.BASE || 'http://localhost:3000';
const MQ = __ENV.MQ || 'kafka';
const COUNT = parseInt(__ENV.COUNT || '1000');
const VUS = parseInt(__ENV.VUS || '50');
const SIZE = __ENV.SIZE || '256';
const DB = __ENV.DB || 'true';

export const options = {
  scenarios: {
    orders: { executor: 'shared-iterations', vus: VUS, iterations: COUNT, maxDuration: '10m' },
  },
  teardownTimeout: '10m',
};

export function setup() {
  http.del(`${BASE}/runs`); // 이전 결과 초기화
  const res = http.post(`${BASE}/runs?mq=${MQ}&expected=${COUNT}&size=${SIZE}&db=${DB}`);
  return { runId: res.json('runId') };
}

export default function (data) {
  const i = __ITER;
  const body = JSON.stringify({ userId: ((i * 7919) % 1000) + 1, amount: ((i * 37) % 99900) + 100 });
  const res = http.post(`${BASE}/orders?runId=${data.runId}`, body, { headers: { 'Content-Type': 'application/json' } });
  check(res, { 'status is 202': (r) => r.status === 202 });
}

// 모든 요청이 끝나면 소비와 DB 저장이 끝날 때까지 기다렸다가 결과를 출력한다.
export function teardown(data) {
  let result;
  let last = -1;
  let stalled = 0;
  for (let t = 0; t < 3000; t++) {
    result = http.get(`${BASE}/runs/${data.runId}`).json();
    if (result.done) break;
    // 처리 건수가 15초간 늘지 않으면 유실로 보고 종료
    stalled = result.processed === last ? stalled + 1 : 0;
    last = result.processed;
    if (stalled >= 150) break;
    sleep(0.1);
  }
  console.log(`RESULT ${JSON.stringify(result)}`);
}
