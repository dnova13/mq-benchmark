// Redis Streams 최소 예제 (ioredis)
// 사용:
//   node redis.js produce [건수=5]     # practice:stream에 XADD
//   node redis.js consume [컨슈머=c1]  # 그룹 practice-node로 XREADGROUP 후 XACK (Ctrl+C로 종료)
//   node redis.js info                 # 스트림 길이, 그룹, 대기(PEL) 수
const Redis = require('ioredis');

const STREAM = 'practice:stream';
const GROUP = 'practice-node';
const redis = new Redis({ host: process.env.REDIS_HOST || 'localhost', port: Number(process.env.REDIS_PORT || 36397) });

async function produce(count) {
  for (let i = 1; i <= count; i++) {
    // '*' = ID 자동 생성. 필드는 key-value 쌍으로 넣는다
    const id = await redis.xadd(STREAM, '*', 'orderId', String(i), 'createdAt', String(Date.now()));
    console.log(`XADD ${STREAM} → ${id}`);
  }
}

async function consume(consumer) {
  // 그룹이 없으면 만든다. '0' = 스트림 처음부터, MKSTREAM = 스트림이 없으면 생성
  try {
    await redis.xgroup('CREATE', STREAM, GROUP, '0', 'MKSTREAM');
  } catch (e) {
    if (!String(e).includes('BUSYGROUP')) throw e;
  }
  // XREADGROUP BLOCK은 커넥션을 점유하므로 읽기 전용 커넥션을 따로 쓴다
  const reader = redis.duplicate();
  console.log(`그룹 ${GROUP}, 컨슈머 ${consumer}로 대기 중... (Ctrl+C로 종료)`);
  for (;;) {
    // '>' = 아직 이 그룹에 전달되지 않은 메시지. 최대 10건, 5초 대기
    const res = await reader.xreadgroup('GROUP', GROUP, consumer, 'COUNT', 10, 'BLOCK', 5000, 'STREAMS', STREAM, '>');
    if (!res) continue;
    for (const [, entries] of res) {
      for (const [id, fields] of entries) {
        console.log(`받음 ${id}`, Object.fromEntries(chunk(fields)));
        await redis.xack(STREAM, GROUP, id); // 처리 완료 → PEL에서 빠진다
      }
    }
  }
}

async function info() {
  console.log('XLEN', await redis.xlen(STREAM));
  const groups = await redis.xinfo('GROUPS', STREAM).catch(() => []);
  for (const g of groups) console.log('GROUP', Object.fromEntries(chunk(g)));
}

const chunk = (arr) => arr.reduce((acc, v, i) => (i % 2 ? acc[acc.length - 1].push(v) : acc.push([v]), acc), []);

(async () => {
  const [cmd = 'info', arg] = process.argv.slice(2);
  if (cmd === 'produce') await produce(Number(arg || 5));
  else if (cmd === 'consume') await consume(arg || 'c1');
  else await info();
  redis.disconnect();
})().catch((e) => { console.error(e); process.exit(1); });
