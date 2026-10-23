// RabbitMQ 최소 예제 (amqplib)
// 사용:
//   node rabbitmq.js produce [건수=5]   # practice-queue에 persistent 메시지 발행
//   node rabbitmq.js consume            # prefetch 10으로 받아 처리 후 ack (Ctrl+C로 종료)
//   node rabbitmq.js fanout             # fanout exchange에 1건 → 묶인 큐 2개가 모두 받음
//   node rabbitmq.js info               # 큐의 메시지 수, 컨슈머 수
const amqp = require('amqplib');

const URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:35672';
const QUEUE = 'practice-queue';

async function withChannel(fn) {
  const conn = await amqp.connect(URL);
  const ch = await conn.createChannel();
  await ch.assertQueue(QUEUE, { durable: true }); // 브로커가 재시작해도 큐가 남는다
  return fn(ch, conn);
}

const produce = (count) => withChannel(async (ch, conn) => {
  for (let i = 1; i <= count; i++) {
    const body = JSON.stringify({ orderId: i, createdAt: Date.now() });
    // 기본 exchange('')로 보내면 routing key = 큐 이름. persistent: 디스크에 저장
    ch.sendToQueue(QUEUE, Buffer.from(body), { persistent: true });
    console.log(`발행 → ${QUEUE} ${body}`);
  }
  await ch.close(); // close 전에 버퍼의 메시지를 모두 보낸다
  await conn.close();
});

const consume = () => withChannel(async (ch, conn) => {
  await ch.prefetch(10); // ack하지 않은 메시지를 최대 10건까지만 받는다
  console.log(`${QUEUE} 대기 중... (Ctrl+C로 종료)`);
  await ch.consume(QUEUE, (msg) => {
    console.log(`받음 ${msg.content} redelivered=${msg.fields.redelivered}`);
    ch.ack(msg); // ack해야 큐에서 사라진다. ack 없이 연결이 끊기면 다시 큐로 돌아간다
  });
  process.on('SIGINT', async () => { await conn.close(); process.exit(0); });
});

const fanout = () => withChannel(async (ch, conn) => {
  const EX = 'practice.fanout';
  await ch.assertExchange(EX, 'fanout', { durable: true });
  for (const q of ['practice-email', 'practice-sms']) {
    await ch.assertQueue(q, { durable: true });
    await ch.bindQueue(q, EX, ''); // fanout은 routing key를 무시한다
  }
  ch.publish(EX, '', Buffer.from('user-signed-up'), { persistent: true });
  await new Promise((r) => setTimeout(r, 200));
  for (const q of ['practice-email', 'practice-sms']) {
    const { messageCount } = await ch.checkQueue(q);
    console.log(`${q}: ${messageCount}건`);
  }
  await conn.close();
});

const info = () => withChannel(async (ch, conn) => {
  const { messageCount, consumerCount } = await ch.checkQueue(QUEUE);
  console.log(`${QUEUE}: messages=${messageCount} consumers=${consumerCount}`);
  await conn.close();
});

(async () => {
  const [cmd = 'info', arg] = process.argv.slice(2);
  if (cmd === 'produce') await produce(Number(arg || 5));
  else if (cmd === 'consume') await consume();
  else if (cmd === 'fanout') await fanout();
  else await info();
})().catch((e) => { console.error(e); process.exit(1); });
