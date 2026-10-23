// Kafka 최소 예제 (kafkajs)
// 사용:
//   node kafka.js produce [건수=5]   # practice-topic(파티션 3)에 발행. key = user-(i%3)
//   node kafka.js consume [그룹=practice-node]  # 그룹으로 처음부터 소비 (Ctrl+C로 종료)
//   node kafka.js info               # 토픽 파티션별 오프셋, 그룹 오프셋
process.env.KAFKAJS_NO_PARTITIONER_WARNING = '1'; // v2 기본 파티셔너 안내 로그 끄기
const { Kafka, logLevel } = require('kafkajs');

const TOPIC = 'practice-topic';
const kafka = new Kafka({
  clientId: 'practice-node',
  brokers: [process.env.KAFKA_BROKER || 'localhost:39092'],
  logLevel: logLevel.WARN,
});

async function ensureTopic() {
  const admin = kafka.admin();
  await admin.connect();
  if (!(await admin.listTopics()).includes(TOPIC)) {
    await admin.createTopics({ topics: [{ topic: TOPIC, numPartitions: 3, replicationFactor: 1 }] });
  }
  await admin.disconnect();
}

async function produce(count) {
  await ensureTopic();
  const producer = kafka.producer();
  await producer.connect();
  const messages = Array.from({ length: count }, (_, i) => ({
    key: `user-${i % 3}`, // 같은 key → 같은 파티션 → 순서 보장
    value: JSON.stringify({ orderId: i + 1, createdAt: Date.now() }),
  }));
  const res = await producer.send({ topic: TOPIC, messages });
  res.forEach((r) => console.log(`발행 → partition ${r.partition}, baseOffset ${r.baseOffset}`));
  await producer.disconnect();
}

async function consume(groupId) {
  await ensureTopic();
  const consumer = kafka.consumer({ groupId });
  await consumer.connect();
  // fromBeginning: 이 그룹의 커밋 오프셋이 없을 때만 처음부터. 있으면 커밋 위치부터 읽는다
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });
  console.log(`그룹 ${groupId}로 대기 중... (Ctrl+C로 종료)`);
  await consumer.run({
    // 메시지를 처리하고 함수가 끝나면 kafkajs가 오프셋을 자동 커밋한다
    eachMessage: async ({ partition, message }) => {
      console.log(`받음 p${partition} offset ${message.offset} key=${message.key} ${message.value}`);
    },
  });
  process.on('SIGINT', async () => { await consumer.disconnect(); process.exit(0); });
}

async function info() {
  const admin = kafka.admin();
  await admin.connect();
  console.log('topic offsets', await admin.fetchTopicOffsets(TOPIC).catch(() => '토픽 없음'));
  for (const g of (await admin.listGroups()).groups) {
    console.log('group', g.groupId, JSON.stringify(await admin.fetchOffsets({ groupId: g.groupId })));
  }
  await admin.disconnect();
}

(async () => {
  const [cmd = 'info', arg] = process.argv.slice(2);
  if (cmd === 'produce') await produce(Number(arg || 5));
  else if (cmd === 'consume') await consume(arg || 'practice-node');
  else await info();
})().catch((e) => { console.error(e); process.exit(1); });
