import { registerAs } from '@nestjs/config';

export default registerAs('kafka', () => ({
  clientId: process.env.KAFKA_CLIENT_ID ?? 'bench-nest',
  brokers: (process.env.KAFKA_BROKERS ?? 'localhost:39092').split(','),
  topic: process.env.KAFKA_TOPIC ?? 'bench-topic',
  partitions: Number(process.env.KAFKA_PARTITIONS ?? 10),
  replicationFactor: Number(process.env.KAFKA_REPLICATION_FACTOR ?? 1),
  groupId: process.env.KAFKA_GROUP_ID ?? 'bench-nest-group',
  // 파티션 10개를 동시에 처리
  partitionsConsumedConcurrently: Number(process.env.KAFKA_CONSUMER_CONCURRENCY ?? 10),
  // 큰 배치를 처리하는 동안 세션이 끊기지 않도록 기본 30초보다 길게 둔다
  sessionTimeout: Number(process.env.KAFKA_SESSION_TIMEOUT_MS ?? 60000),
}));
