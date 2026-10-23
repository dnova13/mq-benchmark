import { registerAs } from '@nestjs/config';

export default registerAs('redis', () => ({
  host: process.env.REDIS_HOST ?? 'localhost',
  port: Number(process.env.REDIS_PORT ?? 36397),
  streamKey: process.env.REDIS_STREAM_KEY ?? 'bench:stream',
  group: process.env.REDIS_GROUP ?? 'bench-nest-group',
  workers: Number(process.env.REDIS_WORKERS ?? 10), // 같은 그룹 컨슈머 수
  batchSize: Number(process.env.REDIS_BATCH_SIZE ?? 100), // XREADGROUP COUNT
  blockMs: Number(process.env.REDIS_BLOCK_MS ?? 1000), // XREADGROUP BLOCK
}));
