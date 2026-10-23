import { registerAs } from '@nestjs/config';

export default registerAs('rabbitmq', () => ({
  url: process.env.RABBITMQ_URL ?? 'amqp://guest:guest@localhost:35672',
  queue: process.env.RABBITMQ_QUEUE ?? 'bench-queue',
  durable: (process.env.RABBITMQ_DURABLE ?? 'true') === 'true',
  persistent: (process.env.RABBITMQ_PERSISTENT ?? 'true') === 'true',
  prefetch: Number(process.env.RABBITMQ_PREFETCH ?? 100),
}));
