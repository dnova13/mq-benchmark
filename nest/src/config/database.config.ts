import { registerAs } from '@nestjs/config';

export default registerAs('database', () => ({
  host: process.env.DB_HOST ?? 'localhost',
  port: Number(process.env.DB_PORT ?? 33307),
  user: process.env.DB_USER ?? 'root',
  password: process.env.DB_PASSWORD ?? '1234',
  database: process.env.DB_NAME ?? 'bench',
  connectionLimit: Number(process.env.DB_POOL_SIZE ?? 10), // Spring Hikari와 동일
}));
