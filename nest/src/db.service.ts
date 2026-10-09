import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import * as mysql from 'mysql2/promise';

@Injectable()
export class DbService implements OnModuleInit, OnModuleDestroy {
  // 커넥션 풀 10 (Spring Hikari와 동일)
  private pool = mysql.createPool({
    host: 'localhost', port: 33307, user: 'root', password: '1234', database: 'bench', connectionLimit: 10,
  });

  async onModuleInit() {
    await this.pool.query(`CREATE TABLE IF NOT EXISTS orders_result (
      id VARCHAR(80) PRIMARY KEY,
      run_id VARCHAR(40) NOT NULL,
      user_id INT NOT NULL,
      amount INT NOT NULL,
      payload TEXT,
      created_at BIGINT NOT NULL,
      saved_at BIGINT NOT NULL,
      INDEX idx_run (run_id)
    ) ENGINE=InnoDB`);
  }

  // 중복(at-least-once)은 INSERT IGNORE로 무시. 실제로 저장되면 true
  async insert(e: { id: string; runId: string; userId: number; amount: number; pad: string; createdAt: number }) {
    const [res]: any = await this.pool.execute(
      'INSERT IGNORE INTO orders_result (id, run_id, user_id, amount, payload, created_at, saved_at) VALUES (?,?,?,?,?,?,?)',
      [e.id, e.runId, e.userId, e.amount, e.pad, e.createdAt, Date.now()],
    );
    return res.affectedRows === 1;
  }

  async count(runId: string): Promise<number> {
    const [rows]: any = await this.pool.query('SELECT COUNT(*) c FROM orders_result WHERE run_id = ?', [runId]);
    return Number(rows[0].c);
  }

  async truncate() { await this.pool.query('TRUNCATE TABLE orders_result'); }

  async onModuleDestroy() { await this.pool.end(); }
}
