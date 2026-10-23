import { readdir, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { Pool, PoolClient } from 'pg';
import { env } from './config';
import { logger } from './logger';

export type { Pool, PoolClient } from 'pg';

export function createPool(): Pool {
  return new Pool({ connectionString: env('DATABASE_URL'), max: 10 });
}

/** Ejecuta fn dentro de una transacción (COMMIT si resuelve, ROLLBACK si lanza). */
export async function tx<T>(pool: Pool, fn: (c: PoolClient) => Promise<T>): Promise<T> {
  const c = await pool.connect();
  try {
    await c.query('BEGIN');
    const result = await fn(c);
    await c.query('COMMIT');
    return result;
  } catch (e) {
    await c.query('ROLLBACK').catch(() => undefined);
    throw e;
  } finally {
    c.release();
  }
}

/** Aplica en orden los .sql de dir que aún no estén registrados en `table`. */
export async function migrate(pool: Pool, dir: string, table = 'schema_migrations'): Promise<void> {
  const c = await pool.connect();
  try {
    await c.query('SELECT pg_advisory_lock(727274)');
    await c.query(
      `CREATE TABLE IF NOT EXISTS ${table} (name text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`,
    );
    const files = (await readdir(dir)).filter((f) => f.endsWith('.sql')).sort();
    for (const f of files) {
      const done = await c.query(`SELECT 1 FROM ${table} WHERE name = $1`, [f]);
      if (done.rowCount) continue;
      await c.query('BEGIN');
      try {
        await c.query(await readFile(join(dir, f), 'utf8'));
        await c.query(`INSERT INTO ${table} (name) VALUES ($1)`, [f]);
        await c.query('COMMIT');
        logger.info('Migración aplicada', { fichero: f, tabla: table });
      } catch (e) {
        await c.query('ROLLBACK');
        throw e;
      }
    }
  } finally {
    await c.query('SELECT pg_advisory_unlock(727274)').catch(() => undefined);
    c.release();
  }
}
