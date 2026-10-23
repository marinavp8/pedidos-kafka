import { createServer } from 'node:http';
import { Kafka, Producer } from 'kafkajs';
import { sleep } from './config';
import { Pool, createPool, migrate } from './db';
import { createKafka, createProducer, ensureTopics } from './kafka';
import { logger } from './logger';
import { startRelay } from './outbox';
import { dlq } from './topics';

export interface Contexto {
  pool: Pool;
  kafka: Kafka;
  producer: Producer;
}

/**
 * Arranque común: migraciones (y seeds de demo si SEED_DEMO_DATA=true), topics, productor y relay.
 * `consume` son los topics que lee el servicio (se crean también sus DLQ).
 */
export async function bootstrapService(opts: {
  name: string;
  migrationsDir: string;
  seedsDir?: string;
  topics: string[];
  consume?: string[];
  relay?: boolean;
}): Promise<Contexto> {
  process.env.SERVICE_NAME = opts.name;
  const pool = createPool();

  for (let intento = 1; ; intento++) {
    try {
      await migrate(pool, opts.migrationsDir);
      if (opts.seedsDir && process.env.SEED_DEMO_DATA === 'true') {
        await migrate(pool, opts.seedsDir, 'seed_migrations');
      }
      break;
    } catch (e) {
      if (intento >= 15) throw e;
      logger.warn('Base de datos no disponible, reintentando', { intento, error: String(e) });
      await sleep(2000);
    }
  }

  const kafka = createKafka(opts.name);
  await ensureTopics(kafka, [...opts.topics, ...(opts.consume ?? []).map(dlq)]);
  const producer = await createProducer(kafka);
  if (opts.relay !== false) startRelay(pool, producer);

  process.on('SIGTERM', () => {
    logger.info('SIGTERM recibido, saliendo');
    process.exit(0);
  });
  return { pool, kafka, producer };
}

/** Servidor mínimo con /health para servicios sin API HTTP. */
export function startHealth(port: number): void {
  createServer((req, res) => {
    res.writeHead(req.url === '/health' ? 200 : 404);
    res.end(req.url === '/health' ? 'ok' : '');
  }).listen(port);
}
