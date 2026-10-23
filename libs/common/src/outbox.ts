import { Message, Producer } from 'kafkajs';
import { envInt, sleep } from './config';
import { Pool, PoolClient, tx } from './db';
import { logger } from './logger';
import { AGGREGATE_TOPIC } from './topics';

export interface EventoOutbox {
  aggregateType: string; // clave de AGGREGATE_TOPIC
  aggregateId: string; // clave Kafka (pedidoId)
  eventType: string;
  payload: object;
  headers?: Record<string, string>;
}

/** Inserta el evento en la outbox. Debe llamarse dentro de la misma transacción que el cambio de negocio. */
export async function publish(c: PoolClient, e: EventoOutbox): Promise<void> {
  await c.query(
    'INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload, headers) VALUES ($1, $2, $3, $4, $5)',
    [e.aggregateType, e.aggregateId, e.eventType, JSON.stringify(e.payload), JSON.stringify(e.headers ?? {})],
  );
}

async function relayOnce(pool: Pool, producer: Producer): Promise<number> {
  return tx(pool, async (c) => {
    const { rows } = await c.query(
      `SELECT id, aggregate_type, aggregate_id, event_type, payload, headers
         FROM outbox
        WHERE published_at IS NULL
        ORDER BY seq
        LIMIT 100
        FOR UPDATE SKIP LOCKED`,
    );
    if (rows.length === 0) return 0;

    const porTopic = new Map<string, Message[]>();
    for (const r of rows) {
      const topic = AGGREGATE_TOPIC[r.aggregate_type];
      if (!topic) throw new Error(`aggregate_type sin topic: ${r.aggregate_type}`);
      const messages = porTopic.get(topic) ?? [];
      messages.push({
        key: r.aggregate_id,
        value: JSON.stringify({ tipo: r.event_type, ...r.payload }),
        headers: { messageId: r.id, tipo: r.event_type, ...r.headers },
      });
      porTopic.set(topic, messages);
    }
    await producer.sendBatch({
      acks: -1,
      topicMessages: [...porTopic].map(([topic, messages]) => ({ topic, messages })),
    });
    await c.query('UPDATE outbox SET published_at = now() WHERE id = ANY($1::uuid[])', [rows.map((r) => r.id)]);
    return rows.length;
  });
}

/**
 * Relay de la outbox: lee las filas pendientes y las publica en Kafka (poller con SKIP LOCKED).
 * Es un productor, no un consumidor. Puede duplicar mensajes si cae entre el envío y el COMMIT:
 * los consumidores son idempotentes.
 */
export function startRelay(pool: Pool, producer: Producer): { stop: () => void } {
  const pollMs = envInt('RELAY_POLL_MS', 200);
  let parar = false;

  void (async () => {
    while (!parar) {
      let n = 0;
      try {
        n = await relayOnce(pool, producer);
      } catch (e) {
        logger.error('Fallo en el relay de la outbox', { error: String(e) });
        await sleep(1000);
      }
      if (n === 0) await sleep(pollMs);
    }
  })();

  // Purga: outbox publicada > 7 días y processed_message > 14 días.
  const purga = setInterval(
    () => {
      pool
        .query(`DELETE FROM outbox WHERE published_at < now() - interval '7 days'`)
        .then(() => pool.query(`DELETE FROM processed_message WHERE processed_at < now() - interval '14 days'`))
        .catch((e) => logger.error('Fallo en la purga', { error: String(e) }));
    },
    60 * 60 * 1000,
  );
  purga.unref();

  return {
    stop: () => {
      parar = true;
      clearInterval(purga);
    },
  };
}
