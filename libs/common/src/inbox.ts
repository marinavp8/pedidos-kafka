import { IHeaders, Kafka, Producer } from 'kafkajs';
import { sleep } from './config';
import { Pool, PoolClient, tx } from './db';
import { logger } from './logger';
import { dlq } from './topics';

export interface Mensaje {
  messageId: string;
  key: string;
  tipo: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  body: any;
  headers: Record<string, string>;
}

export type Handler = (c: PoolClient, m: Mensaje) => Promise<void>;

const MAX_INTENTOS = 3;

function cabeceras(h: IHeaders | undefined): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(h ?? {})) {
    const x = Array.isArray(v) ? v[0] : v;
    if (x !== undefined) out[k] = x.toString();
  }
  return out;
}

/**
 * Consumidor idempotente. Por cada mensaje abre una transacción, registra el messageId en
 * processed_message (ON CONFLICT DO NOTHING) y ejecuta el handler en la MISMA transacción.
 * Si el mensaje ya estaba registrado, es un duplicado y se ignora.
 * Tras MAX_INTENTOS fallos, el mensaje va a `<topic>.dlq` y se continúa con el siguiente.
 */
export async function runConsumer(opts: {
  kafka: Kafka;
  pool: Pool;
  producer: Producer;
  groupId: string;
  handlers: Record<string, Record<string, Handler>>; // topic -> tipo -> handler
}): Promise<void> {
  const { kafka, pool, producer, groupId, handlers } = opts;
  const consumer = kafka.consumer({ groupId, sessionTimeout: 30000 });
  await consumer.connect();
  for (const topic of Object.keys(handlers)) await consumer.subscribe({ topic, fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const hdr = cabeceras(message.headers);
      const key = message.key?.toString() ?? '';
      const messageId = hdr.messageId ?? `${topic}-${partition}-${message.offset}`;
      let error: unknown;

      for (let intento = 1; intento <= MAX_INTENTOS; intento++) {
        try {
          const body = JSON.parse(message.value?.toString() ?? '{}');
          const tipo: string = hdr.tipo ?? body.tipo;
          const handler = handlers[topic]?.[tipo];
          if (!handler) return;

          await tx(pool, async (c) => {
            const r = await c.query(
              'INSERT INTO processed_message (message_id, consumer) VALUES ($1, $2) ON CONFLICT DO NOTHING',
              [messageId, groupId],
            );
            if (!r.rowCount) {
              logger.info('Mensaje duplicado ignorado', { messageId, tipo });
              return;
            }
            await handler(c, { messageId, key, tipo, body, headers: hdr });
          });
          return;
        } catch (e) {
          error = e;
          logger.warn('Fallo procesando mensaje', { topic, messageId, intento, error: String(e) });
          if (e instanceof SyntaxError) break; // JSON inválido: no tiene sentido reintentar
          await sleep(500 * intento);
        }
      }

      logger.error('Mensaje enviado a la DLQ', { topic, messageId });
      await producer.send({
        topic: dlq(topic),
        acks: -1,
        messages: [
          {
            key,
            value: message.value,
            headers: { ...hdr, error: String(error), origen: groupId },
          },
        ],
      });
    },
  });
}
