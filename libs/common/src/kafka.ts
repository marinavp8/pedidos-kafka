import { Kafka, logLevel, Producer } from 'kafkajs';
import { env, envInt, sleep } from './config';
import { logger } from './logger';

export function createKafka(clientId: string): Kafka {
  return new Kafka({
    clientId,
    brokers: env('KAFKA_BROKERS').split(','),
    logLevel: logLevel.WARN,
    retry: { initialRetryTime: 500, retries: 15 },
  });
}

/** Productor idempotente con acks=all. */
export async function createProducer(kafka: Kafka): Promise<Producer> {
  const producer = kafka.producer({ idempotent: true, maxInFlightRequests: 1, allowAutoTopicCreation: false });
  await producer.connect();
  return producer;
}

/** Crea los topics si no existen (RF y min.insync.replicas según entorno). */
export async function ensureTopics(kafka: Kafka, topics: string[]): Promise<void> {
  const rf = envInt('KAFKA_RF', 1);
  const minIsr = envInt('KAFKA_MIN_ISR', 1);
  const partitions = envInt('KAFKA_PARTITIONS', 6);
  for (let intento = 1; ; intento++) {
    const admin = kafka.admin();
    try {
      await admin.connect();
      await admin.createTopics({
        waitForLeaders: true,
        topics: topics.map((topic) => ({
          topic,
          numPartitions: partitions,
          replicationFactor: rf,
          configEntries: [
            { name: 'min.insync.replicas', value: String(minIsr) },
            { name: 'retention.ms', value: String(7 * 24 * 3600 * 1000) },
          ],
        })),
      });
      return;
    } catch (e) {
      if (intento >= 10) throw e;
      logger.warn('ensureTopics falló, reintentando', { intento, error: String(e) });
      await sleep(3000);
    } finally {
      await admin.disconnect().catch(() => undefined);
    }
  }
}
