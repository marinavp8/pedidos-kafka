import path from 'node:path';
import { bootstrapService, envInt, Handler, logger, runConsumer, startHealth, TOPICS } from '@pk/common';

/** Canal de salida. Implementación de demostración: escribe en el log. Sustituir por SMTP/proveedor push. */
const emailSender = {
  async enviar(clienteId: string, asunto: string) {
    logger.info('Notificación enviada', { canal: 'EMAIL', clienteId, asunto });
  },
};

function notificar(tipo: 'PEDIDO_CONFIRMADO' | 'PEDIDO_CANCELADO', asunto: (b: { pedidoId: string; motivo?: string }) => string): Handler {
  return async (c, m) => {
    const { pedidoId, clienteId } = m.body;
    // UNIQUE (pedido_id, tipo, canal): una sola notificación por evento (RF-51).
    const r = await c.query(
      `INSERT INTO notificacion (pedido_id, cliente_id, tipo, canal, estado) VALUES ($1, $2, $3, 'EMAIL', 'PENDIENTE')
       ON CONFLICT DO NOTHING RETURNING id`,
      [pedidoId, clienteId, tipo],
    );
    if (!r.rowCount) return;
    try {
      await emailSender.enviar(clienteId, asunto(m.body));
      await c.query(`UPDATE notificacion SET estado = 'ENVIADA', intentos = intentos + 1, enviada_at = now() WHERE id = $1`, [
        r.rows[0].id,
      ]);
    } catch (e) {
      await c.query(`UPDATE notificacion SET estado = 'FALLIDA', intentos = intentos + 1 WHERE id = $1`, [r.rows[0].id]);
      logger.error('Fallo enviando notificación', { pedidoId, error: String(e) });
    }
  };
}

async function main() {
  const { pool, kafka, producer } = await bootstrapService({
    name: 'notificaciones',
    migrationsDir: path.join(__dirname, '..', 'migrations'),
    topics: [],
    consume: [TOPICS.pedidosEventos],
    relay: false, // no publica eventos: no tiene outbox
  });
  await runConsumer({
    kafka,
    pool,
    producer,
    groupId: 'notificaciones',
    handlers: {
      [TOPICS.pedidosEventos]: {
        PedidoConfirmado: notificar('PEDIDO_CONFIRMADO', (b) => `Tu pedido ${b.pedidoId} está confirmado`),
        PedidoCancelado: notificar('PEDIDO_CANCELADO', (b) => `Tu pedido ${b.pedidoId} se ha cancelado: ${b.motivo ?? ''}`),
      },
    },
  });
  startHealth(envInt('PORT', 3000));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
