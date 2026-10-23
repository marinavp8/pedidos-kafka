import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { bootstrapService, envInt, Handler, PoolClient, publish, runConsumer, startHealth, TOPICS } from '@pk/common';

/**
 * Adaptador de la pasarela de pago. Esta implementación es una SIMULACIÓN:
 * rechaza importes por encima de PAGO_LIMITE. Para producción, sustituir por el cliente real
 * enviando `idempotencyKey` (sagaId) en cada llamada.
 */
const pasarela = {
  async cobrar(_idempotencyKey: string, importe: number) {
    const limite = envInt('PAGO_LIMITE', 5000);
    if (importe > limite) return { ok: false as const, motivo: `Pago rechazado: importe superior a ${limite}` };
    return { ok: true as const, referencia: `sim_${randomUUID()}` };
  },
  async reembolsar(_idempotencyKey: string, _referencia: string) {
    return { ok: true as const, referencia: `simref_${randomUUID()}` };
  },
};

async function responder(c: PoolClient, sagaId: string, pedidoId: string, tipo: string, extra: object = {}) {
  await publish(c, {
    aggregateType: 'saga-respuesta',
    aggregateId: pedidoId,
    eventType: tipo,
    payload: { sagaId, pedidoId, ...extra },
    headers: { sagaId },
  });
}

const cobrarPedido: Handler = async (c, m) => {
  const { sagaId, pedidoId, importe, moneda } = m.body;

  // Un cobro por saga: si ya existe, se repite la respuesta sin volver a cobrar.
  const nuevo = await c.query(
    `INSERT INTO cobro (saga_id, pedido_id, importe, moneda, estado) VALUES ($1, $2, $3, $4, 'PENDIENTE')
     ON CONFLICT (saga_id) DO NOTHING RETURNING id`,
    [sagaId, pedidoId, importe, moneda],
  );
  if (!nuevo.rowCount) {
    const { id, estado, motivo_rechazo } = (await c.query('SELECT * FROM cobro WHERE saga_id = $1', [sagaId])).rows[0];
    if (estado === 'RECHAZADO') return responder(c, sagaId, pedidoId, 'PagoRechazado', { motivo: motivo_rechazo });
    return responder(c, sagaId, pedidoId, 'PagoCompletado', { cobroId: id });
  }

  const cobroId: string = nuevo.rows[0].id;
  const r = await pasarela.cobrar(sagaId, Number(importe));
  if (r.ok) {
    await c.query(`UPDATE cobro SET estado = 'COMPLETADO', referencia_pasarela = $2, updated_at = now() WHERE id = $1`, [
      cobroId,
      r.referencia,
    ]);
    await responder(c, sagaId, pedidoId, 'PagoCompletado', { cobroId });
  } else {
    await c.query(`UPDATE cobro SET estado = 'RECHAZADO', motivo_rechazo = $2, updated_at = now() WHERE id = $1`, [
      cobroId,
      r.motivo,
    ]);
    await responder(c, sagaId, pedidoId, 'PagoRechazado', { motivo: r.motivo });
  }
};

const reembolsarPago: Handler = async (c, m) => {
  const { sagaId, pedidoId } = m.body;
  const cobro = (await c.query('SELECT * FROM cobro WHERE saga_id = $1 FOR UPDATE', [sagaId])).rows[0];
  if (cobro?.estado === 'COMPLETADO') {
    // Cobro y reembolso quedan ambos registrados.
    const rb = await c.query(
      `INSERT INTO reembolso (cobro_id, importe, estado) VALUES ($1, $2, 'PENDIENTE') ON CONFLICT (cobro_id) DO NOTHING RETURNING id`,
      [cobro.id, cobro.importe],
    );
    if (rb.rowCount) {
      const r = await pasarela.reembolsar(sagaId, cobro.referencia_pasarela);
      await c.query(`UPDATE reembolso SET estado = 'COMPLETADO', referencia_pasarela = $2 WHERE id = $1`, [
        rb.rows[0].id,
        r.referencia,
      ]);
      await c.query(`UPDATE cobro SET estado = 'REEMBOLSADO', updated_at = now() WHERE id = $1`, [cobro.id]);
    }
  }
  await responder(c, sagaId, pedidoId, 'PagoReembolsado'); // idempotente: siempre responde
};

async function main() {
  const { pool, kafka, producer } = await bootstrapService({
    name: 'pagos',
    migrationsDir: path.join(__dirname, '..', 'migrations'),
    topics: [TOPICS.sagaRespuestas],
    consume: [TOPICS.pagosComandos],
  });
  await runConsumer({
    kafka,
    pool,
    producer,
    groupId: 'pagos',
    handlers: { [TOPICS.pagosComandos]: { CobrarPedido: cobrarPedido, ReembolsarPago: reembolsarPago } },
  });
  startHealth(envInt('PORT', 3000));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
