import path from 'node:path';
import { randomBytes } from 'node:crypto';
import { bootstrapService, envInt, Handler, PoolClient, publish, runConsumer, startHealth, TOPICS } from '@pk/common';

async function responder(c: PoolClient, sagaId: string, pedidoId: string, tipo: string, extra: object = {}) {
  await publish(c, {
    aggregateType: 'saga-respuesta',
    aggregateId: pedidoId,
    eventType: tipo,
    payload: { sagaId, pedidoId, ...extra },
    headers: { sagaId },
  });
}

const crearEnvio: Handler = async (c, m) => {
  const { sagaId, pedidoId, direccion } = m.body;

  const previo = await c.query('SELECT * FROM envio WHERE saga_id = $1', [sagaId]);
  if (previo.rowCount) {
    const e = previo.rows[0];
    if (e.estado === 'SIN_TRANSPORTISTA') return responder(c, sagaId, pedidoId, 'SinTransportista', { motivo: 'Sin transportista disponible' });
    return responder(c, sagaId, pedidoId, 'EnvioCreado', { envioId: e.id, tracking: e.tracking });
  }

  // Transportista activo que cubra el país y tenga capacidad hoy. FOR UPDATE serializa asignaciones.
  const pais: string = direccion?.pais ?? 'ES';
  const t = await c.query(
    `SELECT t.id FROM transportista t
      WHERE t.activo AND $1 = ANY(t.paises)
        AND (SELECT count(*) FROM envio e
              WHERE e.transportista_id = t.id AND e.estado = 'CREADO' AND e.created_at::date = current_date)
            < t.capacidad_diaria
      ORDER BY t.nombre LIMIT 1
      FOR UPDATE OF t`,
    [pais],
  );

  if (!t.rowCount) {
    await c.query(`INSERT INTO envio (saga_id, pedido_id, estado, direccion) VALUES ($1, $2, 'SIN_TRANSPORTISTA', $3)`, [
      sagaId,
      pedidoId,
      JSON.stringify(direccion ?? {}),
    ]);
    return responder(c, sagaId, pedidoId, 'SinTransportista', { motivo: `Sin transportista para el país ${pais}` });
  }

  const tracking = `TRK${randomBytes(5).toString('hex').toUpperCase()}`;
  const r = await c.query(
    `INSERT INTO envio (saga_id, pedido_id, transportista_id, estado, direccion, tracking)
     VALUES ($1, $2, $3, 'CREADO', $4, $5) RETURNING id`,
    [sagaId, pedidoId, t.rows[0].id, JSON.stringify(direccion ?? {}), tracking],
  );
  await responder(c, sagaId, pedidoId, 'EnvioCreado', { envioId: r.rows[0].id, tracking });
};

const cancelarEnvio: Handler = async (c, m) => {
  const { sagaId, pedidoId } = m.body;
  await c.query(`UPDATE envio SET estado = 'CANCELADO', updated_at = now() WHERE saga_id = $1 AND estado = 'CREADO'`, [sagaId]);
  await responder(c, sagaId, pedidoId, 'EnvioCancelado'); // idempotente
};

async function main() {
  const { pool, kafka, producer } = await bootstrapService({
    name: 'envios',
    migrationsDir: path.join(__dirname, '..', 'migrations'),
    seedsDir: path.join(__dirname, '..', 'seeds'),
    topics: [TOPICS.sagaRespuestas],
    consume: [TOPICS.enviosComandos],
  });
  await runConsumer({
    kafka,
    pool,
    producer,
    groupId: 'envios',
    handlers: { [TOPICS.enviosComandos]: { CrearEnvio: crearEnvio, CancelarEnvio: cancelarEnvio } },
  });
  startHealth(envInt('PORT', 3000));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
