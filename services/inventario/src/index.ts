import path from 'node:path';
import { bootstrapService, envInt, Handler, PoolClient, publish, runConsumer, startHealth, TOPICS } from '@pk/common';

interface Linea {
  productoId: string;
  cantidad: number;
}

async function responder(c: PoolClient, sagaId: string, pedidoId: string, tipo: string, extra: object = {}) {
  await publish(c, {
    aggregateType: 'saga-respuesta',
    aggregateId: pedidoId,
    eventType: tipo,
    payload: { sagaId, pedidoId, ...extra },
    headers: { sagaId },
  });
}

const reservarStock: Handler = async (c, m) => {
  const { sagaId, pedidoId } = m.body;
  const lineas = [...(m.body.lineas as Linea[])].sort((a, b) => a.productoId.localeCompare(b.productoId)); // orden fijo: evita interbloqueos

  // Idempotencia de negocio: una reserva por saga. Si ya existe, se repite la misma respuesta.
  const previa = await c.query('SELECT id, estado FROM reserva WHERE saga_id = $1', [sagaId]);
  if (previa.rowCount) {
    const { id, estado } = previa.rows[0];
    if (estado === 'RECHAZADA') return responder(c, sagaId, pedidoId, 'StockInsuficiente', { motivo: 'Stock insuficiente' });
    return responder(c, sagaId, pedidoId, 'StockReservado', { reservaId: id });
  }

  await c.query('SAVEPOINT reserva');
  try {
    const r = await c.query(
      `INSERT INTO reserva (saga_id, pedido_id, estado) VALUES ($1, $2, 'RESERVADA') RETURNING id`,
      [sagaId, pedidoId],
    );
    const reservaId: string = r.rows[0].id;
    for (const l of lineas) {
      const u = await c.query(
        `UPDATE producto_stock
            SET disponible = disponible - $2, reservado = reservado + $2, updated_at = now()
          WHERE producto_id = $1 AND disponible >= $2`,
        [l.productoId, l.cantidad],
      );
      if (!u.rowCount) throw new Error(`Stock insuficiente para ${l.productoId}`);
      await c.query('INSERT INTO reserva_linea (reserva_id, producto_id, cantidad) VALUES ($1, $2, $3)', [
        reservaId,
        l.productoId,
        l.cantidad,
      ]);
    }
    await c.query('RELEASE SAVEPOINT reserva');
    await responder(c, sagaId, pedidoId, 'StockReservado', { reservaId });
  } catch (e) {
    await c.query('ROLLBACK TO SAVEPOINT reserva'); // todo o nada
    await c.query(`INSERT INTO reserva (saga_id, pedido_id, estado) VALUES ($1, $2, 'RECHAZADA')`, [sagaId, pedidoId]);
    await responder(c, sagaId, pedidoId, 'StockInsuficiente', { motivo: String((e as Error).message) });
  }
};

const liberarStock: Handler = async (c, m) => {
  const { sagaId, pedidoId } = m.body;
  const r = await c.query(
    `UPDATE reserva SET estado = 'LIBERADA', updated_at = now()
      WHERE saga_id = $1 AND estado = 'RESERVADA' RETURNING id`,
    [sagaId],
  );
  if (r.rowCount) {
    await c.query(
      `UPDATE producto_stock s
          SET disponible = s.disponible + l.cantidad, reservado = s.reservado - l.cantidad, updated_at = now()
         FROM reserva_linea l
        WHERE l.reserva_id = $1 AND s.producto_id = l.producto_id`,
      [r.rows[0].id],
    );
  }
  await responder(c, sagaId, pedidoId, 'StockLiberado'); // idempotente: siempre responde
};

async function main() {
  const { pool, kafka, producer } = await bootstrapService({
    name: 'inventario',
    migrationsDir: path.join(__dirname, '..', 'migrations'),
    seedsDir: path.join(__dirname, '..', 'seeds'),
    topics: [TOPICS.sagaRespuestas],
    consume: [TOPICS.inventarioComandos],
  });
  await runConsumer({
    kafka,
    pool,
    producer,
    groupId: 'inventario',
    handlers: { [TOPICS.inventarioComandos]: { ReservarStock: reservarStock, LiberarStock: liberarStock } },
  });
  startHealth(envInt('PORT', 3000));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
