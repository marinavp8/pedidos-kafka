import path from 'node:path';
import { randomUUID } from 'node:crypto';
import express from 'express';
import { bootstrapService, envInt, logger, publish, runConsumer, TOPICS, tx, PoolClient, Mensaje } from '@pk/common';

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

interface LineaIn {
  productoId: string;
  cantidad: number;
  precioUnitario: number;
}

async function cambiarEstado(c: PoolClient, pedidoId: string, nuevo: string, motivo: string | null) {
  const r = await c.query(
    `UPDATE pedido p SET estado = $2, motivo_cancelacion = $3, updated_at = now(), version = p.version + 1
       FROM (SELECT id, estado FROM pedido WHERE id = $1 FOR UPDATE) old
      WHERE p.id = old.id AND old.estado = 'PENDIENTE'
      RETURNING old.estado AS anterior`,
    [pedidoId, nuevo, motivo],
  );
  if (r.rowCount) {
    await c.query('INSERT INTO pedido_historial (pedido_id, estado_anterior, estado_nuevo) VALUES ($1, $2, $3)', [
      pedidoId,
      r.rows[0].anterior,
      nuevo,
    ]);
  }
}

async function main() {
  const { pool, kafka, producer } = await bootstrapService({
    name: 'pedidos',
    migrationsDir: path.join(__dirname, '..', 'migrations'),
    topics: [TOPICS.pedidosEventos],
    consume: [TOPICS.pedidosEventos],
  });

  await runConsumer({
    kafka,
    pool,
    producer,
    groupId: 'pedidos',
    handlers: {
      [TOPICS.pedidosEventos]: {
        PedidoConfirmado: (c, m: Mensaje) => cambiarEstado(c, m.body.pedidoId, 'CONFIRMADO', null),
        PedidoCancelado: (c, m: Mensaje) => cambiarEstado(c, m.body.pedidoId, 'CANCELADO', m.body.motivo ?? null),
      },
    },
  });

  const app = express();
  app.use(express.json());
  app.get('/health', (_req, res) => res.send('ok'));

  // El gateway autentica y propaga la identidad en cabeceras (esta API no se expone fuera de la red interna).
  app.post('/pedidos', async (req, res) => {
    const clienteId = req.header('x-cliente-id');
    if (!clienteId || !UUID.test(clienteId)) return res.status(401).json({ error: 'cliente no identificado' });

    const { lineas, direccionEnvio, moneda = 'EUR' } = req.body ?? {};
    if (!Array.isArray(lineas) || lineas.length === 0 || !direccionEnvio || typeof direccionEnvio !== 'object') {
      return res.status(400).json({ error: 'se requieren lineas y direccionEnvio' });
    }
    const ids = new Set<string>();
    for (const l of lineas as LineaIn[]) {
      if (
        !UUID.test(l?.productoId) ||
        !Number.isInteger(l.cantidad) ||
        l.cantidad <= 0 ||
        typeof l.precioUnitario !== 'number' ||
        l.precioUnitario < 0 ||
        ids.has(l.productoId)
      ) {
        return res.status(400).json({ error: 'línea inválida (productoId único, cantidad > 0, precioUnitario >= 0)' });
      }
      ids.add(l.productoId);
    }
    const total = Math.round((lineas as LineaIn[]).reduce((s, l) => s + l.cantidad * l.precioUnitario, 0) * 100) / 100;
    const key = req.header('idempotency-key');

    try {
      const { pedidoId, nuevo } = await tx(pool, async (c) => {
        const pedidoId = randomUUID();
        if (key) {
          const r = await c.query(
            `INSERT INTO idempotency_key (cliente_id, key, pedido_id) VALUES ($1, $2, $3)
             ON CONFLICT DO NOTHING RETURNING pedido_id`,
            [clienteId, key, pedidoId],
          );
          if (!r.rowCount) {
            const e = await c.query('SELECT pedido_id FROM idempotency_key WHERE cliente_id = $1 AND key = $2', [
              clienteId,
              key,
            ]);
            return { pedidoId: e.rows[0].pedido_id as string, nuevo: false };
          }
        }
        await c.query(
          `INSERT INTO pedido (id, cliente_id, total, moneda, direccion_envio) VALUES ($1, $2, $3, $4, $5)`,
          [pedidoId, clienteId, total, moneda, JSON.stringify(direccionEnvio)],
        );
        for (const l of lineas as LineaIn[]) {
          await c.query(
            'INSERT INTO pedido_linea (pedido_id, producto_id, cantidad, precio_unitario) VALUES ($1, $2, $3, $4)',
            [pedidoId, l.productoId, l.cantidad, l.precioUnitario],
          );
        }
        await c.query('INSERT INTO pedido_historial (pedido_id, estado_nuevo) VALUES ($1, $2)', [pedidoId, 'PENDIENTE']);
        await publish(c, {
          aggregateType: 'pedido',
          aggregateId: pedidoId,
          eventType: 'PedidoCreado',
          payload: { pedidoId, clienteId, lineas, total, moneda, direccionEnvio },
        });
        return { pedidoId, nuevo: true };
      });
      res.status(nuevo ? 202 : 200).json({ pedidoId, estado: 'PENDIENTE' });
    } catch (e) {
      logger.error('Error creando pedido', { error: String(e) });
      res.status(500).json({ error: 'error interno' });
    }
  });

  app.get('/pedidos', async (req, res) => {
    const operador = req.header('x-rol') === 'operador';
    const r = await pool.query(
      `SELECT id, cliente_id, estado, total, moneda, motivo_cancelacion, created_at
         FROM pedido WHERE ($1::boolean OR cliente_id = $2) ORDER BY created_at DESC LIMIT 50`,
      [operador, req.header('x-cliente-id') ?? null],
    );
    res.json(r.rows);
  });

  app.get('/pedidos/:id', async (req, res) => {
    const id = req.params.id;
    if (!UUID.test(id)) return res.status(400).json({ error: 'id inválido' });
    const p = await pool.query('SELECT * FROM pedido WHERE id = $1', [id]);
    if (!p.rowCount) return res.status(404).json({ error: 'no encontrado' });
    const pedido = p.rows[0];
    if (req.header('x-rol') !== 'operador' && pedido.cliente_id !== req.header('x-cliente-id')) {
      return res.status(404).json({ error: 'no encontrado' });
    }
    const [lineas, historial] = await Promise.all([
      pool.query('SELECT producto_id, cantidad, precio_unitario FROM pedido_linea WHERE pedido_id = $1', [id]),
      pool.query('SELECT estado_anterior, estado_nuevo, fecha FROM pedido_historial WHERE pedido_id = $1 ORDER BY id', [id]),
    ]);
    res.json({ ...pedido, lineas: lineas.rows, historial: historial.rows });
  });

  // RF-06: se trata como fallo de la saga; el orquestador compensa lo ya hecho.
  app.post('/pedidos/:id/cancelar', async (req, res) => {
    const id = req.params.id;
    if (!UUID.test(id)) return res.status(400).json({ error: 'id inválido' });
    const aceptado = await tx(pool, async (c) => {
      const p = await c.query('SELECT cliente_id, estado FROM pedido WHERE id = $1', [id]);
      if (!p.rowCount) return 404;
      if (req.header('x-rol') !== 'operador' && p.rows[0].cliente_id !== req.header('x-cliente-id')) return 404;
      if (p.rows[0].estado !== 'PENDIENTE') return 409;
      await publish(c, {
        aggregateType: 'pedido',
        aggregateId: id,
        eventType: 'CancelacionSolicitada',
        payload: { pedidoId: id },
      });
      return 202;
    });
    res.status(aceptado).json(aceptado === 202 ? { pedidoId: id, mensaje: 'cancelación solicitada' } : { error: 'no cancelable' });
  });

  const port = envInt('PORT', 3000);
  app.listen(port, () => logger.info('Pedidos escuchando', { port }));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
