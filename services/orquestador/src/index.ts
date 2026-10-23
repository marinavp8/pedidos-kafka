import path from 'node:path';
import { randomUUID } from 'node:crypto';
import express from 'express';
import {
  bootstrapService,
  envInt,
  Handler,
  logger,
  Pool,
  PoolClient,
  publish,
  runConsumer,
  TOPICS,
  tx,
} from '@pk/common';

type Paso = 'RESERVAR_STOCK' | 'COBRAR' | 'CREAR_ENVIO';
const ORDEN: Paso[] = ['RESERVAR_STOCK', 'COBRAR', 'CREAR_ENVIO']; // reversible → pivote → reintentable
const TERMINAL = ['COMPLETADA', 'CANCELADA', 'FALLIDA_MANUAL'];
const STEP_TIMEOUT_MS = envInt('SAGA_STEP_TIMEOUT_MS', 15000);
const MAX_INTENTOS = envInt('SAGA_MAX_INTENTOS', 5);

interface Comando {
  aggregateType: string;
  eventType: string;
  payload: object;
}
interface Contexto {
  clienteId: string;
  lineas: { productoId: string; cantidad: number }[];
  importe: number;
  moneda: string;
  direccion: object;
  completados: Paso[];
  pendientesComp: Paso[];
  cancelSolicitada?: boolean;
  ultimoComando?: Comando;
  estadoPrevio?: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  refs: Record<string, any>;
}
interface Saga {
  saga_id: string;
  pedido_id: string;
  estado: string;
  paso_actual: Paso | null;
  intentos: number;
  contexto: Contexto;
  motivo_fallo: string | null;
  version: number;
}

const idPayload = (s: Saga) => ({ sagaId: s.saga_id, pedidoId: s.pedido_id });

const ACCION: Record<Paso, { estado: string; aggregateType: string; accion: string; comp: string; payload: (s: Saga) => object }> = {
  RESERVAR_STOCK: {
    estado: 'RESERVANDO_STOCK',
    aggregateType: 'inventario-cmd',
    accion: 'ReservarStock',
    comp: 'LiberarStock',
    payload: (s) => ({ ...idPayload(s), lineas: s.contexto.lineas }),
  },
  COBRAR: {
    estado: 'COBRANDO',
    aggregateType: 'pagos-cmd',
    accion: 'CobrarPedido',
    comp: 'ReembolsarPago',
    payload: (s) => ({ ...idPayload(s), importe: s.contexto.importe, moneda: s.contexto.moneda }),
  },
  CREAR_ENVIO: {
    estado: 'CREANDO_ENVIO',
    aggregateType: 'envios-cmd',
    accion: 'CrearEnvio',
    comp: 'CancelarEnvio',
    payload: (s) => ({ ...idPayload(s), direccion: s.contexto.direccion }),
  },
};

// ---------- persistencia ----------

async function cargar(c: PoolClient, sagaId: string): Promise<Saga | undefined> {
  const r = await c.query('SELECT * FROM saga_state WHERE saga_id = $1 FOR UPDATE', [sagaId]);
  return r.rows[0];
}

/** Guarda la saga con bloqueo optimista (version). timeoutMs=null ⇒ sin vencimiento. */
async function guardar(c: PoolClient, s: Saga, timeoutMs: number | null): Promise<void> {
  const r = await c.query(
    `UPDATE saga_state
        SET estado = $2, paso_actual = $3, intentos = $4, contexto = $5, motivo_fallo = $6,
            deadline_at = CASE WHEN $7::bigint IS NULL THEN NULL ELSE now() + ($7::bigint * interval '1 millisecond') END,
            version = version + 1, updated_at = now()
      WHERE saga_id = $1 AND version = $8`,
    [s.saga_id, s.estado, s.paso_actual, s.intentos, JSON.stringify(s.contexto), s.motivo_fallo, timeoutMs, s.version],
  );
  if (!r.rowCount) throw new Error(`Conflicto de versión en la saga ${s.saga_id}`);
  s.version++;
}

async function registrarPaso(
  c: PoolClient,
  sagaId: string,
  paso: Paso,
  tipo: 'ACCION' | 'COMPENSACION',
  resultado: 'ENVIADO' | 'OK' | 'FALLO',
  detalle?: object,
) {
  await c.query('INSERT INTO saga_step (saga_id, paso, tipo, resultado, detalle) VALUES ($1, $2, $3, $4, $5)', [
    sagaId,
    paso,
    tipo,
    resultado,
    detalle ? JSON.stringify(detalle) : null,
  ]);
}

async function enviarComando(c: PoolClient, s: Saga, cmd: Comando) {
  s.contexto.ultimoComando = cmd;
  await publish(c, {
    aggregateType: cmd.aggregateType,
    aggregateId: s.pedido_id,
    eventType: cmd.eventType,
    payload: cmd.payload,
    headers: { sagaId: s.saga_id },
  });
}

// ---------- máquina de estados ----------

async function iniciarPaso(c: PoolClient, s: Saga, paso: Paso) {
  const a = ACCION[paso];
  s.estado = a.estado;
  s.paso_actual = paso;
  s.intentos = 0;
  await enviarComando(c, s, { aggregateType: a.aggregateType, eventType: a.accion, payload: a.payload(s) });
  await registrarPaso(c, s.saga_id, paso, 'ACCION', 'ENVIADO');
  await guardar(c, s, STEP_TIMEOUT_MS);
}

async function completar(c: PoolClient, s: Saga) {
  s.estado = 'COMPLETADA';
  s.paso_actual = null;
  await publish(c, {
    aggregateType: 'pedido',
    aggregateId: s.pedido_id,
    eventType: 'PedidoConfirmado',
    payload: { pedidoId: s.pedido_id, clienteId: s.contexto.clienteId },
    headers: { sagaId: s.saga_id },
  });
  await guardar(c, s, null);
}

async function siguienteCompensacion(c: PoolClient, s: Saga) {
  const paso = s.contexto.pendientesComp[0];
  if (!paso) {
    s.estado = 'CANCELADA';
    s.paso_actual = null;
    await publish(c, {
      aggregateType: 'pedido',
      aggregateId: s.pedido_id,
      eventType: 'PedidoCancelado',
      payload: { pedidoId: s.pedido_id, clienteId: s.contexto.clienteId, motivo: s.motivo_fallo },
      headers: { sagaId: s.saga_id },
    });
    await guardar(c, s, null);
    return;
  }
  const a = ACCION[paso];
  s.paso_actual = paso;
  s.intentos = 0;
  await enviarComando(c, s, { aggregateType: a.aggregateType, eventType: a.comp, payload: idPayload(s) });
  await registrarPaso(c, s.saga_id, paso, 'COMPENSACION', 'ENVIADO');
  await guardar(c, s, STEP_TIMEOUT_MS);
}

/** Compensa SOLO lo completado, del último al primero. */
async function compensar(c: PoolClient, s: Saga, motivo: string) {
  s.estado = 'COMPENSANDO';
  s.motivo_fallo = motivo;
  s.contexto.pendientesComp = [...s.contexto.completados].reverse();
  await siguienteCompensacion(c, s);
}

// ---------- handlers de mensajes ----------

const alCrearse: Handler = async (c, m) => {
  const b = m.body;
  const contexto: Contexto = {
    clienteId: b.clienteId,
    lineas: b.lineas.map((l: { productoId: string; cantidad: number }) => ({ productoId: l.productoId, cantidad: l.cantidad })),
    importe: b.total,
    moneda: b.moneda,
    direccion: b.direccionEnvio,
    completados: [],
    pendientesComp: [],
    refs: {},
  };
  const r = await c.query(
    `INSERT INTO saga_state (saga_id, pedido_id, estado, contexto) VALUES ($1, $2, 'INICIADA', $3)
     ON CONFLICT (pedido_id) DO NOTHING RETURNING *`,
    [randomUUID(), b.pedidoId, JSON.stringify(contexto)],
  );
  if (!r.rowCount) return; // PedidoCreado duplicado: ya existe la saga
  await iniciarPaso(c, r.rows[0], 'RESERVAR_STOCK');
};

const alSolicitarCancelacion: Handler = async (c, m) => {
  const r = await c.query('SELECT saga_id FROM saga_state WHERE pedido_id = $1', [m.body.pedidoId]);
  if (!r.rowCount) return;
  const s = await cargar(c, r.rows[0].saga_id);
  if (!s || TERMINAL.includes(s.estado) || s.estado === 'COMPENSANDO') return;
  s.contexto.cancelSolicitada = true; // se aplica al llegar la respuesta del paso en vuelo
  await guardar(c, s, STEP_TIMEOUT_MS);
};

function respuestaAccion(paso: Paso, ok: boolean, motivoPorDefecto: string): Handler {
  return async (c, m) => {
    const s = await cargar(c, m.body.sagaId);
    if (!s || TERMINAL.includes(s.estado)) return;
    if (s.estado !== ACCION[paso].estado || s.paso_actual !== paso) {
      logger.warn('Respuesta fuera de orden ignorada', { sagaId: s.saga_id, tipo: m.tipo, estado: s.estado });
      return;
    }
    s.contexto.refs[paso] = m.body;
    if (!ok) {
      await registrarPaso(c, s.saga_id, paso, 'ACCION', 'FALLO', { motivo: m.body.motivo ?? motivoPorDefecto });
      return compensar(c, s, m.body.motivo ?? motivoPorDefecto);
    }
    await registrarPaso(c, s.saga_id, paso, 'ACCION', 'OK');
    s.contexto.completados.push(paso);
    if (s.contexto.cancelSolicitada) return compensar(c, s, 'Cancelado por el cliente');
    const siguiente = ORDEN[ORDEN.indexOf(paso) + 1];
    return siguiente ? iniciarPaso(c, s, siguiente) : completar(c, s);
  };
}

function respuestaCompensacion(paso: Paso): Handler {
  return async (c, m) => {
    const s = await cargar(c, m.body.sagaId);
    if (!s || s.estado !== 'COMPENSANDO' || s.contexto.pendientesComp[0] !== paso) return;
    await registrarPaso(c, s.saga_id, paso, 'COMPENSACION', 'OK');
    s.contexto.pendientesComp.shift();
    return siguienteCompensacion(c, s);
  };
}

// ---------- timeouts y reintentos ----------

async function escanearVencidas(pool: Pool) {
  await tx(pool, async (c) => {
    const { rows } = await c.query<Saga>(
      `SELECT * FROM saga_state
        WHERE deadline_at < now() AND estado NOT IN ('COMPLETADA','CANCELADA','FALLIDA_MANUAL')
        ORDER BY deadline_at LIMIT 20 FOR UPDATE SKIP LOCKED`,
    );
    for (const s of rows) {
      const cmd = s.contexto.ultimoComando;
      if (!cmd || s.intentos >= MAX_INTENTOS) {
        s.contexto.estadoPrevio = s.estado;
        s.estado = 'FALLIDA_MANUAL';
        await publish(c, {
          aggregateType: 'saga-dlq',
          aggregateId: s.pedido_id,
          eventType: 'SagaFallida',
          payload: { ...idPayload(s), estadoPrevio: s.contexto.estadoPrevio, motivo: 'Reintentos agotados' },
        });
        await guardar(c, s, null);
        logger.error('Saga en FALLIDA_MANUAL', { sagaId: s.saga_id });
      } else {
        await enviarComando(c, s, cmd); // el destinatario es idempotente por (sagaId, paso)
        s.intentos++;
        await guardar(c, s, STEP_TIMEOUT_MS * 2 ** s.intentos); // backoff exponencial
        logger.warn('Reintento de comando', { sagaId: s.saga_id, comando: cmd.eventType, intento: s.intentos });
      }
    }
  });
}

async function main() {
  const { pool, kafka, producer } = await bootstrapService({
    name: 'orquestador',
    migrationsDir: path.join(__dirname, '..', 'migrations'),
    topics: Object.values(TOPICS),
    consume: [TOPICS.pedidosEventos, TOPICS.sagaRespuestas],
  });

  await runConsumer({
    kafka,
    pool,
    producer,
    groupId: 'orquestador',
    handlers: {
      [TOPICS.pedidosEventos]: { PedidoCreado: alCrearse, CancelacionSolicitada: alSolicitarCancelacion },
      [TOPICS.sagaRespuestas]: {
        StockReservado: respuestaAccion('RESERVAR_STOCK', true, ''),
        StockInsuficiente: respuestaAccion('RESERVAR_STOCK', false, 'Stock insuficiente'),
        PagoCompletado: respuestaAccion('COBRAR', true, ''),
        PagoRechazado: respuestaAccion('COBRAR', false, 'Pago rechazado'),
        EnvioCreado: respuestaAccion('CREAR_ENVIO', true, ''),
        SinTransportista: respuestaAccion('CREAR_ENVIO', false, 'Sin transportista'),
        StockLiberado: respuestaCompensacion('RESERVAR_STOCK'),
        PagoReembolsado: respuestaCompensacion('COBRAR'),
        EnvioCancelado: respuestaCompensacion('CREAR_ENVIO'),
      },
    },
  });

  setInterval(
    () => escanearVencidas(pool).catch((e) => logger.error('Fallo en el escaneo de timeouts', { error: String(e) })),
    envInt('SAGA_SCAN_MS', 5000),
  );

  // API de operación (RF-60): el gateway solo enruta aquí a usuarios con rol operador.
  const app = express();
  app.get('/health', (_req, res) => res.send('ok'));

  app.get('/sagas', async (req, res) => {
    const estado = req.query.estado as string | undefined;
    const r = await pool.query(
      `SELECT saga_id, pedido_id, estado, paso_actual, intentos, motivo_fallo, updated_at
         FROM saga_state WHERE ($1::text IS NULL OR estado = $1) ORDER BY updated_at DESC LIMIT 100`,
      [estado ?? null],
    );
    res.json(r.rows);
  });

  app.get('/sagas/:pedidoId', async (req, res) => {
    const s = await pool.query('SELECT * FROM saga_state WHERE pedido_id = $1 OR saga_id = $1', [req.params.pedidoId]).catch(() => null);
    if (!s?.rowCount) return res.status(404).json({ error: 'no encontrada' });
    const pasos = await pool.query('SELECT paso, tipo, resultado, detalle, fecha FROM saga_step WHERE saga_id = $1 ORDER BY id', [
      s.rows[0].saga_id,
    ]);
    res.json({ ...s.rows[0], pasos: pasos.rows });
  });

  app.post('/sagas/:sagaId/reintentar', async (req, res) => {
    const resultado = await tx(pool, async (c) => {
      const s = await cargar(c, req.params.sagaId).catch(() => undefined);
      if (!s || s.estado !== 'FALLIDA_MANUAL' || !s.contexto.ultimoComando) return 409;
      s.estado = s.contexto.estadoPrevio ?? 'COBRANDO';
      s.intentos = 0;
      await enviarComando(c, s, s.contexto.ultimoComando);
      await guardar(c, s, STEP_TIMEOUT_MS);
      return 202;
    });
    res.status(resultado).json({ resultado });
  });

  const port = envInt('PORT', 3000);
  app.listen(port, () => logger.info('Orquestador escuchando', { port }));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
