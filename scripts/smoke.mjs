// Prueba de humo extremo a extremo contra el gateway (escenarios AC-01..AC-04 y RF-04).
//   GATEWAY_URL=http://localhost:18080 JWT_SECRET=... node scripts/smoke.mjs
import { randomUUID } from 'node:crypto';
import { token } from './token.mjs';

const URL_BASE = process.env.GATEWAY_URL ?? 'http://localhost:18080';
const SECRET = process.env.JWT_SECRET ?? 'dev-secret-no-usar-en-produccion';
const cliente = randomUUID();
const hCliente = { authorization: `Bearer ${token(SECRET, cliente, 'cliente')}`, 'content-type': 'application/json' };
const hOper = { authorization: `Bearer ${token(SECRET, randomUUID(), 'operador')}` };

const PORTATIL = '11111111-1111-4111-8111-111111111111';
const RATON = '22222222-2222-4222-8222-222222222222';
const LIMITADA = '33333333-3333-4333-8333-333333333333'; // solo 2 unidades

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function crear(lineas, pais = 'ES', extra = {}) {
  const r = await fetch(`${URL_BASE}/api/pedidos`, {
    method: 'POST',
    headers: { ...hCliente, ...extra },
    body: JSON.stringify({ lineas, direccionEnvio: { calle: 'Gran Vía 1', ciudad: 'Madrid', pais } }),
  });
  if (r.status !== 202 && r.status !== 200) throw new Error(`POST /api/pedidos → ${r.status} ${await r.text()}`);
  return (await r.json()).pedidoId;
}

async function esperarFinal(id, segundos = 40) {
  for (let i = 0; i < segundos * 2; i++) {
    const r = await fetch(`${URL_BASE}/api/pedidos/${id}`, { headers: hCliente });
    const p = await r.json();
    if (p.estado && p.estado !== 'PENDIENTE') return p;
    await sleep(500);
  }
  throw new Error(`Pedido ${id} sigue PENDIENTE tras ${segundos}s`);
}

async function pasos(id) {
  const r = await fetch(`${URL_BASE}/api/sagas/${id}`, { headers: hOper });
  const s = await r.json();
  return (s.pasos ?? []).filter((p) => p.resultado !== 'ENVIADO').map((p) => `${p.tipo === 'COMPENSACION' ? '↩' : ''}${p.paso}:${p.resultado}`);
}

// Espera a que el sistema esté listo (el gateway arranca antes que los servicios tras `up -d`).
async function esperarListo(segundos = 120) {
  for (let i = 0; i < segundos; i++) {
    try {
      const r = await fetch(`${URL_BASE}/api/pedidos/${randomUUID()}`, { headers: hCliente });
      if (r.status === 404) return; // gateway → pedidos respondiendo
    } catch {}
    if (i === 0) console.log('Esperando a que los servicios estén listos…');
    await sleep(1000);
  }
  throw new Error(`El sistema no respondió en ${segundos}s`);
}
await esperarListo();

let fallos = 0;
async function escenario(nombre, lineas, pais, esperado, esperadoPasos) {
  const id = await crear(lineas, pais);
  const p = await esperarFinal(id);
  const ps = await pasos(id);
  const ok = p.estado === esperado && JSON.stringify(ps) === JSON.stringify(esperadoPasos);
  if (!ok) fallos++;
  console.log(`${ok ? 'OK  ' : 'FAIL'} ${nombre}: ${p.estado}${p.motivo_cancelacion ? ` (${p.motivo_cancelacion})` : ''}  [${ps.join(' → ')}]`);
}

await escenario('AC-01 camino feliz', [{ productoId: PORTATIL, cantidad: 1, precioUnitario: 900 }], 'ES', 'CONFIRMADO', [
  'RESERVAR_STOCK:OK',
  'COBRAR:OK',
  'CREAR_ENVIO:OK',
]);
await escenario('AC-02 stock insuficiente', [{ productoId: LIMITADA, cantidad: 5, precioUnitario: 50 }], 'ES', 'CANCELADO', [
  'RESERVAR_STOCK:FALLO',
]);
await escenario('AC-03 pago rechazado', [{ productoId: PORTATIL, cantidad: 10, precioUnitario: 900 }], 'ES', 'CANCELADO', [
  'RESERVAR_STOCK:OK',
  'COBRAR:FALLO',
  '↩RESERVAR_STOCK:OK',
]);
await escenario('AC-04 sin transportista', [{ productoId: RATON, cantidad: 2, precioUnitario: 20 }], 'JP', 'CANCELADO', [
  'RESERVAR_STOCK:OK',
  'COBRAR:OK',
  'CREAR_ENVIO:FALLO',
  '↩COBRAR:OK',
  '↩RESERVAR_STOCK:OK',
]);

// RF-04: misma Idempotency-Key ⇒ mismo pedido
const key = randomUUID();
const l = [{ productoId: RATON, cantidad: 1, precioUnitario: 20 }];
const a = await crear(l, 'ES', { 'idempotency-key': key });
const b = await crear(l, 'ES', { 'idempotency-key': key });
const okIdem = a === b;
if (!okIdem) fallos++;
console.log(`${okIdem ? 'OK  ' : 'FAIL'} RF-04 idempotency-key: ${a === b ? 'mismo pedido' : 'pedidos distintos'}`);
await esperarFinal(a);

// Seguridad básica del gateway
const sinToken = await fetch(`${URL_BASE}/api/pedidos/${a}`);
const comoCliente = await fetch(`${URL_BASE}/api/sagas`, { headers: hCliente });
const okSeg = sinToken.status === 401 && comoCliente.status === 403;
if (!okSeg) fallos++;
console.log(`${okSeg ? 'OK  ' : 'FAIL'} seguridad: sin token=${sinToken.status}, cliente en /api/sagas=${comoCliente.status}`);

console.log(fallos ? `\n${fallos} escenario(s) fallido(s)` : '\nTodos los escenarios correctos');
process.exit(fallos ? 1 : 0);
