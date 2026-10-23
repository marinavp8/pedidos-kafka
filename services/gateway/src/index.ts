import { createHmac, randomUUID, timingSafeEqual } from 'node:crypto';
import path from 'node:path';
import express, { NextFunction, Request, Response } from 'express';

const PORT = parseInt(process.env.PORT ?? '3000', 10);
const PEDIDOS_URL = process.env.PEDIDOS_URL ?? 'http://pedidos:3000';
const ORQUESTADOR_URL = process.env.ORQUESTADOR_URL ?? 'http://orquestador:3000';
const JWT_SECRET = process.env.JWT_SECRET;
// SOLO demostración: permite obtener tokens sin identity provider. Debe estar desactivado en producción.
const DEV_LOGIN = process.env.DEV_LOGIN === 'true';
if (!JWT_SECRET) throw new Error('Falta JWT_SECRET');

interface Claims {
  sub: string; // clienteId (uuid)
  rol: 'cliente' | 'operador';
  exp?: number;
}

/** Verificación JWT HS256 (sin dependencias). */
function verificar(token: string): Claims {
  const [h, p, s] = token.split('.');
  if (!h || !p || !s) throw new Error('formato');
  if (JSON.parse(Buffer.from(h, 'base64url').toString()).alg !== 'HS256') throw new Error('alg');
  const esperado = createHmac('sha256', JWT_SECRET!).update(`${h}.${p}`).digest();
  const dado = Buffer.from(s, 'base64url');
  if (dado.length !== esperado.length || !timingSafeEqual(dado, esperado)) throw new Error('firma');
  const claims = JSON.parse(Buffer.from(p, 'base64url').toString()) as Claims;
  if (claims.exp && claims.exp < Date.now() / 1000) throw new Error('expirado');
  if (!claims.sub) throw new Error('sin sub');
  return claims;
}

type ReqAuth = Request & { claims?: Claims };

function auth(req: ReqAuth, res: Response, next: NextFunction) {
  const m = /^Bearer (.+)$/.exec(req.header('authorization') ?? '');
  try {
    if (!m) throw new Error('sin token');
    req.claims = verificar(m[1]);
    next();
  } catch {
    res.status(401).json({ error: 'no autenticado' });
  }
}

const soloOperador = (req: ReqAuth, res: Response, next: NextFunction) =>
  req.claims?.rol === 'operador' ? next() : res.status(403).json({ error: 'requiere rol operador' });

async function proxy(req: ReqAuth, res: Response, base: string, ruta: string) {
  const headers: Record<string, string> = {
    'content-type': 'application/json',
    // La identidad sale SIEMPRE del token, nunca de lo que envíe el cliente.
    'x-cliente-id': req.claims!.sub,
    'x-rol': req.claims!.rol ?? 'cliente',
  };
  const idem = req.header('idempotency-key');
  if (idem) headers['idempotency-key'] = idem;
  try {
    const r = await fetch(`${base}${ruta}`, {
      method: req.method,
      headers,
      body: ['GET', 'HEAD'].includes(req.method) ? undefined : JSON.stringify(req.body ?? {}),
      signal: AbortSignal.timeout(10000),
    });
    res.status(r.status).type(r.headers.get('content-type') ?? 'application/json').send(await r.text());
  } catch {
    res.status(502).json({ error: 'servicio no disponible' });
  }
}

const app = express();
app.disable('x-powered-by');
app.use(express.json({ limit: '100kb' }));
app.get('/health', (_req, res) => res.send('ok'));

app.get('/auth/config', (_req, res) => res.json({ devLogin: DEV_LOGIN }));
app.post('/auth/dev', (req, res) => {
  if (!DEV_LOGIN) return res.status(404).json({ error: 'no encontrado' });
  const rol = req.body?.rol === 'operador' ? 'operador' : 'cliente';
  const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
  const sub = uuid.test(req.body?.clienteId ?? '') ? req.body.clienteId : randomUUID();
  const b64 = (o: object) => Buffer.from(JSON.stringify(o)).toString('base64url');
  const cuerpo = `${b64({ alg: 'HS256', typ: 'JWT' })}.${b64({ sub, rol, exp: Math.floor(Date.now() / 1000) + 8 * 3600 })}`;
  const firma = createHmac('sha256', JWT_SECRET!).update(cuerpo).digest('base64url');
  res.json({ token: `${cuerpo}.${firma}`, sub, rol });
});

app.use('/api/pedidos', auth, (req: ReqAuth, res) => proxy(req, res, PEDIDOS_URL, `/pedidos${req.url === '/' ? '' : req.url}`));
app.use('/api/sagas', auth, soloOperador, (req: ReqAuth, res) =>
  proxy(req, res, ORQUESTADOR_URL, `/sagas${req.url === '/' ? '' : req.url}`),
);
app.use(express.static(path.join(__dirname, '..', 'public')));
app.use((_req, res) => res.status(404).json({ error: 'no encontrado' }));

app.listen(PORT, () => console.log(JSON.stringify({ servicio: 'gateway', msg: 'escuchando', port: PORT })));
