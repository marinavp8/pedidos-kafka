// Genera un JWT HS256 de desarrollo/operación.
//   JWT_SECRET=... node scripts/token.mjs [clienteId] [cliente|operador]
import { createHmac, randomUUID } from 'node:crypto';

export function token(secret, sub = randomUUID(), rol = 'cliente', ttlSeg = 3600) {
  const b64 = (o) => Buffer.from(JSON.stringify(o)).toString('base64url');
  const cuerpo = `${b64({ alg: 'HS256', typ: 'JWT' })}.${b64({ sub, rol, exp: Math.floor(Date.now() / 1000) + ttlSeg })}`;
  return `${cuerpo}.${createHmac('sha256', secret).update(cuerpo).digest('base64url')}`;
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const secret = process.env.JWT_SECRET;
  if (!secret) {
    console.error('Define JWT_SECRET');
    process.exit(1);
  }
  console.log(token(secret, process.argv[2], process.argv[3]));
}
