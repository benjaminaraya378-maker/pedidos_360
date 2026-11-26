// IDaaS local de prueba (igual que en EP4), SOLO para demostraciones y pruebas en este PC.
// Publica su JWKS en http://127.0.0.1:8099/keys y firma tokens con los mismos claims que emite Entra External ID
// (oid, roles, scp, name, email). Los microservicios validan firma (JWKS), emisor, audiencia, vigencia, rol y scope
// igual que con el IDaaS real: solo cambia quién firma.
//
//   node demo-local/idaas-local.mjs            → escribe admin.jwt, cliente.jwt e idaas.env junto a este script
//   GET /idaas/token?usuario=admin|cliente     → token nuevo (lo usa el frontend en `npm run demo-local`)
//
// La clave RSA se crea la primera vez y se guarda en clave-local.pem (ignorada por git), y los usuarios tienen un oid
// fijo: al reiniciar el IDaaS, los servicios siguen aceptando los tokens y cada usuario sigue viendo sus pedidos.
// Escucha solo en 127.0.0.1. No usar fuera de una demo local: entrega tokens a quien se los pida.
import { createHash, createPrivateKey, createPublicKey, generateKeyPairSync, sign } from 'node:crypto';
import { createServer } from 'node:http';
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const DIR = process.argv[2] ?? fileURLToPath(new URL('.', import.meta.url));
const PUERTO = 8099;
const ISSUER = 'https://pedidos360-local.ciamlogin.com/tenant-local/v2.0';
const AUDIENCE = 'api-pedidos360-local';
const DURACION_S = 12 * 3600;

const ARCHIVO_CLAVE = join(DIR, 'clave-local.pem');
if (!existsSync(ARCHIVO_CLAVE)) {
  const { privateKey } = generateKeyPairSync('rsa', { modulusLength: 2048 });
  writeFileSync(ARCHIVO_CLAVE, privateKey.export({ type: 'pkcs8', format: 'pem' }), { mode: 0o600 });
}
const privateKey = createPrivateKey(readFileSync(ARCHIVO_CLAVE));
const publica = createPublicKey(privateKey).export({ format: 'jwk' });
// kid derivado de la clave: si la clave cambia, cambia el kid y los servicios vuelven a leer el JWKS solos.
const KID = 'clave-local-' + createHash('sha256').update(publica.n).digest('hex').slice(0, 8);
const jwk = { ...publica, kid: KID, alg: 'RS256', use: 'sig' };

const b64 = (obj) => Buffer.from(JSON.stringify(obj)).toString('base64url');
function token(claims) {
  const ahora = Math.floor(Date.now() / 1000);
  const header = b64({ alg: 'RS256', typ: 'JWT', kid: KID });
  const payload = b64({ iss: ISSUER, aud: AUDIENCE, iat: ahora, nbf: ahora, exp: ahora + DURACION_S, ...claims });
  const firma = sign('sha256', Buffer.from(`${header}.${payload}`), privateKey).toString('base64url');
  return `${header}.${payload}.${firma}`;
}

const usuarios = {
  admin: { oid: '0b6a4a62-5d0e-4c1e-9a51-0000000000a1', sub: 'admin-ep5', name: 'Benjamin Araya', email: 'admin@pedidos360.test',
    roles: ['Admin'], scp: 'Pedidos360.Read Pedidos360.Write' },
  cliente: { oid: '0b6a4a62-5d0e-4c1e-9a51-0000000000c1', sub: 'cliente-ep5', name: 'Oscar Baez', email: 'cliente@pedidos360.test',
    roles: ['Cliente'], scp: 'Pedidos360.Read Pedidos360.Write' },
};
for (const [nombre, claims] of Object.entries(usuarios)) {
  writeFileSync(join(DIR, `${nombre}.jwt`), token(claims));
}
writeFileSync(join(DIR, 'idaas.env'),
  `JWT_ISSUER=${ISSUER}\nJWT_JWKS_URI=http://127.0.0.1:${PUERTO}/keys\nJWT_AUDIENCE=${AUDIENCE}\n`);

const json = (res, estado, cuerpo) => {
  res.writeHead(estado, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
  res.end(JSON.stringify(cuerpo));
};

createServer((req, res) => {
  const url = new URL(req.url ?? '/', `http://127.0.0.1:${PUERTO}`);
  if (req.method === 'GET' && (url.pathname === '/keys' || url.pathname === '/idaas/keys')) {
    return json(res, 200, { keys: [jwk] });
  }
  if (req.method === 'GET' && url.pathname === '/idaas/token') {
    const usuario = url.searchParams.get('usuario') ?? 'admin';
    if (!Object.hasOwn(usuarios, usuario)) return json(res, 400, { error: 'usuario debe ser admin o cliente' });
    return json(res, 200, { access_token: token(usuarios[usuario]), token_type: 'Bearer', expires_in: DURACION_S });
  }
  json(res, 404, { error: 'no encontrado' });
}).listen(PUERTO, '127.0.0.1', () =>
  console.log(`IDaaS local: JWKS en http://127.0.0.1:${PUERTO}/keys · tokens en ${DIR} y en /idaas/token`));
