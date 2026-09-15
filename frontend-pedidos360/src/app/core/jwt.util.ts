/** Utilidades para leer claims de un JWT en el navegador (solo lectura, sin validar firma:
 *  la validación real la hacen el API Gateway y el backend). */
export type JwtPayload = Record<string, unknown>;

export function decodeJwt(token: string | null | undefined): JwtPayload | null {
  if (!token) return null;
  try {
    const part = token.split('.')[1];
    const base64 = part.replace(/-/g, '+').replace(/_/g, '/');
    const bin = atob(base64);
    const json = decodeURIComponent(
      Array.from(bin, (c) => '%' + c.charCodeAt(0).toString(16).padStart(2, '0')).join(''),
    );
    return JSON.parse(json) as JwtPayload;
  } catch {
    return null;
  }
}

/** Devuelve un claim como lista de strings: soporta arreglos ("roles") y strings
 *  separados por espacio ("scp"). */
export function claimAsList(payload: JwtPayload | null, claim: string): string[] {
  const value = payload?.[claim];
  if (Array.isArray(value)) return value.map(String);
  if (typeof value === 'string') return value.split(' ').filter(Boolean);
  return [];
}

export function claimAsString(payload: JwtPayload | null, claim: string): string {
  const value = payload?.[claim];
  if (Array.isArray(value)) return value.map(String).join(', ');
  return value == null ? '' : String(value);
}

export function expiraEn(payload: JwtPayload | null): string {
  const exp = payload?.['exp'];
  if (typeof exp !== 'number') return '';
  return new Date(exp * 1000).toLocaleString();
}
