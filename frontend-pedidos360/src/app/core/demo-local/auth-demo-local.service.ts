import { Injectable, computed, inject, signal } from '@angular/core';
import { Router } from '@angular/router';
import { Observable, defer, from, of } from 'rxjs';
import { claimAsList, claimAsString, decodeJwt, expiraEn } from '../jwt.util';

const CLAVE_TOKEN = 'pedidos360-demo-local.token';
const USUARIOS = ['admin', 'cliente'] as const;
type UsuarioDemo = (typeof USUARIOS)[number];

/**
 * Sesión del MODO DEMO LOCAL. Tiene la misma interfaz pública que AuthService (MSAL), así que las vistas,
 * la barra de navegación y roleGuard funcionan igual; solo cambia de dónde sale el access token:
 * en vez de Entra External ID, lo emite el IDaaS local (demo-local/idaas-local.mjs) a través del proxy
 * del dev-server (/idaas). La firma la siguen validando los microservicios con el JWKS de ese IDaaS.
 *
 * Usuario: ?usuario=admin (por defecto) o ?usuario=cliente en la URL al iniciar sesión.
 * No usa HttpClient a propósito: el interceptor depende de este servicio y se formaría un ciclo.
 */
@Injectable()
export class AuthDemoLocalService {
  private readonly router = inject(Router);

  readonly modoDemoLocal = true;
  readonly etiquetaModo = 'Demo local';
  readonly detalleModo = 'Token emitido por el IDaaS local de demo-local/idaas-local.mjs, no por Entra';
  readonly pieModo = 'Demo local · Angular · IDaaS local · Spring Boot en este PC · RabbitMQ y Kafka en Docker';
  readonly accessToken = signal<string | null>(this.tokenGuardado());
  readonly interactionInProgress = signal(false);

  readonly accessClaims = computed(() => decodeJwt(this.accessToken()));
  /** El IDaaS local entrega un solo token: sus claims hacen también de ID token. */
  readonly idTokenClaims = computed(() => this.accessClaims());
  readonly account = computed(() => {
    const claims = this.accessClaims();
    return claims ? { name: claimAsString(claims, 'name'), username: claimAsString(claims, 'email') } : null;
  });
  readonly isLoggedIn = computed(() => this.accessClaims() !== null);

  readonly roles = computed(() => claimAsList(this.accessClaims(), 'roles'));
  readonly scopes = computed(() => claimAsList(this.accessClaims(), 'scp'));
  readonly isAdmin = computed(() => this.roles().some((r) => r.toUpperCase() === 'ADMIN'));
  readonly displayName = computed(() => claimAsString(this.accessClaims(), 'name') || 'Usuario');
  readonly email = computed(() => claimAsString(this.accessClaims(), 'email'));
  readonly tokenExpira = computed(() => expiraEn(this.accessClaims()));

  /** Pide un token al IDaaS local. Devuelve el token o rechaza si el IDaaS no responde. */
  iniciarSesion(): Promise<string> {
    const pedido = new URLSearchParams(window.location.search).get('usuario');
    const usuario: UsuarioDemo = USUARIOS.includes(pedido as UsuarioDemo) ? (pedido as UsuarioDemo) : 'admin';
    return fetch(`/idaas/token?usuario=${usuario}`)
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(`IDaaS local respondió ${r.status}`))))
      .then((cuerpo: { access_token: string }) => {
        sessionStorage.setItem(CLAVE_TOKEN, cuerpo.access_token);
        this.accessToken.set(cuerpo.access_token);
        return cuerpo.access_token;
      });
  }

  login(): void {
    this.iniciarSesion().catch((err) =>
      console.error('No se pudo iniciar sesión: ¿está corriendo demo-local/idaas-local.mjs?', err),
    );
  }

  logout(): void {
    sessionStorage.removeItem(CLAVE_TOKEN);
    this.accessToken.set(null);
    void this.router.navigateByUrl('/');
  }

  refreshAccessToken(): Observable<string> {
    const actual = this.accessToken();
    return actual ? of(actual) : defer(() => from(this.iniciarSesion()));
  }

  hasRole(role: string): boolean {
    return this.roles().some((r) => r.toUpperCase() === role.toUpperCase());
  }

  /** Recupera el token de la pestaña si sigue vigente (sessionStorage, igual que la caché de MSAL). */
  private tokenGuardado(): string | null {
    const token = sessionStorage.getItem(CLAVE_TOKEN);
    const exp = decodeJwt(token)?.['exp'];
    if (token && typeof exp === 'number' && exp * 1000 > Date.now()) return token;
    sessionStorage.removeItem(CLAVE_TOKEN);
    return null;
  }
}
