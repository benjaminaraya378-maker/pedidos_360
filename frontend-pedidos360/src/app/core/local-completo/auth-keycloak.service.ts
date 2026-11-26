import { Injectable, computed, inject, signal } from '@angular/core';
import { Router } from '@angular/router';
import { Log, User, UserManager, WebStorageStateStore } from 'oidc-client-ts';
import { Observable, defer, from, map, of } from 'rxjs';
import { environment } from '../../../environments/environment';
import { claimAsList, claimAsString, decodeJwt, expiraEn } from '../jwt.util';

/** Bloque `oidc` de environment.local-completo.ts. */
interface ConfigOidc {
  authority: string;
  clientId: string;
  scope: string;
}

/**
 * Solo environment.local-completo.ts trae `oidc`. tsconfig.app.json compila todo src/**, así que este archivo
 * también se revisa en production, development y demo-local: por eso el bloque se lee como opcional.
 */
const entorno: { api: { baseUrl: string }; oidc?: ConfigOidc } = environment;

function configOidc(): ConfigOidc {
  if (!entorno.oidc) {
    throw new Error('Falta environment.oidc: el modo local completo se usa con `npm run local-completo`.');
  }
  return entorno.oidc;
}

/** Acepta solo rutas internas de la app como destino después del login (nada de //otro-sitio ni URLs absolutas). */
function rutaInterna(valor: unknown): string {
  return typeof valor === 'string' && valor.startsWith('/') && !valor.startsWith('//') ? valor : '/';
}

/**
 * Sesión del MODO LOCAL COMPLETO. Tiene la misma interfaz pública que AuthService (MSAL), así que las vistas,
 * la barra de navegación y roleGuard funcionan igual; lo que cambia es el IDaaS: en vez de Entra External ID,
 * el usuario inicia sesión en Keycloak (realm "pedidos360") con OpenID Connect, Authorization Code + PKCE (S256).
 * oidc-client-ts genera code_verifier / code_challenge, valida state y canjea el código en el token endpoint;
 * el access token lo valida después Kong (API Gateway local) y, otra vez, cada microservicio.
 *
 * La sesión se guarda en sessionStorage (igual que la caché de MSAL) y se renueva sola con el refresh token
 * un minuto antes de que venza el access token (automaticSilentRenew).
 * No usa HttpClient a propósito: el interceptor depende de este servicio y se formaría un ciclo.
 */
@Injectable()
export class AuthKeycloakService {
  private readonly router = inject(Router);
  private readonly oidc = configOidc();
  private readonly userManager = new UserManager({
    authority: this.oidc.authority,
    client_id: this.oidc.clientId,
    redirect_uri: window.location.origin + '/',
    post_logout_redirect_uri: window.location.origin + '/',
    response_type: 'code',
    scope: this.oidc.scope,
    userStore: new WebStorageStateStore({ store: window.sessionStorage }),
    // state y code_verifier de PKCE mientras el navegador está en Keycloak (por defecto irían a localStorage)
    stateStore: new WebStorageStateStore({ store: window.sessionStorage }),
  });

  readonly modoDemoLocal = true;
  readonly etiquetaModo = 'Local: Keycloak + Kong';
  readonly detalleModo =
    'Sesión de Keycloak (realm pedidos360) en lugar de Entra External ID; el API pasa por Kong en lugar de AWS API Gateway';
  readonly pieModo =
    'Local completo · Angular · Keycloak (IDaaS) · Kong (API Gateway) · Spring Boot en este PC · RabbitMQ y Kafka en Docker';

  readonly accessToken = signal<string | null>(null);
  private readonly idToken = signal<string | null>(null);
  readonly interactionInProgress = signal(true);

  readonly accessClaims = computed(() => decodeJwt(this.accessToken()));
  readonly idTokenClaims = computed(() => decodeJwt(this.idToken()));
  readonly account = computed(() => {
    const claims = this.idTokenClaims() ?? this.accessClaims();
    return claims ? { name: claimAsString(claims, 'name'), username: claimAsString(claims, 'email') } : null;
  });
  readonly isLoggedIn = computed(() => this.accessClaims() !== null);

  /** Roles (claim "roles") y scopes (claim "scp") leídos del access token, igual que con Entra. */
  readonly roles = computed(() => claimAsList(this.accessClaims(), 'roles'));
  readonly scopes = computed(() => claimAsList(this.accessClaims(), 'scp'));
  readonly isAdmin = computed(() => this.roles().some((r) => r.toUpperCase() === 'ADMIN'));
  readonly displayName = computed(
    () =>
      claimAsString(this.accessClaims(), 'name') ||
      claimAsString(this.idTokenClaims(), 'name') ||
      this.email() ||
      'Usuario',
  );
  readonly email = computed(
    () =>
      claimAsString(this.accessClaims(), 'email') ||
      claimAsString(this.idTokenClaims(), 'email') ||
      claimAsString(this.idTokenClaims(), 'preferred_username'),
  );
  readonly tokenExpira = computed(() => expiraEn(this.accessClaims()));

  constructor() {
    Log.setLogger(console);
    Log.setLevel(Log.WARN);

    // Login, renovación con refresh token y cierre de sesión pasan por estos eventos.
    this.userManager.events.addUserLoaded((user) => this.aplicarUsuario(user));
    this.userManager.events.addUserUnloaded(() => this.aplicarUsuario(null));
    this.userManager.events.addAccessTokenExpired(() => void this.userManager.removeUser());
    this.userManager.events.addSilentRenewError((err) =>
      console.error('No se pudo renovar el token con Keycloak', err),
    );
  }

  /**
   * Se ejecuta al arrancar la app (provideAppInitializer), antes de la primera navegación del router:
   *  - si Keycloak acaba de volver con ?code=…&state=…, canjea el código (signinRedirectCallback) y deja en la
   *    barra de direcciones la ruta que se pidió antes del login, sin el código;
   *  - si no, recupera la sesión guardada en la pestaña, si el access token sigue vigente.
   */
  async iniciar(): Promise<void> {
    const parametros = new URLSearchParams(window.location.search);
    const esRespuestaDeKeycloak =
      parametros.has('state') && (parametros.has('code') || parametros.has('error'));
    try {
      if (esRespuestaDeKeycloak) {
        let destino = '/';
        try {
          const user = await this.userManager.signinRedirectCallback();
          this.aplicarUsuario(user);
          destino = rutaInterna(user.state);
        } catch (err) {
          console.error('Keycloak rechazó el inicio de sesión o la respuesta no es válida', err);
        }
        window.history.replaceState(window.history.state, '', destino);
        return;
      }
      const user = await this.userManager.getUser();
      if (user && !user.expired) this.aplicarUsuario(user);
      else if (user) await this.userManager.removeUser();
    } finally {
      this.interactionInProgress.set(false);
    }
  }

  /** Redirige a la pantalla de login de Keycloak; al volver, la app abre `destino`. */
  iniciarSesion(destino: string): Promise<void> {
    this.interactionInProgress.set(true);
    return this.userManager.signinRedirect({ state: rutaInterna(destino) }).catch((err) => {
      this.interactionInProgress.set(false);
      console.error('No se pudo iniciar sesión: ¿está corriendo Keycloak en http://localhost:8180?', err);
      throw err;
    });
  }

  login(): void {
    this.iniciarSesion(this.router.url).catch(() => undefined);
  }

  /** Cierra la sesión en Keycloak (end_session_endpoint con id_token_hint) y vuelve al inicio. */
  logout(): void {
    this.userManager.signoutRedirect().catch((err) => {
      console.error('No se pudo cerrar la sesión en Keycloak; se cierra solo en esta pestaña', err);
      void this.userManager.removeUser();
      void this.router.navigateByUrl('/');
    });
  }

  /** Devuelve el access token vigente; con `forzar` (o si no hay) pide uno nuevo con el refresh token. */
  refreshAccessToken(_scopes?: string[], forzar = false): Observable<string> {
    const actual = this.accessToken();
    if (actual && !forzar) return of(actual);
    return defer(() => from(this.userManager.signinSilent())).pipe(
      map((user) => {
        if (!user) throw new Error('Keycloak no devolvió una sesión');
        this.aplicarUsuario(user);
        return user.access_token;
      }),
    );
  }

  hasRole(role: string): boolean {
    return this.roles().some((r) => r.toUpperCase() === role.toUpperCase());
  }

  private aplicarUsuario(user: User | null): void {
    const vigente = user && !user.expired ? user : null;
    this.accessToken.set(vigente?.access_token ?? null);
    this.idToken.set(vigente?.id_token ?? null);
  }
}
