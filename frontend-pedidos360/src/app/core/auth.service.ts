import { Injectable, computed, inject, signal } from '@angular/core';
import { MsalBroadcastService, MsalService } from '@azure/msal-angular';
import {
  AccountInfo,
  AuthenticationResult,
  EventType,
  InteractionRequiredAuthError,
  InteractionStatus,
} from '@azure/msal-browser';
import { Observable, catchError, filter, map, tap, throwError } from 'rxjs';
import { environment } from '../../environments/environment';
import { claimAsList, claimAsString, decodeJwt, expiraEn } from './jwt.util';

/**
 * Capa de autenticación sobre MSAL:
 *  - inicio / cierre de sesión (Authorization Code + PKCE vía redirect)
 *  - cuenta activa y claims del ID token
 *  - access token para el API Gateway, del cual se leen roles (app roles) y scopes (scp)
 */
@Injectable({ providedIn: 'root' })
export class AuthService {
  private readonly msal = inject(MsalService);
  private readonly broadcast = inject(MsalBroadcastService);

  readonly account = signal<AccountInfo | null>(null);
  readonly accessToken = signal<string | null>(null);
  readonly interactionInProgress = signal(true);

  readonly isLoggedIn = computed(() => this.account() !== null);
  readonly idTokenClaims = computed(() => (this.account()?.idTokenClaims ?? null) as Record<string, unknown> | null);
  readonly accessClaims = computed(() => decodeJwt(this.accessToken()));

  /** Roles de aplicación (claim "roles") leídos del access token. */
  readonly roles = computed(() => claimAsList(this.accessClaims(), 'roles'));
  /** Scopes delegados (claim "scp") leídos del access token. */
  readonly scopes = computed(() => claimAsList(this.accessClaims(), 'scp'));
  readonly isAdmin = computed(() => this.roles().some((r) => r.toUpperCase() === 'ADMIN'));

  readonly displayName = computed(
    () => this.account()?.name || claimAsString(this.idTokenClaims(), 'name') || this.email() || 'Usuario',
  );
  readonly email = computed(() => {
    const claims = this.idTokenClaims();
    return (
      claimAsString(claims, 'email') ||
      claimAsString(claims, 'preferred_username') ||
      claimAsList(claims, 'emails')[0] ||
      this.account()?.username ||
      ''
    );
  });
  readonly tokenExpira = computed(() => expiraEn(this.accessClaims()));

  constructor() {
    // Procesa la respuesta del redirect del IDaaS (authorization code → tokens) en cada carga.
    // MSAL valida aquí state, nonce y el code_verifier de PKCE antes de canjear el código.
    this.msal.handleRedirectObservable().subscribe({ error: (err) => console.error('Error en redirect MSAL', err) });

    // Cuando MSAL termina cualquier interacción (redirect de login, renovación, etc.)
    // sincronizamos la cuenta activa y obtenemos un access token para leer roles/scopes.
    this.broadcast.inProgress$
      .pipe(filter((status) => status === InteractionStatus.None))
      .subscribe(() => {
        this.interactionInProgress.set(false);
        this.syncAccount();
      });

    this.broadcast.msalSubject$
      .pipe(
        filter(
          (e) => e.eventType === EventType.LOGIN_SUCCESS || e.eventType === EventType.ACQUIRE_TOKEN_SUCCESS,
        ),
      )
      .subscribe((e) => {
        const result = e.payload as AuthenticationResult;
        if (result?.account) {
          this.msal.instance.setActiveAccount(result.account);
          this.account.set(result.account);
        }
        // Solo guardamos tokens emitidos para NUESTRA API (no tokens de Graph, por ejemplo)
        if (result?.accessToken && result.scopes?.some((s) => s.includes('Pedidos360.'))) {
          this.accessToken.set(result.accessToken);
        }
      });
  }

  private syncAccount(): void {
    let active = this.msal.instance.getActiveAccount();
    const accounts = this.msal.instance.getAllAccounts();
    if (!active && accounts.length > 0) {
      active = accounts[0];
      this.msal.instance.setActiveAccount(active);
    }
    this.account.set(active);
    if (active && !this.accessToken()) {
      this.refreshAccessToken().subscribe({ error: () => undefined });
    }
  }

  /** Inicia sesión (o registro, según el flujo de usuario) con redirect. */
  login(): void {
    this.msal.loginRedirect({ scopes: environment.api.scopes });
  }

  logout(): void {
    this.accessToken.set(null);
    this.msal.logoutRedirect({
      account: this.account() ?? undefined,
      postLogoutRedirectUri: environment.msal.postLogoutRedirectUri,
    });
  }

  /** Obtiene (silenciosamente, con refresh token si hace falta) un access token para el API. */
  refreshAccessToken(scopes: string[] = environment.api.scopes, forceRefresh = false): Observable<string> {
    return this.msal
      .acquireTokenSilent({ scopes, account: this.account() ?? undefined, forceRefresh })
      .pipe(
        map((r) => r.accessToken),
        tap((token) => this.accessToken.set(token)),
        catchError((err) => {
          if (err instanceof InteractionRequiredAuthError) {
            this.msal.acquireTokenRedirect({ scopes });
          }
          return throwError(() => err);
        }),
      );
  }

  hasRole(role: string): boolean {
    return this.roles().some((r) => r.toUpperCase() === role.toUpperCase());
  }
}
