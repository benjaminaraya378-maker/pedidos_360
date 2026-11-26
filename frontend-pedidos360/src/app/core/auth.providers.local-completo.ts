import { EnvironmentProviders, Provider, inject, provideAppInitializer } from '@angular/core';
import { HTTP_INTERCEPTORS } from '@angular/common/http';
import { MsalGuard } from '@azure/msal-angular';
import { AuthService } from './auth.service';
import { AuthKeycloakService } from './local-completo/auth-keycloak.service';
import { SesionKeycloakGuard } from './local-completo/sesion-keycloak.guard';
import { TokenKeycloakInterceptor } from './local-completo/token-keycloak.interceptor';

/**
 * MODO LOCAL COMPLETO (solo `npm run local-completo`, es decir `ng serve --configuration local-completo`).
 * angular.json usa este archivo en lugar de auth.providers.ts con fileReplacements; `ng build` y `ng serve`
 * normales nunca lo incluyen.
 *
 * Reemplaza los dos servicios de nube que no son Docker por contenedores en este PC
 * (backend-pedidos360/docker-compose.nube-local.yml):
 *  - IDaaS: Keycloak (realm "pedidos360") en lugar de Entra External ID. Login con OpenID Connect,
 *    Authorization Code + PKCE (S256), con oidc-client-ts en lugar de MSAL.
 *  - API Manager: Kong en lugar de AWS API Gateway. El interceptor manda el access token a Kong (:8000), que
 *    valida la firma, el emisor y el vencimiento, y cada microservicio vuelve a validar audiencia, rol y scope.
 */
export const authProviders: (Provider | EnvironmentProviders)[] = [
  { provide: AuthService, useClass: AuthKeycloakService },
  { provide: MsalGuard, useClass: SesionKeycloakGuard },
  { provide: HTTP_INTERCEPTORS, useClass: TokenKeycloakInterceptor, multi: true },
  // Antes de la primera navegación: canjea el código si Keycloak acaba de volver, o recupera la sesión guardada
  provideAppInitializer(() => (inject(AuthService) as unknown as AuthKeycloakService).iniciar()),
];
