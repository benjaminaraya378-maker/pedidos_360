import { EnvironmentProviders, Provider } from '@angular/core';
import { HTTP_INTERCEPTORS } from '@angular/common/http';
import { MsalGuard } from '@azure/msal-angular';
import { AuthService } from './auth.service';
import { AuthDemoLocalService } from './demo-local/auth-demo-local.service';
import { SesionDemoLocalGuard } from './demo-local/sesion-demo-local.guard';
import { TokenDemoLocalInterceptor } from './demo-local/token-demo-local.interceptor';

/**
 * MODO DEMO LOCAL (solo `npm run demo-local`, es decir `ng serve --configuration demo-local`).
 * angular.json usa este archivo en lugar de auth.providers.ts con fileReplacements; `ng build` y
 * `ng serve` normales nunca lo incluyen.
 *
 * Sirve para mostrar el frontend sin el tenant de Entra: el token lo firma el IDaaS local de
 * demo-local/idaas-local.mjs y los microservicios lo validan con su JWKS (firma, emisor, audiencia,
 * vigencia, rol y scope), igual que con Entra. Lo único que se reemplaza es quién emite el token.
 */
export const authProviders: (Provider | EnvironmentProviders)[] = [
  { provide: AuthService, useClass: AuthDemoLocalService },
  { provide: MsalGuard, useClass: SesionDemoLocalGuard },
  { provide: HTTP_INTERCEPTORS, useClass: TokenDemoLocalInterceptor, multi: true },
];
