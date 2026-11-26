import { Injectable, inject } from '@angular/core';
import { CanActivate, Router, UrlTree } from '@angular/router';
import { AuthService } from '../auth.service';
import { AuthDemoLocalService } from './auth-demo-local.service';

/**
 * Reemplaza a MsalGuard en el modo demo local: si no hay sesión, la pide al IDaaS local
 * (como MsalGuard redirige a Entra) y deja pasar; si el IDaaS no responde, manda a /no-autorizado.
 */
@Injectable()
export class SesionDemoLocalGuard implements CanActivate {
  private readonly auth = inject(AuthService) as unknown as AuthDemoLocalService;
  private readonly router = inject(Router);

  canActivate(): boolean | Promise<boolean | UrlTree> {
    if (this.auth.isLoggedIn()) return true;
    return this.auth.iniciarSesion().then(
      () => true,
      () => this.router.createUrlTree(['/no-autorizado']),
    );
  }
}
