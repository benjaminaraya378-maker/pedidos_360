import { Injectable, inject } from '@angular/core';
import { ActivatedRouteSnapshot, CanActivate, Router, RouterStateSnapshot, UrlTree } from '@angular/router';
import { AuthService } from '../auth.service';
import { AuthKeycloakService } from './auth-keycloak.service';

/**
 * Reemplaza a MsalGuard en el modo local completo: si no hay sesión, redirige a la pantalla de login de Keycloak
 * (como MsalGuard redirige a Entra) y, al volver, la app abre la ruta que se había pedido. Si Keycloak no responde,
 * manda a /no-autorizado.
 */
@Injectable()
export class SesionKeycloakGuard implements CanActivate {
  private readonly auth = inject(AuthService) as unknown as AuthKeycloakService;
  private readonly router = inject(Router);

  canActivate(_route: ActivatedRouteSnapshot, state: RouterStateSnapshot): boolean | Promise<boolean | UrlTree> {
    if (this.auth.isLoggedIn()) return true;
    // El navegador se va a Keycloak: la navegación actual no sigue
    return this.auth.iniciarSesion(state.url).then(
      () => false,
      () => this.router.createUrlTree(['/no-autorizado']),
    );
  }
}
