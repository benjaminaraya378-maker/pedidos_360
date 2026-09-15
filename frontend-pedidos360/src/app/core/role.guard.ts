import { inject } from '@angular/core';
import { CanActivateFn, Router } from '@angular/router';
import { catchError, map, of } from 'rxjs';
import { AuthService } from './auth.service';

/**
 * Guard de autorización por rol. Se encadena DESPUÉS de MsalGuard (que garantiza el login).
 * Lee el rol desde el claim "roles" del access token.
 */
export function roleGuard(role: string): CanActivateFn {
  return () => {
    const auth = inject(AuthService);
    const router = inject(Router);
    const denied = router.createUrlTree(['/no-autorizado'], { queryParams: { rol: role } });

    if (auth.hasRole(role)) return true;
    if (auth.accessClaims()) return denied;

    // Aún no tenemos access token en memoria: lo pedimos y evaluamos.
    return auth.refreshAccessToken().pipe(
      map(() => (auth.hasRole(role) ? true : denied)),
      catchError(() => of(denied)),
    );
  };
}
