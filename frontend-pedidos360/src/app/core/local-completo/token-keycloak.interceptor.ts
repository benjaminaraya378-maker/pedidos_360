import { HttpEvent, HttpHandler, HttpInterceptor, HttpRequest } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { Observable } from 'rxjs';
import { environment } from '../../../environments/environment';
import { AuthService } from '../auth.service';

/**
 * Reemplaza a MsalInterceptor en el modo local completo: agrega "Authorization: Bearer <access token de Keycloak>"
 * solo a las llamadas al API Gateway local (Kong, http://localhost:8000/api/**), igual que el protectedResourceMap
 * de MSAL. A Keycloak y a cualquier otro sitio no se les envía el token.
 */
@Injectable()
export class TokenKeycloakInterceptor implements HttpInterceptor {
  private readonly auth = inject(AuthService);

  intercept(req: HttpRequest<unknown>, next: HttpHandler): Observable<HttpEvent<unknown>> {
    const token = this.auth.accessToken();
    if (!token || !req.url.startsWith(`${environment.api.baseUrl}/api/`)) return next.handle(req);
    return next.handle(req.clone({ setHeaders: { Authorization: `Bearer ${token}` } }));
  }
}
