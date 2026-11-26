import { HttpEvent, HttpHandler, HttpInterceptor, HttpRequest } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { Observable } from 'rxjs';
import { environment } from '../../../environments/environment';
import { AuthService } from '../auth.service';

/**
 * Reemplaza a MsalInterceptor en el modo demo local: agrega "Authorization: Bearer <token>"
 * solo a las llamadas al API (/api/**), igual que el protectedResourceMap de MSAL.
 */
@Injectable()
export class TokenDemoLocalInterceptor implements HttpInterceptor {
  private readonly auth = inject(AuthService);

  intercept(req: HttpRequest<unknown>, next: HttpHandler): Observable<HttpEvent<unknown>> {
    const token = this.auth.accessToken();
    const esApi = req.url.startsWith(`${environment.api.baseUrl}/api/`) || req.url.startsWith('/api/');
    if (!token || !esApi) return next.handle(req);
    return next.handle(req.clone({ setHeaders: { Authorization: `Bearer ${token}` } }));
  }
}
