import { HttpClient, HttpErrorResponse } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { Observable } from 'rxjs';
import { environment } from '../../environments/environment';
import { ApiError, CheckoutRequest, Dashboard, Me, Pedido, Producto, ProductoRequest } from './models';

/**
 * Cliente HTTP del sistema. Todas las llamadas pasan por el API Manager (API Gateway);
 * MsalInterceptor adjunta automáticamente "Authorization: Bearer <access token>"
 * a las rutas declaradas en app.config.ts.
 */
@Injectable({ providedIn: 'root' })
export class ApiService {
  private readonly http = inject(HttpClient);
  private readonly base = environment.api.baseUrl;

  // ---- ms-productos ----
  listarProductos(): Observable<Producto[]> {
    return this.http.get<Producto[]>(`${this.base}/api/productos`);
  }
  crearProducto(body: ProductoRequest): Observable<Producto> {
    return this.http.post<Producto>(`${this.base}/api/productos`, body);
  }

  // ---- ms-pedidos ----
  listarPedidos(): Observable<Pedido[]> {
    return this.http.get<Pedido[]>(`${this.base}/api/pedidos`);
  }
  crearPedido(body: CheckoutRequest): Observable<Pedido> {
    return this.http.post<Pedido>(`${this.base}/api/pedidos`, body);
  }

  // ---- BFF ----
  me(): Observable<Me> {
    return this.http.get<Me>(`${this.base}/api/bff/me`);
  }
  dashboard(): Observable<Dashboard> {
    return this.http.get<Dashboard>(`${this.base}/api/bff/dashboard`);
  }
  pedidosAdmin(): Observable<Pedido[]> {
    return this.http.get<Pedido[]>(`${this.base}/api/bff/admin/pedidos`);
  }

  static mensajeDeError(err: unknown): string {
    if (err instanceof HttpErrorResponse) {
      const api = err.error as Partial<ApiError> | null;
      if (api && typeof api === 'object' && api.message) return `${err.status} · ${api.message}`;
      if (err.status === 0) return 'No se pudo contactar al API (¿CORS o servicio caído?)';
      return `${err.status} · ${err.statusText || 'Error'}`;
    }
    return String(err);
  }
}
