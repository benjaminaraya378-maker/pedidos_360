import { HttpClient, HttpErrorResponse } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { Observable } from 'rxjs';
import { environment } from '../../environments/environment';
import {
  ApiError,
  CheckoutRequest,
  ClusterKafka,
  ConfiguracionRequest,
  Dashboard,
  GrupoDetalle,
  GrupoResumen,
  Me,
  Pedido,
  Producto,
  ProductoRequest,
  ResumenAnalitica,
  TopicoDetalle,
  TopicoRequest,
  TopicoResumen,
} from './models';

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
  /** Solo Admin. Publica STOCK_REPUESTO en Kafka: la alerta de stock bajo desaparece de la analítica. */
  reponerStock(id: number, cantidad: number): Observable<Producto> {
    return this.http.patch<Producto>(`${this.base}/api/productos/${id}/stock`, { cantidad });
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

  // ---- ms-analitica (rol Admin) ----
  resumenAnalitica(): Observable<ResumenAnalitica> {
    return this.http.get<ResumenAnalitica>(`${this.base}/api/analitica/resumen`);
  }

  // ---- ms-kafka-admin (rol Admin) ----
  kafkaCluster(): Observable<ClusterKafka> {
    return this.http.get<ClusterKafka>(`${this.base}/api/kafka/cluster`);
  }
  kafkaTopicos(): Observable<TopicoResumen[]> {
    return this.http.get<TopicoResumen[]>(`${this.base}/api/kafka/topics`);
  }
  kafkaTopico(nombre: string): Observable<TopicoDetalle> {
    return this.http.get<TopicoDetalle>(`${this.base}/api/kafka/topics/${encodeURIComponent(nombre)}`);
  }
  crearTopico(body: TopicoRequest): Observable<TopicoDetalle> {
    return this.http.post<TopicoDetalle>(`${this.base}/api/kafka/topics`, body);
  }
  configurarTopico(nombre: string, body: ConfiguracionRequest): Observable<TopicoDetalle> {
    return this.http.patch<TopicoDetalle>(`${this.base}/api/kafka/topics/${encodeURIComponent(nombre)}/config`, body);
  }
  eliminarTopico(nombre: string): Observable<void> {
    return this.http.delete<void>(`${this.base}/api/kafka/topics/${encodeURIComponent(nombre)}`);
  }
  kafkaGrupos(): Observable<GrupoResumen[]> {
    return this.http.get<GrupoResumen[]>(`${this.base}/api/kafka/consumer-groups`);
  }
  kafkaGrupo(grupo: string): Observable<GrupoDetalle> {
    return this.http.get<GrupoDetalle>(`${this.base}/api/kafka/consumer-groups/${encodeURIComponent(grupo)}`);
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
