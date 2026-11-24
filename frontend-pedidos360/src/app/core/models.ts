export interface Producto {
  id: number;
  nombre: string;
  descripcion: string;
  categoria: string;
  precio: number;
  stock: number;
  activo: boolean;
}

export interface ProductoRequest {
  nombre: string;
  descripcion: string;
  categoria: string;
  precio: number;
  stock: number;
}

export interface PedidoItem {
  id?: number;
  productoId: number;
  nombreProducto: string;
  cantidad: number;
  precioUnitario: number;
  subtotal: number;
}

export type EstadoPedido = 'CREADO' | 'EN_PREPARACION' | 'ENVIADO' | 'ENTREGADO' | 'CANCELADO';

export interface Pedido {
  id: number;
  clienteOid: string;
  clienteEmail: string;
  clienteNombre: string;
  fecha: string;
  estado: EstadoPedido;
  total: number;
  /** Por qué está en su estado: lo completa el flujo de Kafka (stock reservado o insuficiente). */
  motivoEstado?: string | null;
  items: PedidoItem[];
}

export interface CheckoutRequest {
  items: { productoId: number; cantidad: number }[];
  observacion?: string;
}

export interface Me {
  nombre: string;
  email: string;
  oid: string;
  roles: string[];
  scopes: string[];
  issuer: string;
  audience: string[];
  expira: string;
  validadoPor: string;
}

export interface Dashboard {
  usuario: Me;
  productosDisponibles: number;
  misPedidos: number;
  ultimosPedidos: Pedido[];
}

export interface ApiError {
  timestamp: string;
  status: number;
  error: string;
  message: string;
  path: string;
}

// ---- ms-analitica (métricas calculadas desde los eventos de Kafka) ----

export interface EventoReciente {
  eventoId: string;
  topico: string;
  particion: number;
  offset: number;
  clave: string;
  tipo: string;
  descripcion: string;
  fecha: string;
  procesado: string;
}

export interface StockProducto {
  productoId: number;
  nombre: string;
  stockActual: number;
  umbral: number;
  stockBajo: boolean;
  ultimoMovimiento: string;
  actualizado: string;
}

export interface ProductoVendido {
  productoId: number;
  nombre: string;
  unidades: number;
  monto: number;
}

export interface ResumenAnalitica {
  generadoEn: string;
  eventosProcesados: number;
  ultimoEvento: string | null;
  latenciaPromedioMs: number | null;
  totalPedidos: number;
  pedidosPorEstado: Record<string, number>;
  pedidosUltimaHora: number;
  ventasConfirmadas: number;
  ticketPromedio: number;
  tasaCancelacion: number;
  unidadesVendidas: number;
  topProductos: ProductoVendido[];
  productosMonitoreados: number;
  alertasStockBajo: StockProducto[];
  ultimosEventos: EventoReciente[];
}

// ---- ms-kafka-admin ----

export interface ClusterKafka {
  clusterId: string;
  controlador: number | null;
  brokers: { id: number; host: string; puerto: number; rack: string | null }[];
}

export interface TopicoResumen {
  nombre: string;
  particiones: number;
  replicas: number;
  interno: boolean;
}

export interface ParticionKafka {
  particion: number;
  lider: number | null;
  replicas: number[];
  isr: number[];
  offsetInicio: number;
  offsetFin: number;
}

export interface TopicoDetalle {
  nombre: string;
  interno: boolean;
  particiones: number;
  replicas: number;
  mensajes: number;
  detalleParticiones: ParticionKafka[];
  configuracion: Record<string, { valor: string; origen: string }>;
}

export interface TopicoRequest {
  nombre: string;
  particiones: number;
  replicas: number;
  configuracion: Record<string, string>;
}

export interface ConfiguracionRequest {
  establecer: Record<string, string>;
  restablecer: string[];
}

export interface GrupoResumen {
  grupo: string;
  estado: string;
  miembros: number;
  lagTotal: number;
}

export interface OffsetParticion {
  topico: string;
  particion: number;
  offsetConfirmado: number | null;
  offsetFin: number;
  lag: number;
  consumidor: string | null;
}

export interface GrupoDetalle {
  grupo: string;
  estado: string;
  coordinador: number | null;
  asignador: string;
  miembros: { consumidorId: string; clienteId: string; host: string; particiones: string[] }[];
  offsets: OffsetParticion[];
  lagTotal: number;
}
