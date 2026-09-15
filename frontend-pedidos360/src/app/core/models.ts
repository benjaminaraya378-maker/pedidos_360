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
