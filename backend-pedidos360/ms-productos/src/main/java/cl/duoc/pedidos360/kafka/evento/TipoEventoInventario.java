package cl.duoc.pedidos360.kafka.evento;

import com.fasterxml.jackson.annotation.JsonCreator;

/** Tipos de evento del tópico de inventario. Un valor que este servicio no conoce se lee como DESCONOCIDO. */
public enum TipoEventoInventario {
  /** Se creó un producto con su stock inicial. */
  PRODUCTO_CREADO,
  /** Se descontó stock por un pedido (trae pedidoId). */
  STOCK_DESCONTADO,
  /** Un pedido pidió más de lo disponible: no se descontó nada (trae pedidoId). */
  STOCK_INSUFICIENTE,
  /** Un administrador repuso stock. */
  STOCK_REPUESTO,
  DESCONOCIDO;

  @JsonCreator
  public static TipoEventoInventario desde(String valor) {
    try {
      return valueOf(valor);
    } catch (IllegalArgumentException | NullPointerException ex) {
      return DESCONOCIDO;
    }
  }
}
