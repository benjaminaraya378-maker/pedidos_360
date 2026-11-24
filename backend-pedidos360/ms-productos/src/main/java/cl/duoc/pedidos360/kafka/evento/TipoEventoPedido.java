package cl.duoc.pedidos360.kafka.evento;

import com.fasterxml.jackson.annotation.JsonCreator;

/** Tipos de evento del tópico de pedidos. Un valor que este servicio no conoce se lee como DESCONOCIDO. */
public enum TipoEventoPedido {
  /** ms-pedidos guardó un pedido nuevo: ms-productos debe reservar su stock. */
  PEDIDO_CREADO,
  /** El inventario reservó el stock: el pedido pasa a EN_PREPARACION. */
  PEDIDO_CONFIRMADO,
  /** No había stock suficiente: el pedido pasa a CANCELADO. */
  PEDIDO_CANCELADO,
  DESCONOCIDO;

  /** Compatibilidad hacia adelante: un tipo nuevo de otra versión no rompe la lectura del evento. */
  @JsonCreator
  public static TipoEventoPedido desde(String valor) {
    try {
      return valueOf(valor);
    } catch (IllegalArgumentException | NullPointerException ex) {
      return DESCONOCIDO;
    }
  }
}
