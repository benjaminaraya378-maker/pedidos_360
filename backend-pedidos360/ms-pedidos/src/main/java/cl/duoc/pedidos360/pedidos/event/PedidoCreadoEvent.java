package cl.duoc.pedidos360.pedidos.event;

import cl.duoc.pedidos360.pedidos.model.Pedido;
import cl.duoc.pedidos360.pedidos.model.PedidoItem;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/** Evento "pedido creado": es también el cuerpo (JSON) del mensaje que viaja por RabbitMQ. */
public record PedidoCreadoEvent(
    Long pedidoId,
    String clienteOid,
    String clienteEmail,
    String clienteNombre,
    Instant fecha,
    BigDecimal total,
    List<Item> items) {

  public record Item(Long productoId, String nombreProducto, Integer cantidad,
                     BigDecimal precioUnitario, BigDecimal subtotal) {
    static Item de(PedidoItem i) {
      return new Item(i.getProductoId(), i.getNombreProducto(), i.getCantidad(), i.getPrecioUnitario(),
          i.getSubtotal());
    }
  }

  public static PedidoCreadoEvent de(Pedido p) {
    return new PedidoCreadoEvent(p.getId(), p.getClienteOid(), p.getClienteEmail(), p.getClienteNombre(),
        p.getFecha(), p.getTotal(), p.getItems().stream().map(Item::de).toList());
  }
}
