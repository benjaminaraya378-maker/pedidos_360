package cl.duoc.pedidos360.pedidos.dto;

import cl.duoc.pedidos360.pedidos.model.EstadoPedido;
import cl.duoc.pedidos360.pedidos.model.Pedido;
import cl.duoc.pedidos360.pedidos.model.PedidoItem;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

public record PedidoResponse(
    Long id,
    String clienteOid,
    String clienteEmail,
    String clienteNombre,
    Instant fecha,
    EstadoPedido estado,
    BigDecimal total,
    String observacion,
    List<Item> items) {

  public record Item(Long id, Long productoId, String nombreProducto, Integer cantidad,
                     BigDecimal precioUnitario, BigDecimal subtotal) {
    static Item de(PedidoItem i) {
      return new Item(i.getId(), i.getProductoId(), i.getNombreProducto(), i.getCantidad(),
          i.getPrecioUnitario(), i.getSubtotal());
    }
  }

  public static PedidoResponse de(Pedido p) {
    return new PedidoResponse(p.getId(), p.getClienteOid(), p.getClienteEmail(), p.getClienteNombre(),
        p.getFecha(), p.getEstado(), p.getTotal(), p.getObservacion(),
        p.getItems().stream().map(Item::de).toList());
  }
}
