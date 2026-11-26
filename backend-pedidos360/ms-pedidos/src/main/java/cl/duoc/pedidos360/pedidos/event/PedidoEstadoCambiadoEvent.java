package cl.duoc.pedidos360.pedidos.event;

import cl.duoc.pedidos360.pedidos.model.EstadoPedido;
import cl.duoc.pedidos360.pedidos.model.Pedido;
import java.math.BigDecimal;
import java.util.List;

/**
 * Evento de Spring "el pedido cambió de estado". Lo emite {@code EstadoPedidoService} y, justo antes del commit,
 * {@code PedidoKafkaProducer} lo publica en Kafka como PEDIDO_CONFIRMADO o PEDIDO_CANCELADO y espera la confirmación.
 */
public record PedidoEstadoCambiadoEvent(
    Long pedidoId,
    String clienteOid,
    EstadoPedido estado,
    BigDecimal total,
    String motivo,
    List<PedidoCreadoEvent.Item> items) {

  public static PedidoEstadoCambiadoEvent de(Pedido p) {
    return new PedidoEstadoCambiadoEvent(p.getId(), p.getClienteOid(), p.getEstado(), p.getTotal(),
        p.getMotivoEstado(), PedidoCreadoEvent.de(p).items());
  }
}
