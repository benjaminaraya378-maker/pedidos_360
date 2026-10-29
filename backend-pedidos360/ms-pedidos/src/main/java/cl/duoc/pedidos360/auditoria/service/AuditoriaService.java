package cl.duoc.pedidos360.auditoria.service;

import cl.duoc.pedidos360.pedidos.event.PedidoCreadoEvent;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Auditoría de pedidos. Cada pedido creado queda registrado en el log con una línea "AUDITORIA" (id, cliente,
 * ítems, total y fecha); no hay un almacén de auditoría aparte.
 */
@Service
public class AuditoriaService {

  private static final Logger log = LoggerFactory.getLogger(AuditoriaService.class);

  /** @throws IllegalArgumentException si el evento no trae id de pedido (no recuperable). */
  public void registrarPedidoCreado(PedidoCreadoEvent evento) {
    if (evento.pedidoId() == null) {
      throw new IllegalArgumentException("Evento de pedido incompleto: sin id");
    }
    log.info("AUDITORIA | pedido #{} | cliente {} <{}> | ítems: {} | total ${} | fecha {}",
        evento.pedidoId(), evento.clienteNombre(), evento.clienteEmail(), resumenItems(evento.items()),
        evento.total(), evento.fecha());
  }

  private static String resumenItems(List<PedidoCreadoEvent.Item> items) {
    if (items == null || items.isEmpty()) {
      return "(sin ítems)";
    }
    return items.stream()
        .map(i -> i.cantidad() + " x " + i.nombreProducto() + " (producto " + i.productoId() + ")")
        .reduce((a, b) -> a + "; " + b)
        .orElse("(sin ítems)");
  }
}
