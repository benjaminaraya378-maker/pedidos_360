package cl.duoc.pedidos360.pedidos.service;

import cl.duoc.pedidos360.kafka.EventoInvalidoException;
import cl.duoc.pedidos360.kafka.evento.InventarioEvento;
import cl.duoc.pedidos360.pedidos.event.PedidoEstadoCambiadoEvent;
import cl.duoc.pedidos360.pedidos.model.EstadoPedido;
import cl.duoc.pedidos360.pedidos.model.Pedido;
import cl.duoc.pedidos360.pedidos.repository.PedidoRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Reacción del pedido al resultado del inventario (lógica de negocio, sin Kafka):
 * <pre>
 *   STOCK_DESCONTADO   → CREADO pasa a EN_PREPARACION
 *   STOCK_INSUFICIENTE → CREADO pasa a CANCELADO (con el motivo)
 *   otros tipos o eventos sin pedido (altas, reposiciones) → no afectan pedidos
 * </pre>
 * Es idempotente: solo cambia pedidos que siguen en CREADO. Un pedido con dos productos recibe dos
 * STOCK_DESCONTADO (uno por producto); el primero lo confirma y el segundo no hace nada. El pedido se lee con
 * bloqueo de fila, así dos hilos que procesan eventos del mismo pedido no lo cambian a la vez.
 */
@Service
public class EstadoPedidoService {

  private static final Logger log = LoggerFactory.getLogger(EstadoPedidoService.class);

  private final PedidoRepository repository;
  private final ApplicationEventPublisher eventos;

  public EstadoPedidoService(PedidoRepository repository, ApplicationEventPublisher eventos) {
    this.repository = repository;
    this.eventos = eventos;
  }

  @Transactional
  public void aplicarResultadoInventario(InventarioEvento evento) {
    EstadoPedido destino = switch (evento.tipo()) {
      case STOCK_DESCONTADO -> EstadoPedido.EN_PREPARACION;
      case STOCK_INSUFICIENTE -> EstadoPedido.CANCELADO;
      default -> null;
    };
    if (destino == null || evento.pedidoId() == null) {
      log.debug("Evento {} del producto {} no afecta pedidos", evento.tipo(), evento.productoId());
      return;
    }

    Pedido pedido = repository.findByIdForUpdate(evento.pedidoId())
        .orElseThrow(() -> new EventoInvalidoException(
            "El pedido " + evento.pedidoId() + " del evento " + evento.eventoId() + " no existe"));

    if (pedido.getEstado() != EstadoPedido.CREADO) {
      log.info("Pedido {} ya está en {}: el evento {} no lo cambia", pedido.getId(), pedido.getEstado(),
          evento.eventoId());
      return;
    }

    pedido.setEstado(destino);
    pedido.setMotivoEstado(destino == EstadoPedido.CANCELADO
        ? evento.motivo()
        : "Stock reservado por el inventario");
    log.info("Pedido {}: CREADO → {} ({})", pedido.getId(), destino, pedido.getMotivoEstado());
    eventos.publishEvent(PedidoEstadoCambiadoEvent.de(pedido));
  }
}
