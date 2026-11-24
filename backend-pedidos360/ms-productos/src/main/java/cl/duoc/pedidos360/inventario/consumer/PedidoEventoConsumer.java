package cl.duoc.pedidos360.inventario.consumer;

import cl.duoc.pedidos360.inventario.service.InventarioService;
import cl.duoc.pedidos360.kafka.evento.PedidoEvento;
import cl.duoc.pedidos360.kafka.evento.TipoEventoPedido;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Consumidor Kafka del tópico {@code pedidos360.pedidos} en el grupo {@code pedidos360-inventario}.
 *
 * <p>Por cada PEDIDO_CREADO pide a {@link InventarioService} que descuente el stock; los demás tipos
 * (CONFIRMADO, CANCELADO) no tocan el inventario y se ignoran. Solo conoce la mensajería: recibe el DTO validado
 * ({@code @Valid}) y delega la lógica de negocio.
 *
 * <p>El grupo es propio de ms-productos: ms-analitica lee el mismo tópico con otro grupo y recibe todos los eventos
 * por su cuenta. Dentro del grupo, 3 hilos (concurrency) se reparten las 3 particiones; si se levanta otra instancia
 * de ms-productos, Kafka redistribuye las particiones entre ambas. Los errores los maneja {@code KafkaConfig}.
 */
@Component
public class PedidoEventoConsumer {

  private static final Logger log = LoggerFactory.getLogger(PedidoEventoConsumer.class);

  private final InventarioService inventarioService;

  public PedidoEventoConsumer(InventarioService inventarioService) {
    this.inventarioService = inventarioService;
  }

  @KafkaListener(
      id = "ms-productos-inventario",
      topics = "${pedidos360.kafka.topicos.pedidos.nombre}",
      groupId = "${pedidos360.kafka.grupos.inventario}",
      concurrency = "${spring.kafka.listener.concurrency:3}")
  public void onPedidoEvento(@Payload @Valid PedidoEvento evento,
                             @Header(KafkaHeaders.RECEIVED_PARTITION) int particion,
                             @Header(KafkaHeaders.OFFSET) long offset) {
    if (evento.tipo() != TipoEventoPedido.PEDIDO_CREADO) {
      log.debug("Evento {} del pedido {} no requiere inventario", evento.tipo(), evento.pedidoId());
      return;
    }
    log.info("Kafka: PEDIDO_CREADO del pedido {} recibido desde partición {} offset {}", evento.pedidoId(),
        particion, offset);
    InventarioService.Resultado resultado = inventarioService.reservarStock(evento);
    log.info("Pedido {}: inventario {}", evento.pedidoId(), resultado);
  }
}
