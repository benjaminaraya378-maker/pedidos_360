package cl.duoc.pedidos360.pedidos.consumer;

import cl.duoc.pedidos360.kafka.evento.InventarioEvento;
import cl.duoc.pedidos360.pedidos.service.EstadoPedidoService;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Consumidor Kafka del tópico {@code pedidos360.inventario} en el grupo {@code pedidos360-estado-pedidos}.
 *
 * <p>Cuando ms-productos descuenta el stock de un pedido (o no puede hacerlo), este consumidor pasa el pedido de
 * CREADO a EN_PREPARACION o a CANCELADO, sin intervención de nadie. Solo conoce la mensajería: recibe el DTO ya
 * validado ({@code @Valid}) y delega la decisión de negocio en {@link EstadoPedidoService}.
 *
 * <p>concurrency=3: tres hilos en el mismo grupo, uno por partición del tópico. Si hay más instancias de ms-pedidos
 * con el mismo grupo, Kafka reparte las particiones entre todas. Los errores los maneja {@code KafkaConfig}
 * (reintentos y DLT).
 */
@Component
public class InventarioEventoConsumer {

  private static final Logger log = LoggerFactory.getLogger(InventarioEventoConsumer.class);

  private final EstadoPedidoService estadoPedidoService;

  public InventarioEventoConsumer(EstadoPedidoService estadoPedidoService) {
    this.estadoPedidoService = estadoPedidoService;
  }

  @KafkaListener(
      id = "ms-pedidos-estado",
      topics = "${pedidos360.kafka.topicos.inventario.nombre}",
      groupId = "${pedidos360.kafka.grupos.estado-pedidos}",
      concurrency = "${spring.kafka.listener.concurrency:3}")
  public void onInventarioEvento(@Payload @Valid InventarioEvento evento,
                                 @Header(KafkaHeaders.RECEIVED_PARTITION) int particion,
                                 @Header(KafkaHeaders.OFFSET) long offset) {
    log.info("Kafka: {} recibido (producto {}, pedido {}) desde partición {} offset {}", evento.tipo(),
        evento.productoId(), evento.pedidoId(), particion, offset);
    estadoPedidoService.aplicarResultadoInventario(evento);
  }
}
