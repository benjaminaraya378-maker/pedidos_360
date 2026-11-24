package cl.duoc.pedidos360.pedidos.producer;

import cl.duoc.pedidos360.kafka.KafkaTopicosProperties;
import cl.duoc.pedidos360.kafka.evento.PedidoEvento;
import cl.duoc.pedidos360.kafka.evento.TipoEventoPedido;
import cl.duoc.pedidos360.pedidos.event.PedidoCreadoEvent;
import cl.duoc.pedidos360.pedidos.event.PedidoEstadoCambiadoEvent;
import cl.duoc.pedidos360.pedidos.model.EstadoPedido;
import java.util.List;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * Productor Kafka del dominio pedidos: publica en el tópico {@code pedidos360.pedidos} cada cambio del pedido.
 *
 * <ul>
 *   <li>PEDIDO_CREADO: al guardarse un pedido nuevo. Lo consume ms-productos para descontar el stock.</li>
 *   <li>PEDIDO_CONFIRMADO / PEDIDO_CANCELADO: cuando el inventario respondió. Lo consume ms-analitica.</li>
 * </ul>
 *
 * <p>Convive con {@link PedidoEventProducer} (RabbitMQ): los dos escuchan el mismo evento de Spring y no se conocen;
 * agregar Kafka no cambió nada de las colas existentes. Igual que en RabbitMQ, se publica después del commit
 * (nunca se anuncia un pedido que no quedó guardado) y, si Kafka no responde, el pedido igual se crea y el fallo
 * queda en el log.
 *
 * <p>La clave del mensaje es el id del pedido: todos sus eventos caen en la misma partición y se consumen en orden.
 */
@Component
public class PedidoKafkaProducer {

  private static final Logger log = LoggerFactory.getLogger(PedidoKafkaProducer.class);

  private final KafkaTemplate<String, PedidoEvento> kafkaTemplate;
  private final KafkaTopicosProperties props;

  public PedidoKafkaProducer(KafkaTemplate<String, PedidoEvento> kafkaTemplate, KafkaTopicosProperties props) {
    this.kafkaTemplate = kafkaTemplate;
    this.props = props;
  }

  @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
  public void onPedidoCreado(PedidoCreadoEvent e) {
    publicar(PedidoEvento.nuevo(TipoEventoPedido.PEDIDO_CREADO, e.pedidoId(), e.clienteOid(),
        EstadoPedido.CREADO.name(), e.total(), null, items(e.items())));
  }

  @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
  public void onEstadoCambiado(PedidoEstadoCambiadoEvent e) {
    TipoEventoPedido tipo = e.estado() == EstadoPedido.CANCELADO
        ? TipoEventoPedido.PEDIDO_CANCELADO
        : TipoEventoPedido.PEDIDO_CONFIRMADO;
    publicar(PedidoEvento.nuevo(tipo, e.pedidoId(), e.clienteOid(), e.estado().name(), e.total(), e.motivo(),
        items(e.items())));
  }

  private void publicar(PedidoEvento evento) {
    String topico = props.topicos().pedidos().nombre();
    String clave = String.valueOf(evento.pedidoId());
    try {
      kafkaTemplate.send(topico, clave, evento).whenComplete((resultado, error) -> {
        if (error != null) {
          log.error("No se pudo publicar {} del pedido {} en '{}': {}", evento.tipo(), evento.pedidoId(), topico,
              error.getMessage());
        } else {
          log.info("Kafka: {} del pedido {} publicado en {}-{}@{} (evento {})", evento.tipo(), evento.pedidoId(),
              topico, resultado.getRecordMetadata().partition(), resultado.getRecordMetadata().offset(),
              evento.eventoId());
        }
      });
    } catch (KafkaException ex) {
      // Kafka no respondió a tiempo (max.block.ms): el pedido ya está guardado, solo se registra el fallo
      log.error("Kafka no disponible: no se publicó {} del pedido {}", evento.tipo(), evento.pedidoId(), ex);
    }
  }

  private static List<PedidoEvento.Item> items(List<PedidoCreadoEvent.Item> items) {
    return items.stream()
        .map(i -> new PedidoEvento.Item(i.productoId(), i.nombreProducto(), i.cantidad(), i.precioUnitario(),
            i.subtotal()))
        .toList();
  }
}
