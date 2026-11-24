package cl.duoc.pedidos360.inventario.producer;

import cl.duoc.pedidos360.inventario.event.MovimientosInventarioEvent;
import cl.duoc.pedidos360.kafka.KafkaTopicosProperties;
import cl.duoc.pedidos360.kafka.evento.InventarioEvento;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * Productor Kafka del dominio inventario: publica cada movimiento de stock en {@code pedidos360.inventario}, después
 * del commit (nunca se anuncia un descuento que no quedó guardado).
 *
 * <p>La clave es el id del producto: los movimientos de un producto quedan en orden en su partición y, como el tópico
 * se compacta por clave, siempre se conserva al menos el último evento de cada producto (su stock vigente).
 */
@Component
public class InventarioKafkaProducer {

  private static final Logger log = LoggerFactory.getLogger(InventarioKafkaProducer.class);

  private final KafkaTemplate<String, InventarioEvento> kafkaTemplate;
  private final KafkaTopicosProperties props;

  public InventarioKafkaProducer(KafkaTemplate<String, InventarioEvento> kafkaTemplate, KafkaTopicosProperties props) {
    this.kafkaTemplate = kafkaTemplate;
    this.props = props;
  }

  @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
  public void onMovimientos(MovimientosInventarioEvent e) {
    e.movimientos().forEach(this::publicar);
  }

  private void publicar(InventarioEvento evento) {
    String topico = props.topicos().inventario().nombre();
    String clave = String.valueOf(evento.productoId());
    try {
      kafkaTemplate.send(topico, clave, evento).whenComplete((resultado, error) -> {
        if (error != null) {
          log.error("No se pudo publicar {} del producto {} en '{}': {}", evento.tipo(), evento.productoId(), topico,
              error.getMessage());
        } else {
          log.info("Kafka: {} del producto {} (stock {}{}) publicado en {}-{}@{}", evento.tipo(),
              evento.productoId(), evento.stockActual(), evento.stockBajo() ? ", BAJO" : "", topico,
              resultado.getRecordMetadata().partition(), resultado.getRecordMetadata().offset());
        }
      });
    } catch (KafkaException ex) {
      log.error("Kafka no disponible: no se publicó {} del producto {}", evento.tipo(), evento.productoId(), ex);
    }
  }
}
