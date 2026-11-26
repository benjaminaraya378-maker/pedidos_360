package cl.duoc.pedidos360.pedidos.producer;

import cl.duoc.pedidos360.kafka.KafkaTopicosProperties;
import cl.duoc.pedidos360.kafka.PublicacionKafkaException;
import cl.duoc.pedidos360.kafka.evento.PedidoEvento;
import cl.duoc.pedidos360.kafka.evento.TipoEventoPedido;
import cl.duoc.pedidos360.pedidos.event.PedidoCreadoEvent;
import cl.duoc.pedidos360.pedidos.event.PedidoEstadoCambiadoEvent;
import cl.duoc.pedidos360.pedidos.model.EstadoPedido;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * Productor Kafka del dominio pedidos: publica en el tópico {@code pedidos360.pedidos} cada cambio del pedido.
 *
 * <ul>
 *   <li>PEDIDO_CREADO: al guardarse un pedido nuevo (petición HTTP). Pasa por el <b>outbox</b> ({@link OutboxKafka}):
 *       antes del commit se guarda en la tabla {@code eventos_kafka_pendientes}, en la misma transacción que el
 *       pedido, y después del commit se publica sin hacer esperar a la petición. Nunca se anuncia un pedido que no
 *       quedó guardado, y si Kafka no responde el pedido igual se crea y el relé del outbox publica el evento cuando
 *       Kafka vuelva: ya no se pierde.</li>
 *   <li>PEDIDO_CONFIRMADO / PEDIDO_CANCELADO: cuando el inventario respondió, mientras se procesa ese evento. Se
 *       publica <b>antes del commit y esperando la confirmación del broker</b>: si Kafka no confirma, se deshace el
 *       cambio de estado y el evento de inventario se reintenta (y termina en el DLT si Kafka sigue sin responder),
 *       así la analítica nunca queda sin enterarse de una confirmación o cancelación.</li>
 * </ul>
 *
 * <p>Convive con {@link PedidoEventProducer} (RabbitMQ): los dos escuchan el mismo evento de Spring y no se conocen;
 * agregar Kafka no cambió nada de las colas existentes.
 *
 * <p>La clave del mensaje es el id del pedido: todos sus eventos caen en la misma partición y se consumen en orden.
 */
@Component
public class PedidoKafkaProducer {

  private static final Logger log = LoggerFactory.getLogger(PedidoKafkaProducer.class);

  private final KafkaTemplate<String, PedidoEvento> kafkaTemplate;
  private final KafkaTopicosProperties props;
  private final OutboxKafka outbox;
  private final long esperaConfirmacionMs;

  /**
   * @param esperaConfirmacionMs cuánto se espera la confirmación del broker al publicar un cambio de estado antes de
   *                             deshacer la transacción. Debe ser mayor que {@code delivery.timeout.ms} del productor
   */
  public PedidoKafkaProducer(KafkaTemplate<String, PedidoEvento> kafkaTemplate, KafkaTopicosProperties props,
                             OutboxKafka outbox,
                             @Value("${pedidos360.kafka.publicacion.espera-confirmacion-ms:15000}")
                             long esperaConfirmacionMs) {
    this.kafkaTemplate = kafkaTemplate;
    this.props = props;
    this.outbox = outbox;
    this.esperaConfirmacionMs = esperaConfirmacionMs;
  }

  /**
   * Corre dentro de la transacción de {@code PedidoService.crear}, justo antes del commit: guarda PEDIDO_CREADO en el
   * outbox junto con el pedido. Aquí no se toca Kafka; la publicación ocurre después del commit (ver
   * {@link OutboxKafka}). Si el INSERT del outbox falla, el pedido tampoco se guarda.
   */
  @TransactionalEventListener(phase = TransactionPhase.BEFORE_COMMIT)
  public void onPedidoCreado(PedidoCreadoEvent e) {
    outbox.registrar(props.topicos().pedidos().nombre(), PedidoEvento.nuevo(TipoEventoPedido.PEDIDO_CREADO,
        e.pedidoId(), e.clienteOid(), EstadoPedido.CREADO.name(), e.total(), null, items(e.items())));
  }

  /**
   * Corre dentro de la transacción de {@code EstadoPedidoService}, justo antes del commit. Si lanza una excepción,
   * Spring deshace el cambio de estado y la excepción llega al listener, que reintenta el evento de inventario.
   */
  @TransactionalEventListener(phase = TransactionPhase.BEFORE_COMMIT)
  public void onEstadoCambiado(PedidoEstadoCambiadoEvent e) {
    TipoEventoPedido tipo = e.estado() == EstadoPedido.CANCELADO
        ? TipoEventoPedido.PEDIDO_CANCELADO
        : TipoEventoPedido.PEDIDO_CONFIRMADO;
    publicarYEsperar(PedidoEvento.nuevo(tipo, e.pedidoId(), e.clienteOid(), e.estado().name(), e.total(), e.motivo(),
        items(e.items())));
  }

  private void publicarYEsperar(PedidoEvento evento) {
    String topico = props.topicos().pedidos().nombre();
    SendResult<String, PedidoEvento> resultado;
    try {
      resultado = kafkaTemplate.send(topico, String.valueOf(evento.pedidoId()), evento)
          .get(esperaConfirmacionMs, TimeUnit.MILLISECONDS);
    } catch (ExecutionException ex) {
      // El broker rechazó el envío o venció delivery.timeout.ms (sin réplicas suficientes, brokers caídos...)
      throw noConfirmado(evento, topico, ex.getCause() != null ? ex.getCause() : ex);
    } catch (TimeoutException ex) {
      throw noConfirmado(evento, topico, new TimeoutException("sin respuesta del broker en " + esperaConfirmacionMs
          + " ms"));
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw noConfirmado(evento, topico, ex);
    } catch (KafkaException | org.springframework.kafka.KafkaException ex) {
      // send() falló antes de enviar: sin metadata del tópico en max.block.ms, error al serializar, etc.
      throw noConfirmado(evento, topico, ex);
    }
    publicado(evento, topico, resultado);
  }

  private static void publicado(PedidoEvento evento, String topico, SendResult<String, PedidoEvento> resultado) {
    log.info("Kafka: {} del pedido {} publicado en {}-{}@{} (evento {})", evento.tipo(), evento.pedidoId(), topico,
        resultado.getRecordMetadata().partition(), resultado.getRecordMetadata().offset(), evento.eventoId());
  }

  private static PublicacionKafkaException noConfirmado(PedidoEvento evento, String topico, Throwable error) {
    Throwable raiz = error;
    while (raiz.getCause() != null && raiz.getCause() != raiz) {
      raiz = raiz.getCause();
    }
    String motivo = raiz.getMessage() != null ? raiz.getMessage() : raiz.getClass().getSimpleName();
    log.error("Kafka no confirmó {} del pedido {} en '{}': {}. Se deshace el cambio de estado", evento.tipo(),
        evento.pedidoId(), topico, motivo);
    return new PublicacionKafkaException("Kafka no confirmó el evento " + evento.tipo() + " del pedido "
        + evento.pedidoId() + " (" + motivo + ")", error);
  }

  private static List<PedidoEvento.Item> items(List<PedidoCreadoEvent.Item> items) {
    return items.stream()
        .map(i -> new PedidoEvento.Item(i.productoId(), i.nombreProducto(), i.cantidad(), i.precioUnitario(),
            i.subtotal()))
        .toList();
  }
}
