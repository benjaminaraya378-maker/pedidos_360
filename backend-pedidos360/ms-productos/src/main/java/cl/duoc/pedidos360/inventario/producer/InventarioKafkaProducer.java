package cl.duoc.pedidos360.inventario.producer;

import cl.duoc.pedidos360.inventario.event.MovimientosInventarioEvent;
import cl.duoc.pedidos360.kafka.KafkaTopicosProperties;
import cl.duoc.pedidos360.kafka.PublicacionKafkaException;
import cl.duoc.pedidos360.kafka.evento.InventarioEvento;
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
 * Productor Kafka del dominio inventario: publica cada movimiento de stock en {@code pedidos360.inventario} dentro de
 * la misma transacción que lo produjo, justo antes del commit, y espera a que el broker lo confirme (acks=all).
 *
 * <p>Si Kafka no confirma un evento, se lanza {@link PublicacionKafkaException} y Spring deshace la transacción
 * completa: no queda stock descontado ni repuesto, ni producto creado, ni el registro del evento procesado. Nunca
 * queda guardado un cambio del que el resto del sistema no se enteró:
 * <ul>
 *   <li>Desde el consumidor de PEDIDO_CREADO, el error llega al {@code DefaultErrorHandler} de {@code KafkaConfig},
 *       que reintenta el evento (1 s, 2 s, 4 s) y, si Kafka sigue sin confirmar, lo manda al DLT. Como el registro del
 *       evento procesado también se deshizo, el reintento vuelve a descontar (no se descarta como DUPLICADO).</li>
 *   <li>Desde una petición HTTP (crear producto, reponer stock), {@code GlobalExceptionHandler} responde 503.</li>
 * </ul>
 *
 * <p>La clave es el id del producto: los movimientos de un producto quedan en orden en su partición y, como el tópico
 * se compacta por clave, siempre se conserva al menos el último evento de cada producto (su stock vigente).
 */
@Component
public class InventarioKafkaProducer {

  private static final Logger log = LoggerFactory.getLogger(InventarioKafkaProducer.class);

  private final KafkaTemplate<String, InventarioEvento> kafkaTemplate;
  private final KafkaTopicosProperties props;
  private final long esperaConfirmacionMs;

  /**
   * @param esperaConfirmacionMs cuánto se espera la confirmación del broker antes de deshacer la transacción. Debe ser
   *                             mayor que {@code delivery.timeout.ms} del productor (ver application.yml)
   */
  public InventarioKafkaProducer(KafkaTemplate<String, InventarioEvento> kafkaTemplate, KafkaTopicosProperties props,
                                 @Value("${pedidos360.kafka.publicacion.espera-confirmacion-ms:15000}")
                                 long esperaConfirmacionMs) {
    this.kafkaTemplate = kafkaTemplate;
    this.props = props;
    this.esperaConfirmacionMs = esperaConfirmacionMs;
  }

  /**
   * Corre dentro de la transacción, justo antes del commit. Si lanza una excepción, Spring no hace el commit: deshace
   * la transacción (rollback) y la excepción le llega a quien llamó al servicio (el listener o el controlador).
   */
  @TransactionalEventListener(phase = TransactionPhase.BEFORE_COMMIT)
  public void onMovimientos(MovimientosInventarioEvent e) {
    // Uno a uno: si Kafka no confirma un evento, los siguientes ya no se envían
    e.movimientos().forEach(this::publicar);
  }

  private void publicar(InventarioEvento evento) {
    String topico = props.topicos().inventario().nombre();
    String clave = String.valueOf(evento.productoId());
    SendResult<String, InventarioEvento> resultado;
    try {
      resultado = kafkaTemplate.send(topico, clave, evento).get(esperaConfirmacionMs, TimeUnit.MILLISECONDS);
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
    log.info("Kafka: {} del producto {} (stock {}{}) publicado en {}-{}@{}", evento.tipo(), evento.productoId(),
        evento.stockActual(), evento.stockBajo() ? ", BAJO" : "", topico, resultado.getRecordMetadata().partition(),
        resultado.getRecordMetadata().offset());
  }

  private static PublicacionKafkaException noConfirmado(InventarioEvento evento, String topico, Throwable error) {
    String motivo = causaRaiz(error);
    log.error("Kafka no confirmó {} del producto {} en '{}': {}. Se deshace la transacción", evento.tipo(),
        evento.productoId(), topico, motivo);
    return new PublicacionKafkaException("Kafka no confirmó el evento " + evento.tipo() + " del producto "
        + evento.productoId() + " (" + motivo + ")", error);
  }

  /** El mensaje de la causa más profunda (la de Kafka), sin los envoltorios de Spring. */
  private static String causaRaiz(Throwable error) {
    Throwable t = error;
    while (t.getCause() != null && t.getCause() != t) {
      t = t.getCause();
    }
    return t.getMessage() != null ? t.getMessage() : t.getClass().getSimpleName();
  }
}
