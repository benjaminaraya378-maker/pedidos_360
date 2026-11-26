package cl.duoc.pedidos360.kafkaadmin.service;

import java.time.Duration;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;

/**
 * Crea el consumidor y el productor de cada lectura o reproceso de un DLT. Quien los pide los cierra. Es una interfaz
 * para que las pruebas de {@link DltService} usen {@code MockConsumer} y {@code MockProducer} en vez de un clúster.
 *
 * <p>Clave y valor van en bytes ({@code ByteArrayDeserializer} / {@code ByteArraySerializer}): el administrador no
 * conoce los DTO de los eventos y reinyecta exactamente los bytes que llegaron al DLT, sea JSON válido o no.
 */
public interface ClientesDlt {

  /** Consumidor del grupo {@link ReglasDlt#GRUPO_REPROCESO}, sin auto-commit. */
  Consumer<byte[], byte[]> consumidor();

  /** Productor con acks=all e idempotencia, para reinyectar en el tópico de origen. */
  Producer<byte[], byte[]> productor();

  /**
   * Tope en que el productor confirma o rechaza cada envío ({@code delivery.timeout.ms}). El servicio espera un poco
   * más que esto: así nunca abandona un envío que todavía podría escribirse sin que su offset quede confirmado.
   */
  Duration esperaEntrega();
}
