package cl.duoc.pedidos360.messaging;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Nombres de la topología RabbitMQ, leídos de {@code pedidos360.messaging} en application.yml.
 * Ninguna otra clase escribe nombres de colas, exchanges o routing keys: todas los toman de aquí.
 *
 * <ul>
 *   <li>exchange: exchange topic donde se publican los eventos del dominio pedidos.</li>
 *   <li>deadLetterExchange: exchange direct al que el broker envía los mensajes rechazados (NACK sin requeue).</li>
 *   <li>pedidoCreadoRoutingKey: routing key del evento "pedido creado".</li>
 *   <li>maxReintentos: reintentos ante errores recuperables antes de mandar el mensaje a la DLQ.</li>
 *   <li>notificaciones / comprobantes: cola, binding key y DLQ de cada caso de uso.</li>
 * </ul>
 */
@ConfigurationProperties(prefix = "pedidos360.messaging")
public record MessagingProperties(
    String exchange,
    String deadLetterExchange,
    String pedidoCreadoRoutingKey,
    int maxReintentos,
    Ruta notificaciones,
    Ruta comprobantes) {

  /** Una ruta de mensajería: la cola que consume, su binding sobre el exchange y su DLQ. */
  public record Ruta(String cola, String bindingKey, String dlq) {}
}
