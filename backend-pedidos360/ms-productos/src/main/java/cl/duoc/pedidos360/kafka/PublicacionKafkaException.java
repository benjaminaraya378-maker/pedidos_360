package cl.duoc.pedidos360.kafka;

/**
 * Kafka no confirmó un evento que este servicio debía publicar (brokers caídos, sin réplicas en sincronía suficientes
 * o sin respuesta a tiempo). Se lanza dentro de la transacción, antes del commit, así que el cambio que el evento
 * anunciaba se deshace completo:
 * <ul>
 *   <li>En un {@code @KafkaListener} es un error recuperable: el manejador de errores de {@code KafkaConfig} reintenta
 *       el evento con espera exponencial y, si Kafka sigue sin confirmar, lo manda al DLT.</li>
 *   <li>En una petición HTTP, {@code GlobalExceptionHandler} responde 503: no se guardó nada y se puede reintentar.</li>
 * </ul>
 */
public class PublicacionKafkaException extends RuntimeException {

  public PublicacionKafkaException(String message, Throwable cause) {
    super(message, cause);
  }
}
