package cl.duoc.pedidos360.kafka;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Tópicos, consumer groups y política de reintentos de Kafka, leídos de {@code pedidos360.kafka} en application.yml.
 * Ninguna otra clase escribe nombres de tópicos o de grupos: todas los toman de aquí (igual que
 * {@code MessagingProperties} con RabbitMQ).
 *
 * <ul>
 *   <li>topicos.pedidos: eventos del ciclo de vida del pedido. Dueño: ms-pedidos; aquí se consume.</li>
 *   <li>topicos.inventario: eventos de stock. Dueño: ms-productos, que lo declara con sus parámetros.</li>
 *   <li>dlt: tópico de mensajes fallidos (dead letter topic) de cada tópico: mismo nombre + sufijo.</li>
 *   <li>grupos: consumer groups de este microservicio.</li>
 *   <li>reintentos: reintentos con espera exponencial ante errores recuperables, antes de mandar el evento al DLT.</li>
 * </ul>
 */
@ConfigurationProperties(prefix = "pedidos360.kafka")
public record KafkaTopicosProperties(
    Topicos topicos,
    Dlt dlt,
    Grupos grupos,
    Reintentos reintentos) {

  public record Topicos(Topico pedidos, Topico inventario) {}

  /**
   * Parámetros de administración de un tópico. Los valores nulos no se envían al broker (queda su valor por defecto).
   *
   * @param nombre            nombre del tópico
   * @param particiones       unidades de paralelismo: cada partición la consume un solo hilo de cada grupo
   * @param replicas          copias de cada partición en brokers distintos (factor de replicación)
   * @param cleanupPolicy     delete (borra por antigüedad) o compact (conserva el último valor por clave)
   * @param retencionMs       cuánto tiempo se conservan los mensajes con cleanup.policy=delete
   * @param minInsyncReplicas réplicas que deben confirmar una escritura con acks=all
   * @param minCompactionLagMs con compact: antigüedad mínima antes de que un mensaje pueda ser compactado
   * @param segmentMs         cada cuánto se cierra un segmento del log (solo los segmentos cerrados se limpian)
   */
  public record Topico(
      String nombre,
      Integer particiones,
      Short replicas,
      String cleanupPolicy,
      Long retencionMs,
      Integer minInsyncReplicas,
      Long minCompactionLagMs,
      Long segmentMs) {}

  /** @param sufijo se agrega al nombre del tópico original; @param retencionMs cuánto se guardan los fallidos */
  public record Dlt(String sufijo, Long retencionMs) {

    public String de(String topico) {
      return topico + sufijo;
    }
  }

  /** @param inventario grupo que descuenta el stock de cada pedido creado */
  public record Grupos(String inventario) {}

  /**
   * Espera exponencial: el reintento N espera {@code esperaInicialMs * multiplicador^(N-1)}, con tope
   * {@code esperaMaximaMs}. Con 3 reintentos, 1 s, 2 s y 4 s.
   */
  public record Reintentos(int maximo, long esperaInicialMs, double multiplicador, long esperaMaximaMs) {}
}
