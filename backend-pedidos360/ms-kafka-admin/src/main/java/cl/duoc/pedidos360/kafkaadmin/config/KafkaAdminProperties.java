package cl.duoc.pedidos360.kafkaadmin.config;

import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @param timeout           cuánto espera cada operación a Kafka antes de responder 503
 * @param topicosProtegidos tópicos del sistema que no se pueden borrar desde la API
 */
@ConfigurationProperties(prefix = "pedidos360.kafka-admin")
public record KafkaAdminProperties(Duration timeout, List<String> topicosProtegidos) {

  public KafkaAdminProperties {
    if (timeout == null) timeout = Duration.ofSeconds(10);
    if (topicosProtegidos == null) topicosProtegidos = List.of();
  }
}
