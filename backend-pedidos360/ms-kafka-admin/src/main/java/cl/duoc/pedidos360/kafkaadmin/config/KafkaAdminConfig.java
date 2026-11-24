package cl.duoc.pedidos360.kafkaadmin.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * Cliente de administración de Kafka. Se arma con la misma configuración del {@link KafkaAdmin} de Spring Boot
 * (bootstrap servers y {@code spring.kafka.admin.properties}) y se cierra al detener el servicio.
 */
@Configuration
public class KafkaAdminConfig {

  @Bean(destroyMethod = "close")
  AdminClient adminClient(KafkaAdmin kafkaAdmin) {
    return AdminClient.create(kafkaAdmin.getConfigurationProperties());
  }
}
