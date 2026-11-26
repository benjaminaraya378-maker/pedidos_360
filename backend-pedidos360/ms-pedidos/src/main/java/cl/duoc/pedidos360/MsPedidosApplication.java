package cl.duoc.pedidos360;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/** {@code @EnableScheduling}: activa el relé del outbox de Kafka ({@code OutboxKafka#publicarPendientes}). */
@SpringBootApplication
@ConfigurationPropertiesScan
@EnableScheduling
public class MsPedidosApplication {
  public static void main(String[] args) {
    SpringApplication.run(MsPedidosApplication.class, args);
  }
}
