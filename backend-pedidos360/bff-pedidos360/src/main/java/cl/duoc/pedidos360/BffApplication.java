package cl.duoc.pedidos360;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * BFF (Backend For Frontend) de Pedidos360.
 * Valida el JWT del IDaaS igual que el API Manager y compone respuestas
 * a la medida del frontend a partir de ms-productos y ms-pedidos.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class BffApplication {
  public static void main(String[] args) {
    SpringApplication.run(BffApplication.class, args);
  }
}
