package cl.duoc.pedidos360;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class MsPedidosApplication {
  public static void main(String[] args) {
    SpringApplication.run(MsPedidosApplication.class, args);
  }
}
