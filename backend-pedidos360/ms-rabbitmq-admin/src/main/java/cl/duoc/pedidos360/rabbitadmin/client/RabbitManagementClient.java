package cl.duoc.pedidos360.rabbitadmin.client;

import cl.duoc.pedidos360.rabbitadmin.dto.QueueResponse;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/**
 * Cliente de la API de gestión de RabbitMQ (la de la consola web). AMQP no sabe listar colas, así que el listado
 * se pide aquí, con las mismas credenciales y virtual host que usa el servicio para el resto de las operaciones.
 * Los conteos de mensajes los publica el broker cada pocos segundos, por lo que pueden ir levemente atrasados.
 */
@Component
public class RabbitManagementClient {

  private final RestClient client;
  private final String vhost;

  public RabbitManagementClient(RestClient.Builder builder, RabbitProperties rabbit,
                                @Value("${pedidos360.rabbit-admin.management-url}") String managementUrl) {
    this.client = builder.baseUrl(managementUrl)
        .defaultHeaders(headers -> headers.setBasicAuth(rabbit.getUsername(), rabbit.getPassword()))
        .build();
    this.vhost = rabbit.getVirtualHost();
  }

  /** Todas las colas del virtual host, ordenadas por nombre. */
  public List<QueueResponse> colas() {
    ColaGestion[] colas = client.get()
        .uri("/api/queues/{vhost}?columns=name,messages,consumers", vhost)
        .retrieve()
        .body(ColaGestion[].class);
    if (colas == null) {
      return List.of();
    }
    return Arrays.stream(colas)
        .map(c -> new QueueResponse(c.name(), valor(c.messages()), valor(c.consumers())))
        .sorted(Comparator.comparing(QueueResponse::nombre))
        .toList();
  }

  /** Una cola recién creada aún no trae estadísticas: sin dato, 0. */
  private static int valor(Integer n) {
    return n == null ? 0 : n;
  }

  record ColaGestion(String name, Integer messages, Integer consumers) {}
}
