package cl.duoc.pedidos360.bff.client;

import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

@Component
public class PedidosClient {

  private static final ParameterizedTypeReference<List<Map<String, Object>>> LISTA =
      new ParameterizedTypeReference<>() {};

  private final RestClient client;

  public PedidosClient(RestClient.Builder builder, @Value("${pedidos360.clients.pedidos-url}") String url) {
    this.client = TokenRelay.cliente(builder, url);
  }

  public List<Map<String, Object>> listar() {
    List<Map<String, Object>> lista = client.get().uri("/api/pedidos").retrieve().body(LISTA);
    return lista == null ? List.of() : lista;
  }

}
