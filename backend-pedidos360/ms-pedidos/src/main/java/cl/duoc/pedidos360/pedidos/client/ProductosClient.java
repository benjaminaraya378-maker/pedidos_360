package cl.duoc.pedidos360.pedidos.client;

import cl.duoc.pedidos360.common.ResourceNotFoundException;
import cl.duoc.pedidos360.security.JwtUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

/**
 * Cliente HTTP hacia ms-productos con "token relay": reenvía el mismo JWT del usuario,
 * de modo que ms-productos también lo valida y autoriza (defensa en profundidad).
 */
@Component
public class ProductosClient {

  private final RestClient client;

  public ProductosClient(RestClient.Builder builder,
                         @Value("${pedidos360.clients.productos-url}") String productosUrl) {
    this.client = builder.baseUrl(productosUrl).build();
  }

  public ProductoDto obtener(Long productoId) {
    try {
      return client.get()
          .uri("/api/productos/{id}", productoId)
          .header(HttpHeaders.AUTHORIZATION, "Bearer " + JwtUtils.tokenValue())
          .retrieve()
          .body(ProductoDto.class);
    } catch (HttpClientErrorException.NotFound ex) {
      throw new ResourceNotFoundException("Producto " + productoId + " no existe");
    }
  }

}
