package cl.duoc.pedidos360.bff.client;

import cl.duoc.pedidos360.security.JwtUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.web.client.RestClient;

/** Construye clientes REST que reenvían el JWT del usuario a cada microservicio. */
final class TokenRelay {
  private TokenRelay() {}

  static RestClient cliente(RestClient.Builder builder, String baseUrl) {
    return builder
        .baseUrl(baseUrl)
        .requestInitializer(req -> req.getHeaders().set(HttpHeaders.AUTHORIZATION, "Bearer " + JwtUtils.tokenValue()))
        .build();
  }
}
