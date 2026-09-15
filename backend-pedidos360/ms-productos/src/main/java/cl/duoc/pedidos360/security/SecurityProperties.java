package cl.duoc.pedidos360.security;

import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Parámetros del IDaaS (Microsoft Entra External ID / Azure AD B2C) usados para validar el JWT.
 *
 * <ul>
 *   <li>issuer: claim "iss" exacto del token (cópialo desde https://jwt.ms).</li>
 *   <li>jwkSetUri: endpoint de llaves públicas (JWKS) con el que se verifica la firma.</li>
 *   <li>audiences: valores aceptados para el claim "aud" (client ID de la app "Pedidos360-API").</li>
 *   <li>allowedOrigins: orígenes permitidos para CORS cuando se invoca el servicio directamente.</li>
 *   <li>rolesClaim: claim del que se leen los roles (por defecto "roles" = app roles de Entra).</li>
 * </ul>
 */
@ConfigurationProperties(prefix = "pedidos360.security")
public record SecurityProperties(
    String issuer,
    String jwkSetUri,
    List<String> audiences,
    List<String> allowedOrigins,
    String rolesClaim) {

  public SecurityProperties {
    if (rolesClaim == null || rolesClaim.isBlank()) {
      rolesClaim = "roles";
    }
    if (allowedOrigins == null || allowedOrigins.isEmpty()) {
      allowedOrigins = List.of("http://localhost:4200");
    }
    if (audiences == null) {
      audiences = List.of();
    }
  }
}
