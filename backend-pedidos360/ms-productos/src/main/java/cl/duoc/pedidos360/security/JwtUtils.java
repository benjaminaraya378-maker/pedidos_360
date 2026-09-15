package cl.duoc.pedidos360.security;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;

/** Lectura de claims del JWT autenticado (roles, scopes, identidad) y acceso al token crudo para "token relay". */
public final class JwtUtils {

  private JwtUtils() {}

  /** JWT de la petición actual (si existe). */
  public static Optional<Jwt> current() {
    Authentication auth = SecurityContextHolder.getContext().getAuthentication();
    if (auth instanceof JwtAuthenticationToken token) {
      return Optional.of(token.getToken());
    }
    return Optional.empty();
  }

  /** Valor crudo del token, para reenviarlo a otros microservicios (Authorization: Bearer ...). */
  public static String tokenValue() {
    return current().map(Jwt::getTokenValue).orElse("");
  }

  /** Identificador estable del usuario en el tenant: "oid" (Entra) o "sub" como respaldo. */
  public static String userId(Jwt jwt) {
    String oid = jwt.getClaimAsString("oid");
    return oid != null ? oid : jwt.getSubject();
  }

  public static String email(Jwt jwt) {
    for (String claim : List.of("email", "preferred_username", "upn")) {
      String v = jwt.getClaimAsString(claim);
      if (v != null && !v.isBlank()) return v;
    }
    List<String> emails = asList(jwt.getClaim("emails")); // Azure AD B2C
    return emails.isEmpty() ? "" : emails.get(0);
  }

  public static String name(Jwt jwt) {
    String name = jwt.getClaimAsString("name");
    if (name != null && !name.isBlank()) return name;
    String given = jwt.getClaimAsString("given_name");
    String family = jwt.getClaimAsString("family_name");
    if (given != null || family != null) {
      return ((given == null ? "" : given) + " " + (family == null ? "" : family)).trim();
    }
    return email(jwt);
  }

  /** Roles de aplicación (claim configurable, por defecto "roles"). */
  public static List<String> roles(Jwt jwt, String rolesClaim) {
    return asList(jwt.getClaim(rolesClaim));
  }

  /** Scopes delegados: Entra usa "scp" (string separado por espacios); otros IDaaS usan "scope". */
  public static List<String> scopes(Jwt jwt) {
    Set<String> scopes = new LinkedHashSet<>();
    scopes.addAll(asList(jwt.getClaim("scp")));
    scopes.addAll(asList(jwt.getClaim("scope")));
    return new ArrayList<>(scopes);
  }

  public static boolean hasRole(Jwt jwt, String rolesClaim, String role) {
    return roles(jwt, rolesClaim).stream().anyMatch(r -> r.equalsIgnoreCase(role));
  }

  /** Convierte un claim (lista, string separado por espacios/comas o null) en lista de strings. */
  public static List<String> asList(Object claim) {
    if (claim == null) return List.of();
    if (claim instanceof Collection<?> c) {
      return c.stream().map(String::valueOf).toList();
    }
    return Arrays.stream(String.valueOf(claim).split("[ ,]+")).filter(s -> !s.isBlank()).toList();
  }
}
