package cl.duoc.pedidos360.security;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.oauth2.core.OAuth2TokenValidatorResult;
import org.springframework.security.oauth2.jwt.Jwt;

/**
 * Pruebas unitarias (sin contexto Spring) de cómo el BFF interpreta el JWT del IDaaS: roles y scopes → authorities,
 * y validación de la audiencia.
 */
class SeguridadJwtTest {

  private static Jwt.Builder token() {
    return Jwt.withTokenValue("token").header("alg", "RS256").subject("usuario");
  }

  private static List<String> nombres(Collection<GrantedAuthority> authorities) {
    return authorities.stream().map(GrantedAuthority::getAuthority).toList();
  }

  @Test
  void convierteRolesYScopesEnAuthorities() {
    Jwt jwt = token().claim("roles", List.of("Admin")).claim("scp", "Pedidos360.Read Pedidos360.Write").build();

    List<String> authorities = nombres(new ClaimsAuthoritiesConverter("roles").convert(jwt));

    assertThat(authorities).containsExactly("ROLE_ADMIN", "SCOPE_Pedidos360.Read", "SCOPE_Pedidos360.Write");
  }

  @Test
  void usaElClaimDeRolesConfigurado() {
    Jwt jwt = token().claim("app_roles", List.of("Cliente")).claim("roles", List.of("Admin")).build();

    List<String> authorities = nombres(new ClaimsAuthoritiesConverter("app_roles").convert(jwt));

    assertThat(authorities).containsExactly("ROLE_CLIENTE");
  }

  @Test
  void sinRolesNiScopesNoDaPermisos() {
    Jwt jwt = token().claim("name", "Sin permisos").build();

    assertThat(new ClaimsAuthoritiesConverter("roles").convert(jwt)).isEmpty();
  }

  @Test
  void aceptaUnTokenEmitidoParaEstaApi() {
    Jwt jwt = token().audience(List.of("api-pedidos360")).build();

    OAuth2TokenValidatorResult resultado = new AudienceValidator(List.of("api-pedidos360")).validate(jwt);

    assertThat(resultado.hasErrors()).isFalse();
  }

  @Test
  void rechazaUnTokenDeOtraApi() {
    Jwt jwt = token().audience(List.of("https://graph.microsoft.com")).build();

    OAuth2TokenValidatorResult resultado = new AudienceValidator(List.of("api-pedidos360")).validate(jwt);

    assertThat(resultado.hasErrors()).isTrue();
    assertThat(resultado.getErrors()).first().satisfies(e -> assertThat(e.getErrorCode()).isEqualTo("invalid_token"));
  }
}
