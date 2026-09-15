package cl.duoc.pedidos360.security;

import java.time.Duration;
import java.util.List;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtIssuerValidator;
import org.springframework.security.oauth2.jwt.JwtTimestampValidator;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;

/**
 * Seguridad del servicio como OAuth2 Resource Server.
 *
 * <p>Cada petición debe traer un JWT del IDaaS. El servicio valida, en este orden:
 * <ol>
 *   <li>Firma: con las llaves públicas del JWKS del tenant (NimbusJwtDecoder).</li>
 *   <li>Vigencia: claims "exp"/"nbf" con 60 s de tolerancia de reloj (JwtTimestampValidator).</li>
 *   <li>Emisor: claim "iss" exactamente igual al configurado (JwtIssuerValidator).</li>
 *   <li>Audiencia: claim "aud" contiene el client ID de la API (AudienceValidator).</li>
 * </ol>
 * Luego convierte "roles" y "scp" en authorities para autorizar con {@code @PreAuthorize}.
 * Los errores se entregan en JSON con 401 (autenticación) o 403 (autorización).
 */
@Configuration
@EnableWebSecurity
@EnableMethodSecurity
public class SecurityConfig {

  private final SecurityProperties props;

  public SecurityConfig(SecurityProperties props) {
    this.props = props;
  }

  @Bean
  SecurityFilterChain securityFilterChain(
      HttpSecurity http,
      JwtDecoder jwtDecoder,
      JsonAuthenticationEntryPoint entryPoint,
      JsonAccessDeniedHandler accessDeniedHandler) throws Exception {

    JwtAuthenticationConverter jwtConverter = new JwtAuthenticationConverter();
    jwtConverter.setJwtGrantedAuthoritiesConverter(new ClaimsAuthoritiesConverter(props.rolesClaim()));

    http
        .csrf(AbstractHttpConfigurer::disable)
        .cors(Customizer.withDefaults())
        .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
        .authorizeHttpRequests(auth -> auth
            // Preflight CORS del navegador (no trae Authorization)
            .requestMatchers(HttpMethod.OPTIONS, "/**").permitAll()
            // Todo lo demás exige un JWT válido; los roles/scopes se revisan con @PreAuthorize
            .anyRequest().authenticated())
        .oauth2ResourceServer(oauth2 -> oauth2
            .jwt(jwt -> jwt.decoder(jwtDecoder).jwtAuthenticationConverter(jwtConverter))
            .authenticationEntryPoint(entryPoint)
            .accessDeniedHandler(accessDeniedHandler))
        .exceptionHandling(ex -> ex
            .authenticationEntryPoint(entryPoint)
            .accessDeniedHandler(accessDeniedHandler));

    return http.build();
  }

  @Bean
  JwtDecoder jwtDecoder() {
    NimbusJwtDecoder decoder = NimbusJwtDecoder.withJwkSetUri(props.jwkSetUri()).build();
    OAuth2TokenValidator<Jwt> validadores = new DelegatingOAuth2TokenValidator<>(
        new JwtTimestampValidator(Duration.ofSeconds(60)),
        new JwtIssuerValidator(props.issuer()),
        new AudienceValidator(props.audiences()));
    decoder.setJwtValidator(validadores);
    return decoder;
  }

  /** CORS para llamadas directas al servicio (localhost). Detrás del API Gateway, CORS lo gestiona el gateway. */
  @Bean
  CorsConfigurationSource corsConfigurationSource() {
    CorsConfiguration cfg = new CorsConfiguration();
    cfg.setAllowedOrigins(props.allowedOrigins());
    cfg.setAllowedMethods(List.of("GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"));
    cfg.setAllowedHeaders(List.of("Authorization", "Content-Type", "Accept"));
    cfg.setExposedHeaders(List.of("WWW-Authenticate"));
    cfg.setMaxAge(3600L);
    UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
    source.registerCorsConfiguration("/**", cfg);
    return source;
  }
}
