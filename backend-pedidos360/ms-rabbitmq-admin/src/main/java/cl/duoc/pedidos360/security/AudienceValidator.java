package cl.duoc.pedidos360.security;

import java.util.List;
import org.springframework.security.oauth2.core.OAuth2Error;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidatorResult;
import org.springframework.security.oauth2.jwt.Jwt;

/**
 * Verifica que el token haya sido emitido para ESTA API (claim "aud").
 * Un token válido de otro recurso (por ejemplo Microsoft Graph) es rechazado con 401.
 */
public class AudienceValidator implements OAuth2TokenValidator<Jwt> {

  private final List<String> audiencesPermitidos;

  public AudienceValidator(List<String> audiencesPermitidos) {
    this.audiencesPermitidos = audiencesPermitidos;
  }

  @Override
  public OAuth2TokenValidatorResult validate(Jwt jwt) {
    List<String> aud = jwt.getAudience();
    if (aud != null && aud.stream().anyMatch(audiencesPermitidos::contains)) {
      return OAuth2TokenValidatorResult.success();
    }
    OAuth2Error error = new OAuth2Error(
        "invalid_token",
        "El token no fue emitido para esta API (audience recibido: " + aud + ")",
        null);
    return OAuth2TokenValidatorResult.failure(error);
  }
}
