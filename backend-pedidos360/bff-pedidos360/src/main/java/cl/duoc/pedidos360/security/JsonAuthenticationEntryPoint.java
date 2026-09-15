package cl.duoc.pedidos360.security;

import cl.duoc.pedidos360.common.ApiError;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.oauth2.core.OAuth2AuthenticationException;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.stereotype.Component;

/**
 * Responde 401 en JSON cuando la petición no trae token o el token es inválido
 * (firma incorrecta, expirado, issuer/audience distintos, etc.).
 */
@Component
public class JsonAuthenticationEntryPoint implements AuthenticationEntryPoint {

  private static final Logger log = LoggerFactory.getLogger(JsonAuthenticationEntryPoint.class);
  private final ObjectMapper mapper;

  public JsonAuthenticationEntryPoint(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  @Override
  public void commence(HttpServletRequest request, HttpServletResponse response, AuthenticationException ex)
      throws IOException {
    String mensaje;
    if (ex instanceof OAuth2AuthenticationException oauthEx) {
      String descripcion = oauthEx.getError().getDescription();
      mensaje = "Token inválido: " + (descripcion != null ? descripcion : oauthEx.getError().getErrorCode());
      response.setHeader(HttpHeaders.WWW_AUTHENTICATE,
          "Bearer error=\"invalid_token\", error_description=\"" + limpiar(descripcion) + "\"");
    } else {
      mensaje = "Se requiere un token Bearer emitido por el IDaaS (cabecera Authorization: Bearer <JWT>)";
      response.setHeader(HttpHeaders.WWW_AUTHENTICATE, "Bearer realm=\"pedidos360\"");
    }
    log.warn("401 {} {} -> {}", request.getMethod(), request.getRequestURI(), mensaje);

    response.setStatus(HttpStatus.UNAUTHORIZED.value());
    response.setContentType(MediaType.APPLICATION_JSON_VALUE);
    response.setCharacterEncoding("UTF-8");
    mapper.writeValue(response.getWriter(), ApiError.of(HttpStatus.UNAUTHORIZED, mensaje, request.getRequestURI()));
  }

  private static String limpiar(String texto) {
    return texto == null ? "" : texto.replace("\"", "'").replace("\n", " ");
  }
}
