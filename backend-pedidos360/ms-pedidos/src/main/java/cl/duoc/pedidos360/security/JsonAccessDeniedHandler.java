package cl.duoc.pedidos360.security;

import cl.duoc.pedidos360.common.ApiError;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.web.access.AccessDeniedHandler;
import org.springframework.stereotype.Component;

/** Responde 403 en JSON cuando el token es válido pero no tiene el rol o scope requerido. */
@Component
public class JsonAccessDeniedHandler implements AccessDeniedHandler {

  private static final Logger log = LoggerFactory.getLogger(JsonAccessDeniedHandler.class);
  private final ObjectMapper mapper;

  public JsonAccessDeniedHandler(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  @Override
  public void handle(HttpServletRequest request, HttpServletResponse response, AccessDeniedException ex)
      throws IOException {
    String mensaje = "Token válido, pero sin el rol o scope necesario para " + request.getMethod() + " "
        + request.getRequestURI();
    log.warn("403 {}", mensaje);
    response.setStatus(HttpStatus.FORBIDDEN.value());
    response.setContentType(MediaType.APPLICATION_JSON_VALUE);
    response.setCharacterEncoding("UTF-8");
    mapper.writeValue(response.getWriter(), ApiError.of(HttpStatus.FORBIDDEN, mensaje, request.getRequestURI()));
  }
}
