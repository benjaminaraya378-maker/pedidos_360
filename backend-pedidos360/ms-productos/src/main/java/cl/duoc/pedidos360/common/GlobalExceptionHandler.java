package cl.duoc.pedidos360.common;

import jakarta.servlet.http.HttpServletRequest;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.ResourceAccessException;

/** Traduce excepciones a respuestas JSON coherentes (400, 403, 404, 409, 502, 500). */
@RestControllerAdvice
public class GlobalExceptionHandler {

  private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

  @ExceptionHandler(ResourceNotFoundException.class)
  public ResponseEntity<ApiError> notFound(ResourceNotFoundException ex, HttpServletRequest req) {
    return build(HttpStatus.NOT_FOUND, ex.getMessage(), req, List.of());
  }

  @ExceptionHandler(BusinessException.class)
  public ResponseEntity<ApiError> business(BusinessException ex, HttpServletRequest req) {
    return build(ex.getStatus(), ex.getMessage(), req, List.of());
  }

  @ExceptionHandler(MethodArgumentNotValidException.class)
  public ResponseEntity<ApiError> validation(MethodArgumentNotValidException ex, HttpServletRequest req) {
    List<String> detalles = ex.getBindingResult().getFieldErrors().stream()
        .map(fe -> fe.getField() + ": " + fe.getDefaultMessage())
        .toList();
    return build(HttpStatus.BAD_REQUEST, "Datos de entrada inválidos", req, detalles);
  }

  @ExceptionHandler(HttpMessageNotReadableException.class)
  public ResponseEntity<ApiError> unreadable(HttpMessageNotReadableException ex, HttpServletRequest req) {
    return build(HttpStatus.BAD_REQUEST, "Cuerpo de la petición malformado o vacío", req, List.of());
  }

  /** @PreAuthorize fallido dentro de un controlador: 403 (nunca 500). */
  @ExceptionHandler(AccessDeniedException.class)
  public ResponseEntity<ApiError> denied(AccessDeniedException ex, HttpServletRequest req) {
    return build(HttpStatus.FORBIDDEN,
        "Token válido, pero sin el rol o scope necesario para " + req.getMethod() + " " + req.getRequestURI(),
        req, List.of());
  }

  /** Un microservicio aguas abajo respondió con error: se propaga el mismo código. */
  @ExceptionHandler(HttpStatusCodeException.class)
  public ResponseEntity<ApiError> downstream(HttpStatusCodeException ex, HttpServletRequest req) {
    HttpStatus status = HttpStatus.resolve(ex.getStatusCode().value());
    if (status == null) status = HttpStatus.BAD_GATEWAY;
    String cuerpo = ex.getResponseBodyAsString();
    return build(status, "El servicio remoto respondió " + status.value()
        + (cuerpo.isBlank() ? "" : ": " + cuerpo), req, List.of());
  }

  @ExceptionHandler(ResourceAccessException.class)
  public ResponseEntity<ApiError> unreachable(ResourceAccessException ex, HttpServletRequest req) {
    log.error("Servicio remoto no disponible: {}", ex.getMessage());
    return build(HttpStatus.BAD_GATEWAY, "Un microservicio no está disponible: " + ex.getMessage(), req, List.of());
  }

  @ExceptionHandler(Exception.class)
  public ResponseEntity<ApiError> generic(Exception ex, HttpServletRequest req) {
    log.error("Error no controlado en {} {}", req.getMethod(), req.getRequestURI(), ex);
    return build(HttpStatus.INTERNAL_SERVER_ERROR, "Error interno del servicio", req, List.of());
  }

  private ResponseEntity<ApiError> build(HttpStatus status, String message, HttpServletRequest req, List<String> details) {
    return ResponseEntity.status(status)
        .contentType(MediaType.APPLICATION_JSON)
        .body(ApiError.of(status, message, req.getRequestURI(), details));
  }
}
