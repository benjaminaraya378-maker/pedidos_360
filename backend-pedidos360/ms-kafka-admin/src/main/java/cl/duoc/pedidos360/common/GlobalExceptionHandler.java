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
import org.springframework.web.method.annotation.HandlerMethodValidationException;

/** Traduce excepciones a respuestas JSON coherentes (400, 403, 404, 409, 503, 500). */
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

  @ExceptionHandler(ValidacionException.class)
  public ResponseEntity<ApiError> validacion(ValidacionException ex, HttpServletRequest req) {
    return build(HttpStatus.BAD_REQUEST, ex.getMessage(), req, ex.getDetalles());
  }

  /** DTO inválido (@Valid sobre el cuerpo). */
  @ExceptionHandler(MethodArgumentNotValidException.class)
  public ResponseEntity<ApiError> validation(MethodArgumentNotValidException ex, HttpServletRequest req) {
    List<String> detalles = ex.getBindingResult().getAllErrors().stream()
        .map(e -> e instanceof org.springframework.validation.FieldError fe
            ? fe.getField() + ": " + fe.getDefaultMessage()
            : e.getDefaultMessage())
        .toList();
    return build(HttpStatus.BAD_REQUEST, "Datos de entrada inválidos", req, detalles);
  }

  /** Parámetro de ruta inválido (p. ej. nombre de tópico con caracteres no permitidos). */
  @ExceptionHandler(HandlerMethodValidationException.class)
  public ResponseEntity<ApiError> pathValidation(HandlerMethodValidationException ex, HttpServletRequest req) {
    List<String> detalles = ex.getParameterValidationResults().stream()
        .flatMap(r -> r.getResolvableErrors().stream()
            .map(e -> r.getMethodParameter().getParameterName() + ": " + e.getDefaultMessage()))
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

  @ExceptionHandler(KafkaNoDisponibleException.class)
  public ResponseEntity<ApiError> kafka(KafkaNoDisponibleException ex, HttpServletRequest req) {
    log.error("Kafka no disponible en {} {}: {}", req.getMethod(), req.getRequestURI(), ex.getMessage());
    return build(HttpStatus.SERVICE_UNAVAILABLE, ex.getMessage(), req, List.of());
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
