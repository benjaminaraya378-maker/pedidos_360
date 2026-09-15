package cl.duoc.pedidos360.common;

import org.springframework.http.HttpStatus;

/** Error de negocio con código HTTP explícito (por defecto 409 Conflict). */
public class BusinessException extends RuntimeException {
  private final HttpStatus status;

  public BusinessException(String message) {
    this(HttpStatus.CONFLICT, message);
  }

  public BusinessException(HttpStatus status, String message) {
    super(message);
    this.status = status;
  }

  public HttpStatus getStatus() {
    return status;
  }
}
