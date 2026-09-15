package cl.duoc.pedidos360.common;

import java.time.Instant;
import java.util.List;
import org.springframework.http.HttpStatus;

/** Formato único de error para todo el sistema (lo consume el frontend y los scripts de evidencia). */
public record ApiError(
    String timestamp,
    int status,
    String error,
    String message,
    String path,
    List<String> details) {

  public static ApiError of(HttpStatus status, String message, String path) {
    return new ApiError(Instant.now().toString(), status.value(), status.getReasonPhrase(), message, path, List.of());
  }

  public static ApiError of(HttpStatus status, String message, String path, List<String> details) {
    return new ApiError(Instant.now().toString(), status.value(), status.getReasonPhrase(), message, path, details);
  }
}
