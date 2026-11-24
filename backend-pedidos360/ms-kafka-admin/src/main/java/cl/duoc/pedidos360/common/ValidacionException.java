package cl.duoc.pedidos360.common;

import java.util.List;

/** Entrada inválida detectada en el servicio (por ejemplo, parámetros de tópico): 400 con un detalle por error. */
public class ValidacionException extends RuntimeException {

  private final List<String> detalles;

  public ValidacionException(String message, List<String> detalles) {
    super(message);
    this.detalles = List.copyOf(detalles);
  }

  public List<String> getDetalles() {
    return detalles;
  }
}
