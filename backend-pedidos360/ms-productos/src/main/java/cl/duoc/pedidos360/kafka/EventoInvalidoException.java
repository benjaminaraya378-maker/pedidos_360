package cl.duoc.pedidos360.kafka;

/**
 * Evento que se pudo leer pero no se puede procesar nunca (por ejemplo, apunta a un pedido que no existe).
 * Reintentarlo no lo arregla: el manejador de errores lo envía directo al DLT, sin reintentos.
 */
public class EventoInvalidoException extends RuntimeException {

  public EventoInvalidoException(String message) {
    super(message);
  }
}
