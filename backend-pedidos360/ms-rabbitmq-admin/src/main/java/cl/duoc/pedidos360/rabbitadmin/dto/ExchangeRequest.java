package cl.duoc.pedidos360.rabbitadmin.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

/**
 * Cuerpo de POST /api/rabbit/exchanges.
 *
 * @param nombre     nombre del exchange (obligatorio)
 * @param tipo       direct, topic, fanout o headers (obligatorio)
 * @param durable    sobrevive a un reinicio del broker (por defecto true)
 * @param autoDelete se borra cuando se elimina su último binding (por defecto false)
 */
public record ExchangeRequest(
    @NotBlank(message = ReglasNombre.OBLIGATORIO)
    @Pattern(regexp = ReglasNombre.PATRON, message = ReglasNombre.MENSAJE) String nombre,
    @NotBlank(message = ReglasNombre.OBLIGATORIO)
    @Pattern(regexp = "direct|topic|fanout|headers", message = "debe ser direct, topic, fanout o headers") String tipo,
    Boolean durable,
    Boolean autoDelete) {

  public ExchangeRequest {
    if (durable == null) durable = Boolean.TRUE;
    if (autoDelete == null) autoDelete = Boolean.FALSE;
  }
}
