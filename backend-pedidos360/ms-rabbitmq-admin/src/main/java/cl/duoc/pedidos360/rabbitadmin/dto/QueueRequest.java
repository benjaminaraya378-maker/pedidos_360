package cl.duoc.pedidos360.rabbitadmin.dto;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

/**
 * Cuerpo de POST /api/rabbit/queues.
 *
 * @param nombre               nombre de la cola (obligatorio)
 * @param durable              sobrevive a un reinicio del broker (por defecto true)
 * @param autoDelete           se borra cuando se va su último consumidor (por defecto false)
 * @param deadLetterExchange   exchange al que van los mensajes rechazados o expirados (opcional, debe existir)
 * @param deadLetterRoutingKey routing key con que se reenvían a ese exchange (opcional, requiere deadLetterExchange)
 * @param ttlMs                tiempo de vida de cada mensaje en milisegundos (opcional, mayor que 0)
 * @param maxLength            máximo de mensajes en la cola (opcional, mayor que 0)
 */
public record QueueRequest(
    @NotBlank(message = ReglasNombre.OBLIGATORIO)
    @Pattern(regexp = ReglasNombre.PATRON, message = ReglasNombre.MENSAJE) String nombre,
    Boolean durable,
    Boolean autoDelete,
    @Pattern(regexp = ReglasNombre.PATRON, message = ReglasNombre.MENSAJE) String deadLetterExchange,
    @Size(max = 255, message = "máximo 255 caracteres") String deadLetterRoutingKey,
    @Positive(message = "debe ser mayor que 0") Integer ttlMs,
    @Positive(message = "debe ser mayor que 0") Integer maxLength) {

  public QueueRequest {
    if (durable == null) durable = Boolean.TRUE;
    if (autoDelete == null) autoDelete = Boolean.FALSE;
  }

  @AssertTrue(message = "deadLetterRoutingKey solo se puede indicar junto con deadLetterExchange")
  public boolean isDeadLetterConsistente() {
    return deadLetterRoutingKey == null || deadLetterExchange != null;
  }
}
