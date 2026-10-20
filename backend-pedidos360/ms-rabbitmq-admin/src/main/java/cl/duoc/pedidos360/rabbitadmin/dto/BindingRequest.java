package cl.duoc.pedidos360.rabbitadmin.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

/**
 * Binding cola ← exchange. Cuerpo de POST /api/rabbit/bindings y parámetros de DELETE /api/rabbit/bindings.
 *
 * @param exchange   exchange de origen (obligatorio, debe existir)
 * @param cola       cola de destino (obligatoria, debe existir)
 * @param routingKey clave o patrón de enlace (opcional; vacío para exchanges fanout)
 */
public record BindingRequest(
    @NotBlank(message = ReglasNombre.OBLIGATORIO)
    @Pattern(regexp = ReglasNombre.PATRON, message = ReglasNombre.MENSAJE) String exchange,
    @NotBlank(message = ReglasNombre.OBLIGATORIO)
    @Pattern(regexp = ReglasNombre.PATRON, message = ReglasNombre.MENSAJE) String cola,
    @Size(max = 255, message = "máximo 255 caracteres") String routingKey) {

  public BindingRequest {
    if (routingKey == null) routingKey = "";
  }
}
