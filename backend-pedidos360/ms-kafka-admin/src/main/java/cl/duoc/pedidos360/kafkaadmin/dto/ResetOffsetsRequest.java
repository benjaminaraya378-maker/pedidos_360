package cl.duoc.pedidos360.kafkaadmin.dto;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import java.time.Instant;

/**
 * Cuerpo de POST /api/kafka/consumer-groups/{grupo}/offsets: mueve los offsets de un grupo en un tópico, por ejemplo
 * para reprocesar eventos (INICIO o FECHA) o para saltarse un atraso (FIN). El grupo no debe tener consumidores
 * activos.
 *
 * @param topico     tópico cuyos offsets se mueven
 * @param estrategia INICIO, FIN, FECHA (primer evento desde {@code fecha}) u OFFSET (posición exacta)
 * @param fecha      obligatoria con FECHA, en ISO-8601 (2026-11-24T15:00:00Z)
 * @param offset     obligatorio con OFFSET
 * @param particion  opcional: mover solo esa partición (por defecto, todas)
 */
public record ResetOffsetsRequest(
    @NotBlank(message = ReglasNombre.OBLIGATORIO)
    @Pattern(regexp = ReglasNombre.TOPICO, message = ReglasNombre.MENSAJE_TOPICO) String topico,
    @NotNull(message = "es obligatoria: INICIO, FIN, FECHA u OFFSET") Estrategia estrategia,
    Instant fecha,
    @Min(value = 0, message = "no puede ser negativo") Long offset,
    @Min(value = 0, message = "no puede ser negativa") Integer particion) {

  public enum Estrategia { INICIO, FIN, FECHA, OFFSET }

  @AssertTrue(message = "la estrategia FECHA requiere 'fecha' y OFFSET requiere 'offset'")
  public boolean isCompleta() {
    if (estrategia == Estrategia.FECHA) return fecha != null;
    if (estrategia == Estrategia.OFFSET) return offset != null;
    return true;
  }
}
