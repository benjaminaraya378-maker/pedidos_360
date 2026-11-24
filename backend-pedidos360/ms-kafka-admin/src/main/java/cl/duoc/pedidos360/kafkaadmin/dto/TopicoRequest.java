package cl.duoc.pedidos360.kafkaadmin.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import java.util.Map;

/**
 * Cuerpo de POST /api/kafka/topics.
 *
 * @param nombre        nombre del tópico
 * @param particiones   número de particiones (1 a 50)
 * @param replicas      factor de replicación; no puede superar la cantidad de brokers
 * @param configuracion parámetros del tópico, por ejemplo {"retention.ms": "86400000", "cleanup.policy": "delete"}
 *                      (solo los de la lista permitida, ver {@code ConfiguracionesTopico})
 */
public record TopicoRequest(
    @NotBlank(message = ReglasNombre.OBLIGATORIO)
    @Pattern(regexp = ReglasNombre.TOPICO, message = ReglasNombre.MENSAJE_TOPICO) String nombre,
    @NotNull(message = ReglasNombre.OBLIGATORIO) @Min(value = 1, message = "mínimo 1")
    @Max(value = 50, message = "máximo 50") Integer particiones,
    @NotNull(message = ReglasNombre.OBLIGATORIO) @Min(value = 1, message = "mínimo 1")
    @Max(value = 10, message = "máximo 10") Short replicas,
    Map<String, String> configuracion) {

  public TopicoRequest {
    if (configuracion == null) configuracion = Map.of();
  }
}
