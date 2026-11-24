package cl.duoc.pedidos360.kafkaadmin.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

/**
 * Cuerpo de POST /api/kafka/topics/{nombre}/partitions. Kafka solo permite aumentar particiones: reducirlas
 * obligaría a mover mensajes y rompería el orden por clave.
 *
 * @param total nuevo número total de particiones (mayor que el actual)
 */
public record ParticionesRequest(
    @NotNull(message = ReglasNombre.OBLIGATORIO) @Min(value = 1, message = "mínimo 1")
    @Max(value = 50, message = "máximo 50") Integer total) {}
