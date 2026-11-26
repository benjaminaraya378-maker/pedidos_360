package cl.duoc.pedidos360.kafkaadmin.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

/**
 * Cuerpo de POST /api/kafka/dlt/{topicoDlt}/reprocesar. El tope evita reinyectar de golpe un DLT completo: se
 * reprocesa por tandas, revisando entre una y otra si los mensajes vuelven a fallar.
 *
 * @param max cuántos mensajes pendientes del DLT se reinyectan como máximo en esta operación (1 a 100)
 */
public record ReprocesoRequest(
    @NotNull(message = ReglasNombre.OBLIGATORIO) @Min(value = 1, message = "mínimo 1")
    @Max(value = 100, message = "máximo 100") Integer max) {}
