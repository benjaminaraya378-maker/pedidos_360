package cl.duoc.pedidos360.kafkaadmin.dto;

import jakarta.validation.constraints.AssertTrue;
import java.util.List;
import java.util.Map;

/**
 * Cuerpo de PATCH /api/kafka/topics/{nombre}/config: cambia la configuración del tópico en caliente, sin reiniciar
 * brokers ni consumidores.
 *
 * @param establecer  parámetros a fijar, por ejemplo {"retention.ms": "259200000"}
 * @param restablecer parámetros que vuelven al valor por defecto del broker, por ejemplo ["retention.ms"]
 */
public record ConfiguracionRequest(Map<String, String> establecer, List<String> restablecer) {

  public ConfiguracionRequest {
    if (establecer == null) establecer = Map.of();
    if (restablecer == null) restablecer = List.of();
  }

  @AssertTrue(message = "indique al menos un parámetro en 'establecer' o en 'restablecer'")
  public boolean isConCambios() {
    return !establecer.isEmpty() || !restablecer.isEmpty();
  }

  @AssertTrue(message = "un mismo parámetro no puede estar en 'establecer' y en 'restablecer'")
  public boolean isSinConflictos() {
    return restablecer.stream().noneMatch(establecer::containsKey);
  }
}
