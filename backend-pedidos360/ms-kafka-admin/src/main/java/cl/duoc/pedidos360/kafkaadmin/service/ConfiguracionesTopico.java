package cl.duoc.pedidos360.kafkaadmin.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.kafka.common.config.TopicConfig;

/**
 * Parámetros de tópico que la API permite fijar, con la regla de cada uno. Se validan antes de llamar a Kafka para
 * responder 400 con un mensaje claro por parámetro (y no un error genérico del broker), y para no exponer parámetros
 * peligrosos o irrelevantes para la operación.
 */
public final class ConfiguracionesTopico {

  /** Parámetro → regla. La regla devuelve null si el valor es válido, o el motivo del rechazo. */
  private static final Map<String, Function<String, String>> PERMITIDAS = new LinkedHashMap<>();

  static {
    PERMITIDAS.put(TopicConfig.CLEANUP_POLICY_CONFIG, v ->
        Set.of("delete", "compact", "compact,delete", "delete,compact").contains(v.replace(" ", ""))
            ? null : "debe ser delete, compact o compact,delete");
    PERMITIDAS.put(TopicConfig.RETENTION_MS_CONFIG, menosUnoOPositivo("milisegundos"));
    PERMITIDAS.put(TopicConfig.RETENTION_BYTES_CONFIG, menosUnoOPositivo("bytes"));
    PERMITIDAS.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, entre(1, 10));
    PERMITIDAS.put(TopicConfig.SEGMENT_MS_CONFIG, entre(60_000, Long.MAX_VALUE));
    PERMITIDAS.put(TopicConfig.SEGMENT_BYTES_CONFIG, entre(1_048_576, Integer.MAX_VALUE));
    PERMITIDAS.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, entre(1, 104_857_600));
    PERMITIDAS.put(TopicConfig.COMPRESSION_TYPE_CONFIG, v ->
        Set.of("producer", "uncompressed", "gzip", "snappy", "lz4", "zstd").contains(v)
            ? null : "debe ser producer, uncompressed, gzip, snappy, lz4 o zstd");
    PERMITIDAS.put(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, entre(0, Long.MAX_VALUE));
    PERMITIDAS.put(TopicConfig.DELETE_RETENTION_MS_CONFIG, entre(0, Long.MAX_VALUE));
  }

  private ConfiguracionesTopico() {}

  public static Set<String> permitidas() {
    return PERMITIDAS.keySet();
  }

  /**
   * Valida los parámetros a fijar en un tópico con {@code replicas} réplicas.
   *
   * @return lista de errores, vacía si todo es válido
   */
  public static List<String> validar(Map<String, String> configuracion, int replicas) {
    List<String> errores = new ArrayList<>();
    configuracion.forEach((clave, valor) -> {
      Function<String, String> regla = PERMITIDAS.get(clave);
      if (regla == null) {
        errores.add(clave + ": parámetro no permitido (permitidos: " + String.join(", ", permitidas()) + ")");
      } else if (valor == null || valor.isBlank()) {
        errores.add(clave + ": no puede estar vacío");
      } else {
        String motivo = regla.apply(valor.trim());
        if (motivo != null) errores.add(clave + ": " + motivo);
      }
    });
    String minIsr = configuracion.get(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG);
    if (minIsr != null && minIsr.matches("\\d+") && Integer.parseInt(minIsr) > replicas) {
      errores.add(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG + ": no puede ser mayor que las réplicas del tópico ("
          + replicas + "); ninguna escritura con acks=all podría confirmarse");
    }
    return errores;
  }

  /** Los parámetros a restablecer también deben ser de la lista permitida. */
  public static List<String> validarClaves(Collection<String> claves) {
    return claves.stream()
        .filter(c -> !PERMITIDAS.containsKey(c))
        .map(c -> c + ": parámetro no permitido (permitidos: " + String.join(", ", permitidas()) + ")")
        .toList();
  }

  private static Function<String, String> entre(long minimo, long maximo) {
    return v -> {
      try {
        long n = Long.parseLong(v);
        return n >= minimo && n <= maximo ? null : "debe estar entre " + minimo + " y " + maximo;
      } catch (NumberFormatException ex) {
        return "debe ser un número entero";
      }
    };
  }

  private static Function<String, String> menosUnoOPositivo(String unidad) {
    return v -> {
      try {
        long n = Long.parseLong(v);
        return n == -1 || n > 0 ? null : "debe ser -1 (sin límite) o una cantidad de " + unidad + " mayor que 0";
      } catch (NumberFormatException ex) {
        return "debe ser un número entero";
      }
    };
  }
}
