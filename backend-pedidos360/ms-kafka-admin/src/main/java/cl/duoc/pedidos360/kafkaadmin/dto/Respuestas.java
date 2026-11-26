package cl.duoc.pedidos360.kafkaadmin.dto;

import java.util.List;
import java.util.Map;

/** Respuestas de la API de administración de Kafka. */
public final class Respuestas {

  private Respuestas() {}

  /** GET /api/kafka/cluster: identificador del clúster, broker controlador y brokers vivos. */
  public record Cluster(String clusterId, Integer controlador, List<Broker> brokers) {}

  public record Broker(int id, String host, int puerto, String rack) {}

  /** Una fila de GET /api/kafka/topics. */
  public record TopicoResumen(String nombre, int particiones, int replicas, boolean interno) {}

  /**
   * GET /api/kafka/topics/{nombre}.
   *
   * @param mensajes      mensajes retenidos (suma de offset fin - offset inicio; en un tópico compactado es un máximo)
   * @param configuracion parámetros administrables del tópico con su valor y de dónde sale
   */
  public record TopicoDetalle(String nombre, boolean interno, int particiones, int replicas, long mensajes,
                              List<Particion> detalleParticiones, Map<String, ValorConfiguracion> configuracion) {}

  /**
   * @param lider  broker que atiende lecturas y escrituras de la partición
   * @param isr    réplicas en sincronía (in-sync replicas): las que pueden asumir como líder sin perder datos
   */
  public record Particion(int particion, Integer lider, List<Integer> replicas, List<Integer> isr, long offsetInicio,
                          long offsetFin) {}

  /** @param origen TOPICO (fijado en el tópico), BROKER (configuración del broker) o DEFECTO (valor de Kafka) */
  public record ValorConfiguracion(String valor, String origen) {}

  /** Una fila de GET /api/kafka/consumer-groups. */
  public record GrupoResumen(String grupo, String estado, int miembros, long lagTotal) {}

  /**
   * GET /api/kafka/consumer-groups/{grupo}.
   *
   * @param asignador estrategia con que el grupo reparte las particiones entre sus miembros
   * @param lagTotal  eventos pendientes de procesar del grupo, sumando todas sus particiones
   */
  public record GrupoDetalle(String grupo, String estado, Integer coordinador, String asignador,
                             List<Miembro> miembros, List<OffsetParticion> offsets, long lagTotal) {}

  /** Un consumidor del grupo y las particiones que tiene asignadas (formato tópico-partición). */
  public record Miembro(String consumidorId, String clienteId, String host, List<String> particiones) {}

  /**
   * @param offsetConfirmado último offset confirmado por el grupo (lo siguiente que leerá)
   * @param offsetFin        offset siguiente al último mensaje escrito en la partición
   * @param lag              offsetFin - offsetConfirmado: eventos que el grupo aún no procesa
   * @param consumidor       clienteId del miembro que la tiene asignada (null si nadie)
   */
  public record OffsetParticion(String topico, int particion, Long offsetConfirmado, long offsetFin, long lag,
                                String consumidor) {}

  /** Resultado de mover offsets: posición nueva de cada partición. */
  public record OffsetsMovidos(String grupo, String topico, String estrategia, Map<Integer, Long> nuevosOffsets) {}

  /**
   * GET /api/kafka/dlt/{topicoDlt}: lo que el próximo reproceso reinyectaría, leído sin confirmar offsets (consultarlo
   * no cambia nada).
   *
   * @param grupoReproceso consumer group con que ms-kafka-admin recuerda hasta dónde reprocesó el DLT
   * @param pendientes     mensajes del DLT que aún no se reprocesan, sumando todas las particiones
   * @param particiones    desde dónde y hasta dónde quedan pendientes en cada partición
   * @param mensajes       los primeros pendientes (como máximo {@code max}), por partición y offset
   */
  public record ContenidoDlt(String topico, String grupoReproceso, long pendientes,
                             List<PendientesParticion> particiones, List<MensajeDlt> mensajes) {}

  /**
   * @param siguiente offset del próximo mensaje a reprocesar en la partición
   * @param offsetFin offset siguiente al último mensaje escrito en la partición del DLT
   * @param pendientes offsetFin - siguiente
   */
  public record PendientesParticion(int particion, long siguiente, long offsetFin, long pendientes) {}

  /**
   * Un mensaje del DLT con lo que le agregó el {@code DeadLetterPublishingRecoverer} del servicio que no lo pudo
   * procesar (headers {@code kafka_dlt-*}).
   *
   * @param fecha           cuándo llegó al DLT (ISO-8601)
   * @param topicoOrigen    tópico al que se reinyectaría (header {@code kafka_dlt-original-topic}, o el nombre del DLT
   *                        sin {@code .dlt} si el header falta)
   * @param particionOrigen partición donde estaba el evento original
   * @param offsetOrigen    offset que tenía el evento original
   * @param grupoOrigen     consumer group que no lo pudo procesar
   * @param excepcion       clase de la excepción que lo mandó al DLT (la causa raíz si viene)
   * @param motivo          mensaje de la excepción, recortado
   * @param valor           contenido como texto UTF-8, recortado a 500 caracteres (null si el valor es nulo)
   * @param bytes           tamaño real del valor en bytes
   */
  public record MensajeDlt(int particion, long offset, String fecha, String clave, String topicoOrigen,
                           Integer particionOrigen, Long offsetOrigen, String grupoOrigen, String excepcion,
                           String motivo, String valor, int bytes) {}

  /**
   * POST /api/kafka/dlt/{topicoDlt}/reprocesar.
   *
   * @param reinyectados mensajes publicados de nuevo en su tópico de origen (y confirmados en el grupo de reproceso)
   * @param pendientes   mensajes que siguen en el DLT sin reprocesar después de esta operación
   * @param detalle      de dónde salió cada mensaje en el DLT y dónde quedó en el tópico de origen
   */
  public record ResultadoReproceso(String topicoDlt, String grupoReproceso, int reinyectados, long pendientes,
                                   List<Reinyeccion> detalle) {}

  /** Un mensaje reinyectado: posición en el DLT → tópico, partición y offset nuevos en el tópico de origen. */
  public record Reinyeccion(int particionDlt, long offsetDlt, String clave, String topico, int particion,
                            long offset) {}
}
