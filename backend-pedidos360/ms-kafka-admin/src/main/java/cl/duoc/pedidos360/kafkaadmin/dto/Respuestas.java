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
}
