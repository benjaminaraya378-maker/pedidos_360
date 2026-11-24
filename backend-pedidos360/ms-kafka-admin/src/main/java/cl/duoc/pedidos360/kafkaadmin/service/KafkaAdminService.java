package cl.duoc.pedidos360.kafkaadmin.service;

import cl.duoc.pedidos360.common.BusinessException;
import cl.duoc.pedidos360.common.KafkaNoDisponibleException;
import cl.duoc.pedidos360.common.ResourceNotFoundException;
import cl.duoc.pedidos360.common.ValidacionException;
import cl.duoc.pedidos360.kafkaadmin.config.KafkaAdminProperties;
import cl.duoc.pedidos360.kafkaadmin.dto.ConfiguracionRequest;
import cl.duoc.pedidos360.kafkaadmin.dto.ResetOffsetsRequest;
import cl.duoc.pedidos360.kafkaadmin.dto.Respuestas.Broker;
import cl.duoc.pedidos360.kafkaadmin.dto.Respuestas.Cluster;
import cl.duoc.pedidos360.kafkaadmin.dto.Respuestas.GrupoDetalle;
import cl.duoc.pedidos360.kafkaadmin.dto.Respuestas.GrupoResumen;
import cl.duoc.pedidos360.kafkaadmin.dto.Respuestas.Miembro;
import cl.duoc.pedidos360.kafkaadmin.dto.Respuestas.OffsetParticion;
import cl.duoc.pedidos360.kafkaadmin.dto.Respuestas.OffsetsMovidos;
import cl.duoc.pedidos360.kafkaadmin.dto.Respuestas.Particion;
import cl.duoc.pedidos360.kafkaadmin.dto.Respuestas.TopicoDetalle;
import cl.duoc.pedidos360.kafkaadmin.dto.Respuestas.TopicoResumen;
import cl.duoc.pedidos360.kafkaadmin.dto.Respuestas.ValorConfiguracion;
import cl.duoc.pedidos360.kafkaadmin.dto.TopicoRequest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

/**
 * Administración de Kafka: toda la interacción con el clúster ({@link AdminClient}) está en esta clase; el controlador
 * solo llama a sus métodos y no conoce la librería de Kafka.
 *
 * <p>Cada operación espera a Kafka como máximo {@code pedidos360.kafka-admin.timeout} (503 si no responde) y los
 * errores del broker se traducen a respuestas claras: 404 si el tópico o grupo no existe, 409 si ya existe o si el
 * grupo está activo, 400 si los parámetros no son válidos.
 */
@Service
public class KafkaAdminService {

  private static final Logger log = LoggerFactory.getLogger(KafkaAdminService.class);

  private final AdminClient admin;
  private final KafkaAdminProperties props;

  public KafkaAdminService(AdminClient admin, KafkaAdminProperties props) {
    this.admin = admin;
    this.props = props;
  }

  // ---------- Clúster ----------

  public Cluster cluster() {
    DescribeClusterResult resultado = admin.describeCluster();
    String id = esperar(resultado.clusterId(), "describir el clúster");
    Node controlador = esperar(resultado.controller(), "describir el clúster");
    List<Broker> brokers = esperar(resultado.nodes(), "describir el clúster").stream()
        .sorted(Comparator.comparingInt(Node::id))
        .map(n -> new Broker(n.id(), n.host(), n.port(), n.rack()))
        .toList();
    return new Cluster(id, controlador == null ? null : controlador.id(), brokers);
  }

  // ---------- Tópicos ----------

  public List<TopicoResumen> listarTopicos(boolean incluirInternos) {
    Set<String> nombres = esperar(admin.listTopics(new ListTopicsOptions().listInternal(incluirInternos)).names(),
        "listar los tópicos");
    if (nombres.isEmpty()) {
      return List.of();
    }
    Map<String, TopicDescription> descripciones = esperar(admin.describeTopics(nombres).allTopicNames(),
        "describir los tópicos");
    return descripciones.values().stream()
        .sorted(Comparator.comparing(TopicDescription::name))
        .map(t -> new TopicoResumen(t.name(), t.partitions().size(), replicas(t), t.isInternal()))
        .toList();
  }

  /** Particiones (líder, réplicas, ISR, offsets), mensajes retenidos y configuración administrable del tópico. */
  public TopicoDetalle obtenerTopico(String nombre) {
    TopicDescription t = describir(nombre);
    List<TopicPartition> particiones = particiones(t);
    Map<TopicPartition, Long> inicio = offsets(particiones, OffsetSpec.earliest(), "leer los offsets de " + nombre);
    Map<TopicPartition, Long> fin = offsets(particiones, OffsetSpec.latest(), "leer los offsets de " + nombre);

    List<Particion> detalle = new ArrayList<>();
    for (TopicPartitionInfo p : t.partitions()) {
      TopicPartition tp = new TopicPartition(nombre, p.partition());
      detalle.add(new Particion(p.partition(), p.leader() == null ? null : p.leader().id(), ids(p.replicas()),
          ids(p.isr()), inicio.getOrDefault(tp, 0L), fin.getOrDefault(tp, 0L)));
    }
    long mensajes = detalle.stream().mapToLong(p -> p.offsetFin() - p.offsetInicio()).sum();
    return new TopicoDetalle(nombre, t.isInternal(), t.partitions().size(), replicas(t), mensajes, detalle,
        configuracion(nombre));
  }

  public TopicoDetalle crearTopico(TopicoRequest req) {
    List<String> errores = new ArrayList<>(ConfiguracionesTopico.validar(req.configuracion(), req.replicas()));
    int brokers = cluster().brokers().size();
    if (req.replicas() > brokers) {
      errores.add("replicas: el clúster tiene " + brokers + " broker(s) y cada réplica debe quedar en uno distinto");
    }
    if (!errores.isEmpty()) {
      throw new ValidacionException("Parámetros del tópico inválidos", errores);
    }
    Map<String, String> configuracion = new HashMap<>();
    req.configuracion().forEach((clave, valor) -> configuracion.put(clave, valor.trim()));
    NewTopic nuevo = new NewTopic(req.nombre(), req.particiones(), req.replicas()).configs(configuracion);
    esperar(admin.createTopics(List.of(nuevo)).all(), "crear el tópico '" + req.nombre() + "'");
    log.info("Tópico '{}' creado: {} particiones, {} réplicas, configuración {}", req.nombre(), req.particiones(),
        req.replicas(), configuracion);
    // Los metadatos del tópico nuevo tardan unos milisegundos en llegar a todos los brokers
    return obtenerCuando(req.nombre(), d -> d.particiones() == req.particiones());
  }

  /** Cambia parámetros del tópico en caliente (sin reiniciar brokers ni consumidores). */
  public TopicoDetalle modificarConfiguracion(String nombre, ConfiguracionRequest req) {
    TopicDescription t = describir(nombre);
    List<String> errores = new ArrayList<>(ConfiguracionesTopico.validar(req.establecer(), replicas(t)));
    errores.addAll(ConfiguracionesTopico.validarClaves(req.restablecer()));
    if (!errores.isEmpty()) {
      throw new ValidacionException("Parámetros del tópico inválidos", errores);
    }
    List<AlterConfigOp> cambios = new ArrayList<>();
    req.establecer().forEach((clave, valor) ->
        cambios.add(new AlterConfigOp(new ConfigEntry(clave, valor.trim()), AlterConfigOp.OpType.SET)));
    req.restablecer().forEach(clave ->
        cambios.add(new AlterConfigOp(new ConfigEntry(clave, null), AlterConfigOp.OpType.DELETE)));
    ConfigResource recurso = new ConfigResource(ConfigResource.Type.TOPIC, nombre);
    esperar(admin.incrementalAlterConfigs(Map.of(recurso, cambios)).all(),
        "cambiar la configuración del tópico '" + nombre + "'");
    log.info("Tópico '{}': configuración actualizada (establecer {}, restablecer {})", nombre, req.establecer(),
        req.restablecer());
    return obtenerTopico(nombre);
  }

  /** Aumenta las particiones del tópico. Kafka no permite reducirlas. */
  public TopicoDetalle aumentarParticiones(String nombre, int total) {
    int actuales = describir(nombre).partitions().size();
    if (total <= actuales) {
      throw new ValidacionException("No se pueden reducir ni mantener las particiones",
          List.of("total: debe ser mayor que las particiones actuales (" + actuales + "); Kafka no permite reducirlas"
              + " porque cambiaría la partición que corresponde a cada clave"));
    }
    esperar(admin.createPartitions(Map.of(nombre, NewPartitions.increaseTo(total))).all(),
        "aumentar las particiones del tópico '" + nombre + "'");
    log.info("Tópico '{}': particiones {} → {}", nombre, actuales, total);
    return obtenerCuando(nombre, d -> d.particiones() == total);
  }

  public void eliminarTopico(String nombre) {
    if (props.topicosProtegidos().contains(nombre)) {
      throw new BusinessException(HttpStatus.CONFLICT,
          "El tópico '" + nombre + "' es parte del flujo de Pedidos360 y está protegido: no se puede eliminar");
    }
    describir(nombre);
    esperar(admin.deleteTopics(List.of(nombre)).all(), "eliminar el tópico '" + nombre + "'");
    log.info("Tópico '{}' eliminado", nombre);
  }

  // ---------- Consumer groups ----------

  public List<GrupoResumen> listarGrupos() {
    Collection<ConsumerGroupListing> listado = esperar(admin.listConsumerGroups().all(), "listar los consumer groups");
    List<String> ids = listado.stream().map(ConsumerGroupListing::groupId).sorted().toList();
    if (ids.isEmpty()) {
      return List.of();
    }
    Map<String, ConsumerGroupDescription> descripciones = esperar(admin.describeConsumerGroups(ids).all(),
        "describir los consumer groups");
    return ids.stream()
        .map(id -> {
          ConsumerGroupDescription d = descripciones.get(id);
          long lag = offsetsDelGrupo(id, asignaciones(d)).stream().mapToLong(OffsetParticion::lag).sum();
          return new GrupoResumen(id, d.state().name(), d.members().size(), lag);
        })
        .toList();
  }

  /** Miembros, particiones asignadas a cada uno y, por partición, offset confirmado, offset final y lag. */
  public GrupoDetalle obtenerGrupo(String grupo) {
    ConsumerGroupDescription d = describirGrupo(grupo);
    Map<TopicPartition, String> asignadas = asignaciones(d);
    List<OffsetParticion> offsets = offsetsDelGrupo(grupo, asignadas);
    if (d.state() == ConsumerGroupState.DEAD && d.members().isEmpty() && offsets.isEmpty()) {
      throw new ResourceNotFoundException("El consumer group '" + grupo + "' no existe");
    }
    List<Miembro> miembros = d.members().stream()
        .sorted(Comparator.comparing(MemberDescription::clientId))
        .map(m -> new Miembro(m.consumerId(), m.clientId(), m.host(), m.assignment().topicPartitions().stream()
            .sorted(Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition))
            .map(tp -> tp.topic() + "-" + tp.partition())
            .toList()))
        .toList();
    long lagTotal = offsets.stream().mapToLong(OffsetParticion::lag).sum();
    return new GrupoDetalle(grupo, d.state().name(), d.coordinator() == null ? null : d.coordinator().id(),
        d.partitionAssignor(), miembros, offsets, lagTotal);
  }

  /**
   * Mueve los offsets del grupo en un tópico. Kafka solo lo permite con el grupo vacío: si hubiera consumidores
   * activos, ellos mismos sobrescribirían los offsets al confirmar.
   */
  public OffsetsMovidos moverOffsets(String grupo, ResetOffsetsRequest req) {
    ConsumerGroupDescription d = describirGrupo(grupo);
    if (!d.members().isEmpty()) {
      throw new BusinessException(HttpStatus.CONFLICT, "El grupo '" + grupo + "' tiene " + d.members().size()
          + " consumidor(es) activo(s): deténgalos antes de mover sus offsets");
    }
    TopicDescription t = describir(req.topico());
    List<TopicPartition> particiones = particiones(t).stream()
        .filter(tp -> req.particion() == null || tp.partition() == req.particion())
        .toList();
    if (particiones.isEmpty()) {
      throw new ValidacionException("Partición inexistente", List.of("particion: el tópico tiene "
          + t.partitions().size() + " particiones (0 a " + (t.partitions().size() - 1) + ")"));
    }
    String operacion = "calcular los offsets de " + req.topico();
    Map<TopicPartition, Long> destino = switch (req.estrategia()) {
      case INICIO -> offsets(particiones, OffsetSpec.earliest(), operacion);
      case FIN -> offsets(particiones, OffsetSpec.latest(), operacion);
      case FECHA -> {
        Map<TopicPartition, Long> porFecha =
            offsets(particiones, OffsetSpec.forTimestamp(req.fecha().toEpochMilli()), operacion);
        Map<TopicPartition, Long> fin = offsets(particiones, OffsetSpec.latest(), operacion);
        // -1: no hay eventos desde esa fecha en la partición → se ubica al final
        yield particiones.stream().collect(Collectors.toMap(tp -> tp,
            tp -> porFecha.getOrDefault(tp, -1L) < 0 ? fin.get(tp) : porFecha.get(tp)));
      }
      case OFFSET -> {
        Map<TopicPartition, Long> inicio = offsets(particiones, OffsetSpec.earliest(), operacion);
        Map<TopicPartition, Long> fin = offsets(particiones, OffsetSpec.latest(), operacion);
        // Un offset fuera del rango retenido se ajusta al extremo más cercano
        yield particiones.stream().collect(Collectors.toMap(tp -> tp,
            tp -> Math.max(inicio.get(tp), Math.min(fin.get(tp), req.offset()))));
      }
    };
    Map<TopicPartition, OffsetAndMetadata> nuevos = new HashMap<>();
    destino.forEach((tp, offset) -> nuevos.put(tp, new OffsetAndMetadata(offset)));
    esperar(admin.alterConsumerGroupOffsets(grupo, nuevos).all(), "mover los offsets del grupo '" + grupo + "'");

    Map<Integer, Long> porParticion = new TreeMap<>();
    destino.forEach((tp, offset) -> porParticion.put(tp.partition(), offset));
    log.info("Grupo '{}' en '{}': offsets movidos ({}) a {}", grupo, req.topico(), req.estrategia(), porParticion);
    return new OffsetsMovidos(grupo, req.topico(), req.estrategia().name(), porParticion);
  }

  public void eliminarGrupo(String grupo) {
    esperar(admin.deleteConsumerGroups(List.of(grupo)).all(), "eliminar el consumer group '" + grupo + "'");
    log.info("Consumer group '{}' eliminado", grupo);
  }

  // ---------- Apoyo ----------

  private TopicDescription describir(String nombre) {
    return esperar(admin.describeTopics(List.of(nombre)).allTopicNames(), "describir el tópico '" + nombre + "'")
        .get(nombre);
  }

  private ConsumerGroupDescription describirGrupo(String grupo) {
    return esperar(admin.describeConsumerGroups(List.of(grupo)).describedGroups().get(grupo),
        "describir el consumer group '" + grupo + "'");
  }

  /** Reintenta la lectura hasta que refleje el cambio recién hecho (como máximo 2 s). */
  private TopicoDetalle obtenerCuando(String nombre, Predicate<TopicoDetalle> listo) {
    for (int intento = 1; ; intento++) {
      try {
        TopicoDetalle detalle = obtenerTopico(nombre);
        if (listo.test(detalle) || intento >= 10) return detalle;
      } catch (ResourceNotFoundException ex) {
        if (intento >= 10) throw ex;
      }
      try {
        Thread.sleep(200);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        return obtenerTopico(nombre);
      }
    }
  }

  /** Para cada partición con offset confirmado o asignada a un miembro: confirmado, final y lag. */
  private List<OffsetParticion> offsetsDelGrupo(String grupo, Map<TopicPartition, String> asignadas) {
    Map<TopicPartition, OffsetAndMetadata> confirmados = esperar(
        admin.listConsumerGroupOffsets(grupo).partitionsToOffsetAndMetadata(),
        "leer los offsets del grupo '" + grupo + "'");
    Set<TopicPartition> todas = new HashSet<>(asignadas.keySet());
    confirmados.forEach((tp, om) -> {
      if (om != null) todas.add(tp);
    });
    if (todas.isEmpty()) {
      return List.of();
    }
    Map<TopicPartition, Long> fin = offsets(todas, OffsetSpec.latest(), "leer los offsets finales");
    Map<TopicPartition, Long> inicio = offsets(todas, OffsetSpec.earliest(), "leer los offsets iniciales");
    return todas.stream()
        .sorted(Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition))
        .map(tp -> {
          OffsetAndMetadata om = confirmados.get(tp);
          Long confirmado = om == null ? null : om.offset();
          long ultimo = fin.getOrDefault(tp, 0L);
          // Sin offset confirmado, el grupo leerá desde el inicio (auto-offset-reset: earliest)
          long desde = confirmado != null ? confirmado : inicio.getOrDefault(tp, 0L);
          return new OffsetParticion(tp.topic(), tp.partition(), confirmado, ultimo, Math.max(0, ultimo - desde),
              asignadas.get(tp));
        })
        .toList();
  }

  private static Map<TopicPartition, String> asignaciones(ConsumerGroupDescription d) {
    Map<TopicPartition, String> asignadas = new HashMap<>();
    for (MemberDescription m : d.members()) {
      m.assignment().topicPartitions().forEach(tp -> asignadas.put(tp, m.clientId()));
    }
    return asignadas;
  }

  private Map<TopicPartition, Long> offsets(Collection<TopicPartition> particiones, OffsetSpec spec,
                                            String operacion) {
    Map<TopicPartition, OffsetSpec> pedido = new HashMap<>();
    particiones.forEach(tp -> pedido.put(tp, spec));
    Map<TopicPartition, ListOffsetsResultInfo> resultado = esperar(admin.listOffsets(pedido).all(), operacion);
    Map<TopicPartition, Long> offsets = new HashMap<>();
    resultado.forEach((tp, info) -> offsets.put(tp, info.offset()));
    return offsets;
  }

  private Map<String, ValorConfiguracion> configuracion(String nombre) {
    ConfigResource recurso = new ConfigResource(ConfigResource.Type.TOPIC, nombre);
    Config config = esperar(admin.describeConfigs(List.of(recurso)).all(),
        "leer la configuración del tópico '" + nombre + "'").get(recurso);
    Map<String, ValorConfiguracion> resultado = new TreeMap<>();
    for (String clave : ConfiguracionesTopico.permitidas()) {
      ConfigEntry entrada = config.get(clave);
      if (entrada != null) {
        resultado.put(clave, new ValorConfiguracion(entrada.value(), origen(entrada.source())));
      }
    }
    return resultado;
  }

  private static String origen(ConfigEntry.ConfigSource fuente) {
    return switch (fuente) {
      case DYNAMIC_TOPIC_CONFIG -> "TOPICO";
      case DEFAULT_CONFIG -> "DEFECTO";
      default -> "BROKER";
    };
  }

  private static List<TopicPartition> particiones(TopicDescription t) {
    return t.partitions().stream().map(p -> new TopicPartition(t.name(), p.partition())).toList();
  }

  private static int replicas(TopicDescription t) {
    return t.partitions().isEmpty() ? 0 : t.partitions().getFirst().replicas().size();
  }

  private static List<Integer> ids(List<Node> nodos) {
    return nodos.stream().map(Node::id).toList();
  }

  /** Espera el resultado de Kafka con tope de tiempo y traduce sus errores. */
  private <T> T esperar(KafkaFuture<T> futuro, String operacion) {
    try {
      return futuro.get(props.timeout().toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new KafkaNoDisponibleException("Se interrumpió la operación: " + operacion);
    } catch (java.util.concurrent.TimeoutException ex) {
      throw new KafkaNoDisponibleException(
          "Kafka no respondió en " + props.timeout().toSeconds() + " s al " + operacion);
    } catch (ExecutionException ex) {
      throw traducir(ex.getCause(), operacion);
    }
  }

  private RuntimeException traducir(Throwable causa, String operacion) {
    String detalle = causa.getMessage();
    if (causa instanceof TopicExistsException) {
      return new BusinessException(HttpStatus.CONFLICT, "No se pudo " + operacion + ": el tópico ya existe");
    }
    if (causa instanceof UnknownTopicOrPartitionException) {
      return new ResourceNotFoundException("No se pudo " + operacion + ": el tópico no existe");
    }
    if (causa instanceof GroupIdNotFoundException) {
      return new ResourceNotFoundException("No se pudo " + operacion + ": el grupo no existe");
    }
    if (causa instanceof GroupNotEmptyException || causa instanceof UnknownMemberIdException
        || causa instanceof RebalanceInProgressException) {
      return new BusinessException(HttpStatus.CONFLICT,
          "No se pudo " + operacion + ": el grupo tiene consumidores activos; deténgalos primero");
    }
    if (causa instanceof InvalidReplicationFactorException || causa instanceof InvalidPartitionsException
        || causa instanceof InvalidConfigurationException || causa instanceof InvalidTopicException
        || causa instanceof InvalidRequestException || causa instanceof PolicyViolationException) {
      return new ValidacionException("Kafka rechazó la operación: " + operacion, List.of(detalle));
    }
    if (causa instanceof org.apache.kafka.common.errors.TimeoutException || causa instanceof DisconnectException) {
      return new KafkaNoDisponibleException("Kafka no está disponible: no se pudo " + operacion);
    }
    log.error("Error de Kafka al {}", operacion, causa);
    return new BusinessException(HttpStatus.BAD_GATEWAY, "Kafka rechazó la operación (" + operacion + "): " + detalle);
  }
}
