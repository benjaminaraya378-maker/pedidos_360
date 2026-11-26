package cl.duoc.pedidos360.kafkaadmin.service;

import cl.duoc.pedidos360.common.BusinessException;
import cl.duoc.pedidos360.common.KafkaNoDisponibleException;
import cl.duoc.pedidos360.common.ResourceNotFoundException;
import cl.duoc.pedidos360.common.ValidacionException;
import cl.duoc.pedidos360.kafkaadmin.config.KafkaAdminProperties;
import cl.duoc.pedidos360.kafkaadmin.dto.Respuestas.ContenidoDlt;
import cl.duoc.pedidos360.kafkaadmin.dto.Respuestas.PendientesParticion;
import cl.duoc.pedidos360.kafkaadmin.dto.Respuestas.Reinyeccion;
import cl.duoc.pedidos360.kafkaadmin.dto.Respuestas.ResultadoReproceso;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

/**
 * Lectura y reproceso de los DLT (dead letter topics) de Pedidos360: {@code pedidos360.pedidos.dlt} y
 * {@code pedidos360.inventario.dlt}.
 *
 * <p>Un evento llega al DLT cuando un consumidor agotó sus reintentos o el evento no se podía procesar. Una vez
 * corregida la causa (un servicio caído, un dato maestro que faltaba), este servicio lo devuelve a su tópico de origen
 * para que los consumidores lo procesen de nuevo. Para no reinyectar dos veces el mismo mensaje usa un consumer group
 * propio ({@link ReglasDlt#GRUPO_REPROCESO}): lee desde su último offset confirmado, publica cada mensaje, espera la
 * confirmación de Kafka y recién entonces confirma (commitSync) el offset siguiente al último reinyectado.
 *
 * <p>Garantía: al menos una vez. Un mensaje solo se repite si Kafka confirmó su envío pero no el commit posterior, y en
 * ese caso la respuesta lo dice (503). El reproceso se hace de a uno por instancia del servicio: el grupo no tiene
 * miembros (las particiones se asignan a mano), así que dos reprocesos simultáneos leerían lo mismo.
 */
@Service
public class DltService {

  private static final Logger log = LoggerFactory.getLogger(DltService.class);

  /** Espera de cada poll: corta para no retener la petición HTTP cuando ya se leyó todo lo pendiente. */
  private static final Duration ESPERA_POLL = Duration.ofMillis(200);

  /** Margen sobre {@code delivery.timeout.ms}: el productor siempre responde antes de que se acabe la espera. */
  private static final Duration MARGEN_ENVIO = Duration.ofSeconds(2);

  private static final Duration ESPERA_CIERRE = Duration.ofSeconds(2);

  private final KafkaAdminService admin;
  private final ClientesDlt clientes;
  private final KafkaAdminProperties props;
  private final ReentrantLock reprocesando = new ReentrantLock();

  public DltService(KafkaAdminService admin, ClientesDlt clientes, KafkaAdminProperties props) {
    this.admin = admin;
    this.clientes = clientes;
    this.props = props;
  }

  /**
   * Muestra los primeros {@code max} mensajes pendientes del DLT, es decir, lo que reinyectaría el próximo reproceso.
   * No confirma offsets ni publica nada: consultar un DLT no lo cambia.
   */
  public ContenidoDlt listar(String topicoDlt, int max) {
    List<TopicPartition> particiones = particionesDelDlt(topicoDlt);
    Consumer<byte[], byte[]> consumidor = null;
    try {
      consumidor = clientes.consumidor();
      Lectura lectura = leer(consumidor, particiones, max);
      return new ContenidoDlt(topicoDlt, ReglasDlt.GRUPO_REPROCESO, lectura.pendientes(Map.of()), lectura.resumen(),
          lectura.registros().stream().map(r -> ReglasDlt.aMensaje(r, topicoDlt)).toList());
    } catch (KafkaException ex) {
      throw KafkaAdminService.traducir(ex, "leer el DLT '" + topicoDlt + "'");
    } finally {
      cerrar(consumidor);
    }
  }

  /**
   * Reinyecta hasta {@code max} mensajes pendientes del DLT en su tópico de origen, con la misma clave, el mismo valor
   * en bytes y los headers del evento (sin los {@code kafka_dlt-*}). Antes de publicar nada revisa que cada tópico de
   * origen exista; si un envío falla, confirma lo ya reinyectado y se detiene.
   */
  public ResultadoReproceso reprocesar(String topicoDlt, int max) {
    List<TopicPartition> particiones = particionesDelDlt(topicoDlt);
    if (!reprocesando.tryLock()) {
      throw new BusinessException(HttpStatus.CONFLICT,
          "Ya hay un reproceso de DLT en curso; espere a que termine para no reinyectar dos veces lo mismo");
    }
    Consumer<byte[], byte[]> consumidor = null;
    Producer<byte[], byte[]> productor = null;
    try {
      consumidor = clientes.consumidor();
      Lectura lectura = leer(consumidor, particiones, max);
      if (lectura.registros().isEmpty()) {
        log.info("DLT '{}': no hay mensajes pendientes de reprocesar", topicoDlt);
        return new ResultadoReproceso(topicoDlt, ReglasDlt.GRUPO_REPROCESO, 0, lectura.pendientes(Map.of()), List.of());
      }
      verificarDestinos(topicoDlt, lectura.registros());

      productor = clientes.productor();
      Map<TopicPartition, OffsetAndMetadata> avance = new HashMap<>();
      List<Reinyeccion> detalle = new ArrayList<>();
      for (ConsumerRecord<byte[], byte[]> r : lectura.registros()) {
        String destino = ReglasDlt.topicoOrigen(r.headers(), topicoDlt);
        // Partición null: la elige la clave, igual que con el evento original. Fecha null: la del reenvío
        ProducerRecord<byte[], byte[]> nuevo = new ProducerRecord<>(destino, null, r.key(), r.value(),
            ReglasDlt.headersParaReinyectar(r.headers()));
        RecordMetadata escrito;
        try {
          escrito = enviar(productor, nuevo);
        } catch (RuntimeException ex) {
          confirmar(consumidor, avance, topicoDlt, detalle.size());   // lo ya reinyectado no se repite
          throw KafkaAdminService.traducir(ex, "reinyectar " + posicion(r) + " en '" + destino + "' (antes del fallo"
              + " se reinyectaron y confirmaron " + detalle.size() + " mensaje(s))");
        }
        avance.put(new TopicPartition(r.topic(), r.partition()), new OffsetAndMetadata(r.offset() + 1));
        String clave = r.key() == null ? null : new String(r.key(), StandardCharsets.UTF_8);
        detalle.add(new Reinyeccion(r.partition(), r.offset(), clave, escrito.topic(), escrito.partition(),
            escrito.offset()));
        log.info("Reinyectado {} → {}-{}@{} clave={}", posicion(r), escrito.topic(), escrito.partition(),
            escrito.offset(), clave);
      }
      confirmar(consumidor, avance, topicoDlt, detalle.size());
      long pendientes = lectura.pendientes(avance);
      log.info("DLT '{}': {} mensaje(s) reinyectado(s) y confirmado(s) en el grupo '{}'; quedan {} pendiente(s)",
          topicoDlt, detalle.size(), ReglasDlt.GRUPO_REPROCESO, pendientes);
      return new ResultadoReproceso(topicoDlt, ReglasDlt.GRUPO_REPROCESO, detalle.size(), pendientes, detalle);
    } catch (KafkaException ex) {
      throw KafkaAdminService.traducir(ex, "reprocesar el DLT '" + topicoDlt + "'");
    } finally {
      cerrar(productor);
      cerrar(consumidor);
      reprocesando.unlock();
    }
  }

  // ---------- Apoyo ----------

  /** Valida el nombre (400) y que el DLT exista (404); devuelve sus particiones. */
  private List<TopicPartition> particionesDelDlt(String topicoDlt) {
    List<String> errores = ReglasDlt.validarNombre(topicoDlt);
    if (!errores.isEmpty()) {
      throw new ValidacionException("Tópico DLT inválido", errores);
    }
    return admin.particionesDe(topicoDlt).stream().map(p -> new TopicPartition(topicoDlt, p)).toList();
  }

  /**
   * Lee, sin confirmar, hasta {@code max} mensajes pendientes del DLT. Cada partición empieza donde el grupo de
   * reproceso confirmó (o en el primer mensaje retenido) y termina en el offset final que tenía al empezar: un mensaje
   * que vuelve a fallar mientras se reprocesa queda para la próxima vez. Lo leído en cada partición es un tramo
   * continuo desde su posición, así que confirmar "último leído + 1" nunca se salta un mensaje.
   */
  private Lectura leer(Consumer<byte[], byte[]> consumidor, List<TopicPartition> particiones, int max) {
    Duration espera = props.timeout();
    consumidor.assign(particiones);
    Map<TopicPartition, Long> inicio = consumidor.beginningOffsets(particiones, espera);
    Map<TopicPartition, Long> fin = consumidor.endOffsets(particiones, espera);
    Map<TopicPartition, Long> desde = new HashMap<>();
    for (TopicPartition tp : particiones) {
      // Si la retención ya borró mensajes que el grupo no alcanzó a reprocesar, se parte del primero que queda
      desde.put(tp, Math.max(consumidor.position(tp, espera), inicio.getOrDefault(tp, 0L)));
    }
    Set<TopicPartition> abiertas = new HashSet<>();
    particiones.stream().filter(tp -> desde.get(tp) < fin.getOrDefault(tp, 0L)).forEach(abiertas::add);

    List<ConsumerRecord<byte[], byte[]>> registros = new ArrayList<>();
    long limite = System.nanoTime() + espera.toNanos();
    while (!abiertas.isEmpty() && registros.size() < max && System.nanoTime() < limite) {
      for (ConsumerRecord<byte[], byte[]> r : consumidor.poll(ESPERA_POLL)) {
        TopicPartition tp = new TopicPartition(r.topic(), r.partition());
        if (registros.size() < max && abiertas.contains(tp) && r.offset() < fin.get(tp)) {
          registros.add(r);
        }
      }
      abiertas.removeIf(tp -> consumidor.position(tp, espera) >= fin.get(tp));
    }
    if (registros.isEmpty() && !abiertas.isEmpty()) {
      throw new KafkaNoDisponibleException("Kafka no entregó en " + espera.toSeconds() + " s los mensajes pendientes de '"
          + particiones.getFirst().topic() + "'");
    }
    registros.sort(Comparator.comparingInt(ConsumerRecord<byte[], byte[]>::partition)
        .thenComparingLong(ConsumerRecord::offset));
    return new Lectura(registros, desde, fin);
  }

  /**
   * Antes de publicar nada: cada tópico de origen debe existir (Kafka de Pedidos360 no crea tópicos solo) y no puede
   * ser el mismo DLT, que se llenaría con sus propias copias.
   */
  private void verificarDestinos(String topicoDlt, List<ConsumerRecord<byte[], byte[]>> registros) {
    Set<String> destinos = new LinkedHashSet<>();
    registros.forEach(r -> destinos.add(ReglasDlt.topicoOrigen(r.headers(), topicoDlt)));
    for (String destino : destinos) {
      if (destino.equals(topicoDlt)) {
        throw new BusinessException(HttpStatus.CONFLICT, "Hay mensajes de '" + topicoDlt + "' cuyo tópico de origen es"
            + " el mismo DLT: no se reinyecta nada");
      }
      try {
        admin.particionesDe(destino);
      } catch (ResourceNotFoundException ex) {
        throw new BusinessException(HttpStatus.CONFLICT, "El tópico de origen '" + destino + "' de mensajes de '"
            + topicoDlt + "' no existe: créelo antes de reprocesar (no se reinyectó nada)");
      }
    }
  }

  /** Publica y espera la confirmación de Kafka (acks=all). */
  private RecordMetadata enviar(Producer<byte[], byte[]> productor, ProducerRecord<byte[], byte[]> registro) {
    long esperaMs = clientes.esperaEntrega().plus(MARGEN_ENVIO).toMillis();
    try {
      return productor.send(registro).get(esperaMs, TimeUnit.MILLISECONDS);
    } catch (ExecutionException ex) {
      throw ex.getCause() instanceof RuntimeException re ? re : new KafkaException(ex.getCause());
    } catch (java.util.concurrent.TimeoutException ex) {
      throw new TimeoutException("Kafka no confirmó el envío en " + esperaMs + " ms");
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new InterruptException(ex);
    }
  }

  /** Confirma en el grupo de reproceso el offset siguiente al último mensaje reinyectado de cada partición. */
  private void confirmar(Consumer<byte[], byte[]> consumidor, Map<TopicPartition, OffsetAndMetadata> avance,
                         String topicoDlt, int reinyectados) {
    if (avance.isEmpty()) {
      return;
    }
    try {
      consumidor.commitSync(avance, props.timeout());
    } catch (KafkaException ex) {
      log.error("DLT '{}': {} mensaje(s) reinyectado(s) sin poder confirmar {} en el grupo '{}'", topicoDlt,
          reinyectados, avance, ReglasDlt.GRUPO_REPROCESO, ex);
      throw new KafkaNoDisponibleException("Se reinyectaron " + reinyectados + " mensaje(s) de '" + topicoDlt
          + "', pero Kafka no confirmó el avance del grupo '" + ReglasDlt.GRUPO_REPROCESO + "' (" + ex.getMessage()
          + "): un nuevo reproceso los repetiría. Revise el grupo en /api/kafka/consumer-groups/"
          + ReglasDlt.GRUPO_REPROCESO);
    }
  }

  private static String posicion(ConsumerRecord<?, ?> r) {
    return r.topic() + "-" + r.partition() + "@" + r.offset();
  }

  private static void cerrar(AutoCloseable cliente) {
    if (cliente == null) {
      return;
    }
    try {
      if (cliente instanceof Consumer<?, ?> c) {
        c.close(ESPERA_CIERRE);
      } else if (cliente instanceof Producer<?, ?> p) {
        p.close(ESPERA_CIERRE);
      } else {
        cliente.close();
      }
    } catch (Exception ex) {
      log.warn("No se pudo cerrar el cliente Kafka del DLT: {}", ex.getMessage());
    }
  }

  /**
   * Lo leído de un DLT.
   *
   * @param registros mensajes pendientes leídos (como máximo {@code max}), por partición y offset
   * @param desde     posición de cada partición al empezar (próximo mensaje a reprocesar)
   * @param fin       offset final de cada partición al empezar
   */
  private record Lectura(List<ConsumerRecord<byte[], byte[]>> registros, Map<TopicPartition, Long> desde,
                         Map<TopicPartition, Long> fin) {

    /** Pendientes después de avanzar las particiones de {@code avance} (vacío: los pendientes al leer). */
    long pendientes(Map<TopicPartition, OffsetAndMetadata> avance) {
      return desde.keySet().stream()
          .mapToLong(tp -> Math.max(0, fin.getOrDefault(tp, 0L)
              - (avance.containsKey(tp) ? avance.get(tp).offset() : desde.get(tp))))
          .sum();
    }

    List<PendientesParticion> resumen() {
      return desde.keySet().stream()
          .sorted(Comparator.comparingInt(TopicPartition::partition))
          .map(tp -> {
            long siguiente = desde.get(tp);
            long ultimo = fin.getOrDefault(tp, 0L);
            return new PendientesParticion(tp.partition(), siguiente, ultimo, Math.max(0, ultimo - siguiente));
          })
          .toList();
    }
  }
}
