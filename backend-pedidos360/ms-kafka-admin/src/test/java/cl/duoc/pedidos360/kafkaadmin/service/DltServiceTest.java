package cl.duoc.pedidos360.kafkaadmin.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import cl.duoc.pedidos360.common.BusinessException;
import cl.duoc.pedidos360.common.KafkaNoDisponibleException;
import cl.duoc.pedidos360.common.ResourceNotFoundException;
import cl.duoc.pedidos360.common.ValidacionException;
import cl.duoc.pedidos360.kafkaadmin.config.KafkaAdminProperties;
import cl.duoc.pedidos360.kafkaadmin.dto.Respuestas.ContenidoDlt;
import cl.duoc.pedidos360.kafkaadmin.dto.Respuestas.MensajeDlt;
import cl.duoc.pedidos360.kafkaadmin.dto.Respuestas.PendientesParticion;
import cl.duoc.pedidos360.kafkaadmin.dto.Respuestas.Reinyeccion;
import cl.duoc.pedidos360.kafkaadmin.dto.Respuestas.ResultadoReproceso;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.support.KafkaHeaders;

/**
 * Reproceso de DLT con un Kafka simulado ({@link MockConsumer} y {@link MockProducer} de kafka-clients): qué se
 * reinyecta, con qué clave, valor y headers, qué offset se confirma y que un mensaje reprocesado no se reinyecte dos
 * veces. El {@link KafkaAdminService} es un mock que dice qué tópicos existen.
 */
class DltServiceTest {

  private static final String DLT = "pedidos360.pedidos.dlt";
  private static final String ORIGEN = "pedidos360.pedidos";
  private static final TopicPartition DLT_0 = new TopicPartition(DLT, 0);
  private static final TopicPartition DLT_1 = new TopicPartition(DLT, 1);

  private final KafkaAdminService admin = mock(KafkaAdminService.class);
  private final KafkaSimulado kafka = new KafkaSimulado();
  private final DltService servicio =
      new DltService(admin, kafka, new KafkaAdminProperties(Duration.ofSeconds(2), List.of()));

  @BeforeEach
  void topicosExistentes() {
    when(admin.particionesDe(DLT)).thenReturn(List.of(0, 1));
    when(admin.particionesDe(ORIGEN)).thenReturn(List.of(0, 1, 2));
  }

  @Test
  void reinyectaEnElOrigenConLaMismaClaveValorYHeadersYConfirmaElOffset() {
    kafka.agregar(0, "P-1", "{\"id\":1}", headersDlt(ORIGEN));
    kafka.agregar(0, "P-2", "{\"id\":2}", headersDlt(null));   // sin header de origen: se deduce del nombre
    kafka.agregar(1, "P-3", "{\"id\":3}", headersDlt(ORIGEN));

    ResultadoReproceso r = servicio.reprocesar(DLT, 10);

    assertThat(r.reinyectados()).isEqualTo(3);
    assertThat(r.pendientes()).isZero();
    assertThat(r.grupoReproceso()).isEqualTo("ms-kafka-admin-reproceso");
    assertThat(r.detalle()).extracting(Reinyeccion::particionDlt, Reinyeccion::offsetDlt, Reinyeccion::clave,
        Reinyeccion::topico).containsExactly(tuple(0, 0L, "P-1", ORIGEN), tuple(0, 1L, "P-2", ORIGEN),
        tuple(1, 0L, "P-3", ORIGEN));

    assertThat(kafka.enviados).hasSize(3).allSatisfy(e -> {
      assertThat(e.topic()).isEqualTo(ORIGEN);
      assertThat(e.partition()).isNull();   // la elige la clave, como con el evento original
      assertThat(e.headers().lastHeader("__TypeId__").value()).isEqualTo(bytes("pedidoEvento"));
      assertThat(e.headers()).noneMatch(h -> h.key().startsWith("kafka_dlt-"));
    });
    assertThat(kafka.enviados.getFirst().key()).isEqualTo(bytes("P-1"));
    assertThat(kafka.enviados.getFirst().value()).isEqualTo(bytes("{\"id\":1}"));

    assertThat(kafka.confirmados).containsOnly(
        entry(DLT_0, new OffsetAndMetadata(2)),
        entry(DLT_1, new OffsetAndMetadata(1)));
  }

  @Test
  void unMensajeYaReprocesadoNoSeReinyectaDosVeces() {
    kafka.agregar(0, "P-1", "{\"id\":1}", headersDlt(ORIGEN));
    kafka.agregar(0, "P-2", "{\"id\":2}", headersDlt(ORIGEN));
    kafka.agregar(0, "P-3", "{\"id\":3}", headersDlt(ORIGEN));

    ResultadoReproceso primero = servicio.reprocesar(DLT, 2);
    ResultadoReproceso segundo = servicio.reprocesar(DLT, 10);
    ResultadoReproceso tercero = servicio.reprocesar(DLT, 10);

    assertThat(primero.reinyectados()).isEqualTo(2);
    assertThat(primero.pendientes()).isEqualTo(1);
    assertThat(segundo.reinyectados()).isEqualTo(1);
    assertThat(segundo.detalle()).extracting(Reinyeccion::offsetDlt).containsExactly(2L);
    assertThat(tercero.reinyectados()).isZero();
    assertThat(tercero.pendientes()).isZero();
    assertThat(kafka.enviados).extracting(e -> texto(e.key())).containsExactly("P-1", "P-2", "P-3");
    assertThat(kafka.confirmados).containsOnly(entry(DLT_0, new OffsetAndMetadata(3)));
  }

  @Test
  void siUnEnvioFallaConfirmaSoloLoYaReinyectadoYResponde503() {
    kafka.agregar(0, "P-1", "{\"id\":1}", headersDlt(ORIGEN));
    kafka.agregar(0, "P-2", "{\"id\":2}", headersDlt(ORIGEN));
    kafka.agregar(0, "P-3", "{\"id\":3}", headersDlt(ORIGEN));
    kafka.fallarEnvio = 2;

    assertThatThrownBy(() -> servicio.reprocesar(DLT, 10))
        .isInstanceOf(KafkaNoDisponibleException.class)
        .hasMessageContaining(DLT + "-0@1")
        .hasMessageContaining("se reinyectaron y confirmaron 1 mensaje(s)");
    assertThat(kafka.enviados).extracting(e -> texto(e.key())).containsExactly("P-1");
    assertThat(kafka.confirmados).containsOnly(entry(DLT_0, new OffsetAndMetadata(1)));

    // Con Kafka de vuelta, el reproceso sigue desde el mensaje que falló
    kafka.fallarEnvio = -1;
    ResultadoReproceso r = servicio.reprocesar(DLT, 10);
    assertThat(r.detalle()).extracting(Reinyeccion::offsetDlt).containsExactly(1L, 2L);
    assertThat(kafka.enviados).extracting(e -> texto(e.key())).containsExactly("P-1", "P-2", "P-3");
  }

  @Test
  void listarMuestraLosPendientesSinConfirmarNiPublicar() {
    kafka.agregar(0, "P-1", "{\"id\":1}", headersDlt(ORIGEN));
    kafka.agregar(0, "P-2", "{\"id\":2}", headersDlt(ORIGEN));
    kafka.agregar(0, "P-3", "{\"id\":3}", headersDlt(ORIGEN));

    ContenidoDlt contenido = servicio.listar(DLT, 2);

    assertThat(contenido.pendientes()).isEqualTo(3);
    assertThat(contenido.particiones()).extracting(PendientesParticion::particion, PendientesParticion::siguiente,
        PendientesParticion::offsetFin, PendientesParticion::pendientes)
        .containsExactly(tuple(0, 0L, 3L, 3L), tuple(1, 0L, 0L, 0L));
    assertThat(contenido.mensajes()).extracting(MensajeDlt::offset).containsExactly(0L, 1L);
    MensajeDlt primero = contenido.mensajes().getFirst();
    assertThat(primero.clave()).isEqualTo("P-1");
    assertThat(primero.topicoOrigen()).isEqualTo(ORIGEN);
    assertThat(primero.particionOrigen()).isEqualTo(2);
    assertThat(primero.offsetOrigen()).isEqualTo(40L);
    assertThat(primero.grupoOrigen()).isEqualTo("pedidos360-analitica");
    assertThat(primero.excepcion()).isEqualTo("cl.duoc.pedidos360.kafka.EventoInvalidoException");
    assertThat(primero.valor()).isEqualTo("{\"id\":1}");

    assertThat(kafka.confirmados).isEmpty();
    assertThat(kafka.productores).isZero();
    // Listar no consumió nada: el reproceso encuentra los tres
    assertThat(servicio.reprocesar(DLT, 10).reinyectados()).isEqualTo(3);
  }

  @Test
  void soloAceptaDltYNoTocaKafkaSiElNombreEsInvalido() {
    assertThatThrownBy(() -> servicio.reprocesar(ORIGEN, 10))
        .isInstanceOfSatisfying(ValidacionException.class, ex ->
            assertThat(ex.getDetalles()).singleElement().asString().contains("debe terminar en '.dlt'"));
    assertThatThrownBy(() -> servicio.listar(ORIGEN, 10)).isInstanceOf(ValidacionException.class);

    verify(admin, never()).particionesDe(anyString());
    assertThat(kafka.consumidores).isZero();
  }

  @Test
  void dltInexistenteResponde404() {
    when(admin.particionesDe("pedidos360.pagos.dlt"))
        .thenThrow(new ResourceNotFoundException("No se pudo describir el tópico: el tópico no existe"));

    assertThatThrownBy(() -> servicio.reprocesar("pedidos360.pagos.dlt", 10))
        .isInstanceOf(ResourceNotFoundException.class);
    assertThat(kafka.consumidores).isZero();
  }

  @Test
  void siElTopicoDeOrigenNoExisteNoReinyectaNada() {
    kafka.agregar(0, "P-1", "{\"id\":1}", headersDlt(ORIGEN));
    kafka.agregar(0, "X-1", "{\"id\":9}", headersDlt("pedidos360.pagos"));
    when(admin.particionesDe("pedidos360.pagos"))
        .thenThrow(new ResourceNotFoundException("No se pudo describir el tópico: el tópico no existe"));

    assertThatThrownBy(() -> servicio.reprocesar(DLT, 10))
        .isInstanceOfSatisfying(BusinessException.class, ex -> {
          assertThat(ex.getStatus()).isEqualTo(HttpStatus.CONFLICT);
          assertThat(ex.getMessage()).contains("'pedidos360.pagos'").contains("no se reinyectó nada");
        });
    assertThat(kafka.productores).isZero();
    assertThat(kafka.enviados).isEmpty();
    assertThat(kafka.confirmados).isEmpty();
  }

  @Test
  void kafkaSinRespuestaResponde503() {
    kafka.agregar(0, "P-1", "{\"id\":1}", headersDlt(ORIGEN));
    kafka.errorOffsets = new TimeoutException("Timeout of 2000ms expired before the offsets could be retrieved");

    assertThatThrownBy(() -> servicio.listar(DLT, 10))
        .isInstanceOf(KafkaNoDisponibleException.class)
        .hasMessageContaining("leer el DLT '" + DLT + "'");
    assertThat(kafka.confirmados).isEmpty();
  }

  // ---------- Apoyo ----------

  /** Headers como los deja el DeadLetterPublishingRecoverer de ms-analitica al mandar un evento al DLT. */
  private static Headers headersDlt(String topicoOrigen) {
    RecordHeaders h = new RecordHeaders();
    h.add("__TypeId__", bytes("pedidoEvento"));
    if (topicoOrigen != null) {
      h.add(KafkaHeaders.DLT_ORIGINAL_TOPIC, bytes(topicoOrigen));
    }
    h.add(KafkaHeaders.DLT_ORIGINAL_PARTITION, ByteBuffer.allocate(Integer.BYTES).putInt(2).array());
    h.add(KafkaHeaders.DLT_ORIGINAL_OFFSET, ByteBuffer.allocate(Long.BYTES).putLong(40L).array());
    h.add(KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP, bytes("pedidos360-analitica"));
    h.add(KafkaHeaders.DLT_EXCEPTION_FQCN, bytes("org.springframework.kafka.listener.ListenerExecutionFailedException"));
    h.add(KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN, bytes("cl.duoc.pedidos360.kafka.EventoInvalidoException"));
    h.add(KafkaHeaders.DLT_EXCEPTION_MESSAGE, bytes("tipo de evento desconocido"));
    return h;
  }

  private static byte[] bytes(String texto) {
    return texto.getBytes(StandardCharsets.UTF_8);
  }

  private static String texto(byte[] bytes) {
    return new String(bytes, StandardCharsets.UTF_8);
  }

  /**
   * Un DLT de 2 particiones en memoria. Lo que persiste entre operaciones (como en un clúster real) son los mensajes
   * y los offsets confirmados del grupo de reproceso; cada operación recibe un consumidor y un productor nuevos.
   */
  private static class KafkaSimulado implements ClientesDlt {

    final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> mensajes = new HashMap<>();
    final Map<TopicPartition, OffsetAndMetadata> confirmados = new HashMap<>();
    final List<ProducerRecord<byte[], byte[]>> enviados = new ArrayList<>();
    int consumidores;
    int productores;
    int envios;
    /** Número de envío (contando desde 1) que Kafka no confirma; -1 si ninguno. */
    int fallarEnvio = -1;
    KafkaException errorOffsets;

    void agregar(int particion, String clave, String valor, Headers headers) {
      List<ConsumerRecord<byte[], byte[]>> lista =
          mensajes.computeIfAbsent(new TopicPartition(DLT, particion), tp -> new ArrayList<>());
      lista.add(new ConsumerRecord<>(DLT, particion, lista.size(), 1_764_000_000_000L, TimestampType.CREATE_TIME,
          clave.length(), valor.length(), bytes(clave), bytes(valor), headers, Optional.empty()));
    }

    @Override
    public Consumer<byte[], byte[]> consumidor() {
      consumidores++;
      MockConsumer<byte[], byte[]> c = new MockConsumer<>(OffsetResetStrategy.EARLIEST) {
        @Override
        public synchronized void assign(Collection<TopicPartition> particiones) {
          super.assign(particiones);
          // MockConsumer olvida lo confirmado al asignar; un broker real lo conserva en __consumer_offsets
          super.commitSync(new HashMap<>(confirmados));
        }

        @Override
        public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
          super.commitSync(offsets);
          confirmados.putAll(offsets);
        }
      };
      Map<TopicPartition, Long> inicio = new HashMap<>();
      Map<TopicPartition, Long> fin = new HashMap<>();
      for (TopicPartition tp : List.of(DLT_0, DLT_1)) {
        inicio.put(tp, 0L);
        fin.put(tp, (long) mensajes.getOrDefault(tp, List.of()).size());
      }
      c.updateBeginningOffsets(inicio);
      c.updateEndOffsets(fin);
      if (errorOffsets != null) {
        c.setOffsetsException(errorOffsets);
      }
      // El poll entrega todo lo retenido desde la posición del grupo (MockConsumer descarta lo anterior)
      c.schedulePollTask(() -> mensajes.values().forEach(lista -> lista.forEach(c::addRecord)));
      return c;
    }

    @Override
    public Producer<byte[], byte[]> productor() {
      productores++;
      return new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer()) {
        @Override
        public synchronized Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> registro, Callback callback) {
          if (++envios == fallarEnvio) {
            return CompletableFuture.failedFuture(new TimeoutException("Expiring 1 record(s): 12000 ms has passed"));
          }
          enviados.add(registro);
          return super.send(registro, callback);
        }
      };
    }

    @Override
    public Duration esperaEntrega() {
      return Duration.ofSeconds(1);
    }
  }
}
