package cl.duoc.pedidos360.pedidos.producer;

import static cl.duoc.pedidos360.pedidos.producer.PedidoKafkaProducerTest.TOPICO;
import static cl.duoc.pedidos360.pedidos.producer.PedidoKafkaProducerTest.confirmado;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import cl.duoc.pedidos360.kafka.evento.PedidoEvento;
import cl.duoc.pedidos360.kafka.evento.TipoEventoPedido;
import cl.duoc.pedidos360.pedidos.model.EventoKafkaPendiente;
import cl.duoc.pedidos360.pedidos.repository.EventoKafkaPendienteRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.domain.Limit;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Outbox de PEDIDO_CREADO: la fila se guarda en la transacción del pedido y nada sale a Kafka antes del commit; después
 * del commit se publica de inmediato y, si Kafka confirma, se borra; si no, queda pendiente con un intento más y el relé
 * la publica en una vuelta posterior. La transacción se simula con {@link TransactionSynchronizationManager}, igual que
 * la maneja Spring: el intento inmediato corre solo si se llega al commit.
 */
class OutboxKafkaTest {

  private static final Map<String, Object> MAPEO_DE_TIPOS =
      Map.of(JsonSerializer.TYPE_MAPPINGS, "pedidoEvento:" + PedidoEvento.class.getName());

  private final EventoKafkaPendienteRepository repository = mock(EventoKafkaPendienteRepository.class);
  @SuppressWarnings("unchecked")
  private final KafkaTemplate<String, PedidoEvento> kafka = mock(KafkaTemplate.class);
  /** Mismo ObjectMapper que arma Spring Boot. */
  private final ObjectMapper objectMapper = Jackson2ObjectMapperBuilder.json()
      .featuresToDisable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS).build();
  /** Runnable::run: la respuesta de Kafka al intento inmediato se procesa en el hilo de la prueba. */
  private final OutboxKafka outbox = new OutboxKafka(repository, kafka, objectMapper, 200, 20_000, 50, Runnable::run);

  private final PedidoEvento evento = PedidoEvento.nuevo(TipoEventoPedido.PEDIDO_CREADO, 7L, "oid-cliente", "CREADO",
      new BigDecimal("129990.00"), null, List.of(new PedidoEvento.Item(1L, "Monitor", 1, new BigDecimal("129990.00"),
          new BigDecimal("129990.00"))));

  @BeforeEach
  void abrirTransaccion() {
    TransactionSynchronizationManager.initSynchronization();
  }

  @AfterEach
  void cerrarTransaccion() {
    TransactionSynchronizationManager.clearSynchronization();
  }

  @Test
  void guardaLaFilaEnLaTransaccionDelPedidoYNoTocaKafkaAntesDelCommit() throws Exception {
    outbox.registrar(TOPICO, evento);

    EventoKafkaPendiente fila = filaGuardada();
    assertThat(fila.getEventoId()).isEqualTo(evento.eventoId());
    assertThat(fila.getTopico()).isEqualTo(TOPICO);
    assertThat(fila.getClave()).isEqualTo("7");
    assertThat(fila.getIntentos()).isZero();
    assertThat(fila.isNew()).isTrue();
    assertThat(objectMapper.readValue(fila.getPayload(), PedidoEvento.class)).isEqualTo(evento);
    verifyNoInteractions(kafka);
  }

  @Test
  void siLaTransaccionDelPedidoSeDeshaceNoSePublicaNada() {
    outbox.registrar(TOPICO, evento);

    rollback();

    verifyNoInteractions(kafka);
  }

  @Test
  void despuesDelCommitLoPublicaDeInmediatoYAlConfirmarseBorraLaFila() {
    when(kafka.send(anyString(), anyString(), any(PedidoEvento.class)))
        .thenAnswer(inv -> confirmado(inv.getArgument(1), inv.getArgument(2)));

    outbox.registrar(TOPICO, evento);
    commit();

    verify(kafka).send(TOPICO, "7", evento);
    verify(repository).borrar(evento.eventoId());
    verify(repository, never()).registrarFallo(anyString(), anyString());
  }

  @Test
  void siKafkaRechazaElIntentoInmediatoLaFilaQuedaPendienteConUnIntentoMas() {
    when(kafka.send(anyString(), anyString(), any(PedidoEvento.class))).thenReturn(CompletableFuture.failedFuture(
        new NotEnoughReplicasException("Messages are rejected since there are fewer in-sync replicas than required.")));

    outbox.registrar(TOPICO, evento);
    commit();

    verify(repository).registrarFallo(eq(evento.eventoId()), contains("NotEnoughReplicasException"));
    verify(repository, never()).borrar(anyString());
  }

  @Test
  void siKafkaEstaCaidoAlCrearElPedidoLaPeticionNoFallaYLaFilaQuedaPendiente() {
    when(kafka.send(anyString(), anyString(), any(PedidoEvento.class))).thenThrow(kafkaCaido());

    outbox.registrar(TOPICO, evento);

    assertThatCode(this::commit).doesNotThrowAnyException();
    verify(repository).registrarFallo(eq(evento.eventoId()), contains("not present in metadata"));
    verify(repository, never()).borrar(anyString());
  }

  @Test
  void elRelePublicaLasFilasPendientesEsperaLaConfirmacionYLasBorra() throws Exception {
    when(repository.findByCreadoEnBeforeOrderByIntentosAscCreadoEnAsc(any(), any()))
        .thenReturn(List.of(pendiente(evento)));
    when(kafka.send(anyString(), anyString(), any(PedidoEvento.class)))
        .thenAnswer(inv -> confirmado(inv.getArgument(1), inv.getArgument(2)));

    Instant antes = Instant.now();
    outbox.publicarPendientes();
    Instant despues = Instant.now();

    // Solo toma filas con más de 20 s (antiguedad-minima-ms), de a 50 (lote)
    ArgumentCaptor<Instant> limite = ArgumentCaptor.forClass(Instant.class);
    ArgumentCaptor<Limit> lote = ArgumentCaptor.forClass(Limit.class);
    verify(repository).findByCreadoEnBeforeOrderByIntentosAscCreadoEnAsc(limite.capture(), lote.capture());
    assertThat(limite.getValue()).isBetween(antes.minusMillis(20_000), despues.minusMillis(20_000));
    assertThat(lote.getValue().max()).isEqualTo(50);

    verify(kafka).send(TOPICO, "7", evento);
    verify(repository).borrar(evento.eventoId());
  }

  @Test
  void siKafkaNoConfirmaElReleSumaUnIntentoGuardaElErrorYCortaLaVuelta() throws Exception {
    PedidoEvento otro = PedidoEvento.nuevo(TipoEventoPedido.PEDIDO_CREADO, 8L, "oid-cliente", "CREADO",
        evento.total(), null, evento.items());
    when(repository.findByCreadoEnBeforeOrderByIntentosAscCreadoEnAsc(any(), any()))
        .thenReturn(List.of(pendiente(evento), pendiente(otro)));
    when(kafka.send(anyString(), anyString(), any(PedidoEvento.class))).thenReturn(CompletableFuture.failedFuture(
        new TimeoutException("Expiring 1 record(s) for pedidos360.pedidos-1: 12000 ms has passed since batch creation")));

    outbox.publicarPendientes();

    verify(repository).registrarFallo(eq(evento.eventoId()), contains("Expiring 1 record(s)"));
    verify(repository, never()).borrar(anyString());
    // Con Kafka caído el segundo también fallaría: queda para la próxima vuelta sin esperar su plazo
    verify(kafka, times(1)).send(anyString(), anyString(), any(PedidoEvento.class));
  }

  @Test
  void siKafkaNoRespondeDentroDelPlazoElReleDejaLaFilaPendiente() throws Exception {
    when(repository.findByCreadoEnBeforeOrderByIntentosAscCreadoEnAsc(any(), any()))
        .thenReturn(List.of(pendiente(evento)));
    when(kafka.send(anyString(), anyString(), any(PedidoEvento.class))).thenReturn(new CompletableFuture<>());

    outbox.publicarPendientes();

    verify(repository).registrarFallo(eq(evento.eventoId()), contains("sin respuesta del broker en 200 ms"));
    verify(repository, never()).borrar(anyString());
  }

  @Test
  void conKafkaCaidoAlCrearElPedidoElReleLoPublicaCuandoKafkaVuelve() {
    when(kafka.send(anyString(), anyString(), any(PedidoEvento.class)))
        .thenThrow(kafkaCaido())                                                  // intento inmediato: Kafka caído
        .thenAnswer(inv -> confirmado(inv.getArgument(1), inv.getArgument(2)));   // relé: Kafka volvió

    outbox.registrar(TOPICO, evento);
    commit();

    verify(repository).registrarFallo(eq(evento.eventoId()), anyString());
    verify(repository, never()).borrar(anyString());

    EventoKafkaPendiente pendiente = filaGuardada();
    when(repository.findByCreadoEnBeforeOrderByIntentosAscCreadoEnAsc(any(), any())).thenReturn(List.of(pendiente));
    outbox.publicarPendientes();

    // Mismo evento (mismo eventoId) en los dos intentos: si llegara dos veces, el consumidor descarta el duplicado
    verify(kafka, times(2)).send(TOPICO, "7", evento);
    verify(repository).borrar(evento.eventoId());
  }

  @Test
  void elRelePublicaElPedidoEventoTipadoMismoJsonYMismoHeaderTypeIdQueSinOutbox() throws Exception {
    when(repository.findByCreadoEnBeforeOrderByIntentosAscCreadoEnAsc(any(), any()))
        .thenReturn(List.of(pendiente(evento)));
    when(kafka.send(anyString(), anyString(), any(PedidoEvento.class)))
        .thenAnswer(inv -> confirmado(inv.getArgument(1), inv.getArgument(2)));

    outbox.publicarPendientes();

    ArgumentCaptor<PedidoEvento> enviado = ArgumentCaptor.forClass(PedidoEvento.class);
    verify(kafka).send(eq(TOPICO), eq("7"), enviado.capture());
    assertThat(enviado.getValue()).isNotSameAs(evento).isEqualTo(evento);   // reconstruido desde el JSON de la fila

    // El JsonSerializer del productor (mismo mapeo de tipos que application.yml) genera los mismos bytes y el mismo
    // header __TypeId__ que si el evento se hubiera publicado directo, sin pasar por el outbox
    try (JsonSerializer<Object> serializer = new JsonSerializer<>()) {
      serializer.configure(MAPEO_DE_TIPOS, false);
      Headers directo = new RecordHeaders();
      Headers desdeOutbox = new RecordHeaders();
      byte[] jsonDirecto = serializer.serialize(TOPICO, directo, evento);
      byte[] jsonDesdeOutbox = serializer.serialize(TOPICO, desdeOutbox, enviado.getValue());

      assertThat(jsonDesdeOutbox).isEqualTo(jsonDirecto);
      assertThat(new String(desdeOutbox.lastHeader("__TypeId__").value(), StandardCharsets.UTF_8))
          .isEqualTo("pedidoEvento");
    }
  }

  private EventoKafkaPendiente filaGuardada() {
    ArgumentCaptor<EventoKafkaPendiente> fila = ArgumentCaptor.forClass(EventoKafkaPendiente.class);
    verify(repository).save(fila.capture());
    return fila.getValue();
  }

  private EventoKafkaPendiente pendiente(PedidoEvento e) throws Exception {
    return new EventoKafkaPendiente(e.eventoId(), TOPICO, String.valueOf(e.pedidoId()),
        objectMapper.writeValueAsString(e), Instant.now().minusSeconds(60));
  }

  private static KafkaException kafkaCaido() {
    return new KafkaException("Send failed",
        new TimeoutException("Topic pedidos360.pedidos not present in metadata after 5000 ms."));
  }

  /** Lo que hace Spring al confirmar la transacción: el intento inmediato corre en afterCommit. */
  private void commit() {
    for (TransactionSynchronization s : TransactionSynchronizationManager.getSynchronizations()) {
      s.beforeCommit(false);
      s.beforeCompletion();
      s.afterCommit();
      s.afterCompletion(TransactionSynchronization.STATUS_COMMITTED);
    }
  }

  private void rollback() {
    for (TransactionSynchronization s : TransactionSynchronizationManager.getSynchronizations()) {
      s.beforeCompletion();
      s.afterCompletion(TransactionSynchronization.STATUS_ROLLED_BACK);
    }
  }
}
