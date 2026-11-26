package cl.duoc.pedidos360.pedidos.producer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import cl.duoc.pedidos360.kafka.KafkaTopicosProperties;
import cl.duoc.pedidos360.kafka.PublicacionKafkaException;
import cl.duoc.pedidos360.kafka.evento.PedidoEvento;
import cl.duoc.pedidos360.kafka.evento.TipoEventoPedido;
import cl.duoc.pedidos360.pedidos.event.PedidoCreadoEvent;
import cl.duoc.pedidos360.pedidos.event.PedidoEstadoCambiadoEvent;
import cl.duoc.pedidos360.pedidos.model.EstadoPedido;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * El productor de ms-pedidos deja PEDIDO_CREADO en el outbox antes del commit, en la transacción del pedido y sin
 * tocar Kafka (lo publica {@link OutboxKafka} después del commit), y publica PEDIDO_CONFIRMADO / PEDIDO_CANCELADO
 * antes del commit esperando la confirmación: si Kafka no confirma, la excepción deshace el cambio de estado y el
 * evento de inventario se reintenta.
 */
class PedidoKafkaProducerTest {

  static final String TOPICO = "pedidos360.pedidos";

  @SuppressWarnings("unchecked")
  private final KafkaTemplate<String, PedidoEvento> kafka = mock(KafkaTemplate.class);
  private final OutboxKafka outbox = mock(OutboxKafka.class);
  private final PedidoKafkaProducer producer = new PedidoKafkaProducer(kafka, propiedades(), outbox, 200);

  @Test
  void elCambioDeEstadoYElPedidoNuevoSeResuelvenAntesDelCommitDentroDeLaTransaccion() throws Exception {
    TransactionalEventListener estado = PedidoKafkaProducer.class
        .getMethod("onEstadoCambiado", PedidoEstadoCambiadoEvent.class).getAnnotation(TransactionalEventListener.class);
    TransactionalEventListener creado = PedidoKafkaProducer.class
        .getMethod("onPedidoCreado", PedidoCreadoEvent.class).getAnnotation(TransactionalEventListener.class);

    assertThat(estado.phase()).isEqualTo(TransactionPhase.BEFORE_COMMIT);
    // El pedido nuevo se guarda en el outbox en la misma transacción: si el pedido no se guarda, el evento tampoco
    assertThat(creado.phase()).isEqualTo(TransactionPhase.BEFORE_COMMIT);
  }

  @Test
  void elPedidoNuevoSeRegistraEnElOutboxComoPedidoCreadoConSusItems() {
    producer.onPedidoCreado(creado());

    ArgumentCaptor<PedidoEvento> evento = ArgumentCaptor.forClass(PedidoEvento.class);
    verify(outbox).registrar(eq(TOPICO), evento.capture());
    assertThat(evento.getValue().tipo()).isEqualTo(TipoEventoPedido.PEDIDO_CREADO);
    assertThat(evento.getValue().pedidoId()).isEqualTo(7L);
    assertThat(evento.getValue().estado()).isEqualTo("CREADO");
    assertThat(evento.getValue().eventoId()).hasSize(36);
    assertThat(evento.getValue().items()).singleElement()
        .satisfies(item -> assertThat(item.nombreProducto()).isEqualTo("Monitor"));
  }

  @Test
  void publicaElPedidoConfirmadoConElIdDelPedidoComoClaveYEsperaLaConfirmacion() {
    when(kafka.send(anyString(), anyString(), any(PedidoEvento.class)))
        .thenAnswer(inv -> confirmado(inv.getArgument(1), inv.getArgument(2)));

    producer.onEstadoCambiado(estado(EstadoPedido.EN_PREPARACION, null));

    verify(kafka).send(eq(TOPICO), eq("7"), argThat(e -> e.tipo() == TipoEventoPedido.PEDIDO_CONFIRMADO));
  }

  @Test
  void unCanceladoSePublicaComoPedidoCanceladoConSuMotivo() {
    when(kafka.send(anyString(), anyString(), any(PedidoEvento.class)))
        .thenAnswer(inv -> confirmado(inv.getArgument(1), inv.getArgument(2)));

    producer.onEstadoCambiado(estado(EstadoPedido.CANCELADO, "Stock insuficiente"));

    verify(kafka).send(eq(TOPICO), eq("7"), argThat(e -> e.tipo() == TipoEventoPedido.PEDIDO_CANCELADO
        && "Stock insuficiente".equals(e.motivo())));
  }

  @Test
  void siElBrokerRechazaElCambioDeEstadoLanzaPublicacionKafkaException() {
    when(kafka.send(anyString(), anyString(), any(PedidoEvento.class))).thenReturn(CompletableFuture.failedFuture(
        new NotEnoughReplicasException("Messages are rejected since there are fewer in-sync replicas than required.")));

    assertThatThrownBy(() -> producer.onEstadoCambiado(estado(EstadoPedido.EN_PREPARACION, null)))
        .isInstanceOf(PublicacionKafkaException.class)
        .hasMessageContaining("PEDIDO_CONFIRMADO del pedido 7")
        .hasCauseInstanceOf(NotEnoughReplicasException.class);
  }

  @Test
  void siKafkaNoRespondeAntesDelPlazoLanzaPublicacionKafkaException() {
    when(kafka.send(anyString(), anyString(), any(PedidoEvento.class))).thenReturn(new CompletableFuture<>());

    assertThatThrownBy(() -> producer.onEstadoCambiado(estado(EstadoPedido.EN_PREPARACION, null)))
        .isInstanceOf(PublicacionKafkaException.class)
        .hasMessageContaining("sin respuesta del broker en 200 ms");
  }

  @Test
  void elPedidoNuevoNoFallaSiKafkaNoRespondeElEventoQuedaEnElOutbox() {
    when(kafka.send(anyString(), anyString(), any(PedidoEvento.class))).thenThrow(
        new org.springframework.kafka.KafkaException("Send failed",
            new org.apache.kafka.common.errors.TimeoutException("Topic pedidos360.pedidos not present in metadata")));

    assertThatCode(() -> producer.onPedidoCreado(creado())).doesNotThrowAnyException();
    // Dentro de la transacción del pedido no se toca Kafka: el evento queda en el outbox
    verify(kafka, never()).send(anyString(), anyString(), any(PedidoEvento.class));
    verify(outbox).registrar(eq(TOPICO), argThat(e -> e.tipo() == TipoEventoPedido.PEDIDO_CREADO));
  }

  static CompletableFuture<SendResult<String, PedidoEvento>> confirmado(String clave, PedidoEvento evento) {
    RecordMetadata metadata = new RecordMetadata(new TopicPartition(TOPICO, 1), 5L, 0, 0L, 0, 0);
    return CompletableFuture.completedFuture(new SendResult<>(new ProducerRecord<>(TOPICO, clave, evento), metadata));
  }

  static KafkaTopicosProperties propiedades() {
    return new KafkaTopicosProperties(
        new KafkaTopicosProperties.Topicos(
            new KafkaTopicosProperties.Topico(TOPICO, 3, (short) 3, "delete", 604800000L, 2, null, null),
            new KafkaTopicosProperties.Topico("pedidos360.inventario", 3, (short) 3, "compact", null, 2, null, null)),
        new KafkaTopicosProperties.Dlt(".dlt", 1209600000L),
        new KafkaTopicosProperties.Grupos("pedidos360-estado-pedidos"),
        new KafkaTopicosProperties.Reintentos(3, 1000, 2.0, 10000));
  }

  private static List<PedidoCreadoEvent.Item> items() {
    return List.of(new PedidoCreadoEvent.Item(1L, "Monitor", 1, new BigDecimal("129990"), new BigDecimal("129990")));
  }

  private static PedidoEstadoCambiadoEvent estado(EstadoPedido estado, String motivo) {
    return new PedidoEstadoCambiadoEvent(7L, "oid-cliente", estado, new BigDecimal("129990"), motivo, items());
  }

  private static PedidoCreadoEvent creado() {
    return new PedidoCreadoEvent(7L, "oid-cliente", "cliente@pedidos360.test", "Cliente", Instant.now(),
        new BigDecimal("129990"), items());
  }
}
