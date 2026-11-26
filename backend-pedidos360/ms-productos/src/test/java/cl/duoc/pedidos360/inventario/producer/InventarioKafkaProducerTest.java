package cl.duoc.pedidos360.inventario.producer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import cl.duoc.pedidos360.inventario.event.InventarioEventos;
import cl.duoc.pedidos360.inventario.event.MovimientosInventarioEvent;
import cl.duoc.pedidos360.kafka.KafkaTopicosProperties;
import cl.duoc.pedidos360.kafka.PublicacionKafkaException;
import cl.duoc.pedidos360.kafka.evento.InventarioEvento;
import cl.duoc.pedidos360.productos.model.Producto;
import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

class InventarioKafkaProducerTest {

  static final String TOPICO = "pedidos360.inventario";

  @SuppressWarnings("unchecked")
  private final KafkaTemplate<String, InventarioEvento> kafka = mock(KafkaTemplate.class);
  private final InventarioKafkaProducer producer = new InventarioKafkaProducer(kafka, propiedades(), 200);

  @Test
  void publicaDentroDeLaTransaccionAntesDelCommit() throws Exception {
    TransactionalEventListener anotacion = InventarioKafkaProducer.class
        .getMethod("onMovimientos", MovimientosInventarioEvent.class)
        .getAnnotation(TransactionalEventListener.class);

    assertThat(anotacion.phase()).isEqualTo(TransactionPhase.BEFORE_COMMIT);
  }

  @Test
  void publicaCadaMovimientoConElIdDelProductoComoClaveYEsperaLaConfirmacion() {
    when(kafka.send(anyString(), anyString(), any(InventarioEvento.class)))
        .thenAnswer(inv -> confirmado(inv.getArgument(1), inv.getArgument(2)));

    producer.onMovimientos(new MovimientosInventarioEvent(List.of(descuento(1L), descuento(2L))));

    verify(kafka).send(eq(TOPICO), eq("1"), any(InventarioEvento.class));
    verify(kafka).send(eq(TOPICO), eq("2"), any(InventarioEvento.class));
  }

  @Test
  void siElBrokerRechazaElEnvioLanzaPublicacionKafkaExceptionYNoEnviaLosSiguientes() {
    when(kafka.send(anyString(), eq("1"), any(InventarioEvento.class))).thenReturn(CompletableFuture.failedFuture(
        new NotEnoughReplicasException("Messages are rejected since there are fewer in-sync replicas than required.")));

    assertThatThrownBy(() -> producer.onMovimientos(
        new MovimientosInventarioEvent(List.of(descuento(1L), descuento(2L)))))
        .isInstanceOf(PublicacionKafkaException.class)
        .hasMessageContaining("STOCK_DESCONTADO del producto 1")
        .hasMessageContaining("fewer in-sync replicas")
        .hasCauseInstanceOf(NotEnoughReplicasException.class);
    verify(kafka, never()).send(anyString(), eq("2"), any(InventarioEvento.class));
  }

  @Test
  void siKafkaNoRespondeAntesDelPlazoLanzaPublicacionKafkaException() {
    // Un futuro que nunca se completa: el broker no responde
    when(kafka.send(anyString(), anyString(), any(InventarioEvento.class))).thenReturn(new CompletableFuture<>());

    assertThatThrownBy(() -> producer.onMovimientos(new MovimientosInventarioEvent(List.of(descuento(1L)))))
        .isInstanceOf(PublicacionKafkaException.class)
        .hasMessageContaining("sin respuesta del broker en 200 ms");
  }

  @Test
  void siSendFallaAntesDeEnviarLanzaPublicacionKafkaException() {
    // Lo que hace KafkaTemplate cuando no consigue la metadata del tópico en max.block.ms
    when(kafka.send(anyString(), anyString(), any(InventarioEvento.class))).thenThrow(
        new org.springframework.kafka.KafkaException("Send failed",
            new org.apache.kafka.common.errors.TimeoutException(
                "Topic pedidos360.inventario not present in metadata after 5000 ms.")));

    assertThatThrownBy(() -> producer.onMovimientos(new MovimientosInventarioEvent(List.of(descuento(1L)))))
        .isInstanceOf(PublicacionKafkaException.class)
        .hasMessageContaining("not present in metadata after 5000 ms");
  }

  static CompletableFuture<SendResult<String, InventarioEvento>> confirmado(String clave, InventarioEvento evento) {
    RecordMetadata metadata = new RecordMetadata(new TopicPartition(TOPICO, 1), 41L, 0, 0L, 0, 0);
    return CompletableFuture.completedFuture(
        new SendResult<>(new ProducerRecord<>(TOPICO, clave, evento), metadata));
  }

  static KafkaTopicosProperties propiedades() {
    return new KafkaTopicosProperties(
        new KafkaTopicosProperties.Topicos(
            new KafkaTopicosProperties.Topico("pedidos360.pedidos", 3, (short) 3, null, null, 2, null, null),
            new KafkaTopicosProperties.Topico(TOPICO, 3, (short) 3, "compact", null, 2, null, null)),
        new KafkaTopicosProperties.Dlt(".dlt", 1209600000L),
        new KafkaTopicosProperties.Grupos("pedidos360-inventario"),
        new KafkaTopicosProperties.Reintentos(3, 1000, 2.0, 10000));
  }

  private static InventarioEvento descuento(Long productoId) {
    Producto p = new Producto();
    p.setId(productoId);
    p.setNombre("Producto " + productoId);
    p.setPrecio(BigDecimal.TEN);
    p.setStock(8);
    return InventarioEventos.descuento(p, 10, 2, 7L, 5);
  }
}
