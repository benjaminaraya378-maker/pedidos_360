package cl.duoc.pedidos360.analitica;

import static org.assertj.core.api.Assertions.assertThat;

import cl.duoc.pedidos360.analitica.dto.ResumenAnalitica;
import cl.duoc.pedidos360.analitica.service.AnaliticaService;
import cl.duoc.pedidos360.kafka.evento.InventarioEvento;
import cl.duoc.pedidos360.kafka.evento.PedidoEvento;
import cl.duoc.pedidos360.kafka.evento.TipoEventoInventario;
import cl.duoc.pedidos360.kafka.evento.TipoEventoPedido;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

/**
 * Prueba de punta a punta del consumidor de la analítica contra un broker Kafka embebido: eventos tipados de los dos
 * tópicos, validación del payload, envío al DLT de lo defectuoso y que un evento malo no detenga la partición.
 */
@SpringBootTest(properties = {
    "pedidos360.kafka.reintentos.espera-inicial-ms=50",
    "pedidos360.kafka.reintentos.espera-maxima-ms=100"
})
@EmbeddedKafka(
    partitions = 3,
    topics = {"pedidos360.pedidos", "pedidos360.inventario", "pedidos360.pedidos.dlt", "pedidos360.inventario.dlt"},
    bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class AnaliticaKafkaTest {

  @Autowired
  private KafkaTemplate<Object, Object> kafkaTemplate;

  @Autowired
  private AnaliticaService analitica;

  @Autowired
  private EmbeddedKafkaBroker broker;

  @Test
  void procesaAmbosTopicosYMandaAlDltLoQueNoPuedeProcesar() throws Exception {
    // 1. Eventos válidos de los dos tópicos: llegan a su @KafkaHandler como DTO
    kafkaTemplate.send("pedidos360.pedidos", "1", pedido(1L, 25990)).get();
    kafkaTemplate.send("pedidos360.inventario", "3", stockDescontado(3L, 1L, 6, 4)).get();
    ResumenAnalitica resumen = esperar(r -> r.totalPedidos() == 1 && r.alertasStockBajo().size() == 1);
    assertThat(resumen.pedidosPorEstado()).containsEntry("CREADO", 1L);
    assertThat(resumen.alertasStockBajo().getFirst().stockActual()).isEqualTo(4);

    // 2. JSON ilegible y 3. JSON válido que no cumple el contrato (pedidoId negativo, sin ítems)
    try (var bytes = new DefaultKafkaProducerFactory<>(KafkaTestUtils.producerProps(broker), new StringSerializer(),
        new ByteArraySerializer()).createProducer()) {
      ProducerRecord<String, byte[]> ilegible = new ProducerRecord<>("pedidos360.pedidos", "2",
          "{esto no es json".getBytes(StandardCharsets.UTF_8));
      ilegible.headers().add("__TypeId__", "pedidoEvento".getBytes(StandardCharsets.UTF_8));
      bytes.send(ilegible).get();
    }
    PedidoEvento invalido = new PedidoEvento(UUID.randomUUID().toString(), TipoEventoPedido.PEDIDO_CREADO, 1,
        Instant.now(), -5L, "cliente-1", "CREADO", BigDecimal.ONE, null, List.of());
    kafkaTemplate.send("pedidos360.pedidos", "-5", invalido).get();

    // 4. Un evento válido después de los defectuosos se procesa igual: la partición no quedó bloqueada
    kafkaTemplate.send("pedidos360.pedidos", "2", pedido(2L, 9990)).get();
    esperar(r -> r.totalPedidos() == 2);

    List<ConsumerRecord<String, byte[]>> dlt = leerDlt("pedidos360.pedidos.dlt", 2);
    assertThat(dlt).hasSize(2);
    assertThat(dlt).allSatisfy(r -> {
      assertThat(header(r, KafkaHeaders.DLT_ORIGINAL_TOPIC)).isEqualTo("pedidos360.pedidos");
      assertThat(header(r, KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP)).isEqualTo("pedidos360-analitica");
    });
    // El mensaje ilegible se conserva tal cual (bytes originales), para poder revisarlo
    assertThat(dlt).anySatisfy(r ->
        assertThat(new String(r.value(), StandardCharsets.UTF_8)).isEqualTo("{esto no es json"));
  }

  private ResumenAnalitica esperar(Predicate<ResumenAnalitica> condicion) throws InterruptedException {
    long limite = System.currentTimeMillis() + 30_000;
    ResumenAnalitica resumen = analitica.resumen();
    while (!condicion.test(resumen) && System.currentTimeMillis() < limite) {
      Thread.sleep(200);
      resumen = analitica.resumen();
    }
    assertThat(condicion).as("métricas esperadas; último resumen: %s", resumen).accepts(resumen);
    return resumen;
  }

  private List<ConsumerRecord<String, byte[]>> leerDlt(String topico, int minimo) {
    Map<String, Object> props = KafkaTestUtils.consumerProps("lector-dlt", "false", broker);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    try (Consumer<String, byte[]> consumer = new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(),
        new ByteArrayDeserializer()).createConsumer()) {
      broker.consumeFromAnEmbeddedTopic(consumer, topico);
      ConsumerRecords<String, byte[]> registros = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(20), minimo);
      return java.util.stream.StreamSupport.stream(registros.spliterator(), false).toList();
    }
  }

  private static String header(ConsumerRecord<?, ?> registro, String nombre) {
    Header h = registro.headers().lastHeader(nombre);
    return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
  }

  private static PedidoEvento pedido(Long id, int total) {
    return new PedidoEvento(UUID.randomUUID().toString(), TipoEventoPedido.PEDIDO_CREADO, 1, Instant.now(), id,
        "cliente-1", "CREADO", BigDecimal.valueOf(total), null,
        List.of(new PedidoEvento.Item(3L, "Mouse", 1, BigDecimal.valueOf(total), BigDecimal.valueOf(total))));
  }

  private static InventarioEvento stockDescontado(Long productoId, Long pedidoId, int antes, int despues) {
    return new InventarioEvento(UUID.randomUUID().toString(), TipoEventoInventario.STOCK_DESCONTADO, 1,
        Instant.now(), productoId, "Mouse", antes - despues, antes, despues, 5, despues <= 5, pedidoId, "prueba");
  }
}
