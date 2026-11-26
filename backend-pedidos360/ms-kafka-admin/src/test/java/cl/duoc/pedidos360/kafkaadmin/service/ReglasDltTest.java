package cl.duoc.pedidos360.kafkaadmin.service;

import static org.assertj.core.api.Assertions.assertThat;

import cl.duoc.pedidos360.kafkaadmin.dto.Respuestas.MensajeDlt;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.SerializationUtils;

/** Reglas puras del reproceso de DLT: nombre aceptado, tópico de origen, headers que viajan y cómo se muestra. */
class ReglasDltTest {

  private static final String DLT = "pedidos360.pedidos.dlt";

  @Test
  void soloAceptaNombresDeDlt() {
    assertThat(ReglasDlt.validarNombre("pedidos360.pedidos.dlt")).isEmpty();
    assertThat(ReglasDlt.validarNombre("pedidos360.inventario.dlt")).isEmpty();

    assertThat(ReglasDlt.validarNombre("pedidos360.pedidos"))
        .singleElement().asString().contains("debe terminar en '.dlt'");
    assertThat(ReglasDlt.validarNombre("pedidos360.pedidos.DLT")).hasSize(1);
    assertThat(ReglasDlt.validarNombre(".dlt")).hasSize(1);   // sin nada antes del sufijo
    assertThat(ReglasDlt.validarNombre("__consumer_offsets.dlt"))
        .singleElement().asString().contains("no puede empezar con '__'");
    assertThat(ReglasDlt.validarNombre("pedidos 360.dlt")).hasSize(1);
    assertThat(ReglasDlt.validarNombre(null)).hasSize(1);
  }

  @Test
  void elTopicoDeOrigenSaleDelHeaderOSeDeduceDelNombreDelDlt() {
    RecordHeaders conHeader = new RecordHeaders();
    conHeader.add(KafkaHeaders.DLT_ORIGINAL_TOPIC, bytes("pedidos360.inventario"));
    assertThat(ReglasDlt.topicoOrigen(conHeader, DLT)).isEqualTo("pedidos360.inventario");

    assertThat(ReglasDlt.topicoOrigen(new RecordHeaders(), DLT)).isEqualTo("pedidos360.pedidos");

    RecordHeaders vacio = new RecordHeaders();
    vacio.add(KafkaHeaders.DLT_ORIGINAL_TOPIC, bytes("  "));
    assertThat(ReglasDlt.topicoOrigen(vacio, DLT)).isEqualTo("pedidos360.pedidos");

    // Pasó dos veces por el DLT: manda el último header
    RecordHeaders dosVeces = new RecordHeaders();
    dosVeces.add(KafkaHeaders.DLT_ORIGINAL_TOPIC, bytes("pedidos360.antiguo"));
    dosVeces.add(KafkaHeaders.DLT_ORIGINAL_TOPIC, bytes("pedidos360.pedidos"));
    assertThat(ReglasDlt.topicoOrigen(dosVeces, DLT)).isEqualTo("pedidos360.pedidos");
  }

  @Test
  void alReinyectarSeQuitanLosHeadersDelDltYSeConservanLosDelEvento() {
    RecordHeaders h = new RecordHeaders();
    h.add("__TypeId__", bytes("pedidoEvento"));
    h.add(KafkaHeaders.DLT_ORIGINAL_TOPIC, bytes("pedidos360.pedidos"));
    h.add(KafkaHeaders.DLT_ORIGINAL_PARTITION, ByteBuffer.allocate(Integer.BYTES).putInt(1).array());
    h.add(KafkaHeaders.DLT_EXCEPTION_FQCN, bytes("org.springframework.kafka.listener.ListenerExecutionFailedException"));
    h.add(KafkaHeaders.DLT_EXCEPTION_STACKTRACE, bytes("at ..."));
    h.add("traceparent", bytes("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"));
    h.add(SerializationUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER, new byte[] {1, 2, 3});

    List<Header> reinyectar = ReglasDlt.headersParaReinyectar(h);

    assertThat(reinyectar).extracting(Header::key).containsExactly("__TypeId__", "traceparent");
    assertThat(reinyectar.getFirst().value()).isEqualTo(bytes("pedidoEvento"));
  }

  @Test
  void muestraOrigenYExcepcionDesdeLosHeadersYRecortaElValor() {
    RecordHeaders h = new RecordHeaders();
    h.add(KafkaHeaders.DLT_ORIGINAL_TOPIC, bytes("pedidos360.pedidos"));
    h.add(KafkaHeaders.DLT_ORIGINAL_PARTITION, ByteBuffer.allocate(Integer.BYTES).putInt(2).array());
    h.add(KafkaHeaders.DLT_ORIGINAL_OFFSET, ByteBuffer.allocate(Long.BYTES).putLong(41L).array());
    h.add(KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP, bytes("pedidos360-analitica"));
    h.add(KafkaHeaders.DLT_EXCEPTION_FQCN, bytes("org.springframework.kafka.listener.ListenerExecutionFailedException"));
    h.add(KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN, bytes("cl.duoc.pedidos360.kafka.EventoInvalidoException"));
    h.add(KafkaHeaders.DLT_EXCEPTION_MESSAGE, bytes("x".repeat(400)));
    byte[] valor = bytes("{\"pedidoId\":" + "9".repeat(600) + "}");
    ConsumerRecord<byte[], byte[]> registro = new ConsumerRecord<>(DLT, 1, 7L, 1_764_000_000_000L,
        TimestampType.CREATE_TIME, 5, valor.length, bytes("P-001"), valor, h, Optional.empty());

    MensajeDlt m = ReglasDlt.aMensaje(registro, DLT);

    assertThat(m.particion()).isEqualTo(1);
    assertThat(m.offset()).isEqualTo(7L);
    assertThat(m.fecha()).isEqualTo("2025-11-24T16:00:00Z");
    assertThat(m.clave()).isEqualTo("P-001");
    assertThat(m.topicoOrigen()).isEqualTo("pedidos360.pedidos");
    assertThat(m.particionOrigen()).isEqualTo(2);
    assertThat(m.offsetOrigen()).isEqualTo(41L);
    assertThat(m.grupoOrigen()).isEqualTo("pedidos360-analitica");
    assertThat(m.excepcion()).isEqualTo("cl.duoc.pedidos360.kafka.EventoInvalidoException");   // la causa, no el envoltorio
    assertThat(m.motivo()).hasSize(ReglasDlt.MAXIMO_MOTIVO + 1).endsWith("…");
    assertThat(m.valor()).hasSize(ReglasDlt.MAXIMO_VALOR + 1).startsWith("{\"pedidoId\":999").endsWith("…");
    assertThat(m.bytes()).isEqualTo(valor.length);
  }

  @Test
  void sinHeadersNiValorMuestraLoQueHay() {
    ConsumerRecord<byte[], byte[]> registro = new ConsumerRecord<>(DLT, 0, 3L, null, null);

    MensajeDlt m = ReglasDlt.aMensaje(registro, DLT);

    assertThat(m.topicoOrigen()).isEqualTo("pedidos360.pedidos");
    assertThat(m.clave()).isNull();
    assertThat(m.particionOrigen()).isNull();
    assertThat(m.offsetOrigen()).isNull();
    assertThat(m.excepcion()).isNull();
    assertThat(m.valor()).isNull();
    assertThat(m.bytes()).isZero();
  }

  @Test
  void leeLosNumerosComoLosGuardaSpringKafka() {
    assertThat(ReglasDlt.numero(new RecordHeader("p", ByteBuffer.allocate(Integer.BYTES).putInt(2).array())))
        .isEqualTo(2L);
    assertThat(ReglasDlt.numero(new RecordHeader("o", ByteBuffer.allocate(Long.BYTES).putLong(123_456_789L).array())))
        .isEqualTo(123_456_789L);
    assertThat(ReglasDlt.numero(new RecordHeader("t", bytes("15")))).isEqualTo(15L);
    assertThat(ReglasDlt.numero(new RecordHeader("t", bytes("abc")))).isNull();
    assertThat(ReglasDlt.numero(null)).isNull();
  }

  @Test
  void recortarNoParteUnEmoji() {
    assertThat(ReglasDlt.recortar("ab😀cd", 3)).isEqualTo("ab…");
    assertThat(ReglasDlt.recortar("abc", 3)).isEqualTo("abc");
    assertThat(ReglasDlt.recortar(null, 3)).isNull();
  }

  private static byte[] bytes(String texto) {
    return texto.getBytes(StandardCharsets.UTF_8);
  }
}
