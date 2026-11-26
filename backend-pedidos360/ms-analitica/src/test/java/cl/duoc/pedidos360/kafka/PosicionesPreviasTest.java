package cl.duoc.pedidos360.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import cl.duoc.pedidos360.analitica.consumer.AnaliticaConsumer;
import cl.duoc.pedidos360.analitica.service.AnaliticaService;
import cl.duoc.pedidos360.kafka.KafkaTopicosProperties.Dlt;
import cl.duoc.pedidos360.kafka.KafkaTopicosProperties.Grupos;
import cl.duoc.pedidos360.kafka.KafkaTopicosProperties.Reintentos;
import cl.duoc.pedidos360.kafka.KafkaTopicosProperties.Topico;
import cl.duoc.pedidos360.kafka.KafkaTopicosProperties.Topicos;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.ConsumerSeekAware.ConsumerSeekCallback;

/**
 * Criterio para no duplicar el DLT al reconstruir las métricas, probado sin broker: qué cuenta como "ya procesado
 * antes del reinicio", que el recuperador no lo vuelva a publicar y que el consumidor guarde la posición antes de
 * volver al inicio de cada partición.
 */
class PosicionesPreviasTest {

  private static final String PEDIDOS = "pedidos360.pedidos";
  private static final String INVENTARIO = "pedidos360.inventario";
  private static final TopicPartition P0 = new TopicPartition(PEDIDOS, 0);
  private static final TopicPartition P1 = new TopicPartition(PEDIDOS, 1);

  private final PosicionesPrevias previas = new PosicionesPrevias();

  @Test
  void particionSinPosicionPreviaTodoEsNuevo() {
    previas.recordar(P0, null);

    assertThat(previas.de(registro(PEDIDOS, 0, 0))).isNull();
    assertThat(previas.yaProcesadoAntes(registro(PEDIDOS, 0, 0))).isFalse();
  }

  @Test
  void soloLoAnteriorALaPosicionPreviaYaSeProceso() {
    previas.recordar(P0, 5L);

    assertThat(previas.yaProcesadoAntes(registro(PEDIDOS, 0, 0))).isTrue();
    assertThat(previas.yaProcesadoAntes(registro(PEDIDOS, 0, 4))).isTrue();
    // La posición es el siguiente offset pendiente del grupo: ese y los que siguen son nuevos
    assertThat(previas.yaProcesadoAntes(registro(PEDIDOS, 0, 5))).isFalse();
    assertThat(previas.yaProcesadoAntes(registro(PEDIDOS, 0, 9))).isFalse();
    // Cada partición de cada tópico tiene su propia posición
    assertThat(previas.yaProcesadoAntes(registro(PEDIDOS, 1, 0))).isFalse();
    assertThat(previas.yaProcesadoAntes(registro(INVENTARIO, 0, 0))).isFalse();
  }

  @Test
  void laPrimeraPosicionNoSeReemplaza() {
    previas.recordar(P0, 5L);
    previas.recordar(P0, 2L); // la partición pasó a otro hilo a mitad de la relectura

    assertThat(previas.de(registro(PEDIDOS, 0, 0))).isEqualTo(5L);
    assertThat(previas.yaProcesadoAntes(registro(PEDIDOS, 0, 3))).isTrue();
  }

  @Test
  void elRecuperadorNoVuelveAPublicarEnElDltLoYaProcesado() {
    previas.recordar(P0, 5L);
    List<String> publicados = new ArrayList<>();
    ConsumerRecordRecoverer publicadorDlt = (registro, error) ->
        publicados.add(registro.topic() + "-" + registro.partition() + "@" + registro.offset());
    ConsumerRecordRecoverer recuperador = KafkaConfig.recuperador(publicadorDlt, previas, new Dlt(".dlt"));
    EventoInvalidoException error = new EventoInvalidoException("payload inválido");

    recuperador.accept(registro(PEDIDOS, 0, 2), error); // ya enviado al DLT antes del reinicio
    recuperador.accept(registro(PEDIDOS, 0, 5), error); // nuevo: justo en la posición previa
    recuperador.accept(registro(PEDIDOS, 0, 7), error); // nuevo
    recuperador.accept(registro(PEDIDOS, 1, 0), error); // partición sin posición previa

    assertThat(publicados).containsExactly(PEDIDOS + "-0@5", PEDIDOS + "-0@7", PEDIDOS + "-1@0");
  }

  @Test
  void elConsumidorGuardaLaPosicionAntesDeVolverAlInicioYSoloLaPrimeraVez() {
    AnaliticaConsumer consumidor = new AnaliticaConsumer(new AnaliticaService(), propiedades(true), previas);
    ConsumerSeekCallback callback = mock(ConsumerSeekCallback.class);
    List<Collection<TopicPartition>> seeks = new ArrayList<>();
    List<Long> posicionAlHacerSeek = new ArrayList<>();
    doAnswer(invocacion -> {
      seeks.add(List.copyOf(invocacion.<Collection<TopicPartition>>getArgument(0)));
      posicionAlHacerSeek.add(previas.de(registro(PEDIDOS, 0, 0)));
      return null;
    }).when(callback).seekToBeginning(anyCollection());

    Map<TopicPartition, Long> asignadas = new LinkedHashMap<>();
    asignadas.put(P0, 5L);
    asignadas.put(P1, 0L);
    consumidor.onPartitionsAssigned(asignadas, callback);
    // Rebalanceo: P0 vuelve a asignarse con una posición de esta misma ejecución; no se relee ni se reemplaza
    consumidor.onPartitionsAssigned(Map.of(P0, 3L), callback);

    assertThat(seeks).containsExactly(List.of(P0, P1));
    assertThat(posicionAlHacerSeek).containsExactly(5L); // ya estaba guardada al volver al inicio
    assertThat(previas.de(registro(PEDIDOS, 0, 0))).isEqualTo(5L);
    assertThat(previas.de(registro(PEDIDOS, 1, 0))).isEqualTo(0L);
  }

  @Test
  void sinReconstruccionNoSeGuardaNadaYTodoFalloVaAlDlt() {
    AnaliticaConsumer consumidor = new AnaliticaConsumer(new AnaliticaService(), propiedades(false), previas);
    ConsumerSeekCallback callback = mock(ConsumerSeekCallback.class);

    consumidor.onPartitionsAssigned(Map.of(P0, 5L), callback);

    verify(callback, never()).seekToBeginning(anyCollection());
    assertThat(previas.yaProcesadoAntes(registro(PEDIDOS, 0, 0))).isFalse();
  }

  private static ConsumerRecord<String, String> registro(String topico, int particion, long offset) {
    return new ConsumerRecord<>(topico, particion, offset, "clave", "{}");
  }

  private static KafkaTopicosProperties propiedades(boolean reconstruirAlIniciar) {
    return new KafkaTopicosProperties(new Topicos(new Topico(PEDIDOS), new Topico(INVENTARIO)), new Dlt(".dlt"),
        new Grupos("pedidos360-analitica"), new Reintentos(3, 1000, 2.0, 10000), reconstruirAlIniciar);
  }
}
