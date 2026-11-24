package cl.duoc.pedidos360.analitica.consumer;

import cl.duoc.pedidos360.analitica.service.AnaliticaService;
import cl.duoc.pedidos360.analitica.service.AnaliticaService.Origen;
import cl.duoc.pedidos360.kafka.EventoInvalidoException;
import cl.duoc.pedidos360.kafka.KafkaTopicosProperties;
import cl.duoc.pedidos360.kafka.evento.InventarioEvento;
import cl.duoc.pedidos360.kafka.evento.PedidoEvento;
import jakarta.validation.Valid;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Consumidor Kafka de la analítica: lee los DOS tópicos ({@code pedidos360.pedidos} y {@code pedidos360.inventario})
 * en su propio grupo, {@code pedidos360-analitica}.
 *
 * <ul>
 *   <li>Un grupo propio significa que recibe todos los eventos aunque ms-productos y ms-pedidos ya los estén
 *       consumiendo con sus grupos: cada grupo lleva su propio offset.</li>
 *   <li>concurrency=3: tres hilos se reparten las 6 particiones (3 de cada tópico), 2 por hilo.</li>
 *   <li>Cada mensaje llega como su DTO según el header {@code __TypeId__} y Spring lo envía al {@code @KafkaHandler}
 *       del tipo que corresponde. Un tipo desconocido se rechaza y va al DLT del tópico de origen.</li>
 *   <li>Las métricas viven en memoria: al arrancar, si {@code reconstruir-al-iniciar} es true, vuelve al inicio de
 *       cada partición y las reconstruye con los eventos que Kafka aún retiene.</li>
 * </ul>
 * La lógica de las métricas está en {@link AnaliticaService}; esta clase solo conoce la mensajería.
 */
@Component
@KafkaListener(
    id = "ms-analitica",
    topics = {"${pedidos360.kafka.topicos.pedidos.nombre}", "${pedidos360.kafka.topicos.inventario.nombre}"},
    groupId = "${pedidos360.kafka.grupos.analitica}",
    concurrency = "${spring.kafka.listener.concurrency:3}")
public class AnaliticaConsumer implements ConsumerSeekAware {

  private static final Logger log = LoggerFactory.getLogger(AnaliticaConsumer.class);

  private final AnaliticaService analitica;
  private final KafkaTopicosProperties props;
  /** Particiones que ya se releyeron desde el inicio en esta ejecución (un rebalanceo posterior no las relee). */
  private final Set<TopicPartition> reconstruidas = ConcurrentHashMap.newKeySet();

  public AnaliticaConsumer(AnaliticaService analitica, KafkaTopicosProperties props) {
    this.analitica = analitica;
    this.props = props;
  }

  @KafkaHandler
  public void onPedido(@Payload @Valid PedidoEvento evento,
                       @Header(KafkaHeaders.RECEIVED_TOPIC) String topico,
                       @Header(KafkaHeaders.RECEIVED_PARTITION) int particion,
                       @Header(KafkaHeaders.OFFSET) long offset) {
    analitica.registrarPedido(evento, new Origen(topico, particion, offset));
  }

  @KafkaHandler
  public void onInventario(@Payload @Valid InventarioEvento evento,
                           @Header(KafkaHeaders.RECEIVED_TOPIC) String topico,
                           @Header(KafkaHeaders.RECEIVED_PARTITION) int particion,
                           @Header(KafkaHeaders.OFFSET) long offset) {
    analitica.registrarInventario(evento, new Origen(topico, particion, offset));
  }

  /** Un JSON válido pero de un tipo que la analítica no conoce: no se puede procesar, va al DLT sin reintentos. */
  @KafkaHandler(isDefault = true)
  public void onDesconocido(Object valor) {
    throw new EventoInvalidoException("Tipo de evento no reconocido por la analítica: " + valor.getClass().getName());
  }

  @Override
  public void onPartitionsAssigned(Map<TopicPartition, Long> asignadas, ConsumerSeekCallback callback) {
    if (!props.reconstruirAlIniciar()) {
      return;
    }
    List<TopicPartition> nuevas = asignadas.keySet().stream().filter(reconstruidas::add).toList();
    if (!nuevas.isEmpty()) {
      log.info("Reconstruyendo métricas: lectura desde el inicio de {}", nuevas);
      callback.seekToBeginning(nuevas);
    }
  }
}
