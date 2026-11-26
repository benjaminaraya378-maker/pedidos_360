package cl.duoc.pedidos360.pedidos.producer;

import cl.duoc.pedidos360.kafka.evento.PedidoEvento;
import cl.duoc.pedidos360.pedidos.model.EventoKafkaPendiente;
import cl.duoc.pedidos360.pedidos.repository.EventoKafkaPendienteRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Limit;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Outbox de Kafka para PEDIDO_CREADO: el evento se guarda en la base junto con el pedido y se publica después, así un
 * pedido guardado nunca se queda sin su evento aunque Kafka esté caído al momento de crearlo (antes el evento se
 * perdía y el pedido quedaba en CREADO para siempre).
 *
 * <ol>
 *   <li><b>Registro</b> ({@link #registrar}): dentro de la transacción que guarda el pedido, justo antes del commit,
 *       se inserta una fila en {@code eventos_kafka_pendientes} con el PedidoEvento en JSON. O quedan el pedido y la
 *       fila, o ninguno de los dos.</li>
 *   <li><b>Intento inmediato</b>: si la transacción hizo commit, el evento se envía a Kafka sin esperar la respuesta,
 *       para no sumar latencia a la petición HTTP. Cuando Kafka confirma se borra la fila; si no confirma, la fila
 *       queda pendiente con un intento más y el motivo del fallo.</li>
 *   <li><b>Relé</b> ({@link #publicarPendientes}): cada {@code pedidos360.kafka.outbox.intervalo-ms} toma las filas con
 *       más de {@code antiguedad-minima-ms} (para entonces el intento inmediato ya terminó y no compiten), las publica
 *       esperando la confirmación y las borra. Si Kafka no confirma, suma el intento, guarda el error y corta la
 *       vuelta, porque con el broker caído las demás fallarían igual; en la vuelta siguiente lo vuelve a intentar.</li>
 * </ol>
 *
 * <p>La garantía es "al menos una vez": un mismo evento puede llegar dos veces a Kafka (el intento inmediato y el relé
 * coinciden, el servicio se cae entre la confirmación y el borrado de la fila, o corren dos instancias de ms-pedidos),
 * siempre con el mismo eventoId. No hace daño porque los consumidores son idempotentes por eventoId: ms-productos lo
 * registra en {@code eventos_procesados} y no descuenta el stock dos veces, y ms-analitica no vuelve a contar un
 * eventoId que ya vio.
 *
 * <p>A Kafka se envía siempre el {@link PedidoEvento} tipado (el relé lo reconstruye desde el JSON de la fila), nunca
 * el texto: el JsonSerializer de {@code KafkaConfig} produce el mismo JSON que antes del outbox y el header
 * {@code __TypeId__} sigue siendo {@code pedidoEvento}, así que los consumidores no notan ninguna diferencia.
 */
@Component
public class OutboxKafka {

  private static final Logger log = LoggerFactory.getLogger(OutboxKafka.class);

  /** Largo de la columna {@code ultimo_error}. */
  private static final int LARGO_MAXIMO_ERROR = 500;

  private final EventoKafkaPendienteRepository repository;
  private final KafkaTemplate<String, PedidoEvento> kafkaTemplate;
  private final ObjectMapper objectMapper;
  private final long esperaConfirmacionMs;
  private final long antiguedadMinimaMs;
  private final int lote;
  private final Executor ejecutor;

  /**
   * @param esperaConfirmacionMs cuánto espera el relé la confirmación de cada envío (la misma espera que los cambios
   *                             de estado; mayor que {@code delivery.timeout.ms} del productor)
   * @param antiguedadMinimaMs   el relé solo toma filas más antiguas que esto
   * @param lote                 cuántas filas toma el relé en cada vuelta
   */
  @Autowired
  public OutboxKafka(EventoKafkaPendienteRepository repository, KafkaTemplate<String, PedidoEvento> kafkaTemplate,
                     ObjectMapper objectMapper,
                     @Value("${pedidos360.kafka.publicacion.espera-confirmacion-ms:15000}") long esperaConfirmacionMs,
                     @Value("${pedidos360.kafka.outbox.antiguedad-minima-ms:20000}") long antiguedadMinimaMs,
                     @Value("${pedidos360.kafka.outbox.lote:50}") int lote) {
    this(repository, kafkaTemplate, objectMapper, esperaConfirmacionMs, antiguedadMinimaMs, lote,
        Executors.newSingleThreadExecutor(Thread.ofPlatform().name("outbox-kafka").daemon().factory()));
  }

  /**
   * @param ejecutor dónde se borra la fila o se anota el fallo cuando responde Kafka al intento inmediato. En
   *                 producción es un hilo propio ({@code outbox-kafka}); en las pruebas, el mismo hilo de la prueba
   */
  OutboxKafka(EventoKafkaPendienteRepository repository, KafkaTemplate<String, PedidoEvento> kafkaTemplate,
              ObjectMapper objectMapper, long esperaConfirmacionMs, long antiguedadMinimaMs, int lote,
              Executor ejecutor) {
    this.repository = repository;
    this.kafkaTemplate = kafkaTemplate;
    this.objectMapper = objectMapper;
    this.esperaConfirmacionMs = esperaConfirmacionMs;
    this.antiguedadMinimaMs = antiguedadMinimaMs;
    this.lote = lote;
    this.ejecutor = ejecutor;
  }

  /**
   * Guarda el evento en el outbox dentro de la transacción en curso (la que guarda el pedido) y deja el intento
   * inmediato programado para después del commit. Si la transacción se deshace, no queda la fila ni se publica nada;
   * si el INSERT falla, tampoco se guarda el pedido.
   */
  @Transactional(propagation = Propagation.MANDATORY)
  public void registrar(String topico, PedidoEvento evento) {
    EventoKafkaPendiente fila = new EventoKafkaPendiente(evento.eventoId(), topico, String.valueOf(evento.pedidoId()),
        aJson(evento), Instant.now());
    repository.save(fila);
    log.debug("Outbox: {} del pedido {} guardado junto con el pedido (evento {})", evento.tipo(), evento.pedidoId(),
        evento.eventoId());

    TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
      @Override
      public void afterCommit() {
        publicarSinEsperar(fila, evento);
      }
    });
  }

  /**
   * Intento inmediato, con el pedido ya confirmado en la base. Todo ocurre en el hilo {@code outbox-kafka}: el envío
   * (con Kafka caído, {@code send()} espera la metadata del tópico hasta {@code max.block.ms}, y la petición HTTP no
   * debe cargar con esa espera) y, cuando Kafka responde, el borrado de la fila o el registro del fallo. Ni el hilo de
   * la petición HTTP ni el hilo de red del productor Kafka tocan Kafka o la base aquí. Nunca lanza: el pedido ya existe.
   */
  void publicarSinEsperar(EventoKafkaPendiente fila, PedidoEvento evento) {
    ejecutor.execute(() -> {
      CompletableFuture<SendResult<String, PedidoEvento>> envio;
      try {
        envio = kafkaTemplate.send(fila.getTopico(), fila.getClave(), evento);
      } catch (RuntimeException ex) {
        // send() no alcanzó a encolar el mensaje: sin metadata del tópico en max.block.ms (Kafka caído)
        envio = CompletableFuture.failedFuture(ex);
      }
      envio.whenCompleteAsync((resultado, error) -> {
        if (error == null) {
          confirmado(fila, evento, resultado, "intento inmediato");
        } else {
          fallido(fila, error, "intento inmediato", "Queda en el outbox: el relé lo reintentará");
        }
      }, ejecutor);
    });
  }

  /**
   * Relé del outbox: publica los eventos que siguen pendientes, de a uno y esperando la confirmación de Kafka. Al
   * primer fallo corta la vuelta (la fila queda con un intento más); la siguiente vuelta lo vuelve a intentar.
   */
  @Scheduled(fixedDelayString = "${pedidos360.kafka.outbox.intervalo-ms:2000}")
  public void publicarPendientes() {
    List<EventoKafkaPendiente> pendientes;
    try {
      pendientes = repository.findByCreadoEnBeforeOrderByIntentosAscCreadoEnAsc(
          Instant.now().minusMillis(antiguedadMinimaMs), Limit.of(lote));
    } catch (RuntimeException ex) {
      log.warn("Relé outbox: no se pudo leer eventos_kafka_pendientes ({}); se reintenta en la próxima vuelta",
          motivo(ex));
      return;
    }
    if (pendientes.isEmpty()) {
      return;
    }

    log.info("Relé outbox: {} evento(s) pendiente(s) de publicar en Kafka", pendientes.size());
    for (int i = 0; i < pendientes.size(); i++) {
      if (!publicarYEsperar(pendientes.get(i))) {
        if (pendientes.size() - i > 1) {
          log.warn("Relé outbox: Kafka no está confirmando; {} evento(s) más quedan para la próxima vuelta",
              pendientes.size() - i - 1);
        }
        return;
      }
    }
  }

  /** Publica una fila y espera la confirmación. Devuelve {@code false} si Kafka no la confirmó. */
  private boolean publicarYEsperar(EventoKafkaPendiente fila) {
    String via = "relé, intento " + (fila.getIntentos() + 1);
    String queSigue = "Se reintenta en la próxima vuelta del relé";
    try {
      PedidoEvento evento = objectMapper.readValue(fila.getPayload(), PedidoEvento.class);
      SendResult<String, PedidoEvento> resultado = kafkaTemplate.send(fila.getTopico(), fila.getClave(), evento)
          .get(esperaConfirmacionMs, TimeUnit.MILLISECONDS);
      confirmado(fila, evento, resultado, via);
      return true;
    } catch (ExecutionException ex) {
      // El broker rechazó el envío o venció delivery.timeout.ms (brokers caídos, sin réplicas suficientes...)
      fallido(fila, ex.getCause() != null ? ex.getCause() : ex, via, queSigue);
    } catch (TimeoutException ex) {
      fallido(fila, new TimeoutException("sin respuesta del broker en " + esperaConfirmacionMs + " ms"), via,
          queSigue);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      fallido(fila, ex, via, queSigue);
    } catch (JsonProcessingException | RuntimeException ex) {
      // JSON de la fila ilegible, o send() no pudo encolar el mensaje (sin metadata del tópico en max.block.ms)
      fallido(fila, ex, via, queSigue);
    }
    return false;
  }

  /** Kafka confirmó: se borra la fila. Si el borrado falla, el relé lo volverá a publicar (duplicado inofensivo). */
  private void confirmado(EventoKafkaPendiente fila, PedidoEvento evento, SendResult<String, PedidoEvento> resultado,
                          String via) {
    log.info("Kafka: {} del pedido {} publicado en {}-{}@{} (evento {}, {})", evento.tipo(), evento.pedidoId(),
        fila.getTopico(), resultado.getRecordMetadata().partition(), resultado.getRecordMetadata().offset(),
        fila.getEventoId(), via);
    try {
      repository.borrar(fila.getEventoId());
    } catch (RuntimeException ex) {
      log.warn("Evento {} publicado, pero no se pudo borrar del outbox ({}): el relé lo publicará de nuevo y los "
          + "consumidores descartarán el duplicado por su eventoId", fila.getEventoId(), motivo(ex));
    }
  }

  /** Kafka no confirmó: la fila sigue pendiente con un intento más y el motivo. */
  private void fallido(EventoKafkaPendiente fila, Throwable error, String via, String queSigue) {
    String motivo = motivo(error);
    log.warn("Kafka no confirmó el evento {} del pedido {} en '{}' ({}): {}. {}", fila.getEventoId(), fila.getClave(),
        fila.getTopico(), via, motivo, queSigue);
    try {
      repository.registrarFallo(fila.getEventoId(), motivo);
    } catch (RuntimeException ex) {
      log.error("No se pudo anotar el fallo del evento {} en el outbox ({}); la fila sigue pendiente igual",
          fila.getEventoId(), motivo(ex));
    }
  }

  private String aJson(PedidoEvento evento) {
    try {
      return objectMapper.writeValueAsString(evento);
    } catch (JsonProcessingException ex) {
      throw new IllegalStateException("No se pudo convertir a JSON el evento " + evento.eventoId() + " del pedido "
          + evento.pedidoId(), ex);
    }
  }

  /** La causa real del error (la última de la cadena), recortada al largo de la columna {@code ultimo_error}. */
  static String motivo(Throwable error) {
    Throwable raiz = error;
    while (raiz.getCause() != null && raiz.getCause() != raiz) {
      raiz = raiz.getCause();
    }
    String motivo = raiz.getClass().getSimpleName() + ": " + raiz.getMessage();
    return motivo.length() > LARGO_MAXIMO_ERROR ? motivo.substring(0, LARGO_MAXIMO_ERROR) : motivo;
  }
}
