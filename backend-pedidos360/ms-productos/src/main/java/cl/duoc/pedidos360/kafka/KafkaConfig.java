package cl.duoc.pedidos360.kafka;

import cl.duoc.pedidos360.kafka.KafkaTopicosProperties.Reintentos;
import cl.duoc.pedidos360.kafka.KafkaTopicosProperties.Topico;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListenerConfigurer;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.messaging.handler.annotation.support.MethodArgumentNotValidException;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

/**
 * Configuración del cliente Kafka de ms-productos. La conexión y las propiedades del productor y del consumidor
 * (bootstrap servers, acks, idempotencia, deserializadores, offsets) están en application.yml bajo
 * {@code spring.kafka}; aquí se arma lo que no se puede expresar como propiedad:
 * <ol>
 *   <li>Los tópicos que este servicio es dueño de crear ({@link NewTopic}): KafkaAdmin los crea al iniciar si no
 *       existen, con sus particiones, réplicas, retención y cleanup policy.</li>
 *   <li>El serializador de valores: JSON para los DTO de eventos y bytes crudos para los mensajes ilegibles que se
 *       reenvían tal cual al DLT.</li>
 *   <li>El manejo de errores de todos los {@code @KafkaListener}: reintentos con espera exponencial ante errores
 *       recuperables y envío al DLT de lo que no se puede procesar.</li>
 *   <li>La validación ({@code @Valid}) de los payloads recibidos.</li>
 * </ol>
 */
@Configuration
public class KafkaConfig {

  private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

  // ---------- Tópicos de los que ms-productos es dueño ----------

  /** Eventos de stock por producto (PRODUCTO_CREADO, STOCK_DESCONTADO, STOCK_INSUFICIENTE, STOCK_REPUESTO). */
  @Bean
  NewTopic topicoInventario(KafkaTopicosProperties props) {
    return topico(props.topicos().inventario());
  }

  /** DLT de inventario: eventos de stock que un consumidor (de cualquier grupo) no pudo procesar. */
  @Bean
  NewTopic topicoInventarioDlt(KafkaTopicosProperties props) {
    return dlt(props.topicos().inventario(), props.dlt());
  }

  static NewTopic topico(Topico t) {
    TopicBuilder topico = TopicBuilder.name(t.nombre()).partitions(t.particiones()).replicas(t.replicas());
    if (t.cleanupPolicy() != null) topico.config(TopicConfig.CLEANUP_POLICY_CONFIG, t.cleanupPolicy());
    if (t.retencionMs() != null) topico.config(TopicConfig.RETENTION_MS_CONFIG, t.retencionMs().toString());
    if (t.minInsyncReplicas() != null) {
      topico.config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, t.minInsyncReplicas().toString());
    }
    if (t.minCompactionLagMs() != null) {
      topico.config(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, t.minCompactionLagMs().toString());
    }
    if (t.segmentMs() != null) topico.config(TopicConfig.SEGMENT_MS_CONFIG, t.segmentMs().toString());
    return topico.build();
  }

  /** El DLT tiene las mismas particiones y réplicas que su tópico, se borra por antigüedad y guarda más tiempo. */
  static NewTopic dlt(Topico t, KafkaTopicosProperties.Dlt dlt) {
    TopicBuilder topico = TopicBuilder.name(dlt.de(t.nombre())).partitions(t.particiones()).replicas(t.replicas())
        .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
        .config(TopicConfig.RETENTION_MS_CONFIG, dlt.retencionMs().toString());
    if (t.minInsyncReplicas() != null) {
      topico.config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, t.minInsyncReplicas().toString());
    }
    return topico.build();
  }

  // ---------- Productor ----------

  /**
   * Fábrica de productores con las propiedades de {@code spring.kafka.producer} (acks=all, idempotencia, etc.).
   * El serializador de valores elige por tipo: un {@code byte[]} (mensaje que no se pudo leer y va al DLT) se envía
   * tal cual, y cualquier DTO se serializa a JSON con su tipo lógico en el header {@code __TypeId__}.
   */
  @Bean
  ProducerFactory<Object, Object> kafkaProducerFactory(KafkaProperties kafka, SslBundles sslBundles) {
    Map<Class<?>, Serializer<?>> porTipo = new LinkedHashMap<>();
    porTipo.put(byte[].class, new ByteArraySerializer());
    porTipo.put(Object.class, new JsonSerializer<>());
    return new DefaultKafkaProducerFactory<Object, Object>(kafka.buildProducerProperties(sslBundles), null,
        new DelegatingByTypeSerializer(porTipo, true));
  }

  // ---------- Manejo de errores de los consumidores ----------

  /**
   * Política única de error para todos los {@code @KafkaListener} de este servicio:
   * <ul>
   *   <li>Error recuperable (p. ej. base de datos caída un momento): se reintenta el mismo evento con espera
   *       exponencial (1 s, 2 s, 4 s). La partición espera, así no se pierde el orden.</li>
   *   <li>Error no recuperable: JSON ilegible (DeserializationException), payload inválido según {@code @Valid}
   *       (MethodArgumentNotValidException) o evento imposible de procesar ({@link EventoInvalidoException}).
   *       Va directo al DLT, sin reintentos.</li>
   *   <li>Reintentos agotados: el evento va al DLT.</li>
   * </ul>
   * En el DLT el evento conserva su clave y valor, y Kafka agrega headers con el tópico, partición y offset de origen,
   * el grupo que falló y la excepción. Después de mandarlo al DLT se confirma el offset y la partición sigue: un
   * evento defectuoso nunca detiene el consumo.
   */
  @Bean
  DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<Object, Object> kafkaTemplate, KafkaTopicosProperties props) {
    DeadLetterPublishingRecoverer publicadorDlt = new DeadLetterPublishingRecoverer(kafkaTemplate,
        // partición -1: la elige Kafka por la clave; el origen queda en los headers del mensaje
        (registro, error) -> new TopicPartition(props.dlt().de(registro.topic()), -1));

    Reintentos r = props.reintentos();
    ExponentialBackOffWithMaxRetries espera = new ExponentialBackOffWithMaxRetries(r.maximo());
    espera.setInitialInterval(r.esperaInicialMs());
    espera.setMultiplier(r.multiplicador());
    espera.setMaxInterval(r.esperaMaximaMs());

    DefaultErrorHandler manejador = new DefaultErrorHandler((registro, error) -> {
      publicadorDlt.accept(registro, error);   // espera la confirmación; si falla, el evento se vuelve a intentar
      log.error("Evento enviado a '{}' | origen {}-{}@{} clave={} | motivo: {}", props.dlt().de(registro.topic()),
          registro.topic(), registro.partition(), registro.offset(), registro.key(), causa(error));
    }, espera);
    manejador.addNotRetryableExceptions(EventoInvalidoException.class, MethodArgumentNotValidException.class);
    manejador.setRetryListeners((registro, error, intento) ->
        log.warn("Intento {} de {} fallido para {}-{}@{} clave={}: {}", intento, r.maximo() + 1, registro.topic(),
            registro.partition(), registro.offset(), registro.key(), causa(error)));
    return manejador;
  }

  /** Activa {@code @Valid} en los parámetros {@code @Payload} de los {@code @KafkaListener}. */
  @Bean
  KafkaListenerConfigurer validacionDeEventos(LocalValidatorFactoryBean validador) {
    return registrar -> registrar.setValidator(validador);
  }

  /** La causa real (la primera excepción de la cadena), sin los envoltorios del contenedor de Spring Kafka. */
  static String causa(Throwable error) {
    for (Throwable t = error; t != null; t = t.getCause()) {
      if (t instanceof MethodArgumentNotValidException invalido && invalido.getBindingResult() != null) {
        return "payload inválido: " + invalido.getBindingResult().getFieldErrors().stream()
            .map(campo -> campo.getField() + " " + campo.getDefaultMessage())
            .collect(Collectors.joining("; "));
      }
    }
    Throwable t = error;
    while (t.getCause() != null && t.getCause() != t) {
      t = t.getCause();
    }
    return t.getClass().getSimpleName() + ": " + t.getMessage();
  }
}
