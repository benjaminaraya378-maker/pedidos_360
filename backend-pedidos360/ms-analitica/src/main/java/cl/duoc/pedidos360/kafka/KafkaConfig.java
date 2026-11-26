package cl.duoc.pedidos360.kafka;

import cl.duoc.pedidos360.kafka.KafkaTopicosProperties.Dlt;
import cl.duoc.pedidos360.kafka.KafkaTopicosProperties.Reintentos;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListenerConfigurer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.messaging.handler.annotation.support.MethodArgumentNotValidException;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

/**
 * Configuración del cliente Kafka de ms-analitica. Este servicio solo consume: no es dueño de ningún tópico (los
 * crean ms-pedidos y ms-productos). El productor existe únicamente para reenviar al DLT los eventos que no pudo
 * procesar. La conexión y las propiedades del consumidor están en application.yml bajo {@code spring.kafka}.
 */
@Configuration
public class KafkaConfig {

  private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

  /** Productor para el DLT: DTO → JSON, y un {@code byte[]} ilegible se reenvía tal cual. */
  @Bean
  ProducerFactory<Object, Object> kafkaProducerFactory(KafkaProperties kafka, SslBundles sslBundles) {
    Map<Class<?>, Serializer<?>> porTipo = new LinkedHashMap<>();
    porTipo.put(byte[].class, new ByteArraySerializer());
    porTipo.put(Object.class, new JsonSerializer<>());
    return new DefaultKafkaProducerFactory<Object, Object>(kafka.buildProducerProperties(sslBundles), null,
        new DelegatingByTypeSerializer(porTipo, true));
  }

  /**
   * Misma política de error que ms-pedidos y ms-productos: reintentos con espera exponencial ante errores
   * recuperables; JSON ilegible, payload inválido o tipo de evento desconocido van directo al DLT del tópico de origen.
   * Un evento defectuoso nunca detiene la analítica, y la relectura al reiniciar no duplica el DLT (ver
   * {@link #recuperador}).
   */
  @Bean
  DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<Object, Object> kafkaTemplate, KafkaTopicosProperties props,
                                        PosicionesPrevias previas) {
    DeadLetterPublishingRecoverer publicadorDlt = new DeadLetterPublishingRecoverer(kafkaTemplate,
        (registro, error) -> new TopicPartition(props.dlt().de(registro.topic()), -1));

    Reintentos r = props.reintentos();
    ExponentialBackOffWithMaxRetries espera = new ExponentialBackOffWithMaxRetries(r.maximo());
    espera.setInitialInterval(r.esperaInicialMs());
    espera.setMultiplier(r.multiplicador());
    espera.setMaxInterval(r.esperaMaximaMs());

    DefaultErrorHandler manejador = new DefaultErrorHandler(recuperador(publicadorDlt, previas, props.dlt()), espera);
    manejador.addNotRetryableExceptions(EventoInvalidoException.class, MethodArgumentNotValidException.class);
    manejador.setRetryListeners((registro, error, intento) ->
        log.warn("Intento {} de {} fallido para {}-{}@{} clave={}: {}", intento, r.maximo() + 1, registro.topic(),
            registro.partition(), registro.offset(), registro.key(), causa(error)));
    return manejador;
  }

  /**
   * Destino final de un evento que no se pudo procesar: el DLT de su tópico de origen.
   *
   * <p>Excepción: al arrancar, la analítica relee cada partición desde el inicio para reconstruir sus métricas, y los
   * eventos defectuosos que ya mandó al DLT en una ejecución anterior vuelven a fallar. Si el offset del evento quedó
   * antes de la posición que el grupo tenía al recibir la partición ({@link PosicionesPrevias}), ya se había procesado
   * antes de este reinicio: solo se deja en el log y no se publica de nuevo. Los eventos nuevos (offset igual o mayor
   * a esa posición, o partición sin posición previa) van al DLT como siempre.
   */
  static ConsumerRecordRecoverer recuperador(ConsumerRecordRecoverer publicadorDlt, PosicionesPrevias previas,
                                             Dlt dlt) {
    return (registro, error) -> {
      if (previas.yaProcesadoAntes(registro)) {
        log.warn("Evento ya enviado a '{}' antes de este reinicio; no se vuelve a publicar | origen {}-{}@{} "
                + "(posición previa del grupo: {}) clave={} | motivo: {}", dlt.de(registro.topic()), registro.topic(),
            registro.partition(), registro.offset(), previas.de(registro), registro.key(), causa(error));
        return;
      }
      publicadorDlt.accept(registro, error);   // espera la confirmación; si falla, el evento se vuelve a intentar
      log.error("Evento enviado a '{}' | origen {}-{}@{} clave={} | motivo: {}", dlt.de(registro.topic()),
          registro.topic(), registro.partition(), registro.offset(), registro.key(), causa(error));
    };
  }

  /** Activa {@code @Valid} en los parámetros {@code @Payload} de los {@code @KafkaHandler}. */
  @Bean
  KafkaListenerConfigurer validacionDeEventos(LocalValidatorFactoryBean validador) {
    return registrar -> registrar.setValidator(validador);
  }

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
