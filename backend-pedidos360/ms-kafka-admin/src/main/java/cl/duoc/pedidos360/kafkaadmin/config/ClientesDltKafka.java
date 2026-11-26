package cl.duoc.pedidos360.kafkaadmin.config;

import cl.duoc.pedidos360.kafkaadmin.service.ClientesDlt;
import cl.duoc.pedidos360.kafkaadmin.service.ReglasDlt;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.stereotype.Component;

/**
 * Clientes Kafka reales del reproceso de DLT. Toman la conexión de {@code spring.kafka} (brokers y seguridad, igual
 * que el {@code AdminClient}) y fijan aquí lo que el reproceso necesita para no reinyectar dos veces un mismo mensaje.
 * Se crean por operación (un {@link KafkaConsumer} no se puede compartir entre hilos) y se cierran al terminar.
 */
@Component
public class ClientesDltKafka implements ClientesDlt {

  /** Tiempo que el broker tiene para responder cada petición de los clientes del reproceso. */
  static final int ESPERA_PETICION_MS = 5000;

  private final KafkaProperties kafka;
  private final SslBundles sslBundles;
  private final KafkaAdminProperties props;
  private final AtomicInteger contador = new AtomicInteger();

  public ClientesDltKafka(KafkaProperties kafka, SslBundles sslBundles, KafkaAdminProperties props) {
    this.kafka = kafka;
    this.sslBundles = sslBundles;
    this.props = props;
  }

  /**
   * Consumidor del grupo de reproceso. Se asigna las particiones a mano (sin rebalanceo) y empieza donde el grupo
   * confirmó por última vez, o en el primer mensaje retenido si nunca confirmó.
   */
  @Override
  public Consumer<byte[], byte[]> consumidor() {
    Map<String, Object> config = kafka.buildConsumerProperties(sslBundles);
    config.put(ConsumerConfig.GROUP_ID_CONFIG, ReglasDlt.GRUPO_REPROCESO);
    config.put(ConsumerConfig.CLIENT_ID_CONFIG, "ms-kafka-admin-dlt-" + contador.incrementAndGet());
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);   // el offset se confirma solo tras reinyectar
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
    config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");   // nunca reinyecta algo abortado
    config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
    config.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, ESPERA_PETICION_MS);
    config.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) props.timeout().toMillis());
    return new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
  }

  /**
   * Productor como el de ms-pedidos: acks=all (lo escribieron las réplicas en sincronía) e idempotente (un reintento
   * interno no duplica). {@code delivery.timeout.ms} es el tope para que Kafka confirme o rechace cada envío, y el
   * servicio espera un poco más que eso: nunca deja de esperar un envío que todavía podría escribirse.
   */
  @Override
  public Producer<byte[], byte[]> productor() {
    Map<String, Object> config = kafka.buildProducerProperties(sslBundles);
    config.put(ProducerConfig.CLIENT_ID_CONFIG, "ms-kafka-admin-reproceso-" + contador.incrementAndGet());
    config.put(ProducerConfig.ACKS_CONFIG, "all");
    config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    config.put(ProducerConfig.LINGER_MS_CONFIG, 0);
    config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, ESPERA_PETICION_MS);
    config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, ESPERA_PETICION_MS);
    config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, (int) esperaEntrega().toMillis());
    return new KafkaProducer<>(config, new ByteArraySerializer(), new ByteArraySerializer());
  }

  /** {@code delivery.timeout.ms}: el timeout del servicio, sin bajar de lo que exige Kafka (linger + request). */
  @Override
  public Duration esperaEntrega() {
    return Duration.ofMillis(Math.max(props.timeout().toMillis(), ESPERA_PETICION_MS));
  }
}
