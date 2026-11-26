package cl.duoc.pedidos360.kafka;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

/**
 * Hasta dónde había llegado el consumer group en cada partición antes de que la analítica volviera a leerla desde el
 * inicio para reconstruir sus métricas.
 *
 * <p>Lo que quedó antes de esa posición ya se procesó en una ejecución anterior; si entonces falló, ya se mandó al
 * DLT. Al releerlo vuelve a fallar, y sin este registro el manejador de errores lo publicaría otra vez en el DLT en
 * cada reinicio. {@link KafkaConfig} consulta aquí antes de publicar.
 *
 * <p>Lo escriben los 3 hilos del consumidor al recibir particiones y lo lee el manejador de errores desde esos mismos
 * hilos: por eso el mapa es concurrente.
 */
@Component
public class PosicionesPrevias {

  private final Map<TopicPartition, Long> posiciones = new ConcurrentHashMap<>();

  /**
   * Guarda la posición con que el grupo recibió la partición, antes de volver al inicio. Solo cuenta la primera vez:
   * si la partición pasa a otro hilo durante la relectura, la posición de ese momento ya es de esta ejecución y no
   * debe reemplazar a la original.
   *
   * @param posicion siguiente offset que el grupo tenía pendiente; {@code null} si no hay posición previa (nada que
   *                 recordar: todo lo de la partición se trata como nuevo)
   */
  public void recordar(TopicPartition particion, Long posicion) {
    if (posicion != null) {
      posiciones.putIfAbsent(particion, posicion);
    }
  }

  /** Posición previa de la partición del registro, o {@code null} si no se releyó desde el inicio. */
  public Long de(ConsumerRecord<?, ?> registro) {
    return posiciones.get(new TopicPartition(registro.topic(), registro.partition()));
  }

  /**
   * true si el registro quedó antes de la posición previa del grupo: ya se procesó (o se envió al DLT) antes de este
   * reinicio. Una partición sin posición previa, o un offset igual o mayor, es un registro nuevo.
   */
  public boolean yaProcesadoAntes(ConsumerRecord<?, ?> registro) {
    Long previa = de(registro);
    return previa != null && registro.offset() < previa;
  }
}
