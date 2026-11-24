package cl.duoc.pedidos360.common;

/** Kafka no respondió dentro del tiempo configurado (brokers caídos o inalcanzables): se responde 503. */
public class KafkaNoDisponibleException extends RuntimeException {

  public KafkaNoDisponibleException(String message) {
    super(message);
  }
}
