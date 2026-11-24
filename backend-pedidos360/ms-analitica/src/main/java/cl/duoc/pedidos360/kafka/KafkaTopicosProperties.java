package cl.duoc.pedidos360.kafka;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Tópicos, consumer group y política de reintentos de ms-analitica, leídos de {@code pedidos360.kafka} en
 * application.yml. Este servicio no es dueño de ningún tópico: solo usa sus nombres para consumirlos.
 *
 * @param topicos              tópicos que consume
 * @param dlt                  sufijo del DLT al que reenvía lo que no puede procesar
 * @param grupos               consumer group propio de la analítica
 * @param reintentos           reintentos con espera exponencial antes de mandar un evento al DLT
 * @param reconstruirAlIniciar si es true, al arrancar vuelve a leer los tópicos desde el inicio para reconstruir las
 *                             métricas (que viven en memoria): lo permite la retención de 7 días de pedidos y la
 *                             compactación de inventario
 */
@ConfigurationProperties(prefix = "pedidos360.kafka")
public record KafkaTopicosProperties(
    Topicos topicos,
    Dlt dlt,
    Grupos grupos,
    Reintentos reintentos,
    boolean reconstruirAlIniciar) {

  public record Topicos(Topico pedidos, Topico inventario) {}

  public record Topico(String nombre) {}

  public record Dlt(String sufijo) {

    public String de(String topico) {
      return topico + sufijo;
    }
  }

  /** @param analitica grupo propio: recibe todos los eventos, independiente de los grupos de los otros servicios */
  public record Grupos(String analitica) {}

  public record Reintentos(int maximo, long esperaInicialMs, double multiplicador, long esperaMaximaMs) {}
}
