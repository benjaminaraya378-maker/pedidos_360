package cl.duoc.pedidos360.kafkaadmin.service;

import cl.duoc.pedidos360.kafkaadmin.dto.ReglasNombre;
import cl.duoc.pedidos360.kafkaadmin.dto.Respuestas.MensajeDlt;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.SerializationUtils;

/**
 * Reglas del reproceso de un DLT, sin Kafka de por medio (se prueban solas): qué nombres se aceptan, a qué tópico
 * vuelve cada mensaje, qué headers viajan con él y cómo se muestra.
 *
 * <p>Los DLT de Pedidos360 los llena el {@code DeadLetterPublishingRecoverer} de spring-kafka (ms-pedidos,
 * ms-productos y ms-analitica): copia la clave, el valor en bytes y los headers del evento que no se pudo procesar
 * (entre ellos {@code __TypeId__}, que dice a qué DTO convertir el JSON) y le agrega los headers {@code kafka_dlt-*}
 * con el tópico, la partición, el offset y el grupo de origen, y la excepción. La partición se guarda como int de 4
 * bytes y el offset como long de 8 bytes.
 */
public final class ReglasDlt {

  /** Sufijo de los DLT de Pedidos360 ({@code pedidos360.kafka.dlt.sufijo} en los demás servicios). */
  public static final String SUFIJO = ".dlt";

  /** Consumer group del reproceso: guarda hasta dónde se reinyectó cada partición del DLT. */
  public static final String GRUPO_REPROCESO = "ms-kafka-admin-reproceso";

  /** Prefijo común de los headers que agrega el DeadLetterPublishingRecoverer ({@code KafkaHeaders.DLT_*}). */
  public static final String PREFIJO_HEADERS_DLT = "kafka_dlt-";

  /** Caracteres del valor que se muestran al listar un DLT. */
  static final int MAXIMO_VALOR = 500;

  /** Caracteres del mensaje de la excepción que se muestran al listar un DLT. */
  static final int MAXIMO_MOTIVO = 300;

  private ReglasDlt() {}

  /**
   * Solo se leen y reprocesan DLT: un nombre de tópico válido que termine en {@code .dlt} y tenga algo antes del sufijo.
   *
   * @return lista de errores, vacía si el nombre es válido
   */
  public static List<String> validarNombre(String topicoDlt) {
    List<String> errores = new ArrayList<>();
    if (topicoDlt == null || !topicoDlt.matches(ReglasNombre.TOPICO)) {
      errores.add("topicoDlt: " + ReglasNombre.MENSAJE_TOPICO);
    } else if (!topicoDlt.endsWith(SUFIJO) || topicoDlt.length() == SUFIJO.length()) {
      errores.add("topicoDlt: solo se pueden leer y reprocesar DLT: el nombre debe terminar en '" + SUFIJO
          + "' (por ejemplo pedidos360.pedidos" + SUFIJO + ")");
    }
    return errores;
  }

  /**
   * Tópico al que vuelve un mensaje del DLT: el del header {@code kafka_dlt-original-topic} (el último, si el mensaje
   * pasó más de una vez por el DLT) o, si falta o viene vacío, el nombre del DLT sin {@code .dlt}.
   */
  public static String topicoOrigen(Headers headers, String topicoDlt) {
    String desdeHeader = texto(headers, KafkaHeaders.DLT_ORIGINAL_TOPIC);
    if (desdeHeader != null && !desdeHeader.isBlank()) {
      return desdeHeader.trim();
    }
    return topicoDlt.substring(0, topicoDlt.length() - SUFIJO.length());
  }

  /**
   * Headers que viajan con el mensaje reinyectado: todos los del evento original, en el mismo orden (así
   * {@code __TypeId__} se conserva y el consumidor sabe a qué DTO convertirlo), menos los {@code kafka_dlt-*}, que
   * describen el fallo anterior y no el evento. Tampoco viajan los {@code springDeserializerException*}: son la
   * excepción serializada de un JSON ilegible, y un consumidor podría tomarlos como un error nuevo.
   */
  public static List<Header> headersParaReinyectar(Headers headers) {
    List<Header> resultado = new ArrayList<>();
    for (Header h : headers) {
      if (!h.key().startsWith(PREFIJO_HEADERS_DLT)
          && !h.key().startsWith(SerializationUtils.DESERIALIZER_EXCEPTION_HEADER_PREFIX)) {
        resultado.add(h);
      }
    }
    return resultado;
  }

  /** Mensaje del DLT tal como se muestra en GET /api/kafka/dlt/{topicoDlt}. */
  public static MensajeDlt aMensaje(ConsumerRecord<byte[], byte[]> registro, String topicoDlt) {
    Headers h = registro.headers();
    String excepcion = primero(texto(h, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN), texto(h, KafkaHeaders.DLT_EXCEPTION_FQCN),
        texto(h, KafkaHeaders.DLT_KEY_EXCEPTION_FQCN));
    String motivo = primero(texto(h, KafkaHeaders.DLT_EXCEPTION_MESSAGE), texto(h, KafkaHeaders.DLT_KEY_EXCEPTION_MESSAGE));
    Long particionOrigen = numero(h.lastHeader(KafkaHeaders.DLT_ORIGINAL_PARTITION));
    byte[] valor = registro.value();
    return new MensajeDlt(
        registro.partition(),
        registro.offset(),
        registro.timestamp() < 0 ? null : Instant.ofEpochMilli(registro.timestamp()).toString(),
        registro.key() == null ? null : new String(registro.key(), StandardCharsets.UTF_8),
        topicoOrigen(h, topicoDlt),
        particionOrigen == null ? null : particionOrigen.intValue(),
        numero(h.lastHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET)),
        texto(h, KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP),
        excepcion,
        recortar(motivo, MAXIMO_MOTIVO),
        valor == null ? null : recortar(new String(valor, StandardCharsets.UTF_8), MAXIMO_VALOR),
        valor == null ? 0 : valor.length);
  }

  /** Recorta un texto a {@code maximo} caracteres y marca el corte con «…» (sin partir un carácter de 2 unidades). */
  static String recortar(String texto, int maximo) {
    if (texto == null || texto.length() <= maximo) {
      return texto;
    }
    int corte = Character.isHighSurrogate(texto.charAt(maximo - 1)) ? maximo - 1 : maximo;
    return texto.substring(0, corte) + "…";
  }

  /**
   * Número guardado en un header: 4 bytes (int, la partición), 8 bytes (long, el offset y la fecha) o, si otro
   * productor lo escribió como texto, sus dígitos. null si falta o no se entiende.
   */
  static Long numero(Header header) {
    if (header == null || header.value() == null) {
      return null;
    }
    byte[] v = header.value();
    if (v.length == Integer.BYTES) {
      return (long) ByteBuffer.wrap(v).getInt();
    }
    if (v.length == Long.BYTES) {
      return ByteBuffer.wrap(v).getLong();
    }
    try {
      return Long.parseLong(new String(v, StandardCharsets.UTF_8).trim());
    } catch (NumberFormatException ex) {
      return null;
    }
  }

  private static String texto(Headers headers, String clave) {
    Header h = headers.lastHeader(clave);
    return h == null || h.value() == null ? null : new String(h.value(), StandardCharsets.UTF_8);
  }

  private static String primero(String... valores) {
    for (String v : valores) {
      if (v != null && !v.isBlank()) return v;
    }
    return null;
  }
}
