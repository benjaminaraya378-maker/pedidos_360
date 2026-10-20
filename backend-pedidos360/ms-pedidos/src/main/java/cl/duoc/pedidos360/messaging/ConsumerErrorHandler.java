package cl.duoc.pedidos360.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

/**
 * Política única de error para todos los consumidores (ACK manual). Cuando el procesamiento falla decide:
 * <ol>
 *   <li><b>Error no recuperable</b> — JSON ilegible ({@link JsonProcessingException}) o datos inválidos
 *       ({@link IllegalArgumentException}). Reintentar no lo arregla: NACK sin requeue y el broker lo
 *       envía a la DLQ de la cola.</li>
 *   <li><b>Error recuperable</b> — cualquier otro (p. ej. I/O). Se reintenta: se publica una copia en la
 *       misma cola con el header {@value #HEADER_REINTENTOS} incrementado y se hace ACK del original.</li>
 *   <li><b>Reintentos agotados</b> ({@code max-reintentos}) — NACK sin requeue → DLQ.</li>
 *   <li><b>No se pudo publicar el reintento</b> (broker con problemas) — NACK con requeue, para no perder
 *       el mensaje.</li>
 * </ol>
 * Todo mensaje enviado a una DLQ queda registrado en el log con la cola, el motivo y su contenido.
 */
@Component
public class ConsumerErrorHandler {

  public static final String HEADER_REINTENTOS = "x-reintentos";

  private static final Logger log = LoggerFactory.getLogger(ConsumerErrorHandler.class);

  private final RabbitTemplate rabbitTemplate;
  private final MessagingProperties props;

  public ConsumerErrorHandler(RabbitTemplate rabbitTemplate, MessagingProperties props) {
    this.rabbitTemplate = rabbitTemplate;
    this.props = props;
  }

  public void manejar(Message message, Channel channel, Exception error) throws IOException {
    MessageProperties mp = message.getMessageProperties();
    long tag = mp.getDeliveryTag();
    String cola = mp.getConsumerQueue();
    String causa = error.getClass().getSimpleName() + ": " + error.getMessage();

    if (esNoRecuperable(error)) {
      enviarADlq(message, channel, "error no recuperable (" + causa + ")");
      return;
    }

    int reintentos = reintentos(mp);
    if (reintentos >= props.maxReintentos()) {
      enviarADlq(message, channel, "reintentos agotados: " + reintentos + " (" + causa + ")");
      return;
    }

    try {
      mp.setHeader(HEADER_REINTENTOS, reintentos + 1);
      mp.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
      // Exchange por defecto ("") con la cola como routing key: la copia vuelve solo a esta cola
      rabbitTemplate.send("", cola, message);
      channel.basicAck(tag, false);
      log.warn("Reintento {}/{} del mensaje en '{}' ({})", reintentos + 1, props.maxReintentos(), cola, causa);
    } catch (AmqpException ex) {
      log.error("No se pudo programar el reintento en '{}'; el mensaje vuelve a la cola", cola, ex);
      channel.basicNack(tag, false, true);
    }
  }

  private void enviarADlq(Message message, Channel channel, String motivo) throws IOException {
    MessageProperties mp = message.getMessageProperties();
    channel.basicNack(mp.getDeliveryTag(), false, false);
    log.error("Mensaje enviado a la DLQ de '{}' | motivo: {} | contenido: {}", mp.getConsumerQueue(), motivo,
        new String(message.getBody(), StandardCharsets.UTF_8));
  }

  private static boolean esNoRecuperable(Exception error) {
    return error instanceof JsonProcessingException || error instanceof IllegalArgumentException;
  }

  private static int reintentos(MessageProperties mp) {
    Object valor = mp.getHeader(HEADER_REINTENTOS);
    return valor instanceof Number n ? n.intValue() : 0;
  }
}
