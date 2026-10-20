package cl.duoc.pedidos360.notificaciones.consumer;

import cl.duoc.pedidos360.messaging.ConsumerErrorHandler;
import cl.duoc.pedidos360.notificaciones.service.NotificacionService;
import cl.duoc.pedidos360.pedidos.event.PedidoCreadoEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

/**
 * Consumidor del dominio notificaciones: escucha la cola de notificaciones (binding "pedido creado" sobre el
 * exchange de pedidos) y avisa al cliente que su pedido fue recibido.
 *
 * <p>ACK manual: el mensaje se confirma con basicAck solo si la notificación se envió. Si algo falla,
 * {@link ConsumerErrorHandler} decide entre reintento, NACK o DLQ.
 */
@Component
public class NotificacionPedidoConsumer {

  private final NotificacionService notificacionService;
  private final ConsumerErrorHandler errorHandler;
  private final ObjectMapper objectMapper;

  public NotificacionPedidoConsumer(NotificacionService notificacionService, ConsumerErrorHandler errorHandler,
                                    ObjectMapper objectMapper) {
    this.notificacionService = notificacionService;
    this.errorHandler = errorHandler;
    this.objectMapper = objectMapper;
  }

  @RabbitListener(queues = "${pedidos360.messaging.notificaciones.cola}")
  public void onPedidoCreado(Message message, Channel channel) throws IOException {
    try {
      PedidoCreadoEvent evento = objectMapper.readValue(message.getBody(), PedidoCreadoEvent.class);
      notificacionService.notificarPedidoCreado(evento);
    } catch (Exception ex) {
      errorHandler.manejar(message, channel, ex);
      return;
    }
    channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
  }
}
