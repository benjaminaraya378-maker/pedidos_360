package cl.duoc.pedidos360.comprobantes.consumer;

import cl.duoc.pedidos360.comprobantes.service.ComprobanteService;
import cl.duoc.pedidos360.messaging.ConsumerErrorHandler;
import cl.duoc.pedidos360.pedidos.event.PedidoCreadoEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

/**
 * Consumidor del dominio comprobantes: escucha la cola de comprobantes (binding "pedido creado" sobre el
 * exchange de pedidos) y genera el ticket del pedido.
 *
 * <p>ACK manual: el mensaje se confirma con basicAck solo si el comprobante quedó escrito. Si algo falla,
 * {@link ConsumerErrorHandler} decide entre reintento (p. ej. error de disco), NACK o DLQ.
 */
@Component
public class ComprobantePedidoConsumer {

  private final ComprobanteService comprobanteService;
  private final ConsumerErrorHandler errorHandler;
  private final ObjectMapper objectMapper;

  public ComprobantePedidoConsumer(ComprobanteService comprobanteService, ConsumerErrorHandler errorHandler,
                                   ObjectMapper objectMapper) {
    this.comprobanteService = comprobanteService;
    this.errorHandler = errorHandler;
    this.objectMapper = objectMapper;
  }

  @RabbitListener(queues = "${pedidos360.messaging.comprobantes.cola}")
  public void onPedidoCreado(Message message, Channel channel) throws IOException {
    try {
      PedidoCreadoEvent evento = objectMapper.readValue(message.getBody(), PedidoCreadoEvent.class);
      comprobanteService.generar(evento);
    } catch (Exception ex) {
      errorHandler.manejar(message, channel, ex);
      return;
    }
    channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
  }
}
