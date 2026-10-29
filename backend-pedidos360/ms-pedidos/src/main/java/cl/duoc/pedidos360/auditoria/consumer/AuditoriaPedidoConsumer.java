package cl.duoc.pedidos360.auditoria.consumer;

import cl.duoc.pedidos360.auditoria.service.AuditoriaService;
import cl.duoc.pedidos360.messaging.ConsumerErrorHandler;
import cl.duoc.pedidos360.pedidos.event.PedidoCreadoEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

/**
 * Consumidor del dominio auditoría: escucha la cola de auditoría (binding "pedido creado" sobre el exchange de
 * pedidos) y registra cada pedido creado.
 *
 * <p>ACK manual: el mensaje se confirma con basicAck solo si la auditoría quedó registrada. Si algo falla,
 * {@link ConsumerErrorHandler} decide entre reintento, NACK o DLQ (la misma política de las otras colas).
 */
@Component
public class AuditoriaPedidoConsumer {

  private final AuditoriaService auditoriaService;
  private final ConsumerErrorHandler errorHandler;
  private final ObjectMapper objectMapper;

  public AuditoriaPedidoConsumer(AuditoriaService auditoriaService, ConsumerErrorHandler errorHandler,
                                 ObjectMapper objectMapper) {
    this.auditoriaService = auditoriaService;
    this.errorHandler = errorHandler;
    this.objectMapper = objectMapper;
  }

  @RabbitListener(queues = "${pedidos360.messaging.auditoria.cola}")
  public void onPedidoCreado(Message message, Channel channel) throws IOException {
    try {
      PedidoCreadoEvent evento = objectMapper.readValue(message.getBody(), PedidoCreadoEvent.class);
      auditoriaService.registrarPedidoCreado(evento);
    } catch (Exception ex) {
      errorHandler.manejar(message, channel, ex);
      return;
    }
    channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
  }
}
