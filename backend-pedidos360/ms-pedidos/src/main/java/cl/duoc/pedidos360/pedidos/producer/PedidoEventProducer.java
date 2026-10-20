package cl.duoc.pedidos360.pedidos.producer;

import cl.duoc.pedidos360.messaging.MessagingProperties;
import cl.duoc.pedidos360.pedidos.event.PedidoCreadoEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * Productor del dominio pedidos: publica el evento "pedido creado" en el exchange de pedidos.
 *
 * <p>Se ejecuta después del commit de la transacción que guardó el pedido, así nunca se anuncia un pedido
 * que no quedó en la base. Si RabbitMQ no está disponible el pedido igual se crea (la mensajería no altera
 * la lógica existente) y el fallo queda en el log.
 */
@Component
public class PedidoEventProducer {

  private static final Logger log = LoggerFactory.getLogger(PedidoEventProducer.class);

  private final RabbitTemplate rabbitTemplate;
  private final MessagingProperties props;

  public PedidoEventProducer(RabbitTemplate rabbitTemplate, MessagingProperties props) {
    this.rabbitTemplate = rabbitTemplate;
    this.props = props;
  }

  @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
  public void publicarPedidoCreado(PedidoCreadoEvent evento) {
    try {
      rabbitTemplate.convertAndSend(props.exchange(), props.pedidoCreadoRoutingKey(), evento);
      log.info("Evento '{}' publicado para el pedido {}", props.pedidoCreadoRoutingKey(), evento.pedidoId());
    } catch (AmqpException ex) {
      log.error("No se pudo publicar el evento del pedido {} en RabbitMQ", evento.pedidoId(), ex);
    }
  }
}
