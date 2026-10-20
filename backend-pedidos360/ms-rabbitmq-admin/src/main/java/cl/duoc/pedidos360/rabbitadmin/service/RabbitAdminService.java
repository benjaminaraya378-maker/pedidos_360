package cl.duoc.pedidos360.rabbitadmin.service;

import cl.duoc.pedidos360.common.BusinessException;
import cl.duoc.pedidos360.common.ResourceNotFoundException;
import cl.duoc.pedidos360.rabbitadmin.dto.BindingRequest;
import cl.duoc.pedidos360.rabbitadmin.dto.BindingResponse;
import cl.duoc.pedidos360.rabbitadmin.dto.ExchangeRequest;
import cl.duoc.pedidos360.rabbitadmin.dto.ExchangeResponse;
import cl.duoc.pedidos360.rabbitadmin.dto.QueueRequest;
import cl.duoc.pedidos360.rabbitadmin.dto.QueueResponse;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.QueueInformation;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

/**
 * Administración de RabbitMQ: toda la interacción con el broker (RabbitAdmin) está en esta clase; el
 * controlador solo llama a sus métodos y no conoce la librería AMQP.
 *
 * <p>Antes de cada operación se revisa el estado real del broker: no se crea lo que ya existe (409),
 * no se elimina ni se enlaza lo que no existe (404) y no se acepta un dead-letter exchange inexistente (400).
 */
@Service
public class RabbitAdminService {

  private static final Logger log = LoggerFactory.getLogger(RabbitAdminService.class);

  private final AmqpAdmin amqpAdmin;
  private final RabbitTemplate rabbitTemplate;

  public RabbitAdminService(AmqpAdmin amqpAdmin, RabbitTemplate rabbitTemplate) {
    this.amqpAdmin = amqpAdmin;
    this.rabbitTemplate = rabbitTemplate;
  }

  // ---------- Colas ----------

  public QueueResponse crearCola(QueueRequest req) {
    if (existeCola(req.nombre())) {
      throw new BusinessException("La cola '" + req.nombre() + "' ya existe");
    }
    if (req.deadLetterExchange() != null && !existeExchange(req.deadLetterExchange())) {
      throw new BusinessException(HttpStatus.BAD_REQUEST,
          "El dead-letter exchange '" + req.deadLetterExchange() + "' no existe");
    }
    QueueBuilder cola = req.durable() ? QueueBuilder.durable(req.nombre()) : QueueBuilder.nonDurable(req.nombre());
    if (req.autoDelete()) cola.autoDelete();
    if (req.deadLetterExchange() != null) cola.deadLetterExchange(req.deadLetterExchange());
    if (req.deadLetterRoutingKey() != null) cola.deadLetterRoutingKey(req.deadLetterRoutingKey());
    if (req.ttlMs() != null) cola.ttl(req.ttlMs());
    if (req.maxLength() != null) cola.maxLength(req.maxLength());

    amqpAdmin.declareQueue(cola.build());
    log.info("Cola '{}' creada", req.nombre());
    return obtenerCola(req.nombre());
  }

  public QueueResponse obtenerCola(String nombre) {
    QueueInformation info = amqpAdmin.getQueueInfo(nombre);
    if (info == null) {
      throw new ResourceNotFoundException("La cola '" + nombre + "' no existe");
    }
    return new QueueResponse(info.getName(), info.getMessageCount(), info.getConsumerCount());
  }

  public void eliminarCola(String nombre) {
    if (!existeCola(nombre)) {
      throw new ResourceNotFoundException("La cola '" + nombre + "' no existe");
    }
    amqpAdmin.deleteQueue(nombre);
    log.info("Cola '{}' eliminada", nombre);
  }

  // ---------- Exchanges ----------

  public ExchangeResponse crearExchange(ExchangeRequest req) {
    if (existeExchange(req.nombre())) {
      throw new BusinessException("El exchange '" + req.nombre() + "' ya existe");
    }
    ExchangeBuilder exchange = new ExchangeBuilder(req.nombre(), req.tipo()).durable(req.durable());
    if (req.autoDelete()) exchange.autoDelete();

    amqpAdmin.declareExchange(exchange.build());
    log.info("Exchange '{}' ({}) creado", req.nombre(), req.tipo());
    return new ExchangeResponse(req.nombre(), req.tipo(), req.durable(), req.autoDelete());
  }

  public void eliminarExchange(String nombre) {
    if (!existeExchange(nombre)) {
      throw new ResourceNotFoundException("El exchange '" + nombre + "' no existe");
    }
    amqpAdmin.deleteExchange(nombre);
    log.info("Exchange '{}' eliminado", nombre);
  }

  // ---------- Bindings ----------

  public BindingResponse crearBinding(BindingRequest req) {
    verificarExtremos(req);
    amqpAdmin.declareBinding(binding(req));
    log.info("Binding '{}' -[{}]-> '{}' creado", req.exchange(), req.routingKey(), req.cola());
    return new BindingResponse(req.exchange(), req.cola(), req.routingKey());
  }

  /** Si el exchange y la cola existen pero el binding no, RabbitMQ no hace nada (operación idempotente). */
  public void eliminarBinding(BindingRequest req) {
    verificarExtremos(req);
    amqpAdmin.removeBinding(binding(req));
    log.info("Binding '{}' -[{}]-> '{}' eliminado", req.exchange(), req.routingKey(), req.cola());
  }

  // ---------- Apoyo ----------

  private void verificarExtremos(BindingRequest req) {
    if (!existeExchange(req.exchange())) {
      throw new ResourceNotFoundException("El exchange '" + req.exchange() + "' no existe");
    }
    if (!existeCola(req.cola())) {
      throw new ResourceNotFoundException("La cola '" + req.cola() + "' no existe");
    }
  }

  private static Binding binding(BindingRequest req) {
    return new Binding(req.cola(), Binding.DestinationType.QUEUE, req.exchange(), req.routingKey(), null);
  }

  private boolean existeCola(String nombre) {
    return amqpAdmin.getQueueInfo(nombre) != null;
  }

  /** Declaración pasiva: si el exchange no existe el broker responde 404 y cierra el canal. */
  private boolean existeExchange(String nombre) {
    Boolean existe = rabbitTemplate.execute(channel -> {
      try {
        channel.exchangeDeclarePassive(nombre);
        return true;
      } catch (IOException ex) {
        return false;
      }
    });
    return Boolean.TRUE.equals(existe);
  }
}
