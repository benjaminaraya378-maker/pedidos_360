package cl.duoc.pedidos360.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.boot.autoconfigure.amqp.RabbitTemplateCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Topología RabbitMQ de ms-pedidos. Solo declara beans (RabbitAdmin los crea en el broker al conectarse);
 * los nombres salen de {@link MessagingProperties}. Ningún servicio de negocio configura colas ni exchanges.
 *
 * <p>Rutas de mensajería (un caso de uso por cola, cada una con su DLQ):
 * <pre>
 *   1. Notificación al cliente
 *      exchange (topic) --[notificaciones.binding-key]--> notificaciones.cola
 *      rechazados: dead-letter-exchange (direct) --[notificaciones.dlq]--> notificaciones.dlq
 *
 *   2. Comprobante (ticket) del pedido
 *      exchange (topic) --[comprobantes.binding-key]--> comprobantes.cola
 *      rechazados: dead-letter-exchange (direct) --[comprobantes.dlq]--> comprobantes.dlq
 * </pre>
 * Cada cola principal declara {@code x-dead-letter-exchange} y {@code x-dead-letter-routing-key}: cuando un
 * consumidor hace NACK sin requeue, el broker reenvía el mensaje a la DLQ de esa misma cola.
 */
@Configuration
public class RabbitMQConfig {

  private final MessagingProperties props;

  public RabbitMQConfig(MessagingProperties props) {
    this.props = props;
  }

  // ---------- Exchanges ----------

  /** Exchange de eventos del dominio pedidos (el productor publica aquí). */
  @Bean
  TopicExchange pedidosExchange() {
    return new TopicExchange(props.exchange(), true, false);
  }

  /** Exchange de mensajes rechazados: enruta cada mensaje a la DLQ de la cola que lo rechazó. */
  @Bean
  DirectExchange deadLetterExchange() {
    return new DirectExchange(props.deadLetterExchange(), true, false);
  }

  // ---------- Caso de uso 1: notificaciones ----------

  @Bean
  Queue notificacionesQueue() {
    return colaConDlq(props.notificaciones());
  }

  @Bean
  Queue notificacionesDlq() {
    return QueueBuilder.durable(props.notificaciones().dlq()).build();
  }

  @Bean
  Binding notificacionesBinding() {
    return BindingBuilder.bind(notificacionesQueue()).to(pedidosExchange())
        .with(props.notificaciones().bindingKey());
  }

  @Bean
  Binding notificacionesDlqBinding() {
    return BindingBuilder.bind(notificacionesDlq()).to(deadLetterExchange())
        .with(props.notificaciones().dlq());
  }

  // ---------- Caso de uso 2: comprobantes ----------

  @Bean
  Queue comprobantesQueue() {
    return colaConDlq(props.comprobantes());
  }

  @Bean
  Queue comprobantesDlq() {
    return QueueBuilder.durable(props.comprobantes().dlq()).build();
  }

  @Bean
  Binding comprobantesBinding() {
    return BindingBuilder.bind(comprobantesQueue()).to(pedidosExchange())
        .with(props.comprobantes().bindingKey());
  }

  @Bean
  Binding comprobantesDlqBinding() {
    return BindingBuilder.bind(comprobantesDlq()).to(deadLetterExchange())
        .with(props.comprobantes().dlq());
  }

  // ---------- Serialización ----------

  /**
   * El productor publica en JSON (ObjectMapper de Spring Boot: fechas ISO-8601). El conversor se aplica solo
   * al RabbitTemplate: los consumidores reciben el Message crudo y lo leen ellos mismos, así un JSON ilegible
   * pasa por ConsumerErrorHandler (NACK → DLQ + log) igual que cualquier otro error.
   */
  @Bean
  RabbitTemplateCustomizer jsonRabbitTemplate(ObjectMapper objectMapper) {
    return template -> template.setMessageConverter(new Jackson2JsonMessageConverter(objectMapper));
  }

  /** Cola durable cuyos mensajes rechazados van a su DLQ a través del dead-letter exchange. */
  private Queue colaConDlq(MessagingProperties.Ruta ruta) {
    return QueueBuilder.durable(ruta.cola())
        .deadLetterExchange(props.deadLetterExchange())
        .deadLetterRoutingKey(ruta.dlq())
        .build();
  }
}
