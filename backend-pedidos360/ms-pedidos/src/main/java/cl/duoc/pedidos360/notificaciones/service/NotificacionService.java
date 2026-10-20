package cl.duoc.pedidos360.notificaciones.service;

import cl.duoc.pedidos360.pedidos.event.PedidoCreadoEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Notificaciones al cliente. El envío se registra en el log (no hay proveedor de correo configurado);
 * el contenido es el que recibiría el cliente.
 */
@Service
public class NotificacionService {

  private static final Logger log = LoggerFactory.getLogger(NotificacionService.class);

  /** @throws IllegalArgumentException si el evento no trae id o correo al que notificar (no recuperable). */
  public void notificarPedidoCreado(PedidoCreadoEvent evento) {
    if (evento.pedidoId() == null) {
      throw new IllegalArgumentException("Evento de pedido incompleto: sin id");
    }
    if (evento.clienteEmail() == null || evento.clienteEmail().isBlank()) {
      throw new IllegalArgumentException("El pedido " + evento.pedidoId() + " no tiene correo de cliente");
    }
    log.info("Notificación enviada a {}: Hola {}, recibimos tu pedido #{} por un total de ${}",
        evento.clienteEmail(), evento.clienteNombre(), evento.pedidoId(), evento.total());
  }
}
