package cl.duoc.pedidos360.comprobantes.service;

import cl.duoc.pedidos360.pedidos.event.PedidoCreadoEvent;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/** Genera el comprobante (ticket) de un pedido como archivo de texto: {directorio}/pedido-{id}.txt. */
@Service
public class ComprobanteService {

  private static final Logger log = LoggerFactory.getLogger(ComprobanteService.class);

  private final Path directorio;

  public ComprobanteService(@Value("${pedidos360.comprobantes.directorio}") String directorio) {
    this.directorio = Path.of(directorio);
  }

  /**
   * Escribe el ticket. Si el archivo ya existe se sobrescribe, así un reintento no duplica comprobantes.
   *
   * @throws IllegalArgumentException si el evento no trae id o ítems (no recuperable).
   * @throws IOException si no se pudo escribir el archivo (recuperable: se reintenta).
   */
  public Path generar(PedidoCreadoEvent evento) throws IOException {
    if (evento.pedidoId() == null || evento.items() == null || evento.items().isEmpty()) {
      throw new IllegalArgumentException("Evento de pedido incompleto: sin id o sin ítems");
    }
    Files.createDirectories(directorio);
    Path archivo = directorio.resolve("pedido-" + evento.pedidoId() + ".txt");
    Files.writeString(archivo, contenido(evento), StandardCharsets.UTF_8);
    log.info("Comprobante del pedido {} generado en {}", evento.pedidoId(), archivo.toAbsolutePath());
    return archivo;
  }

  private static String contenido(PedidoCreadoEvent e) {
    StringBuilder sb = new StringBuilder()
        .append("PEDIDOS360 - COMPROBANTE DE PEDIDO\n")
        .append("Ticket:  PED-").append(String.format("%06d", e.pedidoId())).append('\n')
        .append("Fecha:   ").append(e.fecha()).append('\n')
        .append("Cliente: ").append(e.clienteNombre()).append(" <").append(e.clienteEmail()).append(">\n")
        .append("----------------------------------------\n");
    for (PedidoCreadoEvent.Item i : e.items()) {
      sb.append(i.cantidad()).append(" x ").append(i.nombreProducto())
          .append("  @ $").append(i.precioUnitario())
          .append("  = $").append(i.subtotal()).append('\n');
    }
    return sb.append("----------------------------------------\n")
        .append("TOTAL: $").append(e.total()).append('\n')
        .toString();
  }
}
