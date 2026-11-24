package cl.duoc.pedidos360.inventario.event;

import cl.duoc.pedidos360.kafka.evento.InventarioEvento;
import cl.duoc.pedidos360.kafka.evento.TipoEventoInventario;
import cl.duoc.pedidos360.productos.model.Producto;
import java.time.Instant;
import java.util.UUID;

/** Construye los eventos del tópico de inventario: todos llevan el stock vigente del producto y si está bajo. */
public final class InventarioEventos {

  private static final int VERSION = 1;

  private InventarioEventos() {}

  public static InventarioEvento alta(Producto p, int umbral) {
    return evento(TipoEventoInventario.PRODUCTO_CREADO, p.getId(), p.getNombre(), p.getStock(), 0, p.getStock(),
        umbral, null, "Producto creado con stock inicial " + p.getStock());
  }

  public static InventarioEvento descuento(Producto p, int stockAnterior, int cantidad, Long pedidoId, int umbral) {
    return evento(TipoEventoInventario.STOCK_DESCONTADO, p.getId(), p.getNombre(), cantidad, stockAnterior,
        p.getStock(), umbral, pedidoId, "Pedido " + pedidoId + ": se descontaron " + cantidad + " unidad(es)");
  }

  public static InventarioEvento reposicion(Producto p, int stockAnterior, int cantidad, int umbral) {
    return evento(TipoEventoInventario.STOCK_REPUESTO, p.getId(), p.getNombre(), cantidad, stockAnterior,
        p.getStock(), umbral, null, "Reposición de " + cantidad + " unidad(es)");
  }

  /** No se descontó nada: el stock anterior y el actual son el mismo. */
  public static InventarioEvento insuficiente(Long productoId, String nombre, int stock, int solicitado,
                                              Long pedidoId, String motivo, int umbral) {
    return evento(TipoEventoInventario.STOCK_INSUFICIENTE, productoId, nombre, solicitado, stock, stock, umbral,
        pedidoId, motivo);
  }

  private static InventarioEvento evento(TipoEventoInventario tipo, Long productoId, String nombre, int cantidad,
                                         int stockAnterior, int stockActual, int umbral, Long pedidoId,
                                         String motivo) {
    return new InventarioEvento(UUID.randomUUID().toString(), tipo, VERSION, Instant.now(), productoId, nombre,
        cantidad, stockAnterior, stockActual, umbral, stockActual <= umbral, pedidoId, motivo);
  }
}
