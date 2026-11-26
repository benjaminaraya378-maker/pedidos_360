package cl.duoc.pedidos360.inventario.service;

import cl.duoc.pedidos360.common.ResourceNotFoundException;
import cl.duoc.pedidos360.inventario.InventarioProperties;
import cl.duoc.pedidos360.inventario.event.InventarioEventos;
import cl.duoc.pedidos360.inventario.event.MovimientosInventarioEvent;
import cl.duoc.pedidos360.inventario.model.EventoProcesado;
import cl.duoc.pedidos360.inventario.repository.EventoProcesadoRepository;
import cl.duoc.pedidos360.kafka.evento.InventarioEvento;
import cl.duoc.pedidos360.kafka.evento.PedidoEvento;
import cl.duoc.pedidos360.kafka.evento.TipoEventoInventario;
import cl.duoc.pedidos360.productos.dto.ProductoResponse;
import cl.duoc.pedidos360.productos.model.Producto;
import cl.duoc.pedidos360.productos.repository.ProductoRepository;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Lógica de inventario (sin Kafka): descontar el stock de un pedido y reponer stock. Cada movimiento se anuncia con un
 * evento de Spring que {@code InventarioKafkaProducer} publica en el tópico de inventario justo antes del commit,
 * esperando la confirmación del broker: si Kafka no la da, la transacción completa se deshace y el error se propaga.
 */
@Service
public class InventarioService {

  /** Resultado de procesar un evento de pedido. */
  public enum Resultado { DESCONTADO, INSUFICIENTE, DUPLICADO }

  private static final Logger log = LoggerFactory.getLogger(InventarioService.class);

  private final ProductoRepository productos;
  private final EventoProcesadoRepository procesados;
  private final InventarioProperties props;
  private final ApplicationEventPublisher eventos;

  public InventarioService(ProductoRepository productos, EventoProcesadoRepository procesados,
                           InventarioProperties props, ApplicationEventPublisher eventos) {
    this.productos = productos;
    this.procesados = procesados;
    this.props = props;
    this.eventos = eventos;
  }

  /**
   * Descuenta el stock de un pedido recién creado, todo o nada: si a algún producto le falta stock (o no existe o
   * está inactivo), no se descuenta ninguno y se emite STOCK_INSUFICIENTE; si alcanza para todos, se emite un
   * STOCK_DESCONTADO por producto. ms-productos ya validó el stock al crear el pedido, pero dos pedidos simultáneos
   * pueden pasar esa validación y aquí, con los productos bloqueados, solo uno de los dos lo obtiene.
   *
   * <p>Idempotente: un evento ya procesado (entrega repetida de Kafka) no vuelve a descontar. Si Kafka no confirma
   * los eventos de stock, se deshace todo, también el registro del evento procesado: el reintento que hace el manejador
   * de errores de Kafka vuelve a descontar en vez de descartarse como DUPLICADO.
   */
  @Transactional
  public Resultado reservarStock(PedidoEvento pedido) {
    if (procesados.existsById(pedido.eventoId())) {
      log.info("Evento {} del pedido {} ya fue procesado: se ignora (entrega repetida)", pedido.eventoId(),
          pedido.pedidoId());
      return Resultado.DUPLICADO;
    }
    // Se inserta primero: si otro hilo procesa el mismo evento, la clave primaria lo detiene aquí
    EventoProcesado registro = procesados.saveAndFlush(new EventoProcesado(pedido.eventoId(), pedido.pedidoId()));

    // Cantidad total por producto, en orden de id: los bloqueos se toman siempre en el mismo orden (sin deadlocks)
    Map<Long, Integer> pedidas = new TreeMap<>();
    Map<Long, String> nombres = new HashMap<>();
    for (PedidoEvento.Item item : pedido.items()) {
      pedidas.merge(item.productoId(), item.cantidad(), Integer::sum);
      nombres.putIfAbsent(item.productoId(), item.nombreProducto());
    }

    List<Producto> disponibles = new ArrayList<>();
    List<InventarioEvento> faltantes = new ArrayList<>();
    for (Map.Entry<Long, Integer> linea : pedidas.entrySet()) {
      Long id = linea.getKey();
      int cantidad = linea.getValue();
      Optional<Producto> encontrado = productos.findByIdForUpdate(id);
      if (encontrado.isEmpty() || !Boolean.TRUE.equals(encontrado.get().getActivo())) {
        faltantes.add(InventarioEventos.insuficiente(id, nombres.get(id), 0, cantidad, pedido.pedidoId(),
            "El producto '" + nombres.get(id) + "' ya no está disponible", props.umbralStockBajo()));
      } else if (encontrado.get().getStock() < cantidad) {
        Producto p = encontrado.get();
        faltantes.add(InventarioEventos.insuficiente(id, p.getNombre(), p.getStock(), cantidad, pedido.pedidoId(),
            "Stock insuficiente de '" + p.getNombre() + "': disponible " + p.getStock() + ", solicitado " + cantidad,
            props.umbralStockBajo()));
      } else {
        disponibles.add(encontrado.get());
      }
    }

    List<InventarioEvento> movimientos;
    if (!faltantes.isEmpty()) {
      movimientos = faltantes;
      registro.setResultado(TipoEventoInventario.STOCK_INSUFICIENTE.name());
      log.warn("Pedido {}: stock insuficiente, no se descontó nada ({})", pedido.pedidoId(),
          faltantes.getFirst().motivo());
    } else {
      movimientos = new ArrayList<>();
      for (Producto p : disponibles) {
        int cantidad = pedidas.get(p.getId());
        int anterior = p.getStock();
        p.setStock(anterior - cantidad);
        p.setActualizadoEn(Instant.now());
        movimientos.add(InventarioEventos.descuento(p, anterior, cantidad, pedido.pedidoId(), props.umbralStockBajo()));
        log.info("Pedido {}: '{}' {} → {}", pedido.pedidoId(), p.getNombre(), anterior, p.getStock());
      }
      registro.setResultado(TipoEventoInventario.STOCK_DESCONTADO.name());
    }
    eventos.publishEvent(new MovimientosInventarioEvent(movimientos));
    return faltantes.isEmpty() ? Resultado.DESCONTADO : Resultado.INSUFICIENTE;
  }

  /**
   * Suma unidades al stock de un producto (rol Admin) y lo anuncia como STOCK_REPUESTO. Si Kafka no confirma el
   * evento, la reposición no se guarda (el controlador responde 503).
   */
  @Transactional
  public ProductoResponse reponer(Long productoId, int cantidad) {
    Producto p = productos.findByIdForUpdate(productoId)
        .orElseThrow(() -> new ResourceNotFoundException("Producto " + productoId + " no existe"));
    int anterior = p.getStock();
    p.setStock(anterior + cantidad);
    p.setActualizadoEn(Instant.now());
    eventos.publishEvent(new MovimientosInventarioEvent(
        List.of(InventarioEventos.reposicion(p, anterior, cantidad, props.umbralStockBajo()))));
    log.info("Reposición de '{}': {} → {}", p.getNombre(), anterior, p.getStock());
    return ProductoResponse.de(p);
  }
}
