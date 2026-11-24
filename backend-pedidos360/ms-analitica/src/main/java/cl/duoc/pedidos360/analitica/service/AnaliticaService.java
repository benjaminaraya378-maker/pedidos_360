package cl.duoc.pedidos360.analitica.service;

import cl.duoc.pedidos360.analitica.dto.ResumenAnalitica;
import cl.duoc.pedidos360.analitica.dto.ResumenAnalitica.EventoReciente;
import cl.duoc.pedidos360.analitica.dto.ResumenAnalitica.ProductoVendido;
import cl.duoc.pedidos360.analitica.dto.ResumenAnalitica.StockProducto;
import cl.duoc.pedidos360.kafka.evento.InventarioEvento;
import cl.duoc.pedidos360.kafka.evento.PedidoEvento;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.NumberFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Vista materializada de la analítica (lógica de negocio, sin Kafka). Cada evento actualiza el estado de un pedido o
 * el stock de un producto, y las métricas se calculan sobre ese estado al pedir el resumen.
 *
 * <p>Procesar un evento es idempotente: se reemplaza el estado del pedido o del producto (no se suman contadores) y
 * un eventoId ya visto se ignora. Así, releer un tópico desde el inicio (reconstrucción) o recibir un evento
 * repetido no altera las métricas.
 */
@Service
public class AnaliticaService {

  /** Desde dónde llegó un evento: tópico, partición y offset. */
  public record Origen(String topico, int particion, long offset) {}

  private record PedidoVista(Long id, String estado, BigDecimal total, Instant creado, String motivo,
                             List<PedidoEvento.Item> items) {}

  private static final Logger log = LoggerFactory.getLogger(AnaliticaService.class);
  private static final int MAX_RECIENTES = 15;
  private static final int MAX_LATENCIAS = 50;
  private static final int MAX_VISTOS = 100_000;
  /** Un evento procesado más de 1 minuto después de ocurrir es una relectura, no cuenta para la latencia. */
  private static final Duration LIMITE_EN_VIVO = Duration.ofMinutes(1);

  private final Map<Long, PedidoVista> pedidos = new ConcurrentHashMap<>();
  private final Map<Long, StockProducto> stock = new ConcurrentHashMap<>();
  private final Deque<EventoReciente> recientes = new ArrayDeque<>();
  private final Deque<Long> latencias = new ArrayDeque<>();
  private final Map<String, Boolean> vistos = new LinkedHashMap<>() {
    @Override
    protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
      return size() > MAX_VISTOS;
    }
  };
  private final AtomicLong eventosProcesados = new AtomicLong();
  private volatile Instant ultimoEvento;

  public void registrarPedido(PedidoEvento e, Origen origen) {
    if (!primeraVez(e.eventoId())) {
      return;
    }
    pedidos.compute(e.pedidoId(), (id, actual) -> new PedidoVista(id, estadoDe(e),
        e.total(), actual != null ? actual.creado() : e.fecha(), e.motivo(), e.items()));
    String descripcion = switch (e.tipo()) {
      case PEDIDO_CREADO -> "Pedido #" + e.pedidoId() + " creado por " + pesos(e.total()) + " (" + e.items().size()
          + " producto(s))";
      case PEDIDO_CONFIRMADO -> "Pedido #" + e.pedidoId() + " confirmado: stock reservado";
      case PEDIDO_CANCELADO -> "Pedido #" + e.pedidoId() + " cancelado: " + e.motivo();
      default -> "Pedido #" + e.pedidoId() + ": evento " + e.tipo();
    };
    registrarReciente(e.eventoId(), origen, String.valueOf(e.pedidoId()), e.tipo().name(), descripcion, e.fecha());
  }

  public void registrarInventario(InventarioEvento e, Origen origen) {
    if (!primeraVez(e.eventoId())) {
      return;
    }
    StockProducto nuevo = new StockProducto(e.productoId(), e.nombreProducto(), e.stockActual(), e.umbralStockBajo(),
        e.stockBajo(), e.tipo().name(), e.fecha());
    // El orden por producto lo garantiza la partición; se conserva igual el más reciente por si llega uno atrasado
    stock.merge(e.productoId(), nuevo, (viejo, recibido) ->
        recibido.actualizado().isBefore(viejo.actualizado()) ? viejo : recibido);
    String detalle = switch (e.tipo()) {
      case STOCK_DESCONTADO -> e.stockAnterior() + " → " + e.stockActual() + " (pedido #" + e.pedidoId() + ")";
      case STOCK_INSUFICIENTE -> "sin stock para el pedido #" + e.pedidoId() + " (disponible " + e.stockActual()
          + ", solicitado " + e.cantidad() + ")";
      case STOCK_REPUESTO -> "reposición " + e.stockAnterior() + " → " + e.stockActual();
      case PRODUCTO_CREADO -> "alta con stock " + e.stockActual();
      default -> e.tipo().name();
    };
    if (e.stockBajo()) {
      log.warn("Alerta de stock bajo: '{}' quedó en {} (umbral {})", e.nombreProducto(), e.stockActual(),
          e.umbralStockBajo());
    }
    registrarReciente(e.eventoId(), origen, String.valueOf(e.productoId()), e.tipo().name(),
        e.nombreProducto() + ": " + detalle + (e.stockBajo() ? " · STOCK BAJO" : ""), e.fecha());
  }

  public ResumenAnalitica resumen() {
    Instant ahora = Instant.now();
    Collection<PedidoVista> vista = List.copyOf(pedidos.values());

    Map<String, Long> porEstado = new TreeMap<>();
    BigDecimal ventas = BigDecimal.ZERO;
    long confirmados = 0;
    long cancelados = 0;
    long ultimaHora = 0;
    Map<Long, ProductoVendido> vendidos = new HashMap<>();
    for (PedidoVista p : vista) {
      porEstado.merge(p.estado(), 1L, Long::sum);
      if (p.creado() != null && p.creado().isAfter(ahora.minus(Duration.ofHours(1)))) ultimaHora++;
      if ("CANCELADO".equals(p.estado())) cancelados++;
      if (esConfirmado(p.estado())) {
        confirmados++;
        ventas = ventas.add(p.total());
        for (PedidoEvento.Item i : p.items()) {
          vendidos.merge(i.productoId(), new ProductoVendido(i.productoId(), i.nombreProducto(), i.cantidad(),
              i.subtotal()), (a, b) -> new ProductoVendido(a.productoId(), a.nombre(), a.unidades() + b.unidades(),
              a.monto().add(b.monto())));
        }
      }
    }
    BigDecimal ticket = confirmados == 0 ? BigDecimal.ZERO
        : ventas.divide(BigDecimal.valueOf(confirmados), 0, RoundingMode.HALF_UP);
    double tasaCancelacion = confirmados + cancelados == 0 ? 0 : (double) cancelados / (confirmados + cancelados);
    long unidades = vendidos.values().stream().mapToLong(ProductoVendido::unidades).sum();
    List<ProductoVendido> top = vendidos.values().stream()
        .sorted(Comparator.comparingLong(ProductoVendido::unidades).reversed()).limit(5).toList();
    List<StockProducto> alertas = stock.values().stream().filter(StockProducto::stockBajo)
        .sorted(Comparator.comparingInt(StockProducto::stockActual)).toList();

    List<EventoReciente> ultimos;
    Long latencia;
    synchronized (this) {
      ultimos = new ArrayList<>(recientes);
      latencia = latencias.isEmpty() ? null
          : Math.round(latencias.stream().mapToLong(Long::longValue).average().orElse(0));
    }
    return new ResumenAnalitica(ahora, eventosProcesados.get(), ultimoEvento, latencia, vista.size(), porEstado,
        ultimaHora, ventas, ticket, tasaCancelacion, unidades, top, stock.size(), alertas, ultimos);
  }

  /** Stock vigente de cada producto según el último evento de inventario. */
  public List<StockProducto> stock() {
    return stock.values().stream().sorted(Comparator.comparing(StockProducto::nombre)).toList();
  }

  private synchronized boolean primeraVez(String eventoId) {
    if (vistos.putIfAbsent(eventoId, Boolean.TRUE) != null) {
      return false;
    }
    eventosProcesados.incrementAndGet();
    return true;
  }

  private synchronized void registrarReciente(String eventoId, Origen origen, String clave, String tipo,
                                              String descripcion, Instant fecha) {
    Instant procesado = Instant.now();
    recientes.addFirst(new EventoReciente(eventoId, origen.topico(), origen.particion(), origen.offset(), clave, tipo,
        descripcion, fecha, procesado));
    while (recientes.size() > MAX_RECIENTES) recientes.removeLast();
    if (fecha != null) {
      if (ultimoEvento == null || fecha.isAfter(ultimoEvento)) ultimoEvento = fecha;
      Duration retraso = Duration.between(fecha, procesado);
      if (!retraso.isNegative() && retraso.compareTo(LIMITE_EN_VIVO) < 0) {
        latencias.addFirst(retraso.toMillis());
        while (latencias.size() > MAX_LATENCIAS) latencias.removeLast();
      }
    }
    log.info("Analítica: {} [{}-{}@{} clave={}] {}", tipo, origen.topico(), origen.particion(), origen.offset(), clave,
        descripcion);
  }

  private static String estadoDe(PedidoEvento e) {
    return switch (e.tipo()) {
      case PEDIDO_CONFIRMADO -> "EN_PREPARACION";
      case PEDIDO_CANCELADO -> "CANCELADO";
      case PEDIDO_CREADO -> "CREADO";
      default -> e.estado();
    };
  }

  private static boolean esConfirmado(String estado) {
    return "EN_PREPARACION".equals(estado) || "ENVIADO".equals(estado) || "ENTREGADO".equals(estado);
  }

  private static String pesos(BigDecimal monto) {
    NumberFormat formato = NumberFormat.getIntegerInstance(Locale.forLanguageTag("es-CL"));
    return "$" + formato.format(monto);
  }
}
