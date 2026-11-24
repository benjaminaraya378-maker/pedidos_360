package cl.duoc.pedidos360.analitica.dto;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Respuesta de GET /api/analitica/resumen: métricas calculadas en tiempo cuasi real a partir de los eventos de Kafka.
 *
 * @param generadoEn            momento en que se calculó este resumen
 * @param eventosProcesados     eventos distintos procesados desde que arrancó el servicio (sin contar repetidos)
 * @param ultimoEvento          fecha del último evento recibido
 * @param latenciaPromedioMs    tiempo medio entre que un evento ocurre y la analítica lo procesa (eventos en vivo)
 * @param totalPedidos          pedidos conocidos
 * @param pedidosPorEstado      cantidad por estado (CREADO, EN_PREPARACION, CANCELADO)
 * @param pedidosUltimaHora     pedidos creados en los últimos 60 minutos
 * @param ventasConfirmadas     suma de los pedidos con stock reservado
 * @param ticketPromedio        ventasConfirmadas / pedidos confirmados
 * @param tasaCancelacion       cancelados / (confirmados + cancelados), entre 0 y 1
 * @param unidadesVendidas      unidades de los pedidos confirmados
 * @param topProductos          los 5 productos con más unidades confirmadas
 * @param productosMonitoreados productos con al menos un evento de inventario
 * @param alertasStockBajo      productos con stock en o bajo el umbral, de menor a mayor stock
 * @param ultimosEventos        los últimos eventos procesados, del más nuevo al más antiguo
 */
public record ResumenAnalitica(
    Instant generadoEn,
    long eventosProcesados,
    Instant ultimoEvento,
    Long latenciaPromedioMs,
    int totalPedidos,
    Map<String, Long> pedidosPorEstado,
    long pedidosUltimaHora,
    BigDecimal ventasConfirmadas,
    BigDecimal ticketPromedio,
    double tasaCancelacion,
    long unidadesVendidas,
    List<ProductoVendido> topProductos,
    int productosMonitoreados,
    List<StockProducto> alertasStockBajo,
    List<EventoReciente> ultimosEventos) {

  public record ProductoVendido(Long productoId, String nombre, long unidades, BigDecimal monto) {}

  public record StockProducto(Long productoId, String nombre, int stockActual, int umbral, boolean stockBajo,
                              String ultimoMovimiento, Instant actualizado) {}

  /** Un evento tal como llegó: de qué tópico, partición y offset, con qué clave. */
  public record EventoReciente(String eventoId, String topico, int particion, long offset, String clave, String tipo,
                               String descripcion, Instant fecha, Instant procesado) {}
}
