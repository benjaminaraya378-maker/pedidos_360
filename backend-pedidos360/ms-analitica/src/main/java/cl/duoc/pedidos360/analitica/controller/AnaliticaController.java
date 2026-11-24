package cl.duoc.pedidos360.analitica.controller;

import cl.duoc.pedidos360.analitica.dto.ResumenAnalitica;
import cl.duoc.pedidos360.analitica.dto.ResumenAnalitica.StockProducto;
import cl.duoc.pedidos360.analitica.service.AnaliticaService;
import java.util.List;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * API de la analítica en tiempo real. Exige JWT válido con rol Admin.
 * <pre>
 *   GET /api/analitica/resumen   métricas, alertas de stock bajo y últimos eventos      200 · 401 · 403
 *   GET /api/analitica/stock     stock vigente de cada producto según Kafka              200 · 401 · 403
 * </pre>
 * El frontend consulta el resumen cada pocos segundos: cada evento nuevo de Kafka se ve en la siguiente consulta.
 */
@RestController
@RequestMapping("/api/analitica")
@PreAuthorize("hasRole('ADMIN')")
public class AnaliticaController {

  private final AnaliticaService service;

  public AnaliticaController(AnaliticaService service) {
    this.service = service;
  }

  @GetMapping("/resumen")
  public ResumenAnalitica resumen() {
    return service.resumen();
  }

  @GetMapping("/stock")
  public List<StockProducto> stock() {
    return service.stock();
  }
}
